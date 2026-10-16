from unittest.mock import MagicMock, patch

import pytest

from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient
from abstract_block_dumper._internal.services.scheduler import TaskScheduler
from abstract_block_dumper.v1.decorators import block_task
from tests.conftest import MockedBlockProcessor


def _tick_task(block_number: int) -> int:
    return block_number


def _make_scheduler(
    *,
    heads: list[int] | None = None,
    stale_task_timeout: float | None = 1800,
    sweep_interval: float = 60,
    batch_size: int = 200,
) -> tuple[TaskScheduler, MockedBlockProcessor]:
    """A scheduler over a mocked processor whose chain reads walk through `heads` (last value holds)."""
    remaining = list(heads or [101])

    def read_head(*, finalized: bool) -> int:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    bittensor_client.get_block.side_effect = read_head
    state_resolver = MagicMock()
    state_resolver.get_starting_block.return_value = 99
    processor = MockedBlockProcessor(executor=MagicMock(), registry=task_registry)
    scheduler = TaskScheduler(
        block_processor=processor,
        bittensor_client=bittensor_client,
        state_resolver=state_resolver,
        poll_interval=0,
        window_backfiller=MagicMock(),
        stale_task_timeout=stale_task_timeout,
        stale_task_sweep_interval=sweep_interval,
        stale_task_batch_size=batch_size,
    )
    return scheduler, processor


def _run_ticks(scheduler: TaskScheduler, *, count: int, seconds_per_tick: float) -> None:
    """Run `count` loop iterations on a fake monotonic clock that advances at each sleep."""
    clock = {"now": 1000.0}
    sleeps = {"count": 0}

    def sleep(_seconds: float) -> None:
        sleeps["count"] += 1
        clock["now"] += seconds_per_tick
        if sleeps["count"] >= count:
            raise KeyboardInterrupt

    with (
        patch("abstract_block_dumper._internal.services.scheduler.time.sleep", side_effect=sleep),
        patch("abstract_block_dumper._internal.services.scheduler.time.monotonic", side_effect=lambda: clock["now"]),
    ):
        scheduler.start()


@pytest.mark.django_db
def test_sweep_runs_on_the_first_tick_with_the_configured_arguments():
    scheduler, processor = _make_scheduler(stale_task_timeout=900, batch_size=50)

    _run_ticks(scheduler, count=1, seconds_per_tick=0)

    # No task is registered, so no head has been observed yet.
    assert processor.reconcile_calls == [{"stale_after_seconds": 900, "limit": 50, "head_block": None}]


@pytest.mark.django_db
def test_sweep_waits_for_the_interval_between_runs():
    scheduler, processor = _make_scheduler(sweep_interval=60)

    # Ticks at t=1000 (runs), t=1030 (inside the interval), t=1060 (runs again).
    _run_ticks(scheduler, count=3, seconds_per_tick=30)

    assert len(processor.reconcile_calls) == 2


@pytest.mark.django_db
def test_sweep_receives_the_highest_observed_head():
    block_task(_tick_task)
    scheduler, processor = _make_scheduler(heads=[101])

    _run_ticks(scheduler, count=1, seconds_per_tick=0)

    assert processor.processed_blocks == [101]
    assert [call["head_block"] for call in processor.reconcile_calls] == [101]


@pytest.mark.django_db
def test_a_failing_sweep_does_not_stop_polling():
    block_task(_tick_task)
    scheduler, processor = _make_scheduler(heads=[101, 102], sweep_interval=60)
    processor.reconcile_stuck_tasks = MagicMock(side_effect=RuntimeError("database down"))

    _run_ticks(scheduler, count=2, seconds_per_tick=0)

    assert processor.processed_blocks == [101, 102]
    # Failed at t=1000 and not retried on the next tick, which is still inside the interval.
    assert processor.reconcile_stuck_tasks.call_count == 1


@pytest.mark.django_db
def test_sweep_is_disabled_when_the_timeout_is_none():
    scheduler, processor = _make_scheduler(stale_task_timeout=None)

    _run_ticks(scheduler, count=2, seconds_per_tick=120)

    assert processor.reconcile_calls == []
