from unittest.mock import MagicMock, call, patch

import pytest

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.scheduler import TaskScheduler
from abstract_block_dumper._internal.services.window_backfiller import WindowBackfiller
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task


def latest_head_task(block_number: int) -> int:
    return block_number


def finalized_head_task(block_number: int) -> int:
    return block_number


@pytest.mark.django_db
def test_scheduler_runs_each_task_at_its_selected_chain_head() -> None:
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)

    block_processor = block_processor_factory()
    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    bittensor_client.get_block.side_effect = lambda *, finalized: 100 if finalized else 101
    state_resolver = MagicMock()
    state_resolver.get_starting_block.return_value = 99
    scheduler = TaskScheduler(
        block_processor=block_processor,
        bittensor_client=bittensor_client,
        state_resolver=state_resolver,
        poll_interval=0,
        window_backfiller=WindowBackfiller(block_processor.executor),
    )

    with patch(
        "abstract_block_dumper._internal.services.scheduler.time.sleep",
        side_effect=KeyboardInterrupt,
    ):
        scheduler.start()

    attempts = {
        (attempt.executable_path, attempt.block_number, attempt.execution_result)
        for attempt in TaskAttempt.objects.all()
    }
    assert attempts == {
        (abd_utils.get_executable_path(latest_head_task), 101, 101),
        (abd_utils.get_executable_path(finalized_head_task), 100, 100),
    }
    assert bittensor_client.get_block.call_args_list == [
        call(finalized=False),
        call(finalized=True),
    ]


def _scheduler_for(bittensor_client: MagicMock, starting_block: int) -> TaskScheduler:
    block_processor = block_processor_factory()
    state_resolver = MagicMock()
    state_resolver.get_starting_block.return_value = starting_block
    return TaskScheduler(
        block_processor=block_processor,
        bittensor_client=bittensor_client,
        state_resolver=state_resolver,
        poll_interval=0,
        window_backfiller=WindowBackfiller(block_processor.executor),
    )


def _run_one_poll(scheduler: TaskScheduler) -> None:
    """Run the loop until the sleep at the end of the first iteration."""
    with patch(
        "abstract_block_dumper._internal.services.scheduler.time.sleep",
        side_effect=KeyboardInterrupt,
    ):
        scheduler.start()


@pytest.mark.django_db
def test_an_unreadable_chain_head_does_not_block_the_others() -> None:
    """A head registered ahead of a healthy one must not starve it when its reads fail."""
    # Registration order decides polling order, so the broken head goes first.
    block_task(finalized=True)(finalized_head_task)
    block_task(latest_head_task)

    def get_block(*, finalized: bool) -> int:
        if finalized:
            raise ConnectionError("finalized RPC is down")
        return 101

    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    bittensor_client.get_block.side_effect = get_block

    _run_one_poll(_scheduler_for(bittensor_client, starting_block=99))

    attempts = {
        (attempt.executable_path, attempt.block_number, attempt.execution_result)
        for attempt in TaskAttempt.objects.all()
    }
    assert attempts == {(abd_utils.get_executable_path(latest_head_task), 101, 101)}


@pytest.mark.django_db
def test_scheduler_reports_block_lag_per_chain_head() -> None:
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)

    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    # Finality trails the latest head by 3 blocks.
    bittensor_client.get_block.side_effect = lambda *, finalized: 98 if finalized else 101

    with patch("abstract_block_dumper._internal.services.scheduler.set_block_lag") as set_block_lag:
        _run_one_poll(_scheduler_for(bittensor_client, starting_block=97))

    assert set_block_lag.call_args_list == [
        call("realtime", 0, source="latest"),
        call("realtime", 3, source="finalized"),
    ]


@pytest.mark.django_db
def test_scheduler_reports_growing_lag_for_a_head_that_stops_advancing() -> None:
    """A head that cannot be read keeps its last processed block, so its lag grows."""
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)

    def get_block(*, finalized: bool) -> int:
        if finalized:
            raise ConnectionError("finalized RPC is down")
        return 110

    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    bittensor_client.get_block.side_effect = get_block

    with patch("abstract_block_dumper._internal.services.scheduler.set_block_lag") as set_block_lag:
        _run_one_poll(_scheduler_for(bittensor_client, starting_block=100))

    assert set_block_lag.call_args_list == [
        call("realtime", 0, source="latest"),
        call("realtime", 10, source="finalized"),
    ]
