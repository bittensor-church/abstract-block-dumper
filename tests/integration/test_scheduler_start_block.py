"""
Where each chain head starts from, per BLOCK_DUMPER_START_FROM_BLOCK.

The finalized head trails the latest one, so a starting block shared between them would
put finalized tasks past blocks that are produced but not yet finalized - and the poll
loop only fires on a strictly higher head, so those blocks would never be processed.
"""

from unittest.mock import MagicMock, call, patch

import pytest
from django.test import override_settings
from django.utils import timezone

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.scheduler import DefaultBlockStateResolver, TaskScheduler
from abstract_block_dumper._internal.services.window_backfiller import WindowBackfiller
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from tests.fatories import TaskAttemptFactory


def latest_head_task(block_number: int) -> int:
    return block_number


def finalized_head_task(block_number: int) -> int:
    return block_number


LATEST_PATH = abd_utils.get_executable_path(latest_head_task)
FINALIZED_PATH = abd_utils.get_executable_path(finalized_head_task)


def _scheduler_reading_heads(latest_heads: list[int], finalized_heads: list[int]) -> tuple[TaskScheduler, MagicMock]:
    """
    A scheduler whose chain reads walk each head through the given block numbers.

    Every read advances that head one step and the last value then holds, so a run that
    reads a head more or fewer times than expected still sees a real block number and
    fails on what it processed rather than on a drained mock.
    """
    remaining = {False: list(latest_heads), True: list(finalized_heads)}

    def read_head(*, finalized: bool) -> int:
        heads = remaining[finalized]
        return heads.pop(0) if len(heads) > 1 else heads[0]

    bittensor_client = MagicMock(spec=BittensorConnectionClient)
    bittensor_client.get_block.side_effect = read_head

    block_processor = block_processor_factory()
    scheduler = TaskScheduler(
        block_processor=block_processor,
        bittensor_client=bittensor_client,
        state_resolver=DefaultBlockStateResolver(bittensor_client=bittensor_client),
        poll_interval=0,
        window_backfiller=WindowBackfiller(block_processor.executor),
    )
    return scheduler, bittensor_client


def _run_one_poll(scheduler: TaskScheduler) -> None:
    with patch(
        "abstract_block_dumper._internal.services.scheduler.time.sleep",
        side_effect=KeyboardInterrupt,
    ):
        scheduler.start()


def _record_completed_attempt(executable_path: str, block_number: int) -> None:
    """
    Store a block a previous scheduler run finished, as the cursor a restart resumes from.

    The execution details are what makes it a real record: an attempt with no worker and no
    attempt time is treated as a phantom and pruned when a block processor is built.
    """
    TaskAttemptFactory(
        is_success=True,
        executable_path=executable_path,
        block_number=block_number,
        celery_task_id=f"celery-{executable_path}-{block_number}",
        last_attempted_at=timezone.now(),
    )


def _recorded_attempts() -> set[tuple[str, int]]:
    return {(attempt.executable_path, attempt.block_number) for attempt in TaskAttempt.objects.all()}


@pytest.mark.django_db
@override_settings(BLOCK_DUMPER_START_FROM_BLOCK="current")
def test_current_starts_each_task_at_its_own_chain_head() -> None:
    """Blocks 998-1000 are produced but not finalized, so finalized tasks must still get 998."""
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)

    scheduler, bittensor_client = _scheduler_reading_heads(
        latest_heads=[1000, 1001],
        finalized_heads=[997, 998],
    )

    _run_one_poll(scheduler)

    assert _recorded_attempts() == {(LATEST_PATH, 1001), (FINALIZED_PATH, 998)}
    assert bittensor_client.get_block.call_args_list == [
        call(finalized=False),  # resolve the latest head's starting block
        call(finalized=True),  # resolve the finalized head's starting block
        call(finalized=False),
        call(finalized=True),
    ]


@pytest.mark.django_db
@override_settings(BLOCK_DUMPER_START_FROM_BLOCK=None)
def test_resume_reads_each_head_cursor_from_that_head_own_attempts() -> None:
    """A stored latest-head cursor must not advance a finalized task past unprocessed blocks."""
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)
    _record_completed_attempt(LATEST_PATH, 1000)
    _record_completed_attempt(FINALIZED_PATH, 997)

    # Both cursors come from the database, so neither head is read before the poll.
    scheduler, bittensor_client = _scheduler_reading_heads(
        latest_heads=[1001],
        finalized_heads=[998],
    )

    _run_one_poll(scheduler)

    assert _recorded_attempts() == {
        (LATEST_PATH, 1000),
        (FINALIZED_PATH, 997),
        (LATEST_PATH, 1001),
        (FINALIZED_PATH, 998),
    }


@pytest.mark.django_db
@override_settings(BLOCK_DUMPER_START_FROM_BLOCK=None)
def test_resume_falls_back_to_its_own_head_for_a_task_with_no_attempts() -> None:
    """A newly added finalized task has no stored blocks, so it starts at the finalized head."""
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)
    _record_completed_attempt(LATEST_PATH, 1000)

    scheduler, bittensor_client = _scheduler_reading_heads(
        latest_heads=[1001],
        finalized_heads=[997, 998],
    )

    _run_one_poll(scheduler)

    assert _recorded_attempts() == {
        (LATEST_PATH, 1000),
        (LATEST_PATH, 1001),
        (FINALIZED_PATH, 998),
    }
    assert bittensor_client.get_block.call_args_list == [
        call(finalized=True),  # only the finalized head needs resolving from the chain
        call(finalized=False),
        call(finalized=True),
    ]


@pytest.mark.django_db
@override_settings(BLOCK_DUMPER_START_FROM_BLOCK=990)
def test_an_explicit_starting_block_applies_to_every_chain_head() -> None:
    block_task(latest_head_task)
    block_task(finalized=True)(finalized_head_task)

    scheduler, bittensor_client = _scheduler_reading_heads(
        latest_heads=[1001],
        finalized_heads=[998],
    )

    _run_one_poll(scheduler)

    assert _recorded_attempts() == {(LATEST_PATH, 1001), (FINALIZED_PATH, 998)}
    # The setting answers both heads outright, so the only reads are the poll's own.
    assert bittensor_client.get_block.call_args_list == [
        call(finalized=False),
        call(finalized=True),
    ]
