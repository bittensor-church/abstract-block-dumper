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
