"""Unit tests for scheduler module."""

from unittest.mock import MagicMock, patch

import pytest
from django.test import override_settings

from abstract_block_dumper.models import BlockDumperConfig, BlockDumperExecution, ScheduledTask
from abstract_block_dumper.scheduler import BlockScheduler
from tests.factories import BlockDumperConfigFactory


@pytest.fixture
def mock_subtensor() -> MagicMock:
    mock = MagicMock()
    mock.get_current_block.return_value = 100
    return mock


@pytest.fixture
def scheduler(mock_subtensor) -> BlockScheduler:
    with patch("abstract_block_dumper.scheduler.get_bittensor_client", return_value=mock_subtensor):
        return BlockScheduler()


@pytest.fixture
def block_config() -> BlockDumperConfig:  # type: ignore[return]  # Factory returns correct type
    return BlockDumperConfigFactory(  # type: ignore[return]
        name="test_task",
        description="Test task",
        function_path="test.function",
    )


@override_settings(BLOCK_DUMPER_START_FROM_BLOCK=200)
def test_initialize_last_block_specific(scheduler):
    scheduler._initialize_last_block()

    assert scheduler.last_processed_block == 200


@override_settings(BLOCK_DUMPER_START_FROM_BLOCK=None)
@pytest.mark.django_db
def test_initialize_last_block_from_database(scheduler, mock_subtensor):
    # Create existing execution
    BlockDumperExecution.objects.create(block_number=180, total_tasks_scheduled=1)

    scheduler._initialize_last_block()

    assert scheduler.last_processed_block == 180


def test_process_block_range_multiple_blocks_within_limit(scheduler):
    scheduler.last_processed_block = 95
    scheduler.max_blocks_behind = 10

    with patch.object(scheduler, "_schedule_task_for_block") as mock_schedule:
        scheduler._process_block_range(100)

    # Should process blocks 96-100 (5 blocks)
    expected_calls = [((block,),) for block in range(96, 101)]
    assert mock_schedule.call_args_list == expected_calls
    assert scheduler.last_processed_block == 100


@pytest.mark.django_db
def test_schedule_task_for_block_creates_execution(scheduler, block_config):
    with patch.object(scheduler, "schedule_celery_tasks", return_value=0):
        scheduler._schedule_task_for_block(100)

    execution = BlockDumperExecution.objects.get(block_number=100)
    assert execution.started_at is not None


@pytest.mark.django_db
def test_schedule_celery_tasks_success(scheduler, block_config):
    task = ScheduledTask.objects.create(
        config=block_config,
        block_number=100,
        status=ScheduledTask.Status.PENDING,
    )

    with patch("abstract_block_dumper.scheduler.execute_block_task") as mock_task:
        mock_result = MagicMock()
        mock_result.id = "celery-task-123"
        mock_task.apply_async.return_value = mock_result

        count = scheduler.schedule_celery_tasks([(task, block_config)])

    assert count == 1
    mock_task.apply_async.assert_called_once_with(args=[task.id], queue=block_config.queue)

    # Task should be updated with celery ID
    task.refresh_from_db()
    assert task.celery_task_id == "celery-task-123"


def test_scheduler_stop(scheduler):
    """Test scheduler stop method."""
    scheduler.is_running = True
    scheduler.stop()
    assert scheduler.is_running is False
