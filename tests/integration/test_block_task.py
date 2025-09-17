"""
Integration tests for abstract_block_dumper app.

These tests verify the complete workflow of the block dumper:
1. Task registration via decorators
2. Block scheduling and task creation
3. Task execution via Celery
4. Statistics tracking and model updates
"""

from unittest.mock import MagicMock, patch

import pytest

from abstract_block_dumper.decorators import BlockDumperRegistry, block_task
from abstract_block_dumper.discovery import sync_block_task_functions
from abstract_block_dumper.models import (
    BlockDumperConfig,
    BlockDumperExecution,
    ConditionType,
    NetuidType,
    ScheduledTask,
)
from abstract_block_dumper.scheduler import BlockScheduler
from abstract_block_dumper.tasks import execute_block_task


def every_block_task_func(block_number: int, netuid: int | None = None):
    """Function for every block execution."""
    return f"Processed block {block_number}"


def modulo_task_func(block_number: int, netuid: int | None = None):
    """Function for modulo condition execution."""
    return f"Modulo task processed block {block_number} for netuid {netuid}"


def failing_task_func(block_number: int, netuid: int | None = None):
    """Function that always fails."""
    raise ValueError("Test error")


@pytest.fixture
def setup_test_tasks():
    """Set up test data and mocks."""
    # Clear any existing registrations
    BlockDumperRegistry.clear_pendings()

    # Create test tasks using the decorator with module-level functions
    block_task(name="test_every_block")(every_block_task_func)

    block_task(name="test_modulo_task", condition=ConditionType.MODULO, modulo=5, netuid=[1, 2])(modulo_task_func)

    yield

    BlockDumperRegistry.clear_pendings()


@pytest.mark.django_db
def test_task_registration_workflow(setup_test_tasks):
    """Test that tasks are properly registered via decorators."""
    # Verify tasks are in registry
    assert len(BlockDumperRegistry._pending_registrations) == 2

    # Register tasks in database
    sync_block_task_functions()

    # Verify database records were created
    configs = BlockDumperConfig.objects.all()
    assert configs.count() == 2

    every_block_config = BlockDumperConfig.objects.get(name="test_every_block")
    assert every_block_config.condition_type == ConditionType.EVERY_BLOCK
    assert every_block_config.netuid_type == NetuidType.NONE

    modulo_config = BlockDumperConfig.objects.get(name="test_modulo_task")
    assert modulo_config.condition_type == ConditionType.MODULO
    assert modulo_config.condition_params["modulo"] == 5
    assert modulo_config.netuid_type == NetuidType.MULTIPLE
    assert modulo_config.netuid_values == [1, 2]


@pytest.mark.django_db
@patch("abstract_block_dumper.scheduler.get_bittensor_client")
def test_block_scheduling_workflow(mock_get_client, setup_test_tasks):
    """Test the complete block scheduling workflow."""
    # Setup mocks
    mock_subtensor = MagicMock()
    mock_subtensor.get_current_block.return_value = 100
    mock_get_client.return_value = mock_subtensor

    # Register tasks
    sync_block_task_functions()

    # Create scheduler and process a single block
    scheduler = BlockScheduler()
    scheduler.last_processed_block = 99
    scheduler._schedule_task_for_block(100)

    # Verify BlockDumperExecution was created
    execution = BlockDumperExecution.objects.get(block_number=100)
    assert execution.started_at is not None

    # Verify ScheduledTasks were created
    tasks = ScheduledTask.objects.filter(block_number=100)

    # Should have 1 task for every_block + 2 tasks for modulo task (netuids 1,2)
    # Modulo condition: 100 % 5 == 0, so it should execute
    assert tasks.count() == 3

    # Verify every_block task
    every_block_task = tasks.filter(config__name="test_every_block").first()
    assert every_block_task is not None
    assert every_block_task.netuid is None

    # Verify modulo tasks
    modulo_tasks = tasks.filter(config__name="test_modulo_task")
    assert modulo_tasks.count() == 2
    netuids = [task.netuid for task in modulo_tasks]
    assert 1 in netuids
    assert 2 in netuids


@pytest.mark.django_db
@patch("abstract_block_dumper.tasks.execute_block_task.apply_async")
@patch("abstract_block_dumper.scheduler.get_bittensor_client")
def test_celery_task_scheduling(mock_get_client, mock_apply_async, setup_test_tasks):
    """Test that Celery tasks are properly scheduled."""
    # Setup mocks
    mock_subtensor = MagicMock()
    mock_get_client.return_value = mock_subtensor

    mock_result = MagicMock()
    mock_result.id = "test-celery-id"
    mock_apply_async.return_value = mock_result

    # Register tasks and create a scheduled task
    sync_block_task_functions()
    config = BlockDumperConfig.objects.get(name="test_every_block")

    task = ScheduledTask.objects.create(config=config, block_number=100, status=ScheduledTask.Status.PENDING)

    # Schedule the Celery task
    scheduler = BlockScheduler()
    scheduled_count = scheduler.schedule_celery_tasks([(task, config)])

    # Verify Celery task was scheduled
    assert scheduled_count == 1
    mock_apply_async.assert_called_once_with(args=[task.id], queue=config.queue)

    # Verify task was updated with Celery ID
    task.refresh_from_db()
    assert task.celery_task_id == "test-celery-id"


@pytest.mark.django_db
def test_task_execution_success(setup_test_tasks):
    """Test successful task execution."""
    # Register tasks
    sync_block_task_functions()
    config = BlockDumperConfig.objects.get(name="test_every_block")

    # Create execution tracking
    execution = BlockDumperExecution.objects.create(block_number=100, total_tasks_scheduled=1)

    # Create scheduled task
    task = ScheduledTask.objects.create(config=config, block_number=100, status=ScheduledTask.Status.PENDING)

    # Execute the task directly (bypassing Celery for integration test)
    execute_block_task(task.id)

    # Verify task was marked as successful
    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.SUCCESS
    assert task.started_at is not None
    assert task.finished_at is not None
    assert task.result_data["result"] == "Processed block 100"

    # Verify config stats were updated
    config.refresh_from_db()
    assert config.total_executions == 1
    assert config.successful_executions == 1
    assert config.failed_executions == 0
    assert config.last_execution_at is not None
    assert config.last_success_at is not None

    # Verify execution stats were updated
    execution.refresh_from_db()
    assert execution.tasks_completed == 1
    assert execution.tasks_failed == 0
    assert execution.tasks_pending == 0
    assert execution.all_completed is True
    assert execution.has_failures is False


@pytest.mark.django_db
@patch("abstract_block_dumper.tasks.execute_block_task.apply_async")
def test_task_execution_failure_and_retry(mock_apply_async, setup_test_tasks):
    """Test task execution failure and retry mechanism."""
    # Create a task that will fail using the module-level function
    block_task(name="failing_task", max_retries=2)(failing_task_func)

    # Register the failing task
    sync_block_task_functions()
    config = BlockDumperConfig.objects.get(name="failing_task")

    # Create execution tracking
    execution = BlockDumperExecution.objects.create(block_number=100, total_tasks_scheduled=1)

    # Create scheduled task
    task = ScheduledTask.objects.create(config=config, block_number=100, status=ScheduledTask.Status.PENDING)

    # Execute the task (should fail)
    execute_block_task(task.id)

    # Verify task failed and retry was scheduled
    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.FAILED
    assert task.retry_count == 1
    assert task.can_retry()
    assert task.next_retry_at is not None
    assert "Test error" in task.error_message

    # Verify retry was scheduled
    mock_apply_async.assert_called_once()

    # Verify config stats were updated
    config.refresh_from_db()
    assert config.total_executions == 1
    assert config.successful_executions == 0
    assert config.failed_executions == 1

    # Verify execution stats were updated
    execution.refresh_from_db()
    assert execution.tasks_completed == 0
    assert execution.tasks_failed == 1
    assert execution.tasks_pending == 0
    # Note: all_completed is True when no tasks are pending, even if some failed
    assert execution.all_completed is True
    assert execution.has_failures is True


@pytest.mark.django_db
def test_condition_evaluation(setup_test_tasks):
    """Test different condition types are evaluated correctly."""
    # Register tasks
    sync_block_task_functions()

    every_block_config = BlockDumperConfig.objects.get(name="test_every_block")
    modulo_config = BlockDumperConfig.objects.get(name="test_modulo_task")

    # Test every_block condition
    assert every_block_config.should_execute_at_block(100) is True
    assert every_block_config.should_execute_at_block(101) is True
    assert every_block_config.should_execute_at_block(999) is True

    # Test modulo condition (modulo=5)
    assert modulo_config.should_execute_at_block(100) is True  # 100 % 5 == 0
    assert modulo_config.should_execute_at_block(105) is True  # 105 % 5 == 0
    assert modulo_config.should_execute_at_block(101) is False  # 101 % 5 != 0
    assert modulo_config.should_execute_at_block(103) is False  # 103 % 5 != 0


@pytest.mark.django_db
def test_netuid_handling(setup_test_tasks):
    """Test that netuid handling works correctly."""
    # Register tasks
    sync_block_task_functions()

    every_block_config = BlockDumperConfig.objects.get(name="test_every_block")
    modulo_config = BlockDumperConfig.objects.get(name="test_modulo_task")

    # Test NONE netuid type
    assert every_block_config.get_netuids() == [None]

    # Test MULTIPLE netuid type
    assert modulo_config.get_netuids() == [1, 2]


@pytest.mark.django_db
@patch("abstract_block_dumper.scheduler.get_bittensor_client")
def test_end_to_end_workflow(mock_get_client, setup_test_tasks):
    """Test the complete end-to-end workflow."""
    # Setup mocks
    mock_subtensor = MagicMock()
    mock_subtensor.get_current_block.return_value = 105
    mock_get_client.return_value = mock_subtensor

    # Register tasks
    sync_block_task_functions()

    # Create scheduler
    scheduler = BlockScheduler()
    scheduler.last_processed_block = 104

    # Process block 105 (should trigger modulo task since 105 % 5 == 0)
    with patch("abstract_block_dumper.scheduler.BlockScheduler.schedule_celery_tasks") as mock_schedule:
        mock_schedule.return_value = 3  # 1 every_block + 2 modulo tasks
        scheduler._schedule_task_for_block(105)

    # Verify execution was created
    execution = BlockDumperExecution.objects.get(block_number=105)
    assert execution.total_tasks_scheduled == 3

    # Verify all expected tasks were created
    tasks = ScheduledTask.objects.filter(block_number=105)
    assert tasks.count() == 3

    # Execute all tasks
    for task in tasks:
        execute_block_task(task.id)

    # Verify all tasks completed successfully
    execution.refresh_from_db()
    assert execution.tasks_completed == 3
    assert execution.tasks_failed == 0
    assert execution.all_completed is True
    assert execution.has_failures is False

    # Verify config stats
    every_block_config = BlockDumperConfig.objects.get(name="test_every_block")
    modulo_config = BlockDumperConfig.objects.get(name="test_modulo_task")

    assert every_block_config.successful_executions == 1
    assert modulo_config.successful_executions == 2  # 2 netuids
