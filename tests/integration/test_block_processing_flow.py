from unittest.mock import patch

import pytest

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.scheduler import task_scheduler_factory
from tests.conftest import failing_task_func


@pytest.mark.django_db
@patch("abstract_block_dumper.utils.get_bittensor_client")
def test_block_processing_flow(mock_get_bittensor_client, setup_test_tasks):
    current_block = 100

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_block
    
    # Create scheduler and process block
    scheduler = task_scheduler_factory()
    scheduler.last_processed_block = 99
    scheduler.process_block(current_block)
    
    # Verify tasks were created for block current_block
    task_attempts = TaskAttempt.objects.filter(block_number=current_block)
    
    # Should have: 1 every_block + 2 modulo tasks (100 % 5 == 0)
    assert task_attempts.count() == 3
    
    # Verify every_block task
    every_block_task = task_attempts.filter(
        executable_path__contains="every_block_task_func"
    )
    assert every_block_task.exists() is True
    
    # Verify modulo tasks
    modulo_tasks = task_attempts.filter(
        executable_path__contains="modulo_task_func"
    )
    assert modulo_tasks.count() == 2


@pytest.mark.django_db
def test_task_execution_success(setup_test_tasks):
    current_block = 100
    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=current_block,
        executable_path="tests.conftest.every_block_task_func",
        args={}
    )
    
    # Execute task directly using celery_unit (bypassing Celery async for testing)
    result = celery_unit(current_block, {}, "tests.conftest.every_block_task_func")
    
    # Verify task completion
    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.SUCCESS
    assert task_attempt.execution_result == f"Processed block {current_block}"
    assert task_attempt.last_attempted_at is not None
    assert result == f"Processed block {current_block}"



@pytest.mark.django_db
def test_task_execution_failure_and_retry(setup_test_tasks):
    executable_function = "tests.conftest.failing_task_func"
    block_number = 150

    # Failing task
    block_task(condition=lambda bn: True)(failing_task_func)

    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=block_number,
        executable_path=executable_function,
        args={},
    )

    with pytest.raises(ValueError, match="Test error"):
        celery_unit(block_number, {}, executable_function)

    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == 1
    assert task_attempt.can_retry() is True
    assert task_attempt.next_retry_at is not None


@pytest.mark.django_db
@patch("abstract_block_dumper.utils.get_bittensor_client")
def test_complete_e2e_workflow(mock_get_bittensor_client, setup_test_tasks) -> None:
    block_number = 300
    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = block_number

    scheduler = task_scheduler_factory()
    scheduler.last_processed_block = block_number - 1
    scheduler.process_block(block_number)

    task_attempts = TaskAttempt.objects.filter(block_number=block_number)

    assert task_attempts.count() == 3

    for task_attempt in task_attempts:
        celery_unit(task_attempt.block_number, task_attempt.args_dict, task_attempt.executable_path)

    for task_attempt in task_attempts:
        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.SUCCESS
        assert task_attempt.execution_result is not None
