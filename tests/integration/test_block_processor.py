from unittest.mock import patch

import pytest

from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.services.scheduler import task_scheduler_factory


@pytest.mark.django_db
@patch("abstract_block_dumper.services.utils.get_bittensor_client")
def test_complete_e2e_workflow(mock_get_bittensor_client, setup_test_tasks) -> None:
    block_number = 300
    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = block_number

    scheduler = task_scheduler_factory()
    scheduler.last_processed_block = block_number - 1
    scheduler.block_processor.process_block(block_number)

    task_attempts = TaskAttempt.objects.filter(block_number=block_number)

    assert task_attempts.count() == 3

    for task_attempt in task_attempts:
        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.SUCCESS
        assert task_attempt.execution_result is not None


@pytest.mark.django_db
@patch("abstract_block_dumper.services.utils.get_bittensor_client")
def test_block_processing_flow(mock_get_bittensor_client, setup_test_tasks):
    current_block = 100

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_block

    # Create scheduler and process block
    scheduler = task_scheduler_factory()
    scheduler.last_processed_block = 99
    scheduler.block_processor.process_block(current_block)

    # Verify tasks were created for block current_block
    task_attempts = TaskAttempt.objects.filter(block_number=current_block)

    # Should have: 1 every_block + 2 modulo tasks (100 % 5 == 0)
    assert task_attempts.count() == 3

    # Verify every_block task
    every_block_task = task_attempts.filter(executable_path__contains="every_block_task_func")
    assert every_block_task.exists() is True

    # Verify modulo tasks
    modulo_tasks = task_attempts.filter(executable_path__contains="modulo_task_func")
    assert modulo_tasks.count() == 2
