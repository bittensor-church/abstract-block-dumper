from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.utils import timezone

from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.memory_registry import RegistryItem, task_registry
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.scheduler import task_scheduler_factory


def failing_task(block_number: int):
    raise ValueError("Task failed")


def successful_task(block_number: int):
    return {"result": "success", "block": block_number}


def flaky_function(block_number: int):
    task_attempt = TaskAttempt.objects.get(
        block_number=block_number,
        executable_path=f"{flaky_function.__module__}.{flaky_function.__name__}",
    )

    if task_attempt.attempt_count == 0:
        raise ValueError("Flaky failure")

    return f"Success on retry for block {block_number}"


@pytest.mark.django_db
def test_task_failure_triggers_retry():
    """Test that a failing task is marked as FAILED with retry info set."""
    block_number = 100
    executable_path = f"{failing_task.__module__}.{failing_task.__name__}"

    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=block_number,
        executable_path=executable_path,
    )

    # Execute the task - it will fail but not raise (failure is recorded in DB)
    celery_unit(block_number, {}, executable_path)

    # Reload from DB to see post-failure state
    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == 1
    assert task_attempt.can_retry() is True
    assert task_attempt.next_retry_at is not None


@pytest.mark.django_db
@patch("abstract_block_dumper.executor.celery_unit.apply_async")
def test_restry_schedules_celery_task_with_eta(mock_apply_async) -> None:
    """Test that retry is scheduled with correct ETA."""
    mock_result = MagicMock()
    mock_result.id = "test-result-id"
    mock_apply_async.return_value = mock_result

    block_number = 100
    executable_path = f"{failing_task.__module__}.{failing_task.__name__}"

    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=block_number,
        executable_path=executable_path,
    )

    # Execute the failing task
    celery_unit(block_number, {}, executable_path)

    task_attempt.refresh_from_db()

    # Verify retry was scheduled with correct parameters
    mock_apply_async.assert_called_once()
    call_kwargs = mock_apply_async.call_args[1]

    assert call_kwargs["args"] == [block_number, {}, executable_path]
    assert call_kwargs["eta"] == task_attempt.next_retry_at


@pytest.mark.django_db
@patch("abstract_block_dumper.executor.schedule_retry")
def test_successful_retry_completes_task(mock_schedule_retry):
    """Test that a flaky task fails first, then succeeds on retry."""
    block_number = 100
    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=block_number,
        executable_path=f"{flaky_function.__module__}.{flaky_function.__name__}",
    )

    # First execution - should fail
    celery_unit(block_number, {}, task_attempt.executable_path)

    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == 1

    # Verify retry was scheduled
    mock_schedule_retry.assert_called_once()


@pytest.mark.django_db
@patch("abstract_block_dumper.executor.celery_unit.apply_async")
def test_retry_recover_mechanism(mock_apply_async):
    """Test that scheduler recovers orphaned failed tasks ready for retry."""
    mock_apply_async.return_value = MagicMock(id="test-retry-id")

    block_number = 100
    executable_path = f"{failing_task.__module__}.{failing_task.__name__}"

    past_time = timezone.now() - timedelta(minutes=5)
    task_attempt = TaskAttempt.objects.create(
        block_number=block_number,
        executable_path=executable_path,
        status=TaskAttempt.Status.FAILED,
        attempt_count=1,
        next_retry_at=past_time,
    )

    registry_item = RegistryItem(
        function=failing_task,
        condition=lambda _bn: True,
        args=None,
        celery_kwargs=None,
        backfilling_lookback=None,
    )
    task_registry.register_item(registry_item)

    scheduler = task_scheduler_factory()
    scheduler.block_processor.recover_failed_retries()

    # Verify task was reset to PENDING and Celery task scheduled
    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.PENDING

    # Verify retry was scheduled
    mock_apply_async.assert_called_once()
