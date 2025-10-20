from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

import abstract_block_dumper.dal.django_dal as abd_dal
import abstract_block_dumper.services.utils as abd_utils
from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.services.block_processor import block_processor_factory
from tests.fatories import TaskAttemptFactory


def failing_task(block_number: int):
    raise ValueError("Task failed")


def successful_task(block_number: int):
    return {"result": "success", "block": block_number}


def flaky_function(block_number: int):
    executable_path = abd_utils.get_executable_path(flaky_function)
    task_attempt = TaskAttempt.objects.get(
        block_number=block_number,
        executable_path=executable_path,
    )

    if task_attempt.attempt_count == 0:
        raise ValueError("Flaky failure")

    return f"Success on retry for block {block_number}"


@pytest.mark.django_db
def test_task_failure_triggers_retry():
    """Test that a failing task is marked as FAILED with retry info set."""
    block_number = 100
    executable_path = abd_utils.get_executable_path(failing_task)
    task_attempt, _ = abd_dal.task_create_or_get_pending(block_number, executable_path)

    block_task(condition=lambda _bn: True)(failing_task)

    registry_item = task_registry.get_by_executable_path(executable_path)

    assert registry_item is not None
    assert callable(registry_item.function)

    # Note: CELERY_TASK_ALWAYS_EAGER allows to execute it directly
    # Execute the task - it will fail but not raise (failure is recorded in DB)
    registry_item.function(block_number)
    # This task will be rescheduled more times and will fail due to max retry attempts reached

    # Reload from DB to see post-failure state
    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == abd_utils.get_max_attempt_limit()
    assert abd_dal.task_can_retry(task_attempt) is False
    assert task_attempt.next_retry_at is None


@pytest.mark.django_db
def test_successful_retry_completes_task() -> None:
    """Test that retry is scheduled with correct ETA."""
    current_block = 1000
    executable_path = abd_utils.get_executable_path(flaky_function)
    task_attempt, _ = abd_dal.task_create_or_get_pending(current_block, executable_path)

    block_task(lambda _bn: True)(flaky_function)

    registry_item = task_registry.get_by_executable_path(executable_path)
    assert registry_item is not None
    assert callable(registry_item.function)

    # Note: CELERY_TASK_ALWAYS_EAGER allows to execute it directly
    registry_item.function(current_block)

    task_attempt.refresh_from_db()

    assert task_attempt.status == TaskAttempt.Status.SUCCESS
    assert task_attempt.attempt_count == 1


@pytest.mark.django_db
def test_restry_schedules_celery_task_with_eta():
    """Test that a flaky task fails first, then succeeds on retry."""
    current_block = 100
    executable_path = abd_utils.get_executable_path(failing_task)
    task_attempt, _ = abd_dal.task_create_or_get_pending(current_block, executable_path)

    block_task(condition=lambda _bn: True)(failing_task)

    registry_item = task_registry.get_by_executable_path(executable_path)
    assert registry_item is not None
    assert callable(registry_item.function)

    with patch.object(registry_item.function, "apply_async") as mock_apply_async:
        # Note: CELERY_TASK_ALWAYS_EAGER allows to execute it directly
        registry_item.function(current_block)

        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.PENDING
        assert task_attempt.attempt_count == 1
        assert task_attempt.next_retry_at is not None

        # Verify retry was scheduled with correct parameters
        mock_apply_async.assert_called_once()
        mock_apply_async.assert_called_once_with(
            kwargs={
                "block_number": current_block,
            },
            eta=task_attempt.next_retry_at,
        )


@pytest.mark.django_db
def test_retry_recover_mechanism():
    """Test that scheduler recovers orphaned failed tasks ready for retry."""
    batch_size = 30
    executable_path = abd_utils.get_executable_path(successful_task)

    past_time = timezone.now() - timedelta(days=1)

    pending_attempts = TaskAttemptFactory.create_batch(
        size=batch_size, is_pending=True, executable_path=executable_path, next_retry_at=past_time, attempt_count=1
    )
    pending_attempt_ids = [attempt.id for attempt in pending_attempts]

    failed_attempts = TaskAttemptFactory.create_batch(
        size=batch_size, is_pending=True, executable_path=executable_path, next_retry_at=past_time, attempt_count=1
    )
    failed_attempt_ids = [attempt.id for attempt in failed_attempts]

    TaskAttemptFactory.create_batch(
        size=batch_size, is_success=True, executable_path=executable_path, next_retry_at=past_time, attempt_count=1
    )

    block_task(lambda _bn: True)(successful_task)

    task_registry.get_by_executable_path(executable_path)

    block_processor = block_processor_factory()
    block_processor.recover_failed_retries()

    recover_ids = pending_attempt_ids + failed_attempt_ids

    qs = TaskAttempt.objects.filter(
        id__in=recover_ids,
        status=TaskAttempt.Status.SUCCESS,
    )
    assert qs.count() == len(recover_ids)
