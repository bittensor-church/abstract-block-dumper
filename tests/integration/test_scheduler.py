from unittest.mock import patch

import pytest

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.memory_registry import task_registry
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.utils import get_executable_path


def failing_task(block_number: int):
    raise ValueError("Task failed")


def successful_task(block_number: int):
    return {"result": "success", "block": block_number}


def flaky_function(block_number: int):
    executable_path = get_executable_path(flaky_function)
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
    executable_path = get_executable_path(failing_task)

    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=block_number,
        executable_path=executable_path,
    )

    block_task(condition=lambda _bn: True)(failing_task)

    registry_item = task_registry.get_by_executable_path(executable_path)

    assert registry_item is not None
    assert callable(registry_item.function)

    # Execute the task - it will fail but not raise (failure is recorded in DB)
    registry_item.function(block_number)
    # This task will be rescheduled more times and will fail due to max retry attempts reached

    # Reload from DB to see post-failure state
    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == task_attempt.get_max_attempt()
    assert task_attempt.can_retry() is False
    assert task_attempt.next_retry_at is None


@pytest.mark.django_db
def test_successful_retry_completes_task() -> None:
    """Test that retry is scheduled with correct ETA."""
    current_block = 1000
    executable_path = get_executable_path(flaky_function)

    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=current_block,
        executable_path=executable_path,
    )

    block_task(lambda _bn: True)(flaky_function)

    registry_item = task_registry.get_by_executable_path(executable_path)
    assert registry_item is not None
    assert callable(registry_item.function)

    registry_item.function(current_block)

    task_attempt.refresh_from_db()

    assert task_attempt.status == TaskAttempt.Status.SUCCESS
    assert task_attempt.attempt_count == 1


@pytest.mark.django_db
def test_restry_schedules_celery_task_with_eta():
    """Test that a flaky task fails first, then succeeds on retry."""
    current_block = 100
    executable_path = get_executable_path(failing_task)
    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=current_block,
        executable_path=executable_path,
    )

    block_task(condition=lambda _bn: True)(failing_task)

    registry_item = task_registry.get_by_executable_path(executable_path)
    assert registry_item is not None
    assert callable(registry_item.function)

    with patch.object(registry_item.function, "apply_async") as mock_apply_async:
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


# @pytest.mark.django_db
# @patch("abstract_block_dumper.executor.celery_unit.apply_async")
# def test_retry_recover_mechanism(mock_apply_async):
#     """Test that scheduler recovers orphaned failed tasks ready for retry."""
#     mock_apply_async.return_value = MagicMock(id="test-retry-id")

#     block_number = 100
#     executable_path = f"{failing_task.__module__}.{failing_task.__name__}"

#     past_time = timezone.now() - timedelta(minutes=5)
#     task_attempt = TaskAttempt.objects.create(
#         block_number=block_number,
#         executable_path=executable_path,
#         status=TaskAttempt.Status.FAILED,
#         attempt_count=1,
#         next_retry_at=past_time,
#     )

#     registry_item = RegistryItem(
#         function=failing_task,
#         condition=lambda _bn: True,
#         args=None,
#         celery_kwargs=None,
#         backfilling_lookback=None,
#     )
#     task_registry.register_item(registry_item)

#     scheduler = task_scheduler_factory()
#     scheduler.block_processor.recover_failed_retries()

#     # Verify task was reset to PENDING and Celery task scheduled
#     task_attempt.refresh_from_db()
#     assert task_attempt.status == TaskAttempt.Status.PENDING

#     # Verify retry was scheduled
#     mock_apply_async.assert_called_once()
