from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
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

    block_task(failing_task)

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

    block_task(flaky_function)

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

    block_task(failing_task)

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

    block_task(successful_task)

    task_registry.get_by_executable_path(executable_path)

    block_processor = block_processor_factory()
    block_processor.recover_failed_retries(poll_interval=0)  # No delay for testing

    recover_ids = pending_attempt_ids + failed_attempt_ids

    qs = TaskAttempt.objects.filter(
        id__in=recover_ids,
        status=TaskAttempt.Status.SUCCESS,
    )
    assert qs.count() == len(recover_ids)


def test_bittensor_client_uses_archive_for_old_blocks(
    mock_subtensor,
    mock_archive_subtensor,
) -> None:
    """Test that BittensorConnectionClient uses archive_subtensor for blocks older than 300 from current head."""
    current_block = 1000

    client = BittensorConnectionClient(network="testnet")
    client._subtensor = mock_subtensor
    client._archive_subtensor = mock_archive_subtensor
    client._current_block_cache = current_block

    # For old block (400 behind), should return archive_subtensor
    old_block = 600  # 400 blocks behind (> 300 threshold)
    result = client.get_subtensor_for_block(old_block)
    assert result is mock_archive_subtensor

    # For recent block (within 300), should return regular subtensor
    recent_block = 800  # 200 blocks behind (< 300 threshold)
    result = client.get_subtensor_for_block(recent_block)
    assert result is mock_subtensor

    # For block exactly at threshold (300 behind), should return regular subtensor
    threshold_block = 700  # exactly 300 blocks behind
    result = client.get_subtensor_for_block(threshold_block)
    assert result is mock_subtensor

    # For block just over threshold (301 behind), should return archive_subtensor
    just_over_threshold_block = 699  # 301 blocks behind
    result = client.get_subtensor_for_block(just_over_threshold_block)
    assert result is mock_archive_subtensor
