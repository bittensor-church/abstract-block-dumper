import pytest

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
import abstract_block_dumper.models as abd_models
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper.v1.decorators import block_task
from tests.conftest import every_block_task_func, failing_task_func
from tests.fatories import TaskAttemptFactory


def backfill_task(block_number: int) -> str:
    return f"Backfilled block {block_number}"


@pytest.mark.django_db
def test_task_execution_success(setup_test_tasks):
    current_block = 100
    executable_path = abd_utils.get_executable_path(every_block_task_func)
    task_attempt = TaskAttemptFactory(is_pending=True, block_number=current_block, executable_path=executable_path)

    registry_item = task_registry.get_by_executable_path(task_attempt.executable_path)
    assert registry_item is not None

    raw_output = registry_item.function.delay(current_block)
    output = raw_output.result

    assert isinstance(output, dict)
    assert "result" in output

    assert output["result"] == f"Processed block {current_block}"

    # Verify task completion
    task_attempt.refresh_from_db()
    assert task_attempt.status == abd_models.TaskAttempt.Status.SUCCESS
    assert task_attempt.execution_result == f"Processed block {current_block}"
    assert task_attempt.last_attempted_at is not None


@pytest.mark.django_db
def test_task_execution_failure_and_retry():
    current_block = 150
    executable_path = abd_utils.get_executable_path(failing_task_func)
    task_attempt, _ = abd_dal.task_create_or_get_pending(
        block_number=current_block,
        executable_path=executable_path,
    )

    block_task()(failing_task_func)

    registry_item = task_registry.get_by_executable_path(task_attempt.executable_path)
    assert registry_item is not None

    registry_item.function.delay(current_block)

    # Test that retry reached the limit of attempts
    task_attempt.refresh_from_db()
    assert abd_dal.task_can_retry(task_attempt) is False
    assert task_attempt.status == abd_models.TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == abd_utils.get_max_attempt_limit()
    assert task_attempt.next_retry_at is None


@pytest.mark.django_db
def test_backfill_via_backfill_scheduler():
    """Test backfilling via BackfillScheduler (replaces removed process_backfill)."""
    from unittest.mock import patch

    from abstract_block_dumper._internal.services.backfill_scheduler import backfill_scheduler_factory

    current_block = 100
    backfill_amount = 10
    from_block = current_block - backfill_amount

    block_task(
        backfilling_lookback=backfill_amount,
    )(backfill_task)

    with patch("abstract_block_dumper._internal.services.utils.get_bittensor_client") as mock_client:
        mock_client.return_value.get_current_block.return_value = 500

        # Use BackfillScheduler instead of process_backfill
        scheduler = backfill_scheduler_factory(
            from_block=from_block,
            to_block=current_block - 1,  # exclusive of current_block
            rate_limit=0,
        )
        scheduler.start()

    # Backfilling tasks were created for blocks in range
    task_attempts = abd_models.TaskAttempt.objects.filter(
        executable_path__contains="backfill_task",
        block_number__gte=from_block,
        block_number__lt=current_block,
    )

    assert task_attempts.count() == backfill_amount

    qs = abd_models.TaskAttempt.objects.filter(id__in=task_attempts.values_list("id", flat=True))

    assert qs.count() == qs.filter(status=abd_models.TaskAttempt.Status.SUCCESS).count()
