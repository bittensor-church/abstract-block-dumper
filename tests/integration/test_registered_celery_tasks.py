import pytest

import abstract_block_dumper.dal.django_dal as abd_dal
import abstract_block_dumper.models as abd_models
import abstract_block_dumper.services.utils as abd_utils
from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.services.block_processor import block_processor_factory
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

    block_task(condition=lambda bn: True)(failing_task_func)

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
def test_process_backfill():
    current_block = 100
    backfill_amount = 10

    block_task(
        condition=lambda bn: True,
        backfilling_lookback=backfill_amount,
    )(backfill_task)

    block_processor = block_processor_factory()

    # Get backfilling registry item
    registry_items = task_registry.get_functions()
    backfill_item = registry_items[0]

    # Backfilling process
    block_processor.process_backfill(backfill_item, current_block)

    # Backfilling tasks were created for blocks that match condition
    task_attempts = abd_models.TaskAttempt.objects.filter(
        executable_path__contains="backfill_task",
        block_number__gte=current_block - backfill_amount,
        block_number__lte=current_block,
    )

    assert task_attempts.count() == backfill_amount

    qs = abd_models.TaskAttempt.objects.filter(id__in=task_attempts.values_list("id", flat=True))

    assert qs.count() == qs.filter(status=abd_models.TaskAttempt.Status.SUCCESS).count()
