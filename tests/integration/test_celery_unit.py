import pytest

from abstract_block_dumper.block_processor import block_processor_factory
from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.memory_registry import task_registry
from abstract_block_dumper.models import TaskAttempt
from tests.conftest import failing_task_func


def backfill_task(block_number: int) -> str:
    return f"Backfilled block {block_number}"


@pytest.mark.django_db
def test_task_execution_success(setup_test_tasks):
    current_block = 100
    task_attempt, _ = TaskAttempt.create_or_get_pending(
        block_number=current_block, executable_path="tests.conftest.every_block_task_func", args={}
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

    # Execute the failing task - it won't raise, failure is recorded in DB
    celery_unit(block_number, {}, executable_function)

    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.FAILED
    assert task_attempt.attempt_count == 1
    assert task_attempt.can_retry() is True
    assert task_attempt.next_retry_at is not None


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
    task_attempts = TaskAttempt.objects.filter(
        executable_path__contains="backfill_task",
        block_number__gte=current_block - backfill_amount,
        block_number__lte=current_block,
    )

    assert task_attempts.count() == backfill_amount

    for task_attempt in task_attempts:
        celery_unit(task_attempt.block_number, task_attempt.args_dict, task_attempt.executable_path)
        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.SUCCESS
