import pytest

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.memory_registry import MemoryRegistry
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.scheduler import task_scheduler_factory


def backfill_task(block_number: int) -> str:
    return f"Backfilled block {block_number}"


@pytest.mark.django_db
def test_backfilling_workflow():
    current_block = 100
    backfill_amount = 10

    block_task(
        condition=lambda bn: True,
        backfilling_lookback=backfill_amount,
    )(backfill_task)

    scheduler = task_scheduler_factory()
    scheduler.last_processed_block = current_block - 1

    # Get backfilling registry item
    registry_items = MemoryRegistry.get_functions()
    backfill_item = registry_items[0]

    # Backfilling process
    scheduler.process_backfill(backfill_item, current_block)

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
