import pytest

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper.v1.decorators import block_task


def _task_with_backfill_queue(block_number: int):
    return block_number


def test_block_task_registers_normalized_backfill_queue():
    block_task(backfill_queue="  task-backfill  ")(_task_with_backfill_queue)

    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(_task_with_backfill_queue))

    assert registry_item is not None
    assert registry_item.backfill_queue == "task-backfill"


@pytest.mark.parametrize("backfill_queue", ["", "   ", 1])
def test_block_task_rejects_invalid_backfill_queue(backfill_queue):
    with pytest.raises(ValueError, match="backfill_queue must be a non-empty string or None"):
        block_task(backfill_queue=backfill_queue)(_task_with_backfill_queue)
