import pytest

from abstract_block_dumper.v1.decorators import block_task


@pytest.mark.parametrize("backfill_queue", ["", "   ", 1])
def test_block_task_rejects_invalid_backfill_queue(backfill_queue):
    with pytest.raises(ValueError, match="backfill_queue must be a non-empty string or None"):

        @block_task(backfill_queue=backfill_queue)
        def invalid_task(block_number: int):
            return block_number
