from unittest.mock import MagicMock

from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.window_backfiller import (
    ARCHIVE_BLOCK_THRESHOLD,
    WindowBackfiller,
)


def _make_item(condition=lambda bn, **kw: True, args=None):
    fn = MagicMock(__name__="dummy_task")
    return RegistryItem(condition=condition, function=fn, args=args)


def test_submit_block_skips_already_executed():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item()

    submitted = wb.submit_block(item, 100, {}, executed_blocks={100}, head_block=100)

    assert submitted is False
    executor.execute.assert_not_called()


def test_submit_block_skips_when_condition_false():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item(condition=lambda bn, **kw: bn % 2 == 0)

    submitted = wb.submit_block(item, 101, {}, executed_blocks=set(), head_block=101)

    assert submitted is False
    executor.execute.assert_not_called()


def test_submit_block_executes_recent_block_without_archive():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item()

    # head - block == threshold exactly -> NOT archive
    submitted = wb.submit_block(item, 700, {}, executed_blocks=set(), head_block=700 + ARCHIVE_BLOCK_THRESHOLD)

    assert submitted is True
    executor.execute.assert_called_once_with(item, 700, {}, use_archive=False)


def test_submit_block_executes_old_block_with_archive():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item()

    # head - block == threshold + 1 -> archive
    submitted = wb.submit_block(item, 699, {}, executed_blocks=set(), head_block=699 + ARCHIVE_BLOCK_THRESHOLD + 1)

    assert submitted is True
    executor.execute.assert_called_once_with(item, 699, {}, use_archive=True)


def test_submit_block_passes_args_through():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item()

    submitted = wb.submit_block(item, 100, {"netuid": 5}, executed_blocks=set(), head_block=100)

    assert submitted is True
    executor.execute.assert_called_once_with(item, 100, {"netuid": 5}, use_archive=False)
