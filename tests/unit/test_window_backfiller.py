from unittest.mock import MagicMock

import pytest

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.window_backfiller import (
    ARCHIVE_BLOCK_THRESHOLD,
    WindowBackfiller,
)
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task


def _make_item(condition=lambda bn, **kw: True, args=None, backfill_queue=None):
    fn = MagicMock(__name__="dummy_task")
    return RegistryItem(condition=condition, function=fn, args=args, backfill_queue=backfill_queue)


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


def test_submit_block_routes_to_task_backfill_queue():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    item = _make_item(backfill_queue="block-backfill")

    submitted = wb.submit_block(item, 100, {}, executed_blocks=set(), head_block=100)

    assert submitted is True
    executor.execute.assert_called_once_with(
        item,
        100,
        {},
        use_archive=False,
        queue="block-backfill",
    )


def test_submit_block_routes_each_task_to_its_own_backfill_queue():
    executor = MagicMock()
    wb = WindowBackfiller(executor)
    first_item = _make_item(backfill_queue="first-backfill")
    second_item = _make_item(backfill_queue="second-backfill")

    wb.submit_block(first_item, 100, {}, executed_blocks=set(), head_block=100)
    wb.submit_block(second_item, 100, {}, executed_blocks=set(), head_block=100)

    assert executor.execute.call_args_list == [
        ((first_item, 100, {}), {"use_archive": False, "queue": "first-backfill"}),
        ((second_item, 100, {}), {"use_archive": False, "queue": "second-backfill"}),
    ]


def _every_block(block_number: int):
    return f"ok {block_number}"


@pytest.mark.django_db
def test_process_item_range_submits_missing_blocks_and_skips_executed():
    from abstract_block_dumper._internal.dal.memory_registry import task_registry

    block_task(condition=lambda bn: True)(_every_block)
    executable_path = abd_utils.get_executable_path(_every_block)
    item = task_registry.get_by_executable_path(executable_path)
    assert item is not None

    # Seed block 101 as already SUCCESS -> must be skipped.
    TaskAttempt.objects.create(
        block_number=101,
        executable_path=executable_path,
        args_json=abd_utils.serialize_args({}),
        status=TaskAttempt.Status.SUCCESS,
    )

    executor = MagicMock()
    wb = WindowBackfiller(executor)

    submitted = wb.process_item_range(item, from_block=100, to_block=103, head_block=104)

    # 100, 102, 103 submitted; 101 skipped.
    assert submitted == 3
    submitted_blocks = {call.args[1] for call in executor.execute.call_args_list}
    assert submitted_blocks == {100, 102, 103}


@pytest.mark.django_db
def test_process_item_range_skips_inflight_blocks():
    from abstract_block_dumper._internal.dal.memory_registry import task_registry

    block_task(condition=lambda bn: True)(_every_block)
    executable_path = abd_utils.get_executable_path(_every_block)
    item = task_registry.get_by_executable_path(executable_path)
    assert item is not None

    args_json = abd_utils.serialize_args({})
    # 101 already SUCCESS, 102 in-flight PENDING, 103 in-flight RUNNING -> all skipped.
    TaskAttempt.objects.create(
        block_number=101,
        executable_path=executable_path,
        args_json=args_json,
        status=TaskAttempt.Status.SUCCESS,
    )
    TaskAttempt.objects.create(
        block_number=102,
        executable_path=executable_path,
        args_json=args_json,
        status=TaskAttempt.Status.PENDING,
    )
    TaskAttempt.objects.create(
        block_number=103,
        executable_path=executable_path,
        args_json=args_json,
        status=TaskAttempt.Status.RUNNING,
    )

    executor = MagicMock()
    wb = WindowBackfiller(executor)

    submitted = wb.process_item_range(item, from_block=100, to_block=103, head_block=104)

    # Only 100 is missing; 101 is done, 102/103 are in-flight.
    assert submitted == 1
    submitted_blocks = {call.args[1] for call in executor.execute.call_args_list}
    assert submitted_blocks == {100}


@pytest.mark.django_db
def test_process_item_range_resubmits_failed_blocks():
    from abstract_block_dumper._internal.dal.memory_registry import task_registry

    block_task(condition=lambda bn: True)(_every_block)
    executable_path = abd_utils.get_executable_path(_every_block)
    item = task_registry.get_by_executable_path(executable_path)
    assert item is not None

    args_json = abd_utils.serialize_args({})
    # 101 SUCCESS -> skipped; 102 FAILED -> must be re-submitted to self-heal.
    TaskAttempt.objects.create(
        block_number=101,
        executable_path=executable_path,
        args_json=args_json,
        status=TaskAttempt.Status.SUCCESS,
    )
    TaskAttempt.objects.create(
        block_number=102,
        executable_path=executable_path,
        args_json=args_json,
        status=TaskAttempt.Status.FAILED,
    )

    executor = MagicMock()
    wb = WindowBackfiller(executor)

    submitted = wb.process_item_range(item, from_block=100, to_block=103, head_block=104)

    submitted_blocks = {call.args[1] for call in executor.execute.call_args_list}
    # 101 (SUCCESS) skipped; 102 (FAILED) re-submitted; 100/103 missing.
    assert submitted_blocks == {100, 102, 103}
    assert submitted == 3
