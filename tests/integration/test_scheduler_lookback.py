from unittest.mock import MagicMock

import pytest

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.scheduler import TaskScheduler
from abstract_block_dumper.v1.decorators import block_task
from tests.conftest import MockedBlockProcessor


def _lookback_task(block_number: int):
    return f"lookback {block_number}"


def _plain_task(block_number: int):
    return f"plain {block_number}"


def _finalized_lookback_task(block_number: int):
    return f"finalized lookback {block_number}"


def _make_scheduler(window_backfiller, *, lookback_enabled=True):
    processor = MockedBlockProcessor(executor=MagicMock(), registry=task_registry)
    state_resolver = MagicMock()
    state_resolver.get_starting_block.return_value = 0
    return TaskScheduler(
        block_processor=processor,
        bittensor_client=MagicMock(),
        state_resolver=state_resolver,
        poll_interval=0,
        window_backfiller=window_backfiller,
        lookback_enabled=lookback_enabled,
    )


@pytest.mark.django_db
def test_fill_lookback_runs_only_for_lookback_items():
    block_task(condition=lambda bn: True, backfilling_lookback=50)(_lookback_task)
    block_task(condition=lambda bn: True)(_plain_task)

    window_backfiller = MagicMock()
    scheduler = _make_scheduler(window_backfiller)

    scheduler._fill_lookback(1000)

    lookback_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(_lookback_task))
    window_backfiller.process_item_range.assert_called_once_with(lookback_item, 950, 999, head_block=1000)


@pytest.mark.django_db
def test_fill_lookback_respects_disabled_flag():
    block_task(condition=lambda bn: True, backfilling_lookback=50)(_lookback_task)

    window_backfiller = MagicMock()
    scheduler = _make_scheduler(window_backfiller, lookback_enabled=False)

    scheduler._fill_lookback(1000)

    window_backfiller.process_item_range.assert_not_called()


@pytest.mark.django_db
def test_fill_lookback_clamps_from_block_to_zero():
    block_task(condition=lambda bn: True, backfilling_lookback=5000)(_lookback_task)

    window_backfiller = MagicMock()
    scheduler = _make_scheduler(window_backfiller)

    scheduler._fill_lookback(100)

    lookback_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(_lookback_task))
    window_backfiller.process_item_range.assert_called_once_with(lookback_item, 0, 99, head_block=100)


@pytest.mark.django_db
def test_fill_lookback_uses_the_tasks_selected_head_type():
    block_task(backfilling_lookback=50)(_lookback_task)
    block_task(finalized=True, backfilling_lookback=50)(_finalized_lookback_task)

    window_backfiller = MagicMock()
    scheduler = _make_scheduler(window_backfiller)

    finalized_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(_finalized_lookback_task))
    scheduler._fill_lookback(1000, registry_items=[finalized_item])

    window_backfiller.process_item_range.assert_called_once_with(finalized_item, 950, 999, head_block=1000)
