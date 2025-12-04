"""Tests for the BackfillScheduler."""

from unittest.mock import MagicMock, patch

import pytest

from abstract_block_dumper._internal.services.backfill_scheduler import (
    ARCHIVE_BLOCK_THRESHOLD,
    DryRunStats,
    backfill_scheduler_factory,
)
from abstract_block_dumper.models import TaskAttempt


def simple_task_func(block_number: int):
    """Simple test function."""
    return f"Processed block {block_number}"


@pytest.fixture
def setup_backfill_tasks():
    """Register test tasks for backfill testing."""
    from abstract_block_dumper.v1.decorators import block_task

    # every block
    block_task(condition=lambda bn: True)(simple_task_func)

    yield


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_processes_block_range(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that BackfillScheduler processes a range of blocks."""
    from_block = 100
    to_block = 105
    current_head = 500

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_head

    scheduler = backfill_scheduler_factory(
        from_block=from_block,
        to_block=to_block,
        rate_limit=0,  # No delay for testing
    )

    scheduler.start()

    # Verify tasks were created for each block in range
    for block_number in range(from_block, to_block + 1):
        task_attempts = TaskAttempt.objects.filter(block_number=block_number)
        assert task_attempts.count() == 1
        assert task_attempts.first().status == TaskAttempt.Status.SUCCESS


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_dry_run_returns_stats(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that dry-run mode returns statistics without executing tasks."""
    from_block = 100
    to_block = 110
    current_head = 500

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_head

    scheduler = backfill_scheduler_factory(
        from_block=from_block,
        to_block=to_block,
        dry_run=True,
    )

    stats = scheduler.start()

    assert isinstance(stats, DryRunStats)
    assert stats.total_blocks == 11  # 100 to 110 inclusive
    assert stats.blocks_needing_tasks == 11  # All blocks need tasks
    assert stats.already_processed == 0
    assert stats.estimated_tasks == 11  # 1 task per block

    # Verify no tasks were actually created
    assert TaskAttempt.objects.count() == 0


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_skips_already_processed_blocks(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that backfill skips blocks that have already been processed."""
    from_block = 100
    to_block = 105
    current_head = 500

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_head

    # Process some blocks first
    scheduler1 = backfill_scheduler_factory(
        from_block=from_block,
        to_block=102,
        rate_limit=0,
    )
    scheduler1.start()

    # Verify first batch was processed
    assert TaskAttempt.objects.filter(block_number__lte=102).count() == 3

    # Now run backfill for the full range
    scheduler2 = backfill_scheduler_factory(
        from_block=from_block,
        to_block=to_block,
        rate_limit=0,
    )
    scheduler2.start()

    # Verify only new blocks were processed (no duplicates)
    for block_number in range(from_block, to_block + 1):
        task_attempts = TaskAttempt.objects.filter(block_number=block_number)
        assert task_attempts.count() == 1


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_uses_archive_network_for_old_blocks(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that archive network is used for blocks older than threshold."""
    current_head = 1000
    old_block = current_head - ARCHIVE_BLOCK_THRESHOLD - 100  # Well behind threshold

    mock_subtensor = MagicMock()
    mock_archive_subtensor = MagicMock()

    mock_subtensor.get_current_block.return_value = current_head
    mock_archive_subtensor.get_current_block.return_value = current_head

    def get_client(network):
        if network == "archive":
            return mock_archive_subtensor
        return mock_subtensor

    mock_get_bittensor_client.side_effect = get_client

    scheduler = backfill_scheduler_factory(
        from_block=old_block,
        to_block=old_block,
        rate_limit=0,
    )

    # Verify archive network detection
    subtensor = scheduler.get_subtensor_for_block(old_block)
    assert subtensor == mock_archive_subtensor


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_uses_regular_network_for_recent_blocks(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that regular network is used for recent blocks."""
    current_head = 1000
    recent_block = current_head - 100  # Within threshold

    mock_subtensor = MagicMock()
    mock_archive_subtensor = MagicMock()

    mock_subtensor.get_current_block.return_value = current_head
    mock_archive_subtensor.get_current_block.return_value = current_head

    def get_client(network):
        if network == "archive":
            return mock_archive_subtensor
        return mock_subtensor

    mock_get_bittensor_client.side_effect = get_client

    scheduler = backfill_scheduler_factory(
        from_block=recent_block,
        to_block=recent_block,
        rate_limit=0,
    )

    # Verify regular network is used
    subtensor = scheduler.get_subtensor_for_block(recent_block)
    assert subtensor == mock_subtensor


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_stop(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that scheduler can be stopped."""
    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = 500

    scheduler = backfill_scheduler_factory(
        from_block=100,
        to_block=200,
        rate_limit=0,
    )

    # Stop immediately
    scheduler.stop()
    assert scheduler.is_running is False


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_scheduler_dry_run_counts_already_processed(mock_get_bittensor_client, setup_backfill_tasks):
    """Test that dry-run correctly counts already processed blocks."""
    from_block = 100
    to_block = 110
    current_head = 500

    mock_subtensor = mock_get_bittensor_client.return_value
    mock_subtensor.get_current_block.return_value = current_head

    # Process some blocks first
    scheduler1 = backfill_scheduler_factory(
        from_block=100,
        to_block=104,
        rate_limit=0,
    )
    scheduler1.start()

    # Now dry-run for the full range
    scheduler2 = backfill_scheduler_factory(
        from_block=from_block,
        to_block=to_block,
        dry_run=True,
    )
    stats = scheduler2.start()

    assert stats.total_blocks == 11
    assert stats.already_processed == 5  # 100-104 already done
    assert stats.blocks_needing_tasks == 6  # 105-110 need tasks
    assert stats.estimated_tasks == 6
