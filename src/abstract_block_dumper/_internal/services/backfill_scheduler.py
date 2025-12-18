"""
Backfill scheduler for historical block processing.

This module provides a dedicated scheduler for backfilling historical blocks
with rate limiting and automatic archive network switching.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.services.block_processor import BaseBlockProcessor, block_processor_factory
from abstract_block_dumper._internal.services.metrics import (
    BlockProcessingTimer,
    increment_archive_network_usage,
    increment_blocks_processed,
    set_backfill_progress,
    set_block_lag,
    set_current_block,
)
from abstract_block_dumper._internal.services.utils import serialize_args

if TYPE_CHECKING:
    import bittensor as bt

    from abstract_block_dumper._internal.dal.memory_registry import RegistryItem

logger = structlog.get_logger(__name__)

# Blocks older than this threshold from current head require archive network
ARCHIVE_BLOCK_THRESHOLD = 300

# Progress logging interval
PROGRESS_LOG_INTERVAL = 100
ARCHIVE_NETWORK = "archive"

# Memory cleanup interval (every N blocks)
MEMORY_CLEANUP_INTERVAL = 1000


@dataclass
class DryRunStats:
    """Statistics for dry-run mode."""

    total_blocks: int = 0
    already_processed: int = 0
    blocks_needing_tasks: int = 0
    estimated_tasks: int = 0


class BackfillScheduler:
    """Scheduler for backfilling historical blocks with rate limiting."""

    def __init__(
        self,
        block_processor: BaseBlockProcessor,
        network: str,
        from_block: int,
        to_block: int,
        rate_limit: float = 1.0,
        dry_run: bool = False,
    ) -> None:
        """
        Initialize the backfill scheduler.

        Args:
            block_processor: The block processor to use for task execution.
            network: The bittensor network name (e.g., 'finney').
            from_block: Starting block number (inclusive).
            to_block: Ending block number (inclusive).
            rate_limit: Seconds to sleep between processing each block.
            dry_run: If True, preview what would be processed without executing.

        """
        self.block_processor = block_processor
        self.network = network
        self.from_block = from_block
        self.to_block = to_block
        self.rate_limit = rate_limit
        self.dry_run = dry_run
        self.is_running = False
        self._subtensor: bt.Subtensor | None = None
        self._archive_subtensor: bt.Subtensor | None = None
        self._current_head_cache: int | None = None

    @property
    def subtensor(self) -> bt.Subtensor:
        """Get the regular subtensor connection, creating it if needed."""
        if self._subtensor is None:
            self._subtensor = abd_utils.get_bittensor_client(self.network)
        return self._subtensor

    @property
    def archive_subtensor(self) -> bt.Subtensor:
        """Get the archive subtensor connection, creating it if needed."""
        if self._archive_subtensor is None:
            self._archive_subtensor = abd_utils.get_bittensor_client("archive")
        return self._archive_subtensor

    def get_subtensor_for_block(self, block_number: int) -> bt.Subtensor:
        """
        Get the appropriate subtensor for the given block number.

        Uses archive network for blocks older than ARCHIVE_BLOCK_THRESHOLD
        from the current head.
        """
        if self._current_head_cache is None:
            self._current_head_cache = self.subtensor.get_current_block()

        blocks_behind = self._current_head_cache - block_number

        if blocks_behind > ARCHIVE_BLOCK_THRESHOLD:
            logger.debug(
                "Using archive network for old block",
                block_number=block_number,
                blocks_behind=blocks_behind,
            )
            return self.archive_subtensor
        return self.subtensor

    def _get_network_type_for_block(self, block_number: int) -> str:
        """Get the network type string for a block (for display purposes)."""
        if self._current_head_cache is None:
            self._current_head_cache = self.subtensor.get_current_block()

        blocks_behind = self._current_head_cache - block_number
        return ARCHIVE_NETWORK if blocks_behind > ARCHIVE_BLOCK_THRESHOLD else self.network

    def start(self) -> DryRunStats | None:
        """
        Start processing blocks from from_block to to_block.

        Returns:
            DryRunStats if dry_run is True, None otherwise.

        """
        self.is_running = True

        # Refresh current head for accurate archive network decisions
        self._current_head_cache = self.subtensor.get_current_block()

        total_blocks = self.to_block - self.from_block + 1
        network_type = self._get_network_type_for_block(self.from_block)

        logger.info(
            "BackfillScheduler starting",
            from_block=self.from_block,
            to_block=self.to_block,
            total_blocks=total_blocks,
            rate_limit=self.rate_limit,
            dry_run=self.dry_run,
            network_type=network_type,
            current_head=self._current_head_cache,
        )

        if self.dry_run:
            return self._run_dry_run()

        self._run_backfill()
        return None

    def _run_dry_run(self) -> DryRunStats:
        """
        Run in dry-run mode to preview what would be processed.

        Optimized to fetch all executed blocks in one query per registry item,
        instead of querying for each block individually.
        """
        stats = DryRunStats(total_blocks=self.to_block - self.from_block + 1)

        registry_items = self.block_processor.registry.get_functions()

        # Pre-fetch all executed blocks for each registry item + args combination
        # This reduces N queries (one per block) to M queries (one per registry item + args)
        executed_blocks_cache: dict[tuple[str, str], set[int]] = {}

        logger.info(
            "Dry run: pre-fetching executed blocks",
            from_block=self.from_block,
            to_block=self.to_block,
            registry_items_count=len(registry_items),
        )

        for registry_item in registry_items:
            for args in registry_item.get_execution_args():
                args_json = serialize_args(args)
                cache_key = (registry_item.executable_path, args_json)

                # Fetch all executed blocks in the range with a single query
                executed_blocks_cache[cache_key] = abd_dal.executed_block_numbers(
                    registry_item.executable_path,
                    args_json,
                    self.from_block,
                    self.to_block + 1,
                )

        logger.info(
            "Dry run: analyzing blocks",
            cache_entries=len(executed_blocks_cache),
        )

        # Track which blocks have at least one task
        blocks_with_tasks: set[int] = set()

        for registry_item in registry_items:
            for args in registry_item.get_execution_args():
                args_json = serialize_args(args)
                cache_key = (registry_item.executable_path, args_json)
                executed_blocks = executed_blocks_cache[cache_key]

                for block_number in range(self.from_block, self.to_block + 1):
                    if not self.is_running:
                        break

                    if block_number in executed_blocks:
                        continue

                    # Check if condition matches
                    try:
                        if registry_item.match_condition(block_number, **args):
                            stats.estimated_tasks += 1
                            blocks_with_tasks.add(block_number)
                    except Exception as exc:
                        logger.debug(
                            "Error evaluating match condition during dry run",
                            function_name=registry_item.function.__name__,
                            block_number=block_number,
                            args=args,
                            error=str(exc),
                        )

        stats.blocks_needing_tasks = len(blocks_with_tasks)
        stats.already_processed = stats.total_blocks - stats.blocks_needing_tasks

        return stats

    def _run_backfill(self) -> None:
        """Run the actual backfill process."""
        processed_count = 0
        total_blocks = self.to_block - self.from_block + 1

        # Set initial metrics
        set_backfill_progress(self.from_block, self.to_block, self.from_block)

        # Pre-fetch all executed blocks to avoid per-block DB queries
        logger.info(
            "Pre-fetching executed blocks",
            from_block=self.from_block,
            to_block=self.to_block,
        )
        executed_blocks_cache = self._prefetch_executed_blocks()
        logger.info(
            "Pre-fetch complete",
            cache_entries=len(executed_blocks_cache),
        )

        try:
            for block_number in range(self.from_block, self.to_block + 1):
                if not self.is_running:
                    logger.info("BackfillScheduler stopping early", processed_count=processed_count)
                    break

                try:
                    with BlockProcessingTimer(mode="backfill"):
                        self._process_block(block_number, executed_blocks_cache)

                    processed_count += 1

                    # Update metrics
                    set_current_block("backfill", block_number)
                    set_backfill_progress(self.from_block, self.to_block, block_number)
                    increment_blocks_processed("backfill")

                    # Track block lag (distance from chain head)
                    if self._current_head_cache:
                        set_block_lag("backfill", self._current_head_cache - block_number)

                    # Log progress periodically
                    if processed_count % PROGRESS_LOG_INTERVAL == 0:
                        progress_pct = (processed_count / total_blocks) * 100
                        logger.info(
                            "Backfill progress",
                            processed=processed_count,
                            total=total_blocks,
                            progress_percent=f"{progress_pct:.1f}%",
                            current_block=block_number,
                        )

                    # Rate limiting between block submissions
                    if block_number < self.to_block and self.rate_limit > 0:
                        time.sleep(self.rate_limit)

                except KeyboardInterrupt:
                    raise
                except Exception:
                    logger.exception(
                        "Error processing block during backfill",
                        block_number=block_number,
                    )
                    # Continue with next block
                    time.sleep(self.rate_limit)

        except KeyboardInterrupt:
            logger.info(
                "BackfillScheduler interrupted",
                processed_count=processed_count,
                last_block=self.from_block + processed_count - 1 if processed_count > 0 else self.from_block,
            )
            self.stop()

        logger.info(
            "BackfillScheduler completed",
            processed_count=processed_count,
            total_blocks=total_blocks,
        )

    def _prefetch_executed_blocks(self) -> dict[tuple[str, str], set[int]]:
        """Pre-fetch all executed blocks for all registry items in the range."""
        cache: dict[tuple[str, str], set[int]] = {}

        for registry_item in self.block_processor.registry.get_functions():
            for args in registry_item.get_execution_args():
                args_json = serialize_args(args)
                cache_key = (registry_item.executable_path, args_json)

                cache[cache_key] = abd_dal.executed_block_numbers(
                    registry_item.executable_path,
                    args_json,
                    self.from_block,
                    self.to_block + 1,
                )

        return cache

    def _process_block(
        self,
        block_number: int,
        executed_blocks_cache: dict[tuple[str, str], set[int]],
    ) -> None:
        """Process a single block during backfill."""
        for registry_item in self.block_processor.registry.get_functions():
            try:
                self._process_registry_item_for_backfill(
                    registry_item,
                    block_number,
                    executed_blocks_cache,
                )
            except Exception:
                logger.exception(
                    "Error processing registry item during backfill",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                )

    def _requires_archive_network(self, block_number: int) -> bool:
        """Check if a block requires archive network based on age."""
        if self._current_head_cache is None:
            return False
        blocks_behind = self._current_head_cache - block_number
        return blocks_behind > ARCHIVE_BLOCK_THRESHOLD

    def _process_registry_item_for_backfill(
        self,
        registry_item: RegistryItem,
        block_number: int,
        executed_blocks_cache: dict[tuple[str, str], set[int]],
    ) -> None:
        """Process a registry item for backfill - only submits if not already executed."""
        for args in registry_item.get_execution_args():
            args_json = serialize_args(args)
            cache_key = (registry_item.executable_path, args_json)

            # Check if already executed using pre-fetched cache
            executed_blocks = executed_blocks_cache.get(cache_key, set())

            if block_number in executed_blocks:
                continue

            # Check condition and execute
            try:
                if registry_item.match_condition(block_number, **args):
                    use_archive = self._requires_archive_network(block_number)
                    if use_archive:
                        increment_archive_network_usage()
                    logger.debug(
                        "Backfilling block",
                        function_name=registry_item.function.__name__,
                        block_number=block_number,
                        args=args,
                        use_archive=use_archive,
                    )
                    self.block_processor.executor.execute(
                        registry_item,
                        block_number,
                        args,
                        use_archive=use_archive,
                    )
            except Exception:
                logger.exception(
                    "Error during backfill task execution",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                    args=args,
                )

    def stop(self) -> None:
        """Stop the backfill scheduler."""
        self.is_running = False
        logger.info("BackfillScheduler stopped")


def backfill_scheduler_factory(
    from_block: int,
    to_block: int,
    network: str = "finney",
    rate_limit: float = 1.0,
    dry_run: bool = False,
) -> BackfillScheduler:
    """
    Factory for BackfillScheduler.

    Args:
        from_block: Starting block number (inclusive).
        to_block: Ending block number (inclusive).
        network: Bittensor network name. Defaults to "finney".
        rate_limit: Seconds to sleep between blocks. Defaults to 1.0.
        dry_run: If True, preview without executing. Defaults to False.

    Returns:
        Configured BackfillScheduler instance.

    """
    return BackfillScheduler(
        block_processor=block_processor_factory(),
        network=network,
        from_block=from_block,
        to_block=to_block,
        rate_limit=rate_limit,
        dry_run=dry_run,
    )
