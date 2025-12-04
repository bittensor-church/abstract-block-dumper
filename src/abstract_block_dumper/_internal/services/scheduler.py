import time

import bittensor as bt
import structlog
from django.conf import settings

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.services.block_processor import BlockProcessor, block_processor_factory
from abstract_block_dumper._internal.services.metrics import (
    BlockProcessingTimer,
    increment_blocks_processed,
    set_block_lag,
    set_current_block,
    set_registered_tasks,
)

logger = structlog.get_logger(__name__)

# Blocks older than this threshold from current head require archive network
ARCHIVE_BLOCK_THRESHOLD = 300


class TaskScheduler:
    def __init__(
        self,
        block_processor: BlockProcessor,
        network: str,
        poll_interval: int,
        realtime_head_only: bool = False,
    ) -> None:
        self.block_processor = block_processor
        self.network = network
        self.poll_interval = poll_interval
        self.realtime_head_only = realtime_head_only
        self.last_processed_block = -1
        self.is_running = False
        self._subtensor: bt.Subtensor | None = None
        self._archive_subtensor: bt.Subtensor | None = None
        self._current_block_cache: int | None = None

    @property
    def subtensor(self) -> bt.Subtensor:
        """Get the regular subtensor connection, creating it if needed."""
        if self._subtensor is None:
            self._subtensor = abd_utils.get_bittensor_client(self.network)
        return self._subtensor

    @subtensor.setter
    def subtensor(self, value: bt.Subtensor | None) -> None:
        """Set or reset the subtensor connection."""
        self._subtensor = value

    @property
    def archive_subtensor(self) -> bt.Subtensor:
        """Get the archive subtensor connection, creating it if needed."""
        if self._archive_subtensor is None:
            self._archive_subtensor = abd_utils.get_bittensor_client("archive")
        return self._archive_subtensor

    @archive_subtensor.setter
    def archive_subtensor(self, value: bt.Subtensor | None) -> None:
        """Set or reset the archive subtensor connection."""
        self._archive_subtensor = value

    def get_subtensor_for_block(self, block_number: int) -> bt.Subtensor:
        """
        Get the appropriate subtensor for the given block number.

        Uses archive network for blocks older than ARCHIVE_BLOCK_THRESHOLD
        from the current head.
        """
        if self._current_block_cache is None:
            self._current_block_cache = self.subtensor.get_current_block()

        blocks_behind = self._current_block_cache - block_number

        if blocks_behind > ARCHIVE_BLOCK_THRESHOLD:
            logger.debug(
                "Using archive network for old block",
                block_number=block_number,
                blocks_behind=blocks_behind,
            )
            return self.archive_subtensor
        return self.subtensor

    def refresh_connections(self) -> None:
        """Reset all subtensor connections to force re-establishment."""
        self._subtensor = None
        self._archive_subtensor = None
        self._current_block_cache = None
        logger.info("Subtensor connections reset")

    def start(self) -> None:
        self.is_running = True

        self.initialize_last_block()

        registered_tasks_count = len(self.block_processor.registry.get_functions())
        set_registered_tasks(registered_tasks_count)

        logger.info(
            "TaskScheduler started",
            last_processed_block=self.last_processed_block,
            registry_functions=registered_tasks_count,
        )

        while self.is_running:
            try:
                if self._current_block_cache is not None:
                    self.subtensor = self.get_subtensor_for_block(self._current_block_cache)

                # Update current block cache for archive network decision
                self._current_block_cache = self.subtensor.get_current_block()
                current_block = self._current_block_cache

                if self.realtime_head_only:
                    # Only process the current head block, skip if already processed
                    if current_block != self.last_processed_block:
                        with BlockProcessingTimer(mode="realtime"):
                            self.block_processor.process_block(current_block)

                        set_current_block("realtime", current_block)
                        increment_blocks_processed("realtime")
                        set_block_lag("realtime", 0)  # Head-only mode has no lag
                        self.last_processed_block = current_block

                    time.sleep(self.poll_interval)
                else:
                    # Original behavior: process all blocks from last_processed to current
                    for block_number in range(self.last_processed_block + 1, current_block + 1):
                        with BlockProcessingTimer(mode="realtime"):
                            self.block_processor.process_block(block_number)

                        # Update metrics
                        set_current_block("realtime", block_number)
                        increment_blocks_processed("realtime")
                        set_block_lag("realtime", current_block - block_number)

                        time.sleep(self.poll_interval)
                        self.last_processed_block = block_number

            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.error("Fatal scheduler error", exc_info=True)
                # resume the loop even if task failed
                time.sleep(self.poll_interval)

    def stop(self) -> None:
        self.is_running = False
        logger.info("TaskScheduler stopped.")

    def initialize_last_block(self) -> None:
        # Safe getattr in case setting is not defined
        start_from_block_setting = getattr(settings, "BLOCK_DUMPER_START_FROM_BLOCK", None)

        if start_from_block_setting is not None:
            if start_from_block_setting == "current":
                self.last_processed_block = self.subtensor.get_current_block()
                logger.info("Starting from current blockchain block", block_number=self.last_processed_block)

            elif isinstance(start_from_block_setting, int):
                self.last_processed_block = start_from_block_setting
                logger.info("Starting from configured block", block_number=self.last_processed_block)
            else:
                error_msg = f"Invalid BLOCK_DUMPER_START_FROM_BLOCK value: {start_from_block_setting}"
                raise ValueError(error_msg)
        else:
            # Default behavior - resume from database
            last_block_number = abd_dal.get_the_latest_executed_block_number()

            self.last_processed_block = last_block_number or self.subtensor.get_current_block()
            logger.info(
                "Resume from the last database block or start from the current block",
                last_processed_block=self.last_processed_block,
            )


def task_scheduler_factory(network: str = "finney") -> TaskScheduler:
    """
    Factory for TaskScheduler.

    Args:
        network (str): Bittensor network name. Defaults to "finney"

    """
    return TaskScheduler(
        block_processor=block_processor_factory(),
        network=network,
        poll_interval=getattr(settings, "BLOCK_DUMPER_POLL_INTERVAL", 1),
        realtime_head_only=getattr(settings, "BLOCK_DUMPER_REALTIME_HEAD_ONLY", True),
    )
