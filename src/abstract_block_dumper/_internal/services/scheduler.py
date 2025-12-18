import time
from typing import Protocol

import structlog
from django import db
from django.conf import settings

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient
from abstract_block_dumper._internal.services.block_processor import BaseBlockProcessor, block_processor_factory
from abstract_block_dumper._internal.services.metrics import (
    BlockProcessingTimer,
    increment_blocks_processed,
    set_block_lag,
    set_current_block,
    set_registered_tasks,
)

# Refresh bittensor connections every N blocks to prevent memory leaks from internal caches
CONNECTION_REFRESH_INTERVAL = 1000

logger = structlog.get_logger(__name__)


class BlockStateResolver(Protocol):
    """Protocol defining the interface for block state resolvers."""

    def get_starting_block(self) -> int:
        """Determine which block to start processing from."""
        ...


class DefaultBlockStateResolver:
    """Default implementation that reads from settings and database."""

    def __init__(self, bittensor_client: BittensorConnectionClient) -> None:
        self.bittensor_client = bittensor_client

    def get_starting_block(self) -> int:
        start_setting = getattr(settings, "BLOCK_DUMPER_START_FROM_BLOCK", None)
        if start_setting == "current":
            return self.bittensor_client.subtensor.get_current_block()
        if isinstance(start_setting, int):
            return start_setting
        # Default: resume from DB or current
        return abd_dal.get_the_latest_executed_block_number() or self.bittensor_client.subtensor.get_current_block()


class TaskScheduler:
    def __init__(
        self,
        block_processor: BaseBlockProcessor,
        bittensor_client: BittensorConnectionClient,
        state_resolver: BlockStateResolver,
        poll_interval: int,
    ) -> None:
        self.block_processor = block_processor
        self.poll_interval = poll_interval
        self.bittensor_client = bittensor_client
        self.last_processed_block = state_resolver.get_starting_block()
        self.is_running = False
        self._blocks_since_refresh = 0

    def start(self) -> None:
        self.is_running = True

        registered_tasks_count = len(self.block_processor.registry.get_functions())
        set_registered_tasks(registered_tasks_count)

        logger.info(
            "TaskScheduler started",
            last_processed_block=self.last_processed_block,
            registry_functions=registered_tasks_count,
        )

        while self.is_running:
            try:
                current_block = self.bittensor_client.subtensor.get_current_block()

                # Only process the current head block, skip if already processed
                if current_block != self.last_processed_block:
                    with BlockProcessingTimer(mode="realtime"):
                        self.block_processor.process_block(current_block)

                    set_current_block("realtime", current_block)
                    increment_blocks_processed("realtime")
                    set_block_lag("realtime", 0)  # Head-only mode has no lag
                    self.last_processed_block = current_block
                    self._blocks_since_refresh += 1

                    # Periodic memory cleanup
                    if self._blocks_since_refresh >= CONNECTION_REFRESH_INTERVAL:
                        self._perform_cleanup()

                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.exception("Error in TaskScheduler loop")
                time.sleep(self.poll_interval)

    def stop(self) -> None:
        self.is_running = False
        logger.info("TaskScheduler stopped.")

    def _perform_cleanup(self) -> None:
        """Perform periodic memory cleanup to prevent leaks in long-running processes."""
        # Reset bittensor connections to clear internal caches
        self.bittensor_client.refresh_connections()

        # Clear Django's query log (only accumulates if DEBUG=True)
        db.reset_queries()

        self._blocks_since_refresh = 0
        logger.debug("Memory cleanup performed", blocks_processed=CONNECTION_REFRESH_INTERVAL)


def task_scheduler_factory(network: str = "finney") -> TaskScheduler:
    """
    Factory for TaskScheduler.

    Args:
        network (str): Bittensor network name. Defaults to "finney"

    """
    bittensor_client = BittensorConnectionClient(network=network)
    state_resolver = DefaultBlockStateResolver(bittensor_client=bittensor_client)
    return TaskScheduler(
        block_processor=block_processor_factory(),
        poll_interval=getattr(settings, "BLOCK_DUMPER_POLL_INTERVAL", 5),
        bittensor_client=bittensor_client,
        state_resolver=state_resolver,
    )
