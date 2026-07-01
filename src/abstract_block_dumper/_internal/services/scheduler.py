import time
from typing import Protocol, cast

import structlog
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
from abstract_block_dumper._internal.services.window_backfiller import WindowBackfiller

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
        window_backfiller: WindowBackfiller,
        lookback_enabled: bool = True,
    ) -> None:
        self.block_processor = block_processor
        self.poll_interval = poll_interval
        self.bittensor_client = bittensor_client
        self.last_processed_block = state_resolver.get_starting_block()
        self.is_running = False
        self.window_backfiller = window_backfiller
        self.lookback_enabled = lookback_enabled

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

                    self._fill_lookback(current_block)

                    set_current_block("realtime", current_block)
                    increment_blocks_processed("realtime")
                    set_block_lag("realtime", 0)  # Head-only mode has no lag
                    self.last_processed_block = current_block

                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.exception("Error in TaskScheduler loop")
                time.sleep(self.poll_interval)

    def _fill_lookback(self, head: int) -> None:
        """
        Backfill the trailing lookback window for tasks that declare one.

        For each registry item with backfilling_lookback=N, submit un-executed,
        condition-matching blocks in [head-N, head-1]. Bounded by N per item.
        """
        if not self.lookback_enabled:
            return

        for registry_item in self.block_processor.registry.get_functions():
            if not registry_item.requires_backfilling():
                continue

            # requires_backfilling() guarantees backfilling_lookback is not None.
            lookback = cast(int, registry_item.backfilling_lookback)
            from_block = max(0, head - lookback)
            to_block = head - 1
            if to_block < from_block:
                continue

            try:
                submitted = self.window_backfiller.process_item_range(
                    registry_item, from_block, to_block, head_block=head
                )
                if submitted:
                    logger.info(
                        "Lookback fill submitted tasks",
                        function_name=registry_item.function.__name__,
                        from_block=from_block,
                        to_block=to_block,
                        submitted=submitted,
                    )
            except Exception:
                logger.exception(
                    "Lookback fill failed",
                    function_name=registry_item.function.__name__,
                    head=head,
                )

    def stop(self) -> None:
        self.is_running = False
        self.bittensor_client.close()
        logger.info("TaskScheduler stopped.")


def task_scheduler_factory(network: str | None = None) -> TaskScheduler:
    """
    Factory for TaskScheduler.

    Args:
        network (str | None): Bittensor network name. If None, it is read from the
            ``BITTENSOR_NETWORK`` Django setting, falling back to "finney".

    """
    if network is None:
        network = getattr(settings, "BITTENSOR_NETWORK", "finney")
    bittensor_client = BittensorConnectionClient(network=network)
    state_resolver = DefaultBlockStateResolver(bittensor_client=bittensor_client)
    block_processor = block_processor_factory()
    return TaskScheduler(
        block_processor=block_processor,
        poll_interval=getattr(settings, "BLOCK_DUMPER_POLL_INTERVAL", 5),
        bittensor_client=bittensor_client,
        state_resolver=state_resolver,
        window_backfiller=WindowBackfiller(block_processor.executor),
        lookback_enabled=getattr(settings, "BLOCK_DUMPER_LOOKBACK_ENABLED", True),
    )
