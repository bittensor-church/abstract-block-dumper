import time
from typing import Protocol, cast

import structlog
from django.conf import settings

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.providers.bittensor_client import (
    DEFAULT_RPC_TIMEOUT_SECONDS,
    BittensorConnectionClient,
)
from abstract_block_dumper._internal.services.block_processor import BaseBlockProcessor, block_processor_factory
from abstract_block_dumper._internal.services.block_source import BlockSource
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

    def get_starting_block(self, block_source: BlockSource, registry_items: list[RegistryItem]) -> int:
        """Determine which block to start processing from for one chain head."""
        ...


class DefaultBlockStateResolver:
    """Default implementation that reads from settings and database."""

    def __init__(self, bittensor_client: BittensorConnectionClient) -> None:
        self.bittensor_client = bittensor_client

    def get_starting_block(self, block_source: BlockSource, registry_items: list[RegistryItem]) -> int:
        start_setting = getattr(settings, "BLOCK_DUMPER_START_FROM_BLOCK", None)
        if start_setting == "current":
            return block_source.get_block(self.bittensor_client)
        if isinstance(start_setting, int):
            return start_setting

        # Default: resume from this head's own attempts, or from this head's current block.
        # Both reads stay inside one chain head: the latest head runs ahead of the finalized
        # one, so a shared starting point would put finalized tasks past blocks that are
        # already produced but not yet finalized, and they would never be processed.
        executable_paths = [registry_item.executable_path for registry_item in registry_items]
        latest_executed = abd_dal.get_the_latest_executed_block_number(executable_paths=executable_paths)
        return latest_executed or block_source.get_block(self.bittensor_client)


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
        self.state_resolver = state_resolver
        self.last_processed_blocks: dict[BlockSource, int] = {}
        self.observed_blocks: dict[BlockSource, int] = {}
        self.is_running = False
        self.window_backfiller = window_backfiller
        self.lookback_enabled = lookback_enabled

    def start(self) -> None:
        self.is_running = True

        registry_functions = self.block_processor.registry.get_functions()
        registered_tasks_count = len(registry_functions)
        task_groups = self._group_by_block_source(registry_functions)
        self.last_processed_blocks = {
            block_source: self.state_resolver.get_starting_block(block_source, registry_items)
            for block_source, registry_items in task_groups.items()
        }
        set_registered_tasks(registered_tasks_count)

        logger.info(
            "TaskScheduler started",
            last_processed_blocks={
                block_source.name: block_number for block_source, block_number in self.last_processed_blocks.items()
            },
            registry_functions=registered_tasks_count,
        )

        while self.is_running:
            try:
                for block_source, registry_items in task_groups.items():
                    self._poll_block_source(block_source, registry_items)

                self._report_block_lag()
                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.exception("Error in TaskScheduler loop")
                time.sleep(self.poll_interval)

    def _poll_block_source(self, block_source: BlockSource, registry_items: list[RegistryItem]) -> None:
        """
        Advance one chain head by a single poll.

        Failures stay inside the head that caused them: a head whose read or processing
        raises loses only its own turn, so an unhealthy one (eg. a stalled finalized RPC)
        cannot starve the heads that are still healthy.
        """
        try:
            current_block = block_source.get_block(self.bittensor_client)
            self.observed_blocks[block_source] = current_block

            if current_block <= self.last_processed_blocks[block_source]:
                return

            with BlockProcessingTimer(mode="realtime", source=block_source.name):
                self.block_processor.process_block(current_block, registry_items=registry_items)

            self._fill_lookback(current_block, registry_items=registry_items)

            set_current_block("realtime", current_block, source=block_source.name)
            increment_blocks_processed("realtime", source=block_source.name)
            self.last_processed_blocks[block_source] = current_block
        except Exception:
            logger.exception("Error polling block source", block_source=block_source.name)

    def _report_block_lag(self) -> None:
        """
        Publish how far behind the chain each head has fallen.

        Every head reads the same chain, so the highest block seen this iteration is the
        best available estimate of the tip. A head that stops advancing - because
        finality lags, its reads keep failing, or its blocks fail to process - keeps its
        last processed block and shows up as growing lag.
        """
        if not self.observed_blocks:
            return

        chain_head = max(self.observed_blocks.values())
        for block_source, last_processed_block in self.last_processed_blocks.items():
            set_block_lag("realtime", max(0, chain_head - last_processed_block), source=block_source.name)

    @staticmethod
    def _group_by_block_source(registry_items: list[RegistryItem]) -> dict[BlockSource, list[RegistryItem]]:
        task_groups: dict[BlockSource, list[RegistryItem]] = {}
        for registry_item in registry_items:
            task_groups.setdefault(registry_item.block_source, []).append(registry_item)
        return task_groups

    def _fill_lookback(
        self,
        head: int,
        *,
        registry_items: list[RegistryItem] | None = None,
    ) -> None:
        """
        Backfill the trailing lookback window for tasks that declare one.

        For each registry item with backfilling_lookback=N, submit un-executed,
        condition-matching blocks in [head-N, head-1]. Bounded by N per item.
        """
        if not self.lookback_enabled:
            return

        items = registry_items if registry_items is not None else self.block_processor.registry.get_functions()
        for registry_item in items:
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
    bittensor_client = BittensorConnectionClient(
        network=network,
        rpc_timeout=getattr(settings, "BLOCK_DUMPER_RPC_TIMEOUT", DEFAULT_RPC_TIMEOUT_SECONDS),
    )
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
