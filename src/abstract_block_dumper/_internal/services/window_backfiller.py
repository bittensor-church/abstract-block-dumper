"""
Shared window-backfilling logic used by both the live and backfill schedulers.

Given a registry item and a block, decide whether to submit it (skipping blocks
already executed and blocks whose condition does not match) and choose the
archive-network flag based on the block's distance from the chain head.
"""

from typing import Any

import structlog

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.archive_hint import ARCHIVE_BLOCK_THRESHOLD
from abstract_block_dumper._internal.services.executor import CeleryExecutor
from abstract_block_dumper._internal.services.metrics import increment_archive_network_usage
from abstract_block_dumper._internal.services.utils import serialize_args

logger = structlog.get_logger(__name__)


class WindowBackfiller:
    """Submit un-executed, condition-matching blocks for a registry item."""

    def __init__(self, executor: CeleryExecutor, archive_threshold: int = ARCHIVE_BLOCK_THRESHOLD) -> None:
        self.executor = executor
        self.archive_threshold = archive_threshold

    def submit_block(
        self,
        registry_item: RegistryItem,
        block_number: int,
        args: dict[str, Any],
        executed_blocks: set[int],
        head_block: int,
    ) -> bool:
        """
        Submit one (item, block, args) if it is un-executed and its condition matches.

        Blocks listed in ``executed_blocks`` are skipped. Callers decide what that set
        contains: ``process_item_range`` also folds in in-flight (PENDING/RUNNING) blocks
        to avoid re-dispatching them, whereas ``BackfillScheduler`` passes SUCCESS-only.

        Returns True if a task was submitted, False otherwise.
        """
        if block_number in executed_blocks:
            return False
        if not registry_item.match_condition(block_number, **args):
            return False

        use_archive = (head_block - block_number) > self.archive_threshold
        if use_archive:
            increment_archive_network_usage()

        execute_kwargs: dict[str, Any] = {"use_archive": use_archive}
        if registry_item.backfill_queue is not None:
            execute_kwargs["queue"] = registry_item.backfill_queue
        self.executor.execute(registry_item, block_number, args, **execute_kwargs)
        return True

    def process_item_range(
        self,
        registry_item: RegistryItem,
        from_block: int,
        to_block: int,
        head_block: int,
    ) -> int:
        """
        Process the inclusive range [from_block, to_block] for a registry item.

        Per args set, fetches the already-executed (SUCCESS) blocks and the
        in-flight (PENDING/RUNNING) blocks and skips both, submitting each
        remaining condition-matching block. Returns the number submitted.
        """
        submitted = 0
        for args in registry_item.get_execution_args():
            args_json = serialize_args(args)
            skip_blocks = abd_dal.executed_block_numbers(
                registry_item.executable_path,
                args_json,
                from_block,
                to_block + 1,
            )
            skip_blocks |= abd_dal.inflight_block_numbers(
                registry_item.executable_path,
                args_json,
                from_block,
                to_block + 1,
            )
            for block_number in range(from_block, to_block + 1):
                if self.submit_block(registry_item, block_number, args, skip_blocks, head_block):
                    submitted += 1
        return submitted
