"""
Shared window-backfilling logic used by both the live and backfill schedulers.

Given a registry item and a block, decide whether to submit it (skipping blocks
already executed and blocks whose condition does not match) and choose the
archive-network flag based on the block's distance from the chain head.
"""

from typing import Any

import structlog

from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.executor import CeleryExecutor
from abstract_block_dumper._internal.services.metrics import increment_archive_network_usage

logger = structlog.get_logger(__name__)

# Blocks older than this threshold from the current head require the archive network.
ARCHIVE_BLOCK_THRESHOLD = 300


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

        Returns True if a task was submitted, False otherwise.
        """
        if block_number in executed_blocks:
            return False
        if not registry_item.match_condition(block_number, **args):
            return False

        use_archive = (head_block - block_number) > self.archive_threshold
        if use_archive:
            increment_archive_network_usage()

        self.executor.execute(registry_item, block_number, args, use_archive=use_archive)
        return True
