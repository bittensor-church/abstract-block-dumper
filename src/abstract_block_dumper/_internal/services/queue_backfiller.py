"""
Backfill and lookback submissions of `BlockDumper` tasks.

Like `WindowBackfiller` for `@block_task` tasks, it skips blocks already executed and blocks
whose condition does not match, and chooses the archive-network flag from the block's
distance to the chain head. On top of that it holds submissions back while the target
queue's throttle is full, and counts only messages actually sent.
"""

from collections.abc import Callable
from typing import Any

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.archive_network import ARCHIVE_BLOCK_THRESHOLD
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.throttle import ThrottleGate
from abstract_block_dumper._internal.services.utils import serialize_args


class QueueBackfiller:
    """Submit un-executed, condition-matching blocks of a `BlockDumper` task to its backfill queue."""

    def __init__(self, executor: QueueExecutor, archive_threshold: int = ARCHIVE_BLOCK_THRESHOLD) -> None:
        self.executor = executor
        self.archive_threshold = archive_threshold

    def submit_block(
        self,
        registry_item: RegistryItem,
        block_number: int,
        args: dict[str, Any],
        executed_blocks: set[int],
        head_block: int | Callable[[], int],
        gate: ThrottleGate | None = None,
    ) -> bool:
        """
        Submit one (item, block, args) if it is un-executed and its condition matches.

        ``executed_blocks`` is the set of blocks to skip; ``process_item_range`` also folds
        in in-flight (PENDING/RUNNING) blocks, whereas ``BackfillScheduler`` passes
        SUCCESS-only.

        A `gate` holds the submission back while the target queue's throttle is full. Pass
        `head_block` as a callable when the gate can wait, so the block is measured against
        the head at the time it is sent rather than before the wait.

        Returns whether a message was sent.
        """
        if block_number in executed_blocks:
            return False
        if not registry_item.match_condition(block_number, **args):
            return False

        target = registry_item.backfill_queue if registry_item.backfill_queue is not None else registry_item.live_queue
        if gate is not None and not gate.admit(target):
            return False

        head = head_block() if callable(head_block) else head_block
        use_archive = (head - block_number) > self.archive_threshold

        execute_kwargs: dict[str, Any] = {"use_archive": use_archive, "head_block": head}
        if registry_item.backfill_queue is not None:
            execute_kwargs["queue"] = registry_item.backfill_queue
        if not self.executor.execute(registry_item, block_number, args, **execute_kwargs):
            return False
        if gate is not None:
            gate.spent(target)
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

        Per args set, fetches the already-executed (SUCCESS) blocks and the in-flight
        (PENDING/RUNNING) blocks and skips both, submitting each remaining
        condition-matching block. Blocks the target queue's throttle has no room for are
        left for a later call. Returns the number of messages sent.
        """
        gate = ThrottleGate(wait=False)
        submitted = 0
        for args in registry_item.get_execution_args():
            args_json = serialize_args(args)
            skip_blocks = abd_dal.executed_block_numbers(
                registry_item.executable_path, args_json, from_block, to_block + 1
            )
            skip_blocks |= abd_dal.inflight_block_numbers(
                registry_item.executable_path, args_json, from_block, to_block + 1
            )
            for block_number in range(from_block, to_block + 1):
                if self.submit_block(registry_item, block_number, args, skip_blocks, head_block, gate=gate):
                    submitted += 1
        return submitted
