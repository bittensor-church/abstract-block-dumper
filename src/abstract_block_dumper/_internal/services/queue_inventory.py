"""Which BlockDumper queues exist, and in which roles their tasks use them."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry

if TYPE_CHECKING:
    from abstract_block_dumper._internal.services.block_dumper import BlockDumper
    from abstract_block_dumper._internal.services.queue_policy import Queue

LIVE = "live"
BACKFILL = "backfill"


@dataclass(frozen=True)
class QueueUsage:
    """
    How the tasks use one queue.

    Attributes:
        queue: The queue.
        dumper: The BlockDumper the queue belongs to.
        roles: What is submitted to the queue. A task without a `backfill_queue` also sends
            its backfills to its `live_queue`, which therefore has both roles.
        named_as_backfill_queue: Whether a task names the queue as its `backfill_queue`.
        tasks: The tasks that send to the queue, in registration order.

    """

    queue: Queue
    dumper: BlockDumper
    roles: frozenset[str]
    named_as_backfill_queue: bool
    tasks: tuple[RegistryItem, ...]


def queue_usages() -> list[QueueUsage]:
    """Every queue of every BlockDumper, with the roles (`live`, `backfill`) tasks give it."""
    usages = []
    for dumper in dumper_registry.dumpers():
        for queue in dumper.queues:
            roles = set()
            named_as_backfill_queue = False
            tasks = []
            for item in task_registry.get_functions():
                sends_live = item.live_queue is not None and item.live_queue.name == queue.name
                sends_backfill = getattr(item.backfill_queue, "name", None) == queue.name
                if sends_live:
                    roles.add(LIVE)
                    if item.backfill_queue is None:
                        roles.add(BACKFILL)
                if sends_backfill:
                    roles.add(BACKFILL)
                    named_as_backfill_queue = True
                if sends_live or sends_backfill:
                    tasks.append(item)
            usages.append(
                QueueUsage(
                    queue=queue,
                    dumper=dumper,
                    roles=frozenset(roles),
                    named_as_backfill_queue=named_as_backfill_queue,
                    tasks=tuple(tasks),
                )
            )
    return usages
