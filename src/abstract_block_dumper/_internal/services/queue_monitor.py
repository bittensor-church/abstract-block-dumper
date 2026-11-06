"""Periodic look at the broker side of BlockDumper queues, from the live scheduler."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
from abstract_block_dumper._internal.contacts.broker import broker_contact_factory
from abstract_block_dumper._internal.services.metrics import set_queue_consumers, set_queue_messages
from abstract_block_dumper._internal.services.queue_inventory import queue_usages
from abstract_block_dumper._internal.services.throttle import read_inflight

if TYPE_CHECKING:
    from abstract_block_dumper._internal.contacts.broker import QueueStats
    from abstract_block_dumper._internal.services.queue_inventory import QueueUsage

logger = structlog.get_logger(__name__)


def read_queue_stats(usages: list[QueueUsage]) -> dict[str, QueueStats]:
    """The broker state of the usages' queues by queue name, read over one connection per Celery app."""
    stats: dict[str, QueueStats] = {}
    for dumper in dict.fromkeys(usage.dumper for usage in usages):
        names = [usage.queue.name for usage in usages if usage.dumper is dumper]
        stats |= broker_contact_factory(dumper.celery_app).queue_stats(names)
    return stats


class QueueMonitor:
    """
    Publishes each queue's waiting messages and consumers, and warns about queues nobody reads.

    Throttled queues also get their in-flight count published, so the gauge stays current when
    no backfill reads it. A queue with attempts in flight and no consumer means no worker was
    started with it in `-Q`: its tasks wait until their message expires or the stuck-task
    timeout passes. Each check reads the queues of a Celery app over one broker connection.
    """

    def check(self) -> None:
        usages = queue_usages()
        for usage in usages:
            if usage.queue.throttle is not None:
                read_inflight(usage.queue)
        queue_stats = read_queue_stats(usages)
        for usage in usages:
            name = usage.queue.name
            stats = queue_stats[name]
            set_queue_messages(name, stats.messages)
            set_queue_consumers(name, stats.consumers)
            if stats.consumers == 0:
                inflight = queue_attempts.count_inflight(name, ttl_window=None, stale_before=None)
                if inflight:
                    logger.warning(
                        "Block task queue has no consumers",
                        queue=name,
                        waiting_messages=stats.messages,
                        inflight_attempts=inflight,
                    )
