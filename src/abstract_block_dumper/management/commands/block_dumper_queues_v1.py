from django.core.management.base import BaseCommand, CommandError

import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
from abstract_block_dumper._internal.contacts.broker import QueueStats
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.exceptions import UnsupportedBrokerError
from abstract_block_dumper._internal.numeric import format_number
from abstract_block_dumper._internal.services.queue_inventory import BACKFILL, LIVE, QueueUsage, queue_usages
from abstract_block_dumper._internal.services.queue_monitor import read_queue_stats

COLUMNS = ("QUEUE", "ROLE", "MESSAGE_TTL", "SOFT/HARD", "THROTTLE", "IN_FLIGHT", "WAITING", "CONSUMERS")


class Command(BaseCommand):
    help = "List the BlockDumper queues with their policy and broker state."

    def handle(self, *args, **options) -> None:
        ensure_modules_loaded()
        usages = queue_usages()

        if not usages:
            self.stdout.write("No BlockDumper queues are defined.")
            return

        try:
            stats = read_queue_stats(usages)
        except UnsupportedBrokerError as exc:
            raise CommandError(str(exc)) from exc
        rows = [COLUMNS, *(self._row(usage, stats[usage.queue.name]) for usage in usages)]
        widths = [max(len(row[column]) for row in rows) for column in range(len(COLUMNS))]
        for row in rows:
            self.stdout.write("  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip())

    @staticmethod
    def _row(usage: QueueUsage, stats: QueueStats) -> tuple[str, ...]:
        queue = usage.queue
        inflight = queue_attempts.count_inflight(queue.name, ttl_window=None, stale_before=None)
        limits = f"{_number(queue.soft_time_limit)}/{_number(queue.time_limit)}"
        consumers = str(stats.consumers)
        if stats.consumers == 0 and inflight:
            consumers += "  no consumers"
        return (
            queue.name,
            "+".join(role for role in (LIVE, BACKFILL) if role in usage.roles) or "-",
            "-" if queue.message_ttl is None else f"{_number(queue.message_ttl)} s",
            "-/-" if limits == "-/-" else f"{limits} s",
            _number(queue.throttle),
            str(inflight),
            str(stats.messages),
            consumers,
        )


def _number(value: float | None) -> str:
    return "-" if value is None else format_number(value)
