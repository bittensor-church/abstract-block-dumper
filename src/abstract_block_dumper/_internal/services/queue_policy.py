"""Queues with a policy: the Celery queue a block task is sent to, and how its messages run."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from abstract_block_dumper._internal.numeric import is_positive_integer, is_positive_number
from abstract_block_dumper._internal.services.retry_policy import RetryPolicy


@dataclass(frozen=True)
class Queue:
    """
    A Celery queue for block tasks, with the policy every message sent to it follows.

    Pass it to `BlockDumper.block_task` as `live_queue`, `backfill_queue`, or both. Its
    settings override the same keys in the task's `celery_kwargs`.

    Attributes:
        name: Celery queue name. Leading and trailing whitespace is stripped.
        soft_time_limit: Seconds after which Celery raises `SoftTimeLimitExceeded` in the
            task; the attempt is then recorded as failed and retried. Without a `time_limit`,
            the default hard limit of the task stays above it.
        time_limit: Seconds after which Celery kills the task, in place of its default hard
            limit. Also sets how soon a killed attempt is recovered: its lease is this limit
            plus `BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS`.
        message_ttl: Seconds a message may wait before it is discarded unexecuted, counted
            from its send time or, for a retry, from its ETA. RabbitMQ drops it from the
            queue, and the worker drops one it already prefetched. An attempt whose message
            expired is sent again once the TTL and the margin have passed, without using up
            an attempt.
        throttle: Maximum number of attempts in flight (sent and not finished) on this queue
            at which backfill and lookback stop submitting. `backfill_blocks_v1` waits for room;
            lookback leaves the rest for the next head. Live submissions never wait, but
            count, so on a queue shared with live work backfills get only what live leaves.
            Retries and the sweep's re-sends never wait either, and expired or lost attempts
            stop counting, so once workers come back the attempts in flight can briefly
            exceed the throttle.
        retry: How failed attempts are retried and when they are given up. Shared by all
            tasks and roles of the queue: on a queue serving live and backfill work,
            `give_up_after_blocks` also gives up backfills of older blocks without running them.

    """

    name: str
    soft_time_limit: float | None = None
    time_limit: float | None = None
    message_ttl: float | None = None
    throttle: int | None = None
    retry: RetryPolicy = RetryPolicy()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            msg = "Queue name must be a non-empty string"
            raise ValueError(msg)
        object.__setattr__(self, "name", self.name.strip())

        for field_name in ("soft_time_limit", "time_limit", "message_ttl"):
            value = getattr(self, field_name)
            if value is not None and not is_positive_number(value):
                msg = f"Queue {self.name!r}: {field_name} must be a positive number or None"
                raise ValueError(msg)
        if self.throttle is not None and not is_positive_integer(self.throttle):
            msg = f"Queue {self.name!r}: throttle must be a positive integer or None"
            raise ValueError(msg)
        if not isinstance(self.retry, RetryPolicy):
            msg = f"Queue {self.name!r}: retry must be a RetryPolicy"
            raise ValueError(msg)
        if self.soft_time_limit is not None and self.time_limit is not None and self.soft_time_limit >= self.time_limit:
            msg = f"Queue {self.name!r}: soft_time_limit must be lower than time_limit"
            raise ValueError(msg)


def message_options(queue: Queue) -> dict[str, Any]:
    """The `apply_async` options a message sent to `queue` carries."""
    options: dict[str, Any] = {"queue": queue.name}
    if queue.soft_time_limit is not None:
        options["soft_time_limit"] = queue.soft_time_limit
    if queue.time_limit is not None:
        options["time_limit"] = queue.time_limit
    return options


def message_expiry(queue: Queue, eta: datetime | None, now: datetime) -> datetime | None:
    """
    When a message sent to `queue` at `now` stops being worth executing.

    Counted from the ETA for a retry that is not due yet, so it does not expire while it
    waits for its own start time.
    """
    if queue.message_ttl is None:
        return None
    start = max(now, eta) if eta is not None else now
    return start + timedelta(seconds=queue.message_ttl)
