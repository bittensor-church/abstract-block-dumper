"""
Admission of backfill and lookback submissions against a queue's throttle.

The number of attempts in flight comes from the database, so it covers messages waiting in
the broker, messages prefetched by workers and attempts being executed, whatever the broker.
A gate reads a queue's free room once and spends it as messages go out, and reads it again
only when it is used up, so a large backfill costs one count query per batch, not per block.
"""

import time
from collections.abc import Callable
from datetime import timedelta

import structlog
from django.conf import settings
from django.utils import timezone

import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
from abstract_block_dumper._internal.exceptions import QueueConfigurationError
from abstract_block_dumper._internal.numeric import is_positive_number
from abstract_block_dumper._internal.services.metrics import record_throttled, set_inflight_tasks
from abstract_block_dumper._internal.services.queue_policy import Queue
from abstract_block_dumper._internal.services.recovery_settings import recovery_margin_seconds, stale_task_settings

logger = structlog.get_logger(__name__)

DEFAULT_THROTTLE_POLL_SECONDS = 1


class ThrottleGate:
    """
    Decides whether one more submission may go to its queue.

    With `wait`, `admit` blocks until the queue has room (the `backfill_blocks_v1` gate);
    without it, `admit` refuses and the caller leaves the block for later (the lookback gate).
    Queues without a throttle, and plain queue names, always pass.
    """

    def __init__(self, *, wait: bool, should_continue: Callable[[], bool] = lambda: True) -> None:
        self.wait = wait
        self.should_continue = should_continue
        self._free: dict[str, int] = {}

    def admit(self, queue: Queue | str | None) -> bool:
        if not isinstance(queue, Queue) or queue.throttle is None:
            return True
        if self._free.get(queue.name, 0) > 0:
            return True
        if queue.name in self._free and not self.wait:
            # Already full in this pass; counting again for every remaining block costs a query each.
            record_throttled(queue.name, "lookback")
            return False

        self._free[queue.name] = self._read_free(queue)
        if self._free[queue.name] > 0:
            return True
        if not self.wait:
            record_throttled(queue.name, "lookback")
            return False

        record_throttled(queue.name, "backfill")
        logger.info("Waiting for room in throttled queue", queue=queue.name, throttle=queue.throttle)
        while self._free[queue.name] <= 0:
            time.sleep(throttle_poll_seconds())
            if not self.should_continue():
                return False
            self._free[queue.name] = self._read_free(queue)
        return True

    def spent(self, queue: Queue | str | None) -> None:
        """Account for a message that was actually sent to `queue`."""
        if isinstance(queue, Queue) and queue.name in self._free:
            self._free[queue.name] -= 1

    @staticmethod
    def _read_free(queue: Queue) -> int:
        return max(0, (queue.throttle or 0) - read_inflight(queue))


def read_inflight(queue: Queue) -> int:
    """Attempts of `queue` in flight as its throttle counts them; also published as `block_dumper_inflight_tasks`."""
    stale_timeout = stale_task_settings()[0]
    ttl_window = queue.message_ttl + recovery_margin_seconds() if queue.message_ttl is not None else None
    stale_before = timezone.now() - timedelta(seconds=stale_timeout) if stale_timeout is not None else None
    inflight = queue_attempts.count_inflight(queue.name, ttl_window=ttl_window, stale_before=stale_before)
    set_inflight_tasks(queue.name, inflight)
    return inflight


def throttle_poll_seconds() -> float:
    """How often a backfill waiting on a full throttle checks for room; raises `QueueConfigurationError` if invalid."""
    seconds = getattr(settings, "BLOCK_DUMPER_THROTTLE_POLL_SECONDS", DEFAULT_THROTTLE_POLL_SECONDS)
    if not is_positive_number(seconds):
        msg = "BLOCK_DUMPER_THROTTLE_POLL_SECONDS must be a positive number"
        raise QueueConfigurationError(msg)
    return seconds
