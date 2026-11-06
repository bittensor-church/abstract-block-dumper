"""The retry policy of a BlockDumper queue."""

from dataclasses import dataclass

from abstract_block_dumper._internal.numeric import is_non_negative_integer, is_non_negative_number, is_positive_integer


@dataclass(frozen=True)
class RetryPolicy:
    """
    How the attempts of a `Queue` are retried, and when they are given up.

    An attempt is given up at the first limit it reaches; with both limits None it is retried
    until it succeeds or is stopped with `manage.py block_dumper_give_up_v1`.

    Attributes:
        max_attempts: Most attempts, the first execution included. None means no limit.
        give_up_after_blocks: Give up once the block is about this many blocks behind the
            chain head. Approximate: the library turns it into a deadline in time when the
            attempt is first sent, assuming one block every `BlockDumper.block_time_seconds`,
            so a slower or stalled chain reaches the deadline after fewer blocks, a faster
            one after more. None means no limit.
        fast_retry_attempts: Retries that wait `fast_retry_delay` before the exponential phase.
        fast_retry_delay: Seconds before each fast retry.
        backoff_start: Seconds before the first exponential retry.
        backoff_multiplier: Growth of each further exponential delay; at least 1.
        max_delay: Cap on any delay, in seconds. On RabbitMQ keep it below the broker's
            `consumer_timeout` (30 minutes by default): a retry waits for its ETA inside the
            worker, unacknowledged.

    """

    max_attempts: int | None = 3
    give_up_after_blocks: int | None = None
    fast_retry_attempts: int = 2
    fast_retry_delay: float = 5
    backoff_start: float = 60
    backoff_multiplier: float = 2
    max_delay: float = 1200

    def __post_init__(self) -> None:
        for field_name in ("max_attempts", "give_up_after_blocks"):
            value = getattr(self, field_name)
            if value is not None and not is_positive_integer(value):
                msg = f"RetryPolicy.{field_name} must be a positive integer or None"
                raise ValueError(msg)
        if not is_non_negative_integer(self.fast_retry_attempts):
            msg = "RetryPolicy.fast_retry_attempts must be a non-negative integer"
            raise ValueError(msg)
        for field_name in ("fast_retry_delay", "backoff_start", "max_delay"):
            if not is_non_negative_number(getattr(self, field_name)):
                msg = f"RetryPolicy.{field_name} must be a non-negative number"
                raise ValueError(msg)
        if not is_non_negative_number(self.backoff_multiplier) or self.backoff_multiplier < 1:
            msg = "RetryPolicy.backoff_multiplier must be at least 1"
            raise ValueError(msg)

    def allows_retry(self, attempt_count: int) -> bool:
        return self.max_attempts is None or attempt_count < self.max_attempts

    def delay_seconds(self, attempt_count: int) -> float:
        """Seconds before retrying an attempt that failed on try number `attempt_count`."""
        if attempt_count <= self.fast_retry_attempts:
            delay = self.fast_retry_delay
        else:
            delay = max(self.fast_retry_delay, self._exponential_delay(attempt_count))
        return min(delay, self.max_delay)

    def _exponential_delay(self, attempt_count: int) -> float:
        try:
            return self.backoff_start * self.backoff_multiplier ** (attempt_count - self.fast_retry_attempts - 1)
        except OverflowError:
            # Past what a float holds, so past any `max_delay`; an attempt without a limit gets this far.
            return self.max_delay if self.backoff_start else 0
