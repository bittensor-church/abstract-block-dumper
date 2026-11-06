"""Settings that decide when an unfinished task attempt counts as lost and is recovered."""

from __future__ import annotations

from django.conf import settings

from abstract_block_dumper._internal.numeric import is_non_negative_number, is_positive_integer, is_positive_number

# Stuck-task sweep defaults; see BLOCK_DUMPER_STALE_TASK_* in the README.
DEFAULT_STALE_TASK_TIMEOUT_SECONDS = 1800
DEFAULT_STALE_TASK_SWEEP_INTERVAL_SECONDS = 60
DEFAULT_STALE_TASK_SWEEP_BATCH_SIZE = 200
DEFAULT_RECOVERY_MARGIN_SECONDS = 10


def stale_task_settings() -> tuple[float | None, float, int]:
    """Read and validate the stuck-task sweep settings."""
    timeout = getattr(settings, "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS", DEFAULT_STALE_TASK_TIMEOUT_SECONDS)
    interval = getattr(
        settings, "BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS", DEFAULT_STALE_TASK_SWEEP_INTERVAL_SECONDS
    )
    batch_size = getattr(settings, "BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE", DEFAULT_STALE_TASK_SWEEP_BATCH_SIZE)

    if timeout is not None and not is_positive_number(timeout):
        msg = "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS must be a positive number or None"
        raise ValueError(msg)
    if not is_positive_number(interval):
        msg = "BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS must be a positive number"
        raise ValueError(msg)
    if not is_positive_integer(batch_size):
        msg = "BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE must be a positive integer"
        raise ValueError(msg)
    return timeout, interval, batch_size


def recovery_margin_seconds() -> float:
    margin = getattr(settings, "BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS", DEFAULT_RECOVERY_MARGIN_SECONDS)
    if not is_non_negative_number(margin):
        msg = "BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS must be a non-negative number"
        raise ValueError(msg)
    return margin


def check_recovery_settings() -> None:
    """
    Raise `ValueError` when a `BLOCK_DUMPER_STALE_TASK_*` or `BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS` setting is invalid.

    Workers read them on every claim and the sweep on every run, so a `BlockDumper` task checks
    them when it is declared, whether or not it gets the default hard limit.
    """
    stale_task_settings()
    recovery_margin_seconds()


def lease_seconds(time_limit: float | None) -> float | None:
    """
    How long a claimed attempt may stay RUNNING before it counts as lost.

    With a hard time limit, which the prefork pool enforces by killing the task, the task is
    gone once the limit and the margin have passed. Without one, as after
    `celery_kwargs={"time_limit": None}`, there is no such guarantee, so the lease falls back
    to the stuck-task timeout; None (no stuck-task timeout) means the claim never expires.
    """
    if time_limit:
        return time_limit + recovery_margin_seconds()
    return stale_task_settings()[0]
