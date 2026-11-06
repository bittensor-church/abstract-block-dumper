"""Which retry rules apply to an attempt: its queue's `RetryPolicy`, or the Django settings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import structlog

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.services.metrics import record_task_given_up

if TYPE_CHECKING:
    from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class _Retries(Protocol):
    def allows_retry(self, attempt_count: int) -> bool: ...

    def delay_seconds(self, attempt_count: int) -> float: ...


class SettingsRetries:
    """Retries of attempts without a queue policy, from the `BLOCK_DUMPER_*`/`BLOCK_TASK_*` settings."""

    def allows_retry(self, attempt_count: int) -> bool:
        return attempt_count < abd_utils.get_max_attempt_limit()

    def delay_seconds(self, attempt_count: int) -> float:
        return abd_dal._get_retry_delay_seconds(attempt_count)  # noqa: SLF001


def retries_for(task_attempt: TaskAttempt) -> _Retries:
    """The retry rules of the queue `task_attempt` was sent under, or the settings-based ones."""
    queue = dumper_registry.get_queue(task_attempt.dispatch_queue)
    return queue.retry if queue is not None else SettingsRetries()


def retry_delay(task_attempt: TaskAttempt) -> float | None:
    """Seconds until the retry of `task_attempt`, which just failed; None when no tries remain."""
    retries = retries_for(task_attempt)
    if not retries.allows_retry(task_attempt.attempt_count):
        return None
    return retries.delay_seconds(task_attempt.attempt_count)


def report_given_up(task_attempt: TaskAttempt) -> None:
    """Log and count an attempt that was just given up; `task_attempt` must be freshly read."""
    reason = task_attempt.give_up_reason or "unknown"
    record_task_given_up(task_attempt.executable_path.split(".")[-1], reason)
    logger.warning(
        "Task attempt given up",
        task_id=task_attempt.id,
        block_number=task_attempt.block_number,
        executable_path=task_attempt.executable_path,
        attempt_count=task_attempt.attempt_count,
        reason=reason,
    )
