from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from django.conf import settings
from django.db import OperationalError, transaction
from django.db.models import Max, Min, Q
from django.db.models.query import QuerySet
from django.utils import timezone

import abstract_block_dumper._internal.services.utils as abd_utils
import abstract_block_dumper.models as abd_models

DEFAULT_BLOCK_TASK_RETRY_BACKOFF = 1
DEFAULT_BLOCK_TASK_FAST_RETRY_ATTEMPTS = 2
DEFAULT_BLOCK_TASK_FAST_RETRY_DELAY_SECONDS = 5
DEFAULT_BLOCK_TASK_RETRY_BACKOFF_START_SECONDS = 60
DEFAULT_BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER = 2
MAX_RETRY_DELAY_MINUTES = 1440  # 24 hours max delay

# SQLSTATE PostgreSQL raises when `SELECT ... FOR UPDATE NOWAIT` finds the row locked.
PG_LOCK_NOT_AVAILABLE = "55P03"

STAGED_RETRY_SETTINGS = (
    "BLOCK_TASK_FAST_RETRY_ATTEMPTS",
    "BLOCK_TASK_FAST_RETRY_DELAY_SECONDS",
    "BLOCK_TASK_RETRY_BACKOFF_START_SECONDS",
    "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER",
)


@dataclass(frozen=True)
class _RetryPolicy:
    """Retry schedule: a few fast retries, then a nondecreasing exponential backoff."""

    fast_retry_attempts: int
    fast_retry_delay_seconds: float
    backoff_start_seconds: float
    backoff_multiplier: float

    def delay_seconds(self, attempt_count: int) -> float:
        if attempt_count <= self.fast_retry_attempts:
            return self.fast_retry_delay_seconds

        exponential_delay_seconds = self.backoff_start_seconds * (
            self.backoff_multiplier ** (attempt_count - self.fast_retry_attempts - 1)
        )
        return max(self.fast_retry_delay_seconds, exponential_delay_seconds)


def _legacy_retry_policy() -> _RetryPolicy:
    """
    Compatibility code for old backoff behavior. If any of the new parameters are missing, then
    fast retries are skipped and the legacy (backoff ** attempt_count) formula is used instead.
    """
    backoff_minutes = getattr(settings, "BLOCK_TASK_RETRY_BACKOFF", DEFAULT_BLOCK_TASK_RETRY_BACKOFF)
    return _RetryPolicy(
        fast_retry_attempts=0,
        fast_retry_delay_seconds=0,
        backoff_start_seconds=backoff_minutes * 60,
        backoff_multiplier=backoff_minutes,
    )


def _staged_retry_policy() -> _RetryPolicy:
    policy = _RetryPolicy(
        fast_retry_attempts=getattr(
            settings,
            "BLOCK_TASK_FAST_RETRY_ATTEMPTS",
            DEFAULT_BLOCK_TASK_FAST_RETRY_ATTEMPTS,
        ),
        fast_retry_delay_seconds=getattr(
            settings,
            "BLOCK_TASK_FAST_RETRY_DELAY_SECONDS",
            DEFAULT_BLOCK_TASK_FAST_RETRY_DELAY_SECONDS,
        ),
        backoff_start_seconds=getattr(
            settings,
            "BLOCK_TASK_RETRY_BACKOFF_START_SECONDS",
            DEFAULT_BLOCK_TASK_RETRY_BACKOFF_START_SECONDS,
        ),
        backoff_multiplier=getattr(
            settings,
            "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER",
            DEFAULT_BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER,
        ),
    )
    _validate_staged_retry_policy(policy)
    return policy


def _validate_staged_retry_policy(policy: _RetryPolicy) -> None:
    if (
        not isinstance(policy.fast_retry_attempts, int)
        or isinstance(policy.fast_retry_attempts, bool)
        or policy.fast_retry_attempts < 0
    ):
        msg = "BLOCK_TASK_FAST_RETRY_ATTEMPTS must be a non-negative integer"
        raise ValueError(msg)

    non_negative_settings = (
        ("BLOCK_TASK_FAST_RETRY_DELAY_SECONDS", policy.fast_retry_delay_seconds),
        ("BLOCK_TASK_RETRY_BACKOFF_START_SECONDS", policy.backoff_start_seconds),
    )
    for setting_name, value in non_negative_settings:
        if value < 0:
            msg = f"{setting_name} must be non-negative"
            raise ValueError(msg)

    if policy.backoff_multiplier < 1:
        msg = "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER must be at least 1"
        raise ValueError(msg)


def _get_retry_delay_seconds(attempt_count: int) -> float:
    uses_staged_settings = any(hasattr(settings, setting_name) for setting_name in STAGED_RETRY_SETTINGS)
    policy = _staged_retry_policy() if uses_staged_settings else _legacy_retry_policy()

    max_delay_seconds = getattr(settings, "BLOCK_TASK_MAX_RETRY_DELAY_MINUTES", MAX_RETRY_DELAY_MINUTES) * 60
    return min(policy.delay_seconds(attempt_count), max_delay_seconds)


def executed_block_numbers(executable_path: str, args_json: str, from_block: int, to_block: int) -> set[int]:
    block_numbers = (
        abd_models.TaskAttempt.objects.filter(
            executable_path=executable_path,
            args_json=args_json,
            block_number__gte=from_block,
            block_number__lt=to_block,
            status=abd_models.TaskAttempt.Status.SUCCESS,
        )
        .values_list("block_number", flat=True)
        .iterator()
    )
    return set(block_numbers)


def inflight_block_numbers(executable_path: str, args_json: str, from_block: int, to_block: int) -> set[int]:
    block_numbers = (
        abd_models.TaskAttempt.objects.filter(
            executable_path=executable_path,
            args_json=args_json,
            block_number__gte=from_block,
            block_number__lt=to_block,
            status__in=[abd_models.TaskAttempt.Status.PENDING, abd_models.TaskAttempt.Status.RUNNING],
        )
        .values_list("block_number", flat=True)
        .iterator()
    )
    return set(block_numbers)


def _stuck_task_filter(stale_before: datetime) -> Q:
    """
    Attempts the database says should run but nothing is carrying.

    PENDING/RUNNING: last dispatched (`updated_at`) before `stale_before`, and any retry
    ETA (`next_retry_at`) also before it, so a retry still waiting for its ETA is left
    alone. FAILED: the worker committed the failure but never enqueued the retry, which
    it normally does right after the commit; `next_retry_at` is the moment that was due.
    Exhausted attempts are never stuck.
    """
    status = abd_models.TaskAttempt.Status
    in_flight = Q(status__in=[status.PENDING, status.RUNNING], updated_at__lt=stale_before) & (
        Q(next_retry_at__isnull=True) | Q(next_retry_at__lt=stale_before)
    )
    orphaned_retry = Q(status=status.FAILED, next_retry_at__isnull=False, next_retry_at__lt=stale_before)
    return (in_flight | orphaned_retry) & Q(attempt_count__lt=abd_utils.get_max_attempt_limit())


def get_stuck_task_attempt_ids(stale_before: datetime, limit: int) -> list[int]:
    """Ids of stuck attempts (see `_stuck_task_filter`), oldest `updated_at` first, at most `limit`."""
    return list(
        abd_models.TaskAttempt.objects.filter(_stuck_task_filter(stale_before))
        .order_by("updated_at", "id")
        .values_list("id", flat=True)[:limit]
    )


def is_stuck(task: abd_models.TaskAttempt, stale_before: datetime) -> bool:
    """The `_stuck_task_filter` predicate evaluated on a loaded row, for re-checks under lock."""
    status = abd_models.TaskAttempt.Status
    if task.attempt_count >= abd_utils.get_max_attempt_limit():
        return False
    if task.status in (status.PENDING, status.RUNNING):
        return task.updated_at < stale_before and (task.next_retry_at is None or task.next_retry_at < stale_before)
    if task.status == status.FAILED:
        return task.next_retry_at is not None and task.next_retry_at < stale_before
    return False


def reset_to_pending(task: abd_models.TaskAttempt) -> None:
    task.celery_task_id = None
    task.status = abd_models.TaskAttempt.Status.PENDING
    task.save()


def revert_to_failed(task: abd_models.TaskAttempt) -> None:
    task.status = abd_models.TaskAttempt.Status.FAILED
    task.save()


def get_recent_phantom_tasks() -> QuerySet[abd_models.TaskAttempt]:
    """
    Get tasks marked as SUCCESS but never actually started.

    Only clean up recent phantom tasks to avoid deleting legitimate external successes
    """
    return abd_models.TaskAttempt.objects.filter(
        status=abd_models.TaskAttempt.Status.SUCCESS,
        last_attempted_at__isnull=True,
        celery_task_id__isnull=True,  # Additional safety check
        created_at__gte=timezone.now() - timedelta(hours=1),  # Only recent tasks
    )


def task_can_retry(task: abd_models.TaskAttempt) -> bool:
    blocked_statuses = {task.Status.SUCCESS, task.Status.RUNNING}
    return task.status not in blocked_statuses and task.attempt_count < abd_utils.get_max_attempt_limit()


def task_mark_as_started(task: abd_models.TaskAttempt, celery_task_id: str) -> None:
    task.celery_task_id = celery_task_id
    task.status = abd_models.TaskAttempt.Status.RUNNING
    task.last_attempted_at = timezone.now()
    task.save()


def task_mark_as_success(task: abd_models.TaskAttempt, result_data: dict) -> None:
    task.status = task.Status.SUCCESS
    task.execution_result = result_data
    task.last_attempted_at = timezone.now()
    task.next_retry_at = None
    task.save()


def task_mark_as_failed(task: abd_models.TaskAttempt) -> None:
    task.status = task.Status.FAILED
    task.last_attempted_at = timezone.now()
    task.attempt_count += 1

    if task_can_retry(task):
        retry_delay_seconds = _get_retry_delay_seconds(task.attempt_count)
        task.next_retry_at = timezone.now() + timedelta(seconds=retry_delay_seconds)
    else:
        task.next_retry_at = None
    task.save()


def task_record_dispatch(task: abd_models.TaskAttempt, queue: str | None) -> None:
    """
    Record that the task was just handed to Celery, and on which queue override.

    Always touches `updated_at`, even when the queue is unchanged: on a PENDING row it
    means "last dispatched at", which is what the stuck-task sweep measures staleness
    against. The queue is kept so retries can be re-dispatched to the same place.
    """
    task.celery_queue_override = queue
    task.save(update_fields=["celery_queue_override", "updated_at"])


def task_schedule_to_retry(task: abd_models.TaskAttempt, queue: str | None) -> None:
    """Mark a failed task as pending again, on the queue its retry is being sent to."""
    task.status = abd_models.TaskAttempt.Status.PENDING
    task.celery_queue_override = queue
    task.save()


def is_lock_not_available(exc: OperationalError) -> bool:
    """
    True when `exc` wraps PostgreSQL's lock_not_available error (SQLSTATE 55P03).

    Django re-raises the driver exception as `OperationalError` with the original as
    `__cause__`; psycopg 3 exposes the code as `sqlstate`, psycopg2 as `pgcode`. Any other
    `OperationalError` from a row lock - no code at all, or a connection-class code such as
    08006 - is a connectivity failure, not contention. Backends without SQLSTATE (SQLite)
    never report contention, which is right: they do not implement NOWAIT.
    """
    cause = exc.__cause__
    code = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
    return code == PG_LOCK_NOT_AVAILABLE


def task_create_or_get_pending(
    block_number: int,
    executable_path: str,
    args: dict[str, Any] | None = None,
) -> tuple[abd_models.TaskAttempt, bool]:
    """
    Create or get a pending task attempt.

    Returns (task, created) where created indicates if a new task was created.

    For failed tasks that can retry:
    - If next_retry_at is in the future, leave task as FAILED (will be picked up by scheduler)
    - If next_retry_at is in the past or None, reset to PENDING for immediate execution
    """
    if args is None:
        args = {}

    args_json = abd_utils.serialize_args(args)

    with transaction.atomic():
        task, created = abd_models.TaskAttempt.objects.get_or_create(
            block_number=block_number,
            executable_path=executable_path,
            args_json=args_json,
            defaults={"status": abd_models.TaskAttempt.Status.PENDING},
        )

        # Don't modify tasks that are already in a terminal or active state
        active_state = {abd_models.TaskAttempt.Status.SUCCESS, abd_models.TaskAttempt.Status.RUNNING}
        if created or task.status in active_state:
            return task, created

        # For failed tasks that can retry, only reset to PENDING if retry time has passed
        if task.status == abd_models.TaskAttempt.Status.FAILED and task_can_retry(task):
            now = timezone.now()
            if task.next_retry_at is None or task.next_retry_at <= now:
                task.status = abd_models.TaskAttempt.Status.PENDING
                task.save()
    return task, created


def get_the_latest_executed_block_number(executable_paths: Collection[str] | None = None) -> int | None:
    """
    Get the highest recorded block number, optionally restricted to specific tasks.

    Scoping matters when tasks follow different chain heads: the latest head always runs
    ahead of the finalized one, so an unscoped maximum would hand finalized tasks a cursor
    past blocks they have not processed yet.
    """
    attempts = abd_models.TaskAttempt.objects.all()
    if executable_paths is not None:
        attempts = attempts.filter(executable_path__in=executable_paths)
    result = attempts.aggregate(max_block=Max("block_number"))
    return result["max_block"]


def get_block_range() -> tuple[int | None, int | None]:
    """Get the min and max block numbers from all task attempts."""
    result = abd_models.TaskAttempt.objects.aggregate(
        min_block=Min("block_number"),
        max_block=Max("block_number"),
    )
    return result["min_block"], result["max_block"]


def get_successful_block_numbers(from_block: int, to_block: int) -> set[int]:
    """Get all block numbers with at least one successful task in the range."""
    block_numbers = (
        abd_models.TaskAttempt.objects.filter(
            block_number__gte=from_block,
            block_number__lte=to_block,
            status=abd_models.TaskAttempt.Status.SUCCESS,
        )
        .values_list("block_number", flat=True)
        .distinct()
        .iterator()
    )
    return set(block_numbers)
