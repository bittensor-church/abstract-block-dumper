from collections.abc import Collection, Iterator
from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db import transaction
from django.db.models import Max, Min
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

STAGED_RETRY_SETTINGS = (
    "BLOCK_TASK_FAST_RETRY_ATTEMPTS",
    "BLOCK_TASK_FAST_RETRY_DELAY_SECONDS",
    "BLOCK_TASK_RETRY_BACKOFF_START_SECONDS",
    "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER",
)


def _get_retry_delay_seconds(attempt_count: int) -> float:
    max_delay_seconds = getattr(settings, "BLOCK_TASK_MAX_RETRY_DELAY_MINUTES", MAX_RETRY_DELAY_MINUTES) * 60

    if not any(hasattr(settings, setting_name) for setting_name in STAGED_RETRY_SETTINGS):
        legacy_backoff_minutes = getattr(
            settings,
            "BLOCK_TASK_RETRY_BACKOFF",
            DEFAULT_BLOCK_TASK_RETRY_BACKOFF,
        )
        # B minutes with multiplier B reproduces the legacy B ** attempt_count
        # minutes formula exactly.
        fast_retry_attempts = 0
        fast_retry_delay_seconds = 0
        backoff_start_seconds = legacy_backoff_minutes * 60
        backoff_multiplier = legacy_backoff_minutes
    else:
        fast_retry_attempts = getattr(
            settings,
            "BLOCK_TASK_FAST_RETRY_ATTEMPTS",
            DEFAULT_BLOCK_TASK_FAST_RETRY_ATTEMPTS,
        )
        fast_retry_delay_seconds = getattr(
            settings,
            "BLOCK_TASK_FAST_RETRY_DELAY_SECONDS",
            DEFAULT_BLOCK_TASK_FAST_RETRY_DELAY_SECONDS,
        )
        backoff_start_seconds = getattr(
            settings,
            "BLOCK_TASK_RETRY_BACKOFF_START_SECONDS",
            DEFAULT_BLOCK_TASK_RETRY_BACKOFF_START_SECONDS,
        )
        backoff_multiplier = getattr(
            settings,
            "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER",
            DEFAULT_BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER,
        )

        if not isinstance(fast_retry_attempts, int) or isinstance(fast_retry_attempts, bool) or fast_retry_attempts < 0:
            msg = "BLOCK_TASK_FAST_RETRY_ATTEMPTS must be a non-negative integer"
            raise ValueError(msg)
        if fast_retry_delay_seconds < 0:
            msg = "BLOCK_TASK_FAST_RETRY_DELAY_SECONDS must be non-negative"
            raise ValueError(msg)
        if backoff_start_seconds < 0:
            msg = "BLOCK_TASK_RETRY_BACKOFF_START_SECONDS must be non-negative"
            raise ValueError(msg)
        if backoff_multiplier < 1:
            msg = "BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER must be at least 1"
            raise ValueError(msg)

    if attempt_count <= fast_retry_attempts:
        retry_delay_seconds = fast_retry_delay_seconds
    else:
        exponential_delay_seconds = backoff_start_seconds * (
            backoff_multiplier ** (attempt_count - fast_retry_attempts - 1)
        )
        retry_delay_seconds = max(fast_retry_delay_seconds, exponential_delay_seconds)

    return min(retry_delay_seconds, max_delay_seconds)


def get_ready_to_retry_attempts() -> Iterator[abd_models.TaskAttempt]:
    return (
        abd_models.TaskAttempt.objects.filter(
            next_retry_at__isnull=False,
            next_retry_at__lte=timezone.now(),
            attempt_count__lt=abd_utils.get_max_attempt_limit(),
        )
        .exclude(
            status=abd_models.TaskAttempt.Status.SUCCESS,
        )
        .iterator()
    )


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


def task_record_queue_override(task: abd_models.TaskAttempt, queue: str | None) -> None:
    """Record the queue override a submission used, so its retries can reuse it."""
    if task.celery_queue_override == queue:
        return
    task.celery_queue_override = queue
    task.save(update_fields=["celery_queue_override", "updated_at"])


def task_schedule_to_retry(task: abd_models.TaskAttempt, queue: str | None) -> None:
    """Mark a failed task as pending again, on the queue its retry is being sent to."""
    task.status = abd_models.TaskAttempt.Status.PENDING
    task.celery_queue_override = queue
    task.save()


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
