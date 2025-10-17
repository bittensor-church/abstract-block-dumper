from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db import transaction
from django.db.models.query import QuerySet
from django.utils import timezone

import abstract_block_dumper.models as abd_models
import abstract_block_dumper.services.utils as abd_utils


def get_ready_to_retry_attempts() -> QuerySet[abd_models.TaskAttempt]:
    return abd_models.TaskAttempt.objects.filter(
        next_retry_at__isnull=False,
        next_retry_at__lte=timezone.now(),
        attempt_count__lt=abd_utils.get_max_attempt_limit(),
    ).exclude(
        status=abd_models.TaskAttempt.Status.SUCCESS,
    )


def executed_block_numbers(executable_path: str, args_json: str, from_block: int, to_block: int) -> set[int]:
    block_numbers = abd_models.TaskAttempt.objects.filter(
        executable_path=executable_path,
        args_json=args_json,
        block_number__gte=from_block,
        block_number__lt=to_block,
        status=abd_models.TaskAttempt.Status.SUCCESS,
    ).values_list("block_number", flat=True)
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


def task_mark_as_failed(task) -> None:
    DEFAULT_BLOCK_TASK_RETRY_BACKOFF = 1
    MAX_RETRY_DELAY_MINUTES = 1440  # 24 hours max delay

    task.status = task.Status.FAILED
    task.last_attempted_at = timezone.now()
    task.attempt_count += 1

    if task_can_retry(task):
        base_retry_backoff = getattr(settings, "BLOCK_TASK_RETRY_BACKOFF", DEFAULT_BLOCK_TASK_RETRY_BACKOFF)
        max_delay_minutes = getattr(settings, "BLOCK_TASK_MAX_RETRY_DELAY_MINUTES", MAX_RETRY_DELAY_MINUTES)

        # Calculate exponential backoff with bounds checking
        backoff_minutes = base_retry_backoff**task.attempt_count
        backoff_minutes = min(backoff_minutes, max_delay_minutes)

        task.next_retry_at = timezone.now() + timedelta(minutes=backoff_minutes)
    else:
        task.next_retry_at = None
    task.save()


def task_schedule_to_retry(task):
    task.status = abd_models.TaskAttempt.Status.PENDING
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


def get_the_latest_executed_block_number() -> int | None:
    qs = abd_models.TaskAttempt.objects.order_by("-block_number").first()
    if qs:
        return qs.block_number
    return None
