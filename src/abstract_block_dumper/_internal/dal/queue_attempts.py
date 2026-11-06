"""
Reads of the attempts of `BlockDumper` tasks: which are in flight, lost, or may run again.

Attempts of `BlockDumper` tasks have leases and record the queue whose policy they follow
(`dispatch_queue`), so their recovery rules differ from those of `@block_task` attempts in
`django_dal`.
"""

from collections.abc import Collection
from datetime import datetime, timedelta

from django.db.models import Q
from django.utils import timezone

import abstract_block_dumper._internal.services.utils as abd_utils
import abstract_block_dumper.models as abd_models
from abstract_block_dumper._internal.dal.db_clock import db_now, db_now_offset


def expired_message_filter(ttl_windows: dict[str, float]) -> Q:
    """
    PENDING attempts whose message expired unexecuted, per queue.

    `ttl_windows` maps a queue name to its message TTL plus the recovery margin. The message
    was sent (`dispatched_at`, database clock) longer ago than that, and a retry's ETA
    (`next_retry_at`, the Python clock Celery compares it with) also lies that far back.
    """
    now = timezone.now()
    expired = Q(pk__in=[])
    for queue_name, window in ttl_windows.items():
        expired |= Q(dispatch_queue=queue_name, dispatched_at__lt=db_now_offset(-window)) & (
            Q(next_retry_at__isnull=True) | Q(next_retry_at__lt=now - timedelta(seconds=window))
        )
    return Q(status=abd_models.TaskAttempt.Status.PENDING) & expired


def expired_message_ids(ttl_windows: dict[str, float], limit: int, executable_paths: Collection[str]) -> list[int]:
    """Ids of attempts of these tasks matching `expired_message_filter`, oldest dispatch first, at most `limit`."""
    if not ttl_windows or limit <= 0:
        return []
    return list(
        abd_models.TaskAttempt.objects.filter(expired_message_filter(ttl_windows))
        .filter(executable_path__in=executable_paths)
        .order_by("dispatched_at", "id")
        .values_list("id", flat=True)[:limit]
    )


def count_inflight(queue_name: str, *, ttl_window: float | None, stale_before: datetime | None) -> int:
    """
    Attempts of `queue_name` that were sent and have not finished, nor been lost.

    RUNNING attempts whose lease has not passed or that have none (a task without a hard
    limit when there is no stuck-task timeout), and PENDING attempts that are not waiting
    for a future retry and whose message is neither expired (`ttl_window`, for queues with a
    message TTL) nor older than `stale_before` (None: no stuck-task timeout, so no age
    limit). A retry's message counts from its ETA, as `expired_message_filter` and
    `_stuck_filter` have it. Attempts known to be lost do not count, so they cannot hold the
    queue's budget until the sweep recovers them.
    """
    status = abd_models.TaskAttempt.Status
    now = timezone.now()
    running = Q(status=status.RUNNING) & (Q(lease_expires_at__isnull=True) | Q(lease_expires_at__gte=db_now()))
    pending = Q(status=status.PENDING) & (Q(next_retry_at__isnull=True) | Q(next_retry_at__lte=now))
    if ttl_window is not None:
        pending &= Q(dispatched_at__gte=db_now_offset(-ttl_window)) | Q(
            next_retry_at__gte=now - timedelta(seconds=ttl_window)
        )
    elif stale_before is not None:
        pending &= Q(updated_at__gte=stale_before) | Q(next_retry_at__gte=stale_before)
    return abd_models.TaskAttempt.objects.filter(Q(dispatch_queue=queue_name) & (running | pending)).count()


def _stuck_filter(stale_before: datetime, ttl_queues: Collection[str] = ()) -> Q:
    """
    Attempts the database says should run but nothing is carrying.

    PENDING/RUNNING: last dispatched (`updated_at`) before `stale_before`, and any retry
    ETA (`next_retry_at`) also before it, so a retry still waiting for its ETA is left
    alone. A RUNNING attempt with a lease is never stuck here: its lease says when its
    worker is gone (see `attempt_transitions.expired_lease_ids`). A PENDING attempt in one
    of `ttl_queues` sent with a recorded `dispatched_at` is left to `expired_message_filter`.
    FAILED: the worker committed the failure but never enqueued the retry, which it normally
    does right after the commit; `next_retry_at` is the moment that was due. Exhausted
    attempts are never stuck: an attempt under a queue policy only stays PENDING, or FAILED
    with a retry time, while its policy allows another try; one without a recorded policy
    stops at `BLOCK_DUMPER_MAX_ATTEMPTS`.
    """
    status = abd_models.TaskAttempt.Status
    in_flight = (
        (Q(status=status.PENDING) | Q(status=status.RUNNING, lease_expires_at__isnull=True))
        & Q(updated_at__lt=stale_before)
        & (Q(next_retry_at__isnull=True) | Q(next_retry_at__lt=stale_before))
    )
    in_flight &= ~Q(status=status.PENDING, dispatch_queue__in=list(ttl_queues), dispatched_at__isnull=False)
    orphaned_retry = Q(status=status.FAILED, next_retry_at__isnull=False, next_retry_at__lt=stale_before)
    within_attempts = Q(dispatch_queue__isnull=False) | Q(attempt_count__lt=abd_utils.get_max_attempt_limit())
    return (in_flight | orphaned_retry) & within_attempts


def stuck_attempt_ids(
    stale_before: datetime, limit: int, executable_paths: Collection[str], ttl_queues: Collection[str] = ()
) -> list[int]:
    """Ids of stuck attempts of these tasks (see `_stuck_filter`), oldest `updated_at` first, at most `limit`."""
    if limit <= 0:
        return []
    return list(
        abd_models.TaskAttempt.objects.filter(_stuck_filter(stale_before, ttl_queues))
        .filter(executable_path__in=executable_paths)
        .order_by("updated_at", "id")
        .values_list("id", flat=True)[:limit]
    )


def is_stuck(task: abd_models.TaskAttempt, stale_before: datetime, ttl_queues: Collection[str] = ()) -> bool:
    """The `_stuck_filter` predicate evaluated on a loaded row, for re-checks under lock."""
    status = abd_models.TaskAttempt.Status
    if task.dispatch_queue is None and task.attempt_count >= abd_utils.get_max_attempt_limit():
        return False
    if task.status == status.RUNNING and task.lease_expires_at is not None:
        return False
    if task.status == status.PENDING and task.dispatch_queue in ttl_queues and task.dispatched_at is not None:
        return False
    if task.status in (status.PENDING, status.RUNNING):
        return task.updated_at < stale_before and (task.next_retry_at is None or task.next_retry_at < stale_before)
    if task.status == status.FAILED:
        return task.next_retry_at is not None and task.next_retry_at < stale_before
    return False


def can_run_again(task: abd_models.TaskAttempt) -> bool:
    """Whether the attempt may run again; under a queue policy, failures already record when it may not."""
    if task.status in {task.Status.SUCCESS, task.Status.RUNNING} or task.give_up_reason is not None:
        return False
    if task.dispatch_queue is not None:
        return True
    return task.attempt_count < abd_utils.get_max_attempt_limit()
