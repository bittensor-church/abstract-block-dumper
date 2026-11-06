"""
Status transitions of a TaskAttempt that several processes may race for.

Each transition is one conditional UPDATE whose WHERE clause states the status it starts
from, so of two processes racing for the same row exactly one wins, and the loser learns
it from the returned bool instead of overwriting the winner. `updated_at` is set explicitly
because `QuerySet.update()` skips `auto_now`.
"""

from collections.abc import Collection
from datetime import timedelta
from typing import Any

from django.db import transaction
from django.db.models import Case, CharField, DateTimeField, F, Q, Value, When
from django.db.models.functions import Coalesce
from django.utils import timezone

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.db_clock import db_now, db_now_offset
from abstract_block_dumper._internal.dal.queue_attempts import can_run_again, expired_message_filter
from abstract_block_dumper.models import TaskAttempt

PENDING = TaskAttempt.Status.PENDING
RUNNING = TaskAttempt.Status.RUNNING
SUCCESS = TaskAttempt.Status.SUCCESS
FAILED = TaskAttempt.Status.FAILED
BEFORE_GIVE_UP = Q(give_up_at__isnull=True) | Q(give_up_at__gt=db_now())


def create_or_reopen(block_number: int, executable_path: str, args: dict[str, Any]) -> tuple[TaskAttempt, bool]:
    """
    Get the attempt for this block and arguments, creating it PENDING if it does not exist.

    Returns (attempt, created). A FAILED attempt that may run again goes back to PENDING once
    its retry time has passed, so the caller can send it now; before that it stays FAILED for
    the retry already on its way. An attempt that was given up stays FAILED.
    """
    with transaction.atomic():
        task, created = TaskAttempt.objects.get_or_create(
            block_number=block_number,
            executable_path=executable_path,
            args_json=abd_utils.serialize_args(args),
            defaults={"status": PENDING},
        )
        if created or task.status != FAILED or not can_run_again(task):
            return task, created

        now = timezone.now()
        if task.next_retry_at is None or task.next_retry_at <= now:
            reopened = TaskAttempt.objects.filter(id=task.id, status=FAILED, give_up_reason__isnull=True).update(
                status=PENDING, updated_at=now
            )
            if reopened:
                task.status = PENDING
    return task, created


def record_dispatch(task: TaskAttempt, queue: str | None, *, dispatch_queue: str | None) -> None:
    """
    Record that the attempt was just handed to Celery, and where.

    Always touches `updated_at`, even when the queue is unchanged: on a PENDING row it
    means "last dispatched at", which is what the stuck-task sweep measures staleness
    against. `queue` is kept so retries can be re-dispatched to the same place;
    `dispatch_queue` names the BlockDumper queue whose policy the attempt follows, and
    `dispatched_at` (database clock) when its message was sent.
    """
    now = timezone.now()
    TaskAttempt.objects.filter(id=task.id).update(
        celery_queue_override=queue, dispatch_queue=dispatch_queue, dispatched_at=db_now(), updated_at=now
    )
    task.celery_queue_override = queue
    task.dispatch_queue = dispatch_queue
    task.updated_at = now


def claim_expired_message(task_id: int, ttl_windows: dict[str, float]) -> bool:
    """
    Take an attempt with an expired message for re-dispatch, if it still has one.

    Stamps `dispatched_at`, so if the re-dispatch fails the attempt is picked up again one
    TTL later rather than on every sweep.
    """
    updated = TaskAttempt.objects.filter(Q(id=task_id) & expired_message_filter(ttl_windows)).update(
        celery_task_id=None, dispatched_at=db_now(), updated_at=timezone.now()
    )
    return updated == 1


def find(block_number: int, executable_path: str, args_json: str) -> TaskAttempt | None:
    return TaskAttempt.objects.filter(
        block_number=block_number,
        executable_path=executable_path,
        args_json=args_json,
    ).first()


def claim(task_id: int, *, claim_token: str, lease_seconds: float | None) -> bool:
    """
    Take a PENDING attempt for execution and count it as an attempt.

    `claim_token` identifies the claimant in later fenced writes. A `lease_seconds` of None
    leaves the claim without a deadline; it is then never recovered as expired. An attempt
    past its give-up deadline is not claimed (see `give_up_if_past_deadline`).
    """
    updated = TaskAttempt.objects.filter(BEFORE_GIVE_UP, id=task_id, status=PENDING).update(
        status=RUNNING,
        attempt_count=F("attempt_count") + 1,
        celery_task_id=claim_token,
        last_attempted_at=db_now(),
        lease_expires_at=db_now_offset(lease_seconds) if lease_seconds is not None else None,
        updated_at=timezone.now(),
    )
    return updated == 1


def finish_success(task_id: int, *, claim_token: str, result: Any) -> bool:
    """Record a successful run, provided the claim is still held by `claim_token`."""
    updated = TaskAttempt.objects.filter(id=task_id, status=RUNNING, celery_task_id=claim_token).update(
        status=SUCCESS,
        execution_result=result,
        next_retry_at=None,
        lease_expires_at=None,
        updated_at=timezone.now(),
    )
    return updated == 1


def _failure_fields(retry_delay: float | None) -> dict[str, Any]:
    """
    Fields of an attempt that just failed: its retry time, or why it is given up.

    No retry when no tries remain (`retry_delay` None) or when the retry would start after the
    attempt's give-up deadline; that comparison runs on the database clock the deadline is on.
    `next_retry_at` itself is on the Python clock, which Celery compares an ETA with.
    """
    now = timezone.now()
    if retry_delay is None:
        return {"next_retry_at": None, "give_up_reason": TaskAttempt.GiveUpReason.ATTEMPTS, "updated_at": now}
    too_late = Q(give_up_at__isnull=False, give_up_at__lt=db_now_offset(retry_delay))
    return {
        "next_retry_at": Case(
            When(too_late, then=Value(None)),
            default=Value(now + timedelta(seconds=retry_delay)),
            output_field=DateTimeField(),
        ),
        "give_up_reason": Case(
            When(too_late, then=Value(TaskAttempt.GiveUpReason.BLOCK_AGE)),
            default=Value(None),
            output_field=CharField(),
        ),
        "updated_at": now,
    }


def finish_failure(task_id: int, *, claim_token: str, retry_delay: float | None) -> bool:
    """Record a failed run, provided the claim is still held by `claim_token`."""
    updated = TaskAttempt.objects.filter(id=task_id, status=RUNNING, celery_task_id=claim_token).update(
        status=FAILED,
        lease_expires_at=None,
        **_failure_fields(retry_delay),
    )
    return updated == 1


def expired_lease_ids(limit: int, executable_paths: Collection[str]) -> list[int]:
    """Ids of RUNNING attempts of these tasks whose lease has passed, oldest lease first, at most `limit`."""
    return list(
        TaskAttempt.objects.filter(status=RUNNING, lease_expires_at__lt=db_now(), executable_path__in=executable_paths)
        .order_by("lease_expires_at", "id")
        .values_list("id", flat=True)[:limit]
    )


def fail_expired_lease(task_id: int, *, retry_delay: float | None) -> bool:
    """Mark a RUNNING attempt whose lease has passed as FAILED; the claim already counted the attempt."""
    updated = TaskAttempt.objects.filter(id=task_id, status=RUNNING, lease_expires_at__lt=db_now()).update(
        status=FAILED,
        lease_expires_at=None,
        **_failure_fields(retry_delay),
    )
    return updated == 1


def set_give_up_deadline(task_id: int, seconds_from_now: float) -> None:
    """Record the attempt's give-up deadline, unless an earlier send already did."""
    TaskAttempt.objects.filter(id=task_id).update(give_up_at=Coalesce(F("give_up_at"), db_now_offset(seconds_from_now)))


def give_up_if_past_deadline(task_id: int) -> bool:
    """Give up a PENDING or FAILED attempt whose give-up deadline has passed."""
    updated = TaskAttempt.objects.filter(
        id=task_id, status__in=[PENDING, FAILED], give_up_at__lte=db_now(), give_up_reason__isnull=True
    ).update(
        status=FAILED,
        next_retry_at=None,
        give_up_reason=TaskAttempt.GiveUpReason.BLOCK_AGE,
        updated_at=timezone.now(),
    )
    return updated == 1


def could_still_run(attempts: Any) -> Any:
    """The attempts of the `attempts` queryset that may still run: PENDING or FAILED, and not given up."""
    return attempts.filter(status__in=[PENDING, FAILED], give_up_reason__isnull=True)


def give_up_by_hand(attempts: Any) -> int:
    """Give up every attempt of the `attempts` queryset that `could_still_run`; returns how many."""
    return could_still_run(attempts).update(
        status=FAILED,
        next_retry_at=None,
        give_up_reason=TaskAttempt.GiveUpReason.MANUAL,
        updated_at=timezone.now(),
    )


def requeue_for_retry(task_id: int, *, queue: str) -> bool:
    """
    Move a FAILED attempt that was not given up back to PENDING ahead of sending its retry.

    `queue` names the BlockDumper queue the retry goes to and whose policy the attempt follows
    from then on.
    """
    updated = TaskAttempt.objects.filter(id=task_id, status=FAILED, give_up_reason__isnull=True).update(
        status=PENDING,
        celery_queue_override=queue,
        dispatch_queue=queue,
        dispatched_at=db_now(),
        updated_at=timezone.now(),
    )
    return updated == 1
