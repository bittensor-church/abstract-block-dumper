import threading
from datetime import timedelta
from typing import Any
from unittest.mock import patch

import pytest
from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from tests.fatories import TaskAttemptFactory

PENDING = TaskAttempt.Status.PENDING
RUNNING = TaskAttempt.Status.RUNNING
SUCCESS = TaskAttempt.Status.SUCCESS
FAILED = TaskAttempt.Status.FAILED

STALE_AFTER = timedelta(minutes=30)
PATH = "tests.stuck.some_task"


def _cutoff():
    return timezone.now() - STALE_AFTER


def _attempt(
    *,
    status: str,
    updated_ago: timedelta,
    next_retry_in: timedelta | None = None,
    attempt_count: int = 0,
    executable_path: str = PATH,
    **extra: Any,
) -> TaskAttempt:
    """
    An attempt whose `updated_at` lies `updated_ago` in the past.

    `updated_at` is `auto_now`, so it must be set with a queryset update after creation.
    `next_retry_in` is relative to now; negative values are in the past.
    """
    next_retry_at = timezone.now() + next_retry_in if next_retry_in is not None else None
    attempt = TaskAttemptFactory(
        status=status,
        attempt_count=attempt_count,
        executable_path=executable_path,
        next_retry_at=next_retry_at,
        **extra,
    )
    TaskAttempt.objects.filter(id=attempt.id).update(updated_at=timezone.now() - updated_ago)
    attempt.refresh_from_db()
    return attempt


# (id, status, updated_ago, next_retry_in, attempt_count, expected_stuck)
PREDICATE_CASES = [
    ("stale pending", PENDING, timedelta(hours=1), None, 0, True),
    ("fresh pending", PENDING, timedelta(minutes=1), None, 0, False),
    ("pending retry waiting for its eta", PENDING, timedelta(hours=1), timedelta(hours=1), 1, False),
    ("pending retry whose eta passed long ago", PENDING, timedelta(hours=1), -timedelta(hours=1), 1, True),
    ("pending retry whose eta passed recently", PENDING, timedelta(hours=1), -timedelta(minutes=10), 1, False),
    ("stale running", RUNNING, timedelta(hours=1), None, 0, True),
    ("failed with retry never enqueued", FAILED, timedelta(hours=1), -timedelta(hours=1), 1, True),
    ("failed with retry due recently", FAILED, timedelta(hours=1), -timedelta(minutes=10), 1, False),
    ("failed with no retry scheduled", FAILED, timedelta(hours=1), None, 1, False),
    ("failed and exhausted", FAILED, timedelta(hours=1), -timedelta(hours=1), 3, False),
    ("success", SUCCESS, timedelta(hours=1), -timedelta(hours=1), 1, False),
    ("pending at the attempt limit", PENDING, timedelta(hours=1), None, 3, False),
]


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("status", "updated_ago", "next_retry_in", "attempt_count", "expected"),
    [case[1:] for case in PREDICATE_CASES],
    ids=[case[0] for case in PREDICATE_CASES],
)
def test_stuck_predicate(status, updated_ago, next_retry_in, attempt_count, expected):
    attempt = _attempt(
        status=status, updated_ago=updated_ago, next_retry_in=next_retry_in, attempt_count=attempt_count
    )
    cutoff = _cutoff()

    assert abd_dal.get_stuck_task_attempt_ids(cutoff, limit=10) == ([attempt.id] if expected else [])
    assert abd_dal.is_stuck(attempt, cutoff) is expected


@pytest.mark.django_db
def test_stuck_ids_come_oldest_first_and_respect_the_limit():
    newest = _attempt(status=PENDING, updated_ago=timedelta(hours=1))
    oldest = _attempt(status=PENDING, updated_ago=timedelta(hours=3))
    middle = _attempt(status=PENDING, updated_ago=timedelta(hours=2))

    assert abd_dal.get_stuck_task_attempt_ids(_cutoff(), limit=2) == [oldest.id, middle.id]
    assert abd_dal.get_stuck_task_attempt_ids(_cutoff(), limit=10) == [oldest.id, middle.id, newest.id]


def _some_task(block_number: int) -> int:
    return block_number


SOME_TASK_PATH = abd_utils.get_executable_path(_some_task)


def _sweep(*, head_block: int | None = None, limit: int = 10) -> int:
    return block_processor_factory().reconcile_stuck_tasks(
        stale_after_seconds=STALE_AFTER.total_seconds(), limit=limit, head_block=head_block
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("head_block", "expected_archive"),
    [(None, False), (1200, False), (1301, True)],
    ids=["head unknown", "head within archive threshold", "head beyond archive threshold"],
)
def test_stuck_pending_attempt_is_redispatched_on_its_recorded_queue(head_block, expected_archive):
    """The recorded queue wins over the task's backfill_queue; the archive hint follows the head."""
    block_task(backfill_queue="other-backfill")(_some_task)
    registry_item = task_registry.get_by_executable_path(SOME_TASK_PATH)
    assert registry_item is not None
    attempt = _attempt(
        status=PENDING,
        updated_ago=timedelta(hours=1),
        executable_path=SOME_TASK_PATH,
        block_number=1000,
        celery_queue_override="block-backfill",
    )

    with (
        patch.object(registry_item.function, "apply_async") as mock_apply_async,
        patch("abstract_block_dumper._internal.services.block_processor.record_task_recovered") as mock_record,
    ):
        recovered = _sweep(head_block=head_block)

    assert recovered == 1
    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 1000, "_use_archive_network": expected_archive},
        queue="block-backfill",
    )
    mock_record.assert_called_once_with("_some_task", PENDING)
    attempt.refresh_from_db()
    assert attempt.status == PENDING
    assert attempt.celery_task_id is None
    assert attempt.updated_at > _cutoff()


@pytest.mark.django_db
@pytest.mark.parametrize("status", [RUNNING, FAILED])
def test_previous_status_is_reported_for_running_and_failed_attempts(status):
    block_task(_some_task)
    registry_item = task_registry.get_by_executable_path(SOME_TASK_PATH)
    assert registry_item is not None
    attempt = _attempt(
        status=status,
        updated_ago=timedelta(hours=1),
        next_retry_in=-timedelta(hours=1),
        attempt_count=1,
        executable_path=SOME_TASK_PATH,
    )

    with (
        patch.object(registry_item.function, "apply_async") as mock_apply_async,
        patch("abstract_block_dumper._internal.services.block_processor.record_task_recovered") as mock_record,
    ):
        recovered = _sweep()

    assert recovered == 1
    mock_record.assert_called_once_with("_some_task", status)
    attempt.refresh_from_db()
    assert attempt.status == PENDING
    assert attempt.attempt_count == 1
    # A past ETA is passed through; Celery runs such a message immediately.
    assert mock_apply_async.call_args.kwargs["eta"] == attempt.next_retry_at


@pytest.mark.django_db
def test_attempt_for_an_unregistered_task_is_left_alone():
    attempt = _attempt(status=PENDING, updated_ago=timedelta(hours=1), executable_path="tests.stuck.unregistered")
    before = attempt.updated_at

    assert _sweep() == 0

    attempt.refresh_from_db()
    assert attempt.status == PENDING
    assert attempt.updated_at == before


@pytest.mark.django_db
def test_a_failing_dispatch_keeps_the_claim_and_does_not_stop_the_sweep():
    block_task(_some_task)
    registry_item = task_registry.get_by_executable_path(SOME_TASK_PATH)
    assert registry_item is not None
    first = _attempt(status=PENDING, updated_ago=timedelta(hours=3), executable_path=SOME_TASK_PATH)
    second = _attempt(status=PENDING, updated_ago=timedelta(hours=2), executable_path=SOME_TASK_PATH)

    with patch.object(registry_item.function, "apply_async", side_effect=[RuntimeError("broker down"), None]):
        recovered = _sweep()

    assert recovered == 1
    first.refresh_from_db()
    second.refresh_from_db()
    # The claim stands: the row is PENDING with a fresh updated_at, so it is retried one
    # timeout later instead of on every sweep.
    assert first.status == PENDING
    assert first.updated_at > _cutoff()
    assert second.updated_at > _cutoff()


@pytest.mark.skipif("sqlite" in settings.DATABASES["default"]["ENGINE"], reason="SQLite lacks row-level locking")
@pytest.mark.django_db(transaction=True)
def test_attempt_locked_by_an_executing_worker_is_left_alone():
    """A worker holds the row lock for the whole execution; skip_locked must honour it."""
    block_task(_some_task)
    registry_item = task_registry.get_by_executable_path(SOME_TASK_PATH)
    assert registry_item is not None
    attempt = _attempt(status=PENDING, updated_ago=timedelta(hours=1), executable_path=SOME_TASK_PATH)
    before = attempt.updated_at

    locked = threading.Event()
    release = threading.Event()

    def hold_row_lock() -> None:
        try:
            with transaction.atomic():
                TaskAttempt.objects.select_for_update().get(id=attempt.id)
                locked.set()
                release.wait(timeout=10)
        finally:
            connection.close()

    worker = threading.Thread(target=hold_row_lock)
    worker.start()
    assert locked.wait(timeout=10)
    try:
        with patch.object(registry_item.function, "apply_async") as mock_apply_async:
            recovered = _sweep()
    finally:
        release.set()
        worker.join(timeout=10)

    assert recovered == 0
    mock_apply_async.assert_not_called()
    attempt.refresh_from_db()
    assert attempt.status == PENDING
    assert attempt.updated_at == before
