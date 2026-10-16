from datetime import timedelta
from typing import Any

import pytest
from django.utils import timezone

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper.models import TaskAttempt
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
