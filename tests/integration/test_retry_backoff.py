from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from django.test import override_settings
from django.utils import timezone
from freezegun import freeze_time

from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task


def legacy_backoff_failing_task(block_number: int) -> None:
    raise RuntimeError(f"Failure for block {block_number}")


def staged_backoff_failing_task(block_number: int) -> None:
    raise RuntimeError(f"Failure for block {block_number}")


def assert_retry_delays(celery_task, original_func, expected_delays: list[timedelta]) -> None:
    frozen_now = datetime(2026, 1, 1, tzinfo=UTC)
    executable_path = f"{original_func.__module__}.{original_func.__name__}"

    with freeze_time(frozen_now):
        retry_started_at = timezone.now()
        for retry_number, expected_delay in enumerate(expected_delays, start=1):
            block_number = 1000 + retry_number
            task_attempt = TaskAttempt.objects.create(
                block_number=block_number,
                executable_path=executable_path,
                status=TaskAttempt.Status.PENDING,
                attempt_count=retry_number - 1,
            )

            with patch.object(celery_task, "apply_async"):
                celery_task(block_number)

            task_attempt.refresh_from_db()
            assert task_attempt.attempt_count == retry_number
            assert task_attempt.next_retry_at == retry_started_at + expected_delay


@pytest.mark.django_db
@override_settings(BLOCK_DUMPER_MAX_ATTEMPTS=10, BLOCK_TASK_RETRY_BACKOFF=2)
def test_legacy_backoff_setting_keeps_existing_delays() -> None:
    celery_task = block_task(legacy_backoff_failing_task)

    assert_retry_delays(
        celery_task,
        legacy_backoff_failing_task,
        [timedelta(minutes=2), timedelta(minutes=4), timedelta(minutes=8)],
    )


@pytest.mark.django_db
@override_settings(
    BLOCK_DUMPER_MAX_ATTEMPTS=10,
    BLOCK_TASK_RETRY_BACKOFF=17,
    BLOCK_TASK_FAST_RETRY_ATTEMPTS=2,
    BLOCK_TASK_FAST_RETRY_DELAY_SECONDS=5,
    BLOCK_TASK_RETRY_BACKOFF_START_SECONDS=60,
    BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER=2,
)
def test_staged_backoff_uses_fast_retries_then_exponential_growth() -> None:
    celery_task = block_task(staged_backoff_failing_task)

    assert_retry_delays(
        celery_task,
        staged_backoff_failing_task,
        [
            timedelta(seconds=5),
            timedelta(seconds=5),
            timedelta(minutes=1),
            timedelta(minutes=2),
            timedelta(minutes=4),
        ],
    )
