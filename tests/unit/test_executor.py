from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from django.utils import timezone

from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.executor import CeleryExecutor
from abstract_block_dumper.models import TaskAttempt


def _make_item() -> RegistryItem:
    function = MagicMock()
    function.name = "tests.queue_routing_task"
    return RegistryItem(
        condition=lambda block_number: True,
        function=function,
        celery_kwargs={"queue": "live", "priority": 7},
    )


@pytest.mark.django_db
def test_queue_override_replaces_task_queue_without_losing_other_options():
    item = _make_item()

    CeleryExecutor().execute(item, 100, {"netuid": 3}, queue="block-backfill")

    item.function.apply_async.assert_called_once_with(
        kwargs={
            "block_number": 100,
            "_use_archive_network": False,
            "netuid": 3,
        },
        queue="block-backfill",
        priority=7,
    )
    attempt = TaskAttempt.objects.get()
    assert attempt.block_number == 100
    assert attempt.executable_path == "tests.queue_routing_task"
    assert attempt.args_dict == {"netuid": 3}
    assert attempt.celery_queue_override == "block-backfill"


@pytest.mark.django_db
def test_live_submission_keeps_task_queue_when_there_is_no_override():
    item = _make_item()

    CeleryExecutor().execute(item, 100, {})

    item.function.apply_async.assert_called_once_with(
        kwargs={
            "block_number": 100,
            "_use_archive_network": False,
        },
        queue="live",
        priority=7,
    )
    assert TaskAttempt.objects.get().celery_queue_override is None


@pytest.mark.django_db
def test_resubmitting_a_failed_attempt_on_another_queue_replaces_the_recorded_queue():
    """A block the live scheduler failed on can be picked up later by the backfiller."""
    item = _make_item()
    executor = CeleryExecutor()

    executor.execute(item, 100, {})
    attempt = TaskAttempt.objects.get()
    assert attempt.celery_queue_override is None

    attempt.status = TaskAttempt.Status.FAILED
    attempt.attempt_count = 1
    attempt.next_retry_at = timezone.now() - timedelta(minutes=5)
    attempt.save()

    executor.execute(item, 100, {}, queue="block-backfill")

    attempt.refresh_from_db()
    assert attempt.celery_queue_override == "block-backfill"


@pytest.mark.django_db
def test_skipped_resubmission_leaves_the_recorded_queue_alone():
    """An attempt that is not actually re-dispatched keeps the routing it was submitted with."""
    item = _make_item()
    executor = CeleryExecutor()

    executor.execute(item, 100, {}, queue="block-backfill")
    attempt = TaskAttempt.objects.get()
    attempt.status = TaskAttempt.Status.RUNNING
    attempt.save()

    executor.execute(item, 100, {})

    attempt.refresh_from_db()
    assert attempt.celery_queue_override == "block-backfill"
    assert item.function.apply_async.call_count == 1
