from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import structlog
from celery.signals import task_revoked
from django.utils import timezone
from freezegun import freeze_time

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.db_clock import db_now, db_now_offset
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.celery_signals import connect_worker_signals
from abstract_block_dumper._internal.services.reconciler import queue_reconciler_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.dumper import BlockDumper, Queue
from tests.attempt_records import attempt_record
from tests.fatories import TaskAttemptFactory

TTL_QUEUE = Queue("events-live", time_limit=120, message_ttl=300)
NOW = datetime(2026, 1, 1, tzinfo=UTC)
PENDING = TaskAttempt.Status.PENDING
UNTOUCHED_PENDING = {
    "status": PENDING,
    "attempt_count": 0,
    "celery_task_id": None,
    "celery_queue_override": None,
    "execution_result": None,
    "next_retry_at": None,
    "lease": None,
}


def index_events(block_number: int) -> int:
    return block_number


@pytest.fixture
def events_task(celery_test_app):
    return BlockDumper(celery_app=celery_test_app).block_task(live_queue=TTL_QUEUE)(index_events)


@pytest.fixture
def events_item(events_task):
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(index_events))
    assert registry_item is not None
    return registry_item


@pytest.mark.django_db
@freeze_time(NOW)
@pytest.mark.parametrize(
    ("next_retry_in", "expected_eta", "expected_expires"),
    [
        (None, None, NOW + timedelta(seconds=300)),
        (timedelta(minutes=2), NOW + timedelta(minutes=2), NOW + timedelta(minutes=7)),
        (-timedelta(hours=1), NOW - timedelta(hours=1), NOW + timedelta(seconds=300)),
    ],
    ids=["fresh submission", "retry waiting for its eta", "retry whose eta passed"],
)
def test_message_expires_one_ttl_after_it_can_first_run(
    events_task, events_item, next_retry_in, expected_eta, expected_expires
):
    if next_retry_in is not None:
        TaskAttemptFactory(
            block_number=100,
            executable_path=events_item.executable_path,
            status=TaskAttempt.Status.FAILED,
            attempt_count=1,
            next_retry_at=NOW + next_retry_in,
        )
        TaskAttempt.objects.update(status=PENDING)

    with patch.object(events_task, "apply_async") as mock_apply_async:
        block_processor_factory().process_block(100, registry_items=[events_item])

    expected_call = {
        "kwargs": {"block_number": 100, "_use_archive_network": False},
        "queue": "events-live",
        "time_limit": 120,
        "expires": expected_expires,
    }
    if expected_eta is not None:
        expected_call["eta"] = expected_eta
    mock_apply_async.assert_called_once_with(**expected_call)


@pytest.mark.django_db
def test_dispatch_time_is_recorded_on_the_database_clock(events_task, events_item):
    with patch.object(events_task, "apply_async"):
        block_processor_factory().process_block(100, registry_items=[events_item])

    assert (
        TaskAttempt.objects.filter(dispatched_at__gt=db_now_offset(-60), dispatched_at__lte=db_now_offset(0)).count()
        == 1
    )


def _pending(block_number: int, *, dispatched_ago: float, executable_path: str, **fields) -> TaskAttempt:
    attempt = TaskAttemptFactory(block_number=block_number, executable_path=executable_path, status=PENDING, **fields)
    TaskAttempt.objects.filter(id=attempt.id).update(
        dispatched_at=db_now_offset(-dispatched_ago), updated_at=timezone.now() - timedelta(seconds=dispatched_ago)
    )
    return attempt


@pytest.fixture
def ttl_sweep_attempts(events_item) -> dict[str, TaskAttempt]:
    """Attempts on the TTL queue in every state the sweep must tell apart, by role; only `expired` is due."""
    path = events_item.executable_path
    running = TaskAttemptFactory(
        block_number=5,
        executable_path=path,
        status=TaskAttempt.Status.RUNNING,
        attempt_count=1,
        celery_task_id="worker-claim",
    )
    # The worker's claim still holds: its lease has not run out.
    TaskAttempt.objects.filter(id=running.id).update(
        dispatched_at=db_now_offset(-400),
        dispatch_queue="events-live",
        last_attempted_at=db_now(),
        lease_expires_at=db_now_offset(130),
    )
    return {
        "expired": _pending(
            1, dispatched_ago=400, executable_path=path, dispatch_queue="events-live", celery_task_id="expired-message"
        ),
        "fresh": _pending(
            2, dispatched_ago=60, executable_path=path, dispatch_queue="events-live", celery_task_id="fresh-message"
        ),
        "waiting_for_retry": _pending(
            3,
            dispatched_ago=400,
            executable_path=path,
            dispatch_queue="events-live",
            next_retry_at=timezone.now() + timedelta(minutes=5),
            celery_task_id="retry-message",
        ),
        # No policy recorded: the 30-minute stuck-task rule applies instead of the TTL.
        "without_policy": _pending(4, dispatched_ago=400, executable_path=path, celery_task_id="unpoliced-message"),
        "running": running,
    }


@pytest.mark.django_db
def test_sweep_resends_only_attempts_whose_message_expired(events_task, ttl_sweep_attempts):
    with patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    sent_blocks = [call.kwargs["kwargs"]["block_number"] for call in mock_apply_async.call_args_list]
    records = {role: attempt_record(attempt.id) for role, attempt in ttl_sweep_attempts.items()}
    restamped_blocks = list(
        TaskAttempt.objects.filter(dispatched_at__gt=db_now_offset(-30)).values_list("block_number", flat=True)
    )
    assert (recovered, sent_blocks, records, restamped_blocks) == (
        1,
        [1],
        {
            "expired": {**UNTOUCHED_PENDING, "celery_queue_override": "events-live"},
            "fresh": {**UNTOUCHED_PENDING, "celery_task_id": "fresh-message"},
            "waiting_for_retry": {
                **UNTOUCHED_PENDING,
                "celery_task_id": "retry-message",
                "next_retry_at": ttl_sweep_attempts["waiting_for_retry"].next_retry_at,
            },
            "without_policy": {**UNTOUCHED_PENDING, "celery_task_id": "unpoliced-message"},
            "running": {
                **UNTOUCHED_PENDING,
                "status": TaskAttempt.Status.RUNNING,
                "attempt_count": 1,
                "celery_task_id": "worker-claim",
                "lease": timedelta(seconds=130),
            },
        },
        [1],
    )


@pytest.mark.django_db
def test_expired_message_waits_the_recovery_margin(events_task, events_item):
    _pending(1, dispatched_ago=305, executable_path=events_item.executable_path, dispatch_queue="events-live")

    with patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 0
    mock_apply_async.assert_not_called()


@pytest.mark.django_db
def test_expired_message_past_the_deadline_is_given_up_and_not_counted_as_recovered(events_task, events_item):
    attempt = _pending(1, dispatched_ago=400, executable_path=events_item.executable_path, dispatch_queue="events-live")
    TaskAttempt.objects.filter(id=attempt.id).update(give_up_at=db_now_offset(-1))

    with (
        structlog.testing.capture_logs() as logs,
        patch.object(events_task, "apply_async") as mock_apply_async,
        patch("abstract_block_dumper._internal.services.reconciler.record_task_recovered") as recovered_metric,
    ):
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 0
    mock_apply_async.assert_not_called()
    recovered_metric.assert_not_called()
    assert [log["event"] for log in logs if log["log_level"] == "warning"] == ["Task attempt given up"]
    attempt.refresh_from_db()
    assert (attempt.status, attempt.give_up_reason) == (TaskAttempt.Status.FAILED, "block_age")


class _OtherTask:
    name = "some.other.task"


@pytest.mark.parametrize(
    ("expired", "sender_registered", "expected_queues"),
    [(True, True, ["events-live"]), (False, True, []), (True, False, [])],
    ids=["expired block task", "revoked by hand", "not a block task"],
)
def test_worker_counts_block_task_messages_discarded_after_their_ttl(
    events_task, expired, sender_registered, expected_queues
):
    connect_worker_signals()
    sender = events_task if sender_registered else _OtherTask()
    request = SimpleNamespace(delivery_info={"routing_key": "events-live"})

    with patch("abstract_block_dumper._internal.services.celery_signals.record_message_expired") as mock_record:
        task_revoked.send(sender=sender, request=request, terminated=False, signum=None, expired=expired)

    assert [call.args[0] for call in mock_record.call_args_list] == expected_queues
