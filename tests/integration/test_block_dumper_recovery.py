from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
import structlog
from django.test import override_settings
from django.utils import timezone
from freezegun import freeze_time

import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.db_clock import db_now_offset
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper._internal.services.reconciler import queue_reconciler_factory
from abstract_block_dumper._internal.services.scheduler import task_scheduler_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from abstract_block_dumper.v1.dumper import BlockDumper, Queue
from abstract_block_dumper.v1.testing import BrokerCall, MockBrokerContact, use_broker_contact
from tests.attempt_records import attempt_record
from tests.fatories import TaskAttemptFactory

PENDING = TaskAttempt.Status.PENDING
RUNNING = TaskAttempt.Status.RUNNING
SUCCESS = TaskAttempt.Status.SUCCESS
FAILED = TaskAttempt.Status.FAILED

STALE_AFTER = timedelta(minutes=30)
NOW = datetime(2026, 1, 1, tzinfo=UTC)
LIVE = Queue("recovery-live", time_limit=60)
BACKFILL = Queue("recovery-backfill", time_limit=60)
TTL_BACKFILL = Queue("recovery-ttl", time_limit=60, message_ttl=60)


def index_events(block_number: int) -> int:
    return block_number


def plain_task(block_number: int) -> int:
    return block_number


EVENTS_PATH = abd_utils.get_executable_path(index_events)


@pytest.fixture
def events_task(celery_test_app):
    return BlockDumper(celery_app=celery_test_app).block_task(live_queue=LIVE, backfill_queue=BACKFILL)(index_events)


def _attempt(
    *,
    status: str,
    updated_ago: timedelta,
    lease_expires_in: timedelta | None = None,
    executable_path: str = EVENTS_PATH,
    **fields: Any,
) -> TaskAttempt:
    """
    An attempt whose `updated_at` lies `updated_ago` in the past.

    `lease_expires_in` is relative to now on the database clock, like the lease a worker's claim
    writes; negative values are in the past.
    """
    attempt = TaskAttemptFactory(status=status, executable_path=executable_path, **fields)
    TaskAttempt.objects.filter(id=attempt.id).update(updated_at=timezone.now() - updated_ago)
    if lease_expires_in is not None:
        TaskAttempt.objects.filter(id=attempt.id).update(
            lease_expires_at=db_now_offset(lease_expires_in.total_seconds())
        )
    attempt.refresh_from_db()
    return attempt


@pytest.fixture
def plain_item() -> RegistryItem:
    block_task(plain_task)
    item = task_registry.get_by_executable_path(abd_utils.get_executable_path(plain_task))
    assert item is not None
    return item


def _sweep(*, head_block: int | None = None, limit: int = 10) -> int:
    return queue_reconciler_factory().reconcile(
        stale_after_seconds=STALE_AFTER.total_seconds(), limit=limit, head_block=head_block
    )


def _run_one_sweep(*, head: int = 2000) -> None:
    """
    Run the live scheduler for one loop iteration on a mocked chain whose head is `head`.

    The test settings start at the current head, so the iteration processes no block and only
    sweeps.
    """
    with (
        patch("abstract_block_dumper._internal.services.utils.get_bittensor_client") as get_client,
        patch("abstract_block_dumper._internal.services.scheduler.time.sleep", side_effect=KeyboardInterrupt),
    ):
        get_client.return_value.block = head
        task_scheduler_factory().start()


@pytest.mark.django_db
@pytest.mark.parametrize(
    "lease_expires_in", [timedelta(hours=1), -timedelta(hours=1)], ids=["live lease", "expired lease"]
)
def test_running_attempt_with_a_lease_is_never_stuck_by_age(lease_expires_in):
    """Its lease, not its age, says when its worker is gone; expired leases have their own recovery."""
    attempt = _attempt(
        status=RUNNING, updated_ago=timedelta(hours=1), lease_expires_in=lease_expires_in, dispatch_queue=LIVE.name
    )
    cutoff = timezone.now() - STALE_AFTER

    assert queue_attempts.stuck_attempt_ids(cutoff, 10, [EVENTS_PATH]) == []
    assert queue_attempts.is_stuck(attempt, cutoff) is False


@pytest.mark.django_db
@freeze_time(NOW)
def test_expired_lease_is_failed_and_retried_on_its_recorded_queue(events_task):
    attempt = _attempt(
        status=RUNNING,
        updated_ago=timedelta(minutes=5),
        lease_expires_in=-timedelta(seconds=1),
        attempt_count=1,
        block_number=1000,
        celery_queue_override=BACKFILL.name,
        dispatch_queue=BACKFILL.name,
        celery_task_id="killed-worker",
    )

    with (
        patch.object(events_task, "apply_async") as mock_apply_async,
        patch("abstract_block_dumper._internal.services.reconciler.record_task_recovered") as mock_record,
    ):
        recovered = _sweep(head_block=1301)

    retry_at = NOW + timedelta(seconds=5)  # the default RetryPolicy's first fast retry
    assert recovered == 1
    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 1000, "_use_archive_network": True},
        eta=retry_at,
        queue=BACKFILL.name,
        time_limit=60,
    )
    mock_record.assert_called_once_with("index_events", RUNNING)
    assert attempt_record(attempt.id) == {
        "status": PENDING,
        "attempt_count": 1,
        "celery_task_id": "killed-worker",
        "celery_queue_override": BACKFILL.name,
        "execution_result": None,
        "next_retry_at": retry_at,
        "lease": None,
    }


@pytest.mark.django_db
@freeze_time(NOW)
def test_expired_lease_on_a_queue_no_longer_defined_is_retried_on_the_backfill_queue(events_task):
    """A queue renamed for a new `message_ttl` leaves its old name on the attempts sent before the rename."""
    attempt = _attempt(
        status=RUNNING,
        updated_ago=timedelta(minutes=5),
        lease_expires_in=-timedelta(seconds=1),
        attempt_count=1,
        block_number=1000,
        celery_queue_override="renamed-live",
        dispatch_queue="renamed-live",
        celery_task_id="killed-worker",
    )

    with structlog.testing.capture_logs() as logs, patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = _sweep(head_block=1010)

    assert recovered == 1
    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 1000},
        # BLOCK_TASK_RETRY_BACKOFF ** 1 minutes: the attempt had no policy when its lease passed.
        eta=NOW + timedelta(minutes=2),
        queue=BACKFILL.name,
        time_limit=60,
    )
    assert TaskAttempt.objects.values("status", "celery_queue_override", "dispatch_queue").get(id=attempt.id) == {
        "status": PENDING,
        "celery_queue_override": BACKFILL.name,
        "dispatch_queue": BACKFILL.name,
    }
    assert [log for log in logs if log["event"].startswith("Recorded queue")] == [
        {
            "event": "Recorded queue of task attempt is not defined; sending it to the task's current queue",
            "log_level": "warning",
            "task_id": attempt.id,
            "recorded_queue": "renamed-live",
            "queue": BACKFILL.name,
        }
    ]


@pytest.mark.django_db
def test_expired_lease_on_the_last_attempt_is_given_up(events_task):
    attempt = _attempt(
        status=RUNNING,
        updated_ago=timedelta(minutes=5),
        lease_expires_in=-timedelta(seconds=1),
        attempt_count=LIVE.retry.max_attempts,
        dispatch_queue=LIVE.name,
        celery_task_id="killed-worker",
    )

    with patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = _sweep()

    assert recovered == 0
    mock_apply_async.assert_not_called()
    assert TaskAttempt.objects.values("status", "next_retry_at", "give_up_reason", "lease_expires_at").get(
        id=attempt.id
    ) == {"status": FAILED, "next_retry_at": None, "give_up_reason": "attempts", "lease_expires_at": None}


@pytest.mark.django_db
def test_running_attempt_with_a_live_lease_is_left_alone(events_task):
    attempt = _attempt(
        status=RUNNING,
        updated_ago=timedelta(hours=2),
        lease_expires_in=timedelta(minutes=5),
        attempt_count=1,
        dispatch_queue=LIVE.name,
    )
    before = attempt_record(attempt.id)

    with patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = _sweep()

    assert recovered == 0
    mock_apply_async.assert_not_called()
    assert attempt_record(attempt.id) == before


class _WorkerKilledError(BaseException):
    """Stands in for the worker process dying mid-execution."""


_killed_once: list[bool] = []


def _dies_once(block_number: int) -> int:
    if not _killed_once:
        _killed_once.append(True)
        raise _WorkerKilledError
    return block_number


@pytest.mark.django_db
def test_task_killed_mid_execution_runs_again_after_its_lease_expires(celery_test_app):
    celery_task = BlockDumper(celery_app=celery_test_app).block_task(live_queue=LIVE)(_dies_once)
    attempt = TaskAttemptFactory(
        block_number=1000,
        executable_path=abd_utils.get_executable_path(_dies_once),
        status=PENDING,
        dispatch_queue=LIVE.name,
    )
    _killed_once.clear()

    with pytest.raises(_WorkerKilledError):
        celery_task(1000)
    TaskAttempt.objects.filter(id=attempt.id).update(lease_expires_at=db_now_offset(-1))
    # The eager retry message runs at once, whatever its ETA.
    recovered = _sweep()

    assert recovered == 1
    record = attempt_record(attempt.id)
    assert (record["status"], record["attempt_count"], record["execution_result"]) == (SUCCESS, 2, 1000)


@pytest.mark.django_db
def test_each_path_recovers_its_own_attempts_with_its_own_batch(events_task, plain_item):
    """`@block_task` attempts follow the rules of master; BlockDumper attempts those of their queue."""
    plain = [
        _attempt(
            status=PENDING,
            updated_ago=timedelta(hours=hours),
            executable_path=plain_item.executable_path,
            block_number=block_number,
            celery_queue_override="plain-queue",
        )
        for block_number, hours in ((1, 2), (2, 1))
    ]
    dumper = [
        _attempt(
            status=PENDING,
            updated_ago=timedelta(hours=hours),
            block_number=block_number,
            celery_queue_override=LIVE.name,
            dispatch_queue=LIVE.name,
        )
        for block_number, hours in ((11, 2), (12, 1))
    ]

    with (
        override_settings(BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE=1),
        patch.object(plain_item.function, "apply_async") as plain_apply_async,
        patch.object(events_task, "apply_async") as dumper_apply_async,
    ):
        _run_one_sweep(head=100)

    plain_apply_async.assert_called_once_with(
        kwargs={"block_number": 1, "_use_archive_network": False}, queue="plain-queue"
    )
    dumper_apply_async.assert_called_once_with(
        kwargs={"block_number": 11, "_use_archive_network": False}, queue=LIVE.name, time_limit=60
    )
    assert list(
        TaskAttempt.objects.filter(id__in=[plain[0].id, dumper[0].id])
        .order_by("block_number")
        .values_list("block_number", "dispatch_queue")
    ) == [(1, None), (11, LIVE.name)]


@pytest.fixture
def ttl_events_task(celery_test_app):
    return BlockDumper(celery_app=celery_test_app).block_task(live_queue=LIVE, backfill_queue=TTL_BACKFILL)(
        index_events
    )


@pytest.fixture
def lost_attempts(ttl_events_task, plain_item) -> None:
    """One attempt for each kind of recovery: expired lease, expired message, stuck, stuck `@block_task`."""
    _attempt(
        status=RUNNING,
        updated_ago=timedelta(minutes=5),
        lease_expires_in=-timedelta(seconds=1),
        attempt_count=1,
        block_number=1000,
        dispatch_queue=LIVE.name,
        celery_task_id="killed-worker",
    )
    expired_message = _attempt(
        status=PENDING,
        updated_ago=timedelta(hours=1),
        block_number=1001,
        celery_queue_override=TTL_BACKFILL.name,
        dispatch_queue=TTL_BACKFILL.name,
    )
    TaskAttempt.objects.filter(id=expired_message.id).update(dispatched_at=db_now_offset(-3600))
    _attempt(status=PENDING, updated_ago=timedelta(hours=2), block_number=1002, dispatch_queue=LIVE.name)
    _attempt(status=PENDING, updated_ago=timedelta(hours=2), block_number=1, executable_path=plain_item.executable_path)


def _record_sends(task_name: str, sent: list[tuple[str, int]]) -> Callable[..., None]:
    def apply_async(**options: Any) -> None:
        sent.append((task_name, options["kwargs"]["block_number"]))

    return apply_async


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("stale_timeout", "expected_sent"),
    [
        (None, [("index_events", 1000), ("index_events", 1001)]),
        (1800, [("plain_task", 1), ("index_events", 1000), ("index_events", 1001), ("index_events", 1002)]),
    ],
    ids=["without a stuck-task timeout", "with a stuck-task timeout"],
)
def test_sweep_recovers_expired_attempts_whatever_the_timeout_and_stuck_ones_only_with_one(
    celery_test_app, ttl_events_task, plain_item, lost_attempts, stale_timeout, expected_sent
):
    sent: list[tuple[str, int]] = []
    broker = MockBrokerContact(celery_test_app)

    with (
        override_settings(BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=stale_timeout),
        use_broker_contact(broker),
        patch.object(plain_item.function, "apply_async", side_effect=_record_sends("plain_task", sent)),
        patch.object(ttl_events_task, "apply_async", side_effect=_record_sends("index_events", sent)),
    ):
        _run_one_sweep()

    assert (sent, broker.calls) == (
        expected_sent,
        [BrokerCall("queue_stats", (LIVE.name, TTL_BACKFILL.name), {})],
    )
