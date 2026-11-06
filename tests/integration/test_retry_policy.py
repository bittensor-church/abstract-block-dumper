from datetime import UTC, datetime, timedelta
from io import StringIO
from unittest.mock import patch

import pytest
import structlog
from django.core.management import CommandError, call_command
from django.db.models import DurationField, ExpressionWrapper, F
from django.utils import timezone
from freezegun import freeze_time

import abstract_block_dumper._internal.dal.attempt_transitions as transitions
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.db_clock import db_now_offset
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.queue_backfiller import QueueBackfiller
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.reconciler import queue_reconciler_factory
from abstract_block_dumper._internal.services.window_backfiller import WindowBackfiller
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from abstract_block_dumper.v1.dumper import BlockDumper, Queue, RetryPolicy
from tests.fatories import TaskAttemptFactory

NOW = datetime(2026, 1, 1, tzinfo=UTC)
PENDING = TaskAttempt.Status.PENDING
RUNNING = TaskAttempt.Status.RUNNING
SUCCESS = TaskAttempt.Status.SUCCESS
FAILED = TaskAttempt.Status.FAILED
GIVEN_UP = "abstract_block_dumper._internal.services.retry_rules.record_task_given_up"
RECOVERED = "abstract_block_dumper._internal.services.reconciler.record_task_recovered"
ARCHIVE_USAGE = "abstract_block_dumper._internal.services.queue_executor.increment_archive_network_usage"


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"max_attempts": 0}, "RetryPolicy.max_attempts must be a positive integer or None"),
        ({"max_attempts": True}, "RetryPolicy.max_attempts must be a positive integer or None"),
        ({"give_up_after_blocks": -1}, "RetryPolicy.give_up_after_blocks must be a positive integer or None"),
        ({"give_up_after_blocks": 10.0}, "RetryPolicy.give_up_after_blocks must be a positive integer or None"),
        ({"fast_retry_attempts": 1.5}, "RetryPolicy.fast_retry_attempts must be a non-negative integer"),
        ({"fast_retry_attempts": -1}, "RetryPolicy.fast_retry_attempts must be a non-negative integer"),
        ({"fast_retry_attempts": True}, "RetryPolicy.fast_retry_attempts must be a non-negative integer"),
        ({"max_delay": -1}, "RetryPolicy.max_delay must be a non-negative number"),
        ({"backoff_start": True}, "RetryPolicy.backoff_start must be a non-negative number"),
        ({"backoff_multiplier": 0.5}, "RetryPolicy.backoff_multiplier must be at least 1"),
        ({"backoff_multiplier": True}, "RetryPolicy.backoff_multiplier must be at least 1"),
    ],
)
def test_invalid_retry_policy_is_rejected(fields, message):
    with pytest.raises(ValueError, match=message):
        RetryPolicy(**fields)


def test_delays_follow_fast_retries_then_capped_exponential_growth():
    policy = RetryPolicy(
        fast_retry_attempts=2, fast_retry_delay=5, backoff_start=60, backoff_multiplier=2, max_delay=200
    )

    assert [policy.delay_seconds(attempt) for attempt in range(1, 6)] == [5, 5, 60, 120, 200]


@pytest.mark.parametrize(
    ("fields", "attempt", "expected"),
    [
        ({"backoff_multiplier": 2.0}, 1100, 1200),
        ({"backoff_multiplier": 1.5}, 2000, 1200),
        ({"backoff_start": 60.0}, 1100, 1200),
        ({"backoff_start": 0, "backoff_multiplier": 2.0}, 1100, 5),
    ],
    ids=["float multiplier", "small float multiplier", "float start", "no exponential phase"],
)
def test_delay_stays_capped_after_more_attempts_than_a_float_can_grow(fields, attempt, expected):
    assert RetryPolicy(max_attempts=None, **fields).delay_seconds(attempt) == expected


def always_fails(block_number: int) -> None:
    raise ValueError("feed down")


def records_block(block_number: int) -> int:
    return block_number


@pytest.fixture
def dumper(celery_test_app) -> BlockDumper:
    return BlockDumper(celery_app=celery_test_app, block_time_seconds=6)


def _register(dumper, func, queue: Queue):
    celery_task = dumper.block_task(live_queue=queue, backfill_queue=queue)(func)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(func))
    assert registry_item is not None
    return celery_task, registry_item


def _attempt(func, block_number: int = 100, **fields) -> TaskAttempt:
    return TaskAttemptFactory(
        block_number=block_number,
        executable_path=abd_utils.get_executable_path(func),
        status=fields.pop("status", PENDING),
        **fields,
    )


def _outcome(attempt: TaskAttempt) -> dict:
    return (
        TaskAttempt.objects.filter(id=attempt.id)
        .values("status", "attempt_count", "next_retry_at", "give_up_reason")
        .get()
    )


@pytest.mark.django_db
@freeze_time(NOW)
def test_failure_is_retried_on_the_queue_policy_without_an_attempt_limit(dumper):
    queue = Queue("forever", retry=RetryPolicy(max_attempts=None, fast_retry_attempts=1, fast_retry_delay=7))
    celery_task, _ = _register(dumper, always_fails, queue)
    attempt = _attempt(always_fails, attempt_count=50, dispatch_queue="forever")

    with patch.object(celery_task, "apply_async") as mock_apply_async, patch(GIVEN_UP) as given_up:
        celery_task(100)

    retry_at = NOW + timedelta(seconds=1200)  # attempt 51's exponential delay is capped at the default max_delay
    assert _outcome(attempt) == {
        "status": PENDING,
        "attempt_count": 51,
        "next_retry_at": retry_at,
        "give_up_reason": None,
    }
    assert mock_apply_async.call_args.kwargs["eta"] == retry_at
    given_up.assert_not_called()


@pytest.mark.django_db
@freeze_time(NOW)
def test_failure_after_more_attempts_than_a_float_can_grow_is_still_retried(dumper):
    queue = Queue("forever", retry=RetryPolicy(max_attempts=None, backoff_multiplier=2.0))
    celery_task, _ = _register(dumper, always_fails, queue)
    attempt = _attempt(always_fails, attempt_count=1100, dispatch_queue="forever")

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        celery_task(100)

    retry_at = NOW + timedelta(seconds=1200)
    assert _outcome(attempt) == {
        "status": PENDING,
        "attempt_count": 1101,
        "next_retry_at": retry_at,
        "give_up_reason": None,
    }
    assert mock_apply_async.call_args.kwargs["eta"] == retry_at


@pytest.mark.django_db
def test_expired_lease_after_more_attempts_than_a_float_can_grow_is_still_retried(dumper):
    queue = Queue("forever", retry=RetryPolicy(max_attempts=None, backoff_multiplier=2.0))
    celery_task, _ = _register(dumper, records_block, queue)
    attempt = _attempt(records_block, status=RUNNING, attempt_count=1100, dispatch_queue="forever")
    TaskAttempt.objects.filter(id=attempt.id).update(lease_expires_at=db_now_offset(-1))

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 1
    mock_apply_async.assert_called_once()
    assert TaskAttempt.objects.values_list("status", "attempt_count", "give_up_reason").get() == (PENDING, 1100, None)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("age", "expected_kwargs"),
    [
        (timedelta(minutes=29), {"block_number": 100}),
        (timedelta(minutes=31), {"block_number": 100, "_use_archive_network": True}),
    ],
    ids=["younger than the threshold", "older than the threshold"],
)
def test_retry_switches_to_the_archive_network_once_the_attempt_has_aged(dumper, age, expected_kwargs):
    # 300 blocks at this dumper's 6 s per block are 30 minutes.
    celery_task, _ = _register(dumper, always_fails, Queue("forever", retry=RetryPolicy(max_attempts=None)))
    attempt = _attempt(always_fails, dispatch_queue="forever")
    TaskAttempt.objects.filter(id=attempt.id).update(created_at=timezone.now() - age)

    with patch.object(celery_task, "apply_async") as mock_apply_async, patch(ARCHIVE_USAGE) as archive_usage:
        celery_task(100)

    assert mock_apply_async.call_args.kwargs["kwargs"] == expected_kwargs
    assert archive_usage.call_count == ("_use_archive_network" in expected_kwargs)


@pytest.mark.django_db
def test_last_allowed_attempt_is_given_up(dumper):
    celery_task, _ = _register(dumper, always_fails, Queue("two-tries", retry=RetryPolicy(max_attempts=2)))
    attempt = _attempt(always_fails, attempt_count=1, dispatch_queue="two-tries")

    with patch.object(celery_task, "apply_async") as mock_apply_async, patch(GIVEN_UP) as given_up:
        celery_task(100)

    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 2,
        "next_retry_at": None,
        "give_up_reason": "attempts",
    }
    mock_apply_async.assert_not_called()
    given_up.assert_called_once_with("always_fails", "attempts")


@pytest.mark.django_db
def test_first_send_sets_the_give_up_deadline_from_the_block_age(dumper):
    queue = Queue("events", retry=RetryPolicy(give_up_after_blocks=200))
    celery_task, registry_item = _register(dumper, records_block, queue)

    with patch.object(celery_task, "apply_async"):
        QueueBackfiller(QueueExecutor()).submit_block(registry_item, 100, {}, executed_blocks=set(), head_block=150)

    deadline = (
        TaskAttempt.objects.annotate(
            left=ExpressionWrapper(F("give_up_at") - F("dispatched_at"), output_field=DurationField())
        )
        .values_list("left", flat=True)
        .get()
    )
    # 150 blocks left at 6 s each; the deadline and the dispatch are two statements apart.
    assert timedelta(seconds=900) - timedelta(seconds=1) < deadline <= timedelta(seconds=900)


@pytest.mark.django_db
def test_backfill_of_a_block_already_too_old_is_given_up_without_sending(dumper):
    queue = Queue("shared", retry=RetryPolicy(give_up_after_blocks=300))
    celery_task, registry_item = _register(dumper, records_block, queue)

    with patch.object(celery_task, "apply_async") as mock_apply_async, patch(GIVEN_UP) as given_up:
        QueueBackfiller(QueueExecutor()).submit_block(registry_item, 1000, {}, executed_blocks=set(), head_block=2000)

    mock_apply_async.assert_not_called()
    given_up.assert_called_once_with("records_block", "block_age")
    assert TaskAttempt.objects.values_list("status", "give_up_reason").get() == (FAILED, "block_age")


@pytest.mark.django_db
def test_attempt_past_its_deadline_is_given_up_when_a_worker_picks_it(dumper):
    calls = []
    celery_task, _ = _register(dumper, records_block, Queue("events", retry=RetryPolicy(give_up_after_blocks=10)))
    attempt = _attempt(records_block, dispatch_queue="events")
    TaskAttempt.objects.filter(id=attempt.id).update(give_up_at=db_now_offset(-1))

    with patch(f"{__name__}.records_block", side_effect=calls.append):
        assert celery_task(100) is None

    assert calls == []
    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 0,
        "next_retry_at": None,
        "give_up_reason": "block_age",
    }


@pytest.mark.django_db
def test_failure_whose_retry_would_start_after_the_deadline_is_given_up(dumper):
    queue = Queue("events", retry=RetryPolicy(give_up_after_blocks=10, fast_retry_delay=60))
    celery_task, _ = _register(dumper, always_fails, queue)
    attempt = _attempt(always_fails, dispatch_queue="events")
    TaskAttempt.objects.filter(id=attempt.id).update(give_up_at=db_now_offset(30))

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        celery_task(100)

    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 1,
        "next_retry_at": None,
        "give_up_reason": "block_age",
    }
    mock_apply_async.assert_not_called()


@pytest.mark.django_db
def test_expired_lease_past_the_deadline_is_given_up(dumper):
    celery_task, _ = _register(dumper, records_block, Queue("events", retry=RetryPolicy(give_up_after_blocks=10)))
    attempt = _attempt(records_block, status=RUNNING, attempt_count=1, dispatch_queue="events")
    TaskAttempt.objects.filter(id=attempt.id).update(lease_expires_at=db_now_offset(-1), give_up_at=db_now_offset(-1))

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 0
    mock_apply_async.assert_not_called()
    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 1,
        "next_retry_at": None,
        "give_up_reason": "block_age",
    }


@pytest.mark.django_db
def test_stuck_attempt_past_the_deadline_is_given_up_and_not_counted_as_recovered(dumper):
    celery_task, _ = _register(dumper, records_block, Queue("events", retry=RetryPolicy(give_up_after_blocks=10)))
    attempt = _attempt(records_block, dispatch_queue="events")
    TaskAttempt.objects.filter(id=attempt.id).update(
        give_up_at=db_now_offset(-1), updated_at=timezone.now() - timedelta(hours=1)
    )

    with (
        structlog.testing.capture_logs() as logs,
        patch.object(celery_task, "apply_async") as mock_apply_async,
        patch(RECOVERED) as recovered_metric,
    ):
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 0
    mock_apply_async.assert_not_called()
    recovered_metric.assert_not_called()
    assert [log["event"] for log in logs if log["log_level"] == "warning"] == ["Task attempt given up"]
    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 0,
        "next_retry_at": None,
        "give_up_reason": "block_age",
    }


@pytest.mark.django_db
def test_sweep_giving_up_an_old_block_does_not_count_archive_usage(dumper):
    celery_task, _ = _register(dumper, records_block, Queue("events", retry=RetryPolicy(give_up_after_blocks=10)))
    attempt = _attempt(records_block, dispatch_queue="events")
    TaskAttempt.objects.filter(id=attempt.id).update(
        give_up_at=db_now_offset(-1), updated_at=timezone.now() - timedelta(hours=1)
    )

    with patch.object(celery_task, "apply_async"), patch(ARCHIVE_USAGE) as archive_usage:
        queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=2000)

    archive_usage.assert_not_called()


@pytest.mark.django_db
def test_lookback_does_not_report_a_given_up_block_as_submitted(dumper):
    celery_task, registry_item = _register(dumper, records_block, Queue("events"))
    _attempt(records_block, status=FAILED, give_up_reason="block_age", dispatch_queue="events")
    backfiller = WindowBackfiller(block_processor_factory().executor)

    with patch.object(celery_task, "apply_async") as mock_apply_async, patch(ARCHIVE_USAGE) as archive_usage:
        submitted = backfiller.process_item_range(registry_item, 100, 100, head_block=2000)

    assert submitted == 0
    mock_apply_async.assert_not_called()
    archive_usage.assert_not_called()


def other_task(block_number: int) -> int:
    return block_number


@pytest.fixture
def give_up_candidates(dumper):
    """Attempts of `records_block` and `other_task` in every state the command must tell apart."""
    _register(dumper, records_block, Queue("events"))
    retry_at = datetime(2030, 1, 1, tzinfo=UTC)
    return {
        "pending in range": _attempt(records_block, 100),
        "failed with a retry in range": _attempt(records_block, 101, status=FAILED, next_retry_at=retry_at),
        "already given up": _attempt(records_block, 102, status=FAILED, give_up_reason="attempts"),
        "running": _attempt(records_block, 103, status=RUNNING),
        "succeeded": _attempt(records_block, 104, status=SUCCESS),
        "pending out of range": _attempt(records_block, 300),
        "other task": _attempt(other_task, 100),
    }


def _states(attempts: dict[str, TaskAttempt]) -> dict[str, tuple]:
    return {
        label: TaskAttempt.objects.filter(id=attempt.id).values_list("status", "give_up_reason").get()
        for label, attempt in attempts.items()
    }


@pytest.mark.django_db
def test_give_up_command_stops_only_attempts_that_could_still_run(give_up_candidates):
    out = StringIO()

    call_command(
        "block_dumper_give_up_v1", "--task", "records_block", "--from-block", "100", "--to-block", "200", stdout=out
    )

    assert out.getvalue() == f"Gave up 2 attempt(s) of {abd_utils.get_executable_path(records_block)}.\n"
    assert _states(give_up_candidates) == {
        "pending in range": (FAILED, "manual"),
        "failed with a retry in range": (FAILED, "manual"),
        "already given up": (FAILED, "attempts"),
        "running": (RUNNING, None),
        "succeeded": (SUCCESS, None),
        "pending out of range": (PENDING, None),
        "other task": (PENDING, None),
    }


@pytest.mark.django_db
def test_give_up_command_logs_how_many_attempts_it_stopped(give_up_candidates):
    with structlog.testing.capture_logs() as logs:
        call_command(
            "block_dumper_give_up_v1",
            "--task",
            "records_block",
            "--from-block",
            "100",
            "--to-block",
            "200",
            stdout=StringIO(),
        )

    assert logs == [
        {
            "event": "Task attempts given up by hand",
            "log_level": "warning",
            "executable_path": abd_utils.get_executable_path(records_block),
            "attempts": 2,
            "from_block": 100,
            "to_block": 200,
        }
    ]


@pytest.mark.django_db
def test_give_up_command_with_nothing_to_stop_logs_nothing(dumper):
    _register(dumper, records_block, Queue("events"))
    _attempt(records_block, status=SUCCESS)

    with structlog.testing.capture_logs() as logs:
        call_command("block_dumper_give_up_v1", "--task", "records_block", stdout=StringIO())

    assert logs == []


@pytest.mark.django_db
def test_give_up_dry_run_changes_nothing(give_up_candidates):
    before = _states(give_up_candidates)
    out = StringIO()

    call_command("block_dumper_give_up_v1", "--task", "records_block", "--dry-run", stdout=out)

    assert out.getvalue() == f"Would give up 3 attempt(s) of {abd_utils.get_executable_path(records_block)}.\n"
    assert _states(give_up_candidates) == before


@pytest.mark.django_db
def test_give_up_dry_run_counts_the_attempts_the_command_gives_up(give_up_candidates):
    command = ("block_dumper_give_up_v1", "--task", "records_block", "--from-block", "100", "--to-block", "200")
    dry_run, real_run = StringIO(), StringIO()

    call_command(*command, "--dry-run", stdout=dry_run)
    call_command(*command, stdout=real_run)

    executable_path = abd_utils.get_executable_path(records_block)
    assert (dry_run.getvalue(), real_run.getvalue()) == (
        f"Would give up 2 attempt(s) of {executable_path}.\n",
        f"Gave up 2 attempt(s) of {executable_path}.\n",
    )


@pytest.mark.django_db
def test_retry_of_an_attempt_given_up_after_its_failure_is_not_sent(dumper):
    celery_task, registry_item = _register(dumper, records_block, Queue("events"))
    retry_at = datetime(2030, 1, 1, tzinfo=UTC)
    # Read before the command runs, like a worker between recording the failure and sending the retry.
    attempt = _attempt(records_block, status=FAILED, attempt_count=1, next_retry_at=retry_at, dispatch_queue="events")
    call_command("block_dumper_give_up_v1", "--task", "records_block", stdout=StringIO())

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        sent = QueueExecutor().dispatch_retry(registry_item, attempt)

    assert sent is False
    mock_apply_async.assert_not_called()
    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 1,
        "next_retry_at": None,
        "give_up_reason": "manual",
    }


@pytest.mark.django_db
def test_resubmission_does_not_reopen_an_attempt_given_up_while_it_was_read(dumper):
    _register(dumper, records_block, Queue("events"))
    attempt = _attempt(records_block, status=FAILED, attempt_count=1, dispatch_queue="events")

    def given_up_meanwhile(_task):
        call_command("block_dumper_give_up_v1", "--task", "records_block", stdout=StringIO())
        return True

    with patch.object(transitions, "can_run_again", side_effect=given_up_meanwhile):
        task, created = transitions.create_or_reopen(100, attempt.executable_path, {})

    assert (task.status, created) == (FAILED, False)
    assert _outcome(attempt) == {
        "status": FAILED,
        "attempt_count": 1,
        "next_retry_at": None,
        "give_up_reason": "manual",
    }


@pytest.mark.django_db
def test_give_up_command_needs_an_unambiguous_task_name():
    TaskAttemptFactory(executable_path="app_one.block_tasks.sync", status=PENDING)
    TaskAttemptFactory(executable_path="app_two.block_tasks.sync", status=PENDING)

    with pytest.raises(CommandError, match="Task name 'sync' matches 2 recorded tasks: app_one.block_tasks.sync, "):
        call_command("block_dumper_give_up_v1", "--task", "sync")


@pytest.mark.django_db
def test_give_up_command_refuses_a_task_declared_with_block_task():
    block_task(other_task)
    attempt = _attempt(other_task, status=FAILED, attempt_count=3)

    with pytest.raises(CommandError, match=r"other_task is not a BlockDumper task; this command only gives up"):
        call_command("block_dumper_give_up_v1", "--task", "other_task", stdout=StringIO())

    assert _outcome(attempt) == {"status": FAILED, "attempt_count": 3, "next_retry_at": None, "give_up_reason": None}
