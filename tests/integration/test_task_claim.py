import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
import structlog
from django.contrib.auth import get_user_model
from django.db import OperationalError, connection, transaction
from django.test import override_settings
from freezegun import freeze_time

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.exceptions import CeleryTaskLockedError
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.dumper import BlockDumper, Queue
from tests.attempt_records import attempt_record
from tests.fatories import TaskAttemptFactory

BLOCK = 100
NOW = datetime(2026, 1, 1, tzinfo=UTC)
# Without limits of its own, so the task's limits are the ones the worker applies.
CLAIMS = Queue("claims")
RETRY_AT = NOW + timedelta(minutes=2)  # tests.settings: BLOCK_TASK_RETRY_BACKOFF = 2
FAILED_WITH_A_RETRY_TIME = {
    "status": TaskAttempt.Status.FAILED,
    "attempt_count": 1,
    "celery_task_id": "worker-a",
    "celery_queue_override": None,
    "execution_result": None,
    "next_retry_at": RETRY_AT,
    "lease": None,
}
# Once its retry is sent, the attempt is PENDING on the queue the retry went to.
WAITING_FOR_ITS_RETRY = {
    **FAILED_WITH_A_RETRY_TIME,
    "status": TaskAttempt.Status.PENDING,
    "celery_queue_override": CLAIMS.name,
}


class _WorkerKilledError(BaseException):
    """Stands in for the worker process dying mid-execution: nothing after it runs."""


# One function per Celery configuration: Celery's task registry is global and keyed by name.
def _limited_task(block_number: int, **kwargs: Any) -> Any:
    return _BEHAVIOUR["limited"](block_number, **kwargs)


def _unlimited_task(block_number: int) -> Any:
    return _BEHAVIOUR["unlimited"](block_number)


def _on_the_default_limit(block_number: int) -> Any:
    return _BEHAVIOUR["default"](block_number)


def _on_a_lower_default_limit(block_number: int) -> Any:
    return _BEHAVIOUR["default"](block_number)


def _with_a_soft_limit_above_the_default(block_number: int) -> Any:
    return _BEHAVIOUR["default"](block_number)


def _without_a_stuck_task_timeout(block_number: int) -> Any:
    return _BEHAVIOUR["default"](block_number)


_BEHAVIOUR: dict[str, Any] = {}


@pytest.fixture
def dumper(celery_test_app) -> BlockDumper:
    return BlockDumper(celery_app=celery_test_app)


@pytest.fixture
def limited_task(dumper):
    """A registered task with a 120 s hard time limit; set `behaviour` to choose what it does."""
    celery_task = dumper.block_task(live_queue=CLAIMS, celery_kwargs={"time_limit": 120})(_limited_task)
    _BEHAVIOUR["limited"] = lambda block_number, **kwargs: {"block": block_number}
    yield celery_task
    _BEHAVIOUR.clear()


@pytest.fixture
def unlimited_task(dumper):
    celery_task = dumper.block_task(live_queue=CLAIMS, celery_kwargs={"time_limit": None})(_unlimited_task)
    _BEHAVIOUR["unlimited"] = lambda block_number: {"block": block_number}
    yield celery_task
    _BEHAVIOUR.clear()


@pytest.fixture
def behaviour():
    yield _BEHAVIOUR
    _BEHAVIOUR.clear()


def _pending_attempt(func) -> TaskAttempt:
    return TaskAttemptFactory(
        block_number=BLOCK,
        executable_path=abd_utils.get_executable_path(func),
        status=TaskAttempt.Status.PENDING,
    )


@pytest.fixture
def limited_attempt(limited_task) -> TaskAttempt:
    return _pending_attempt(_limited_task)


@pytest.fixture
def unlimited_attempt(unlimited_task) -> TaskAttempt:
    return _pending_attempt(_unlimited_task)


def _run(celery_task, *, task_id: str = "worker-a", **kwargs: Any):
    return celery_task.apply(kwargs={"block_number": BLOCK, **kwargs}, task_id=task_id).get()


def _usernames() -> list[str]:
    """Usernames in the database; the users a task function creates stand in for its own data."""
    return sorted(get_user_model().objects.values_list("username", flat=True))


@pytest.mark.django_db
def test_claim_marks_the_attempt_running_with_a_lease_before_the_function_runs(limited_task, limited_attempt):
    seen_while_running = []
    _BEHAVIOUR["limited"] = lambda block_number, **kwargs: seen_while_running.append(attempt_record(limited_attempt.id))

    _run(limited_task)

    assert seen_while_running == [
        {
            "status": TaskAttempt.Status.RUNNING,
            "attempt_count": 1,
            "celery_task_id": "worker-a",
            "celery_queue_override": None,
            "execution_result": None,
            "next_retry_at": None,
            "lease": timedelta(seconds=130),  # time_limit + the default 10 s margin
        }
    ]


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("stale_timeout", "expected_lease"),
    [(900, timedelta(seconds=900)), (None, None)],
    ids=["falls back to the stale-task timeout", "never expires without a stuck-task timeout"],
)
def test_lease_without_a_hard_time_limit(unlimited_task, unlimited_attempt, stale_timeout, expected_lease):
    seen_leases = []
    _BEHAVIOUR["unlimited"] = lambda block_number: seen_leases.append(attempt_record(unlimited_attempt.id)["lease"])

    with override_settings(BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=stale_timeout):
        _run(unlimited_task)

    assert seen_leases == [expected_lease]


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("func", "stale_timeout", "celery_kwargs", "expected_lease"),
    [
        (_on_the_default_limit, 1800, {}, timedelta(seconds=1810)),
        (_on_a_lower_default_limit, 900, {}, timedelta(seconds=910)),
        (_with_a_soft_limit_above_the_default, 1800, {"soft_time_limit": 3000}, timedelta(seconds=3020)),
        (_without_a_stuck_task_timeout, None, {}, None),
    ],
    ids=["default limit", "lower default limit", "soft limit above the default", "without a stuck-task timeout"],
)
def test_lease_of_a_task_on_the_default_limit(dumper, behaviour, func, stale_timeout, celery_kwargs, expected_lease):
    attempt = _pending_attempt(func)
    seen_leases = []
    behaviour["default"] = lambda block_number: seen_leases.append(attempt_record(attempt.id)["lease"])

    with override_settings(BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=stale_timeout):
        _run(dumper.block_task(live_queue=CLAIMS, celery_kwargs=celery_kwargs)(func))

    assert seen_leases == [expected_lease]


@pytest.mark.django_db
def test_a_killed_worker_leaves_the_claim_and_its_lease_behind(limited_task, limited_attempt):
    def die(block_number: int, **kwargs: Any) -> None:
        raise _WorkerKilledError

    _BEHAVIOUR["limited"] = die

    with pytest.raises(_WorkerKilledError):
        _run(limited_task)

    assert attempt_record(limited_attempt.id) == {
        "status": TaskAttempt.Status.RUNNING,
        "attempt_count": 1,
        "celery_task_id": "worker-a",
        "celery_queue_override": None,
        "execution_result": None,
        "next_retry_at": None,
        "lease": timedelta(seconds=130),
    }


@pytest.mark.django_db
def test_success_records_the_result_and_releases_the_lease(limited_task, limited_attempt):
    result = _run(limited_task)

    assert result == {"result": {"block": BLOCK}}
    assert attempt_record(limited_attempt.id) == {
        "status": TaskAttempt.Status.SUCCESS,
        "attempt_count": 1,
        "celery_task_id": "worker-a",
        "celery_queue_override": None,
        "execution_result": {"block": BLOCK},
        "next_retry_at": None,
        "lease": None,
    }


@pytest.mark.django_db
@freeze_time(NOW)
def test_failure_counts_the_attempt_once_and_schedules_the_retry(limited_task, limited_attempt):
    def fail(block_number: int, **kwargs: Any) -> None:
        raise ValueError("boom")

    _BEHAVIOUR["limited"] = fail

    with patch.object(limited_task, "apply_async") as mock_apply_async:
        _run(limited_task)

    retry_at = NOW + timedelta(minutes=2)  # tests.settings: BLOCK_TASK_RETRY_BACKOFF = 2
    assert attempt_record(limited_attempt.id) == {
        "status": TaskAttempt.Status.PENDING,
        "attempt_count": 1,
        "celery_task_id": "worker-a",
        "celery_queue_override": CLAIMS.name,
        "execution_result": None,
        "next_retry_at": retry_at,
        "lease": None,
    }
    mock_apply_async.assert_called_once_with(kwargs={"block_number": BLOCK}, eta=retry_at, queue=CLAIMS.name)


@pytest.mark.django_db
def test_retry_keeps_the_archive_network_hint(limited_task, limited_attempt):
    def fail(block_number: int, **kwargs: Any) -> None:
        raise ValueError("boom")

    _BEHAVIOUR["limited"] = fail

    with patch.object(limited_task, "apply_async") as mock_apply_async:
        _run(limited_task, _use_archive_network=True)

    assert mock_apply_async.call_args.kwargs["kwargs"] == {"block_number": BLOCK, "_use_archive_network": True}


@pytest.mark.django_db
@pytest.mark.parametrize("status", [TaskAttempt.Status.SUCCESS, TaskAttempt.Status.FAILED])
def test_finished_attempt_is_dropped_without_running(limited_task, status):
    attempt = TaskAttemptFactory(
        block_number=BLOCK, executable_path=abd_utils.get_executable_path(_limited_task), status=status
    )
    before = attempt_record(attempt.id)
    calls = []
    _BEHAVIOUR["limited"] = lambda block_number, **kwargs: calls.append(block_number)

    assert _run(limited_task) is None

    assert calls == []
    assert attempt_record(attempt.id) == before


@pytest.mark.django_db
def test_attempt_running_on_another_worker_is_reported_as_locked(limited_task):
    attempt = TaskAttemptFactory(
        block_number=BLOCK,
        executable_path=abd_utils.get_executable_path(_limited_task),
        status=TaskAttempt.Status.RUNNING,
        celery_task_id="worker-b",
    )
    before = attempt_record(attempt.id)

    with pytest.raises(CeleryTaskLockedError, match="Task already being processed by another worker"):
        _run(limited_task)

    assert attempt_record(attempt.id) == before


@pytest.mark.django_db
def test_missing_attempt_is_reported_as_locked(limited_task):
    with pytest.raises(CeleryTaskLockedError, match="TaskAttempt not found"):
        _run(limited_task)


@pytest.mark.django_db
def test_claim_lost_to_another_worker_between_read_and_update(limited_task, limited_attempt):
    """The conditional UPDATE, not the earlier read, decides who owns the attempt."""
    competitor_ran = []

    def competing_claim(execute, sql, params, many, context):
        if sql.startswith("UPDATE") and not competitor_ran:
            competitor_ran.append(True)
            TaskAttempt.objects.filter(id=limited_attempt.id).update(
                status=TaskAttempt.Status.RUNNING, celery_task_id="worker-b"
            )
        return execute(sql, params, many, context)

    with connection.execute_wrapper(competing_claim), pytest.raises(CeleryTaskLockedError):
        _run(limited_task)

    record = attempt_record(limited_attempt.id)
    assert (record["status"], record["celery_task_id"], record["attempt_count"]) == (
        TaskAttempt.Status.RUNNING,
        "worker-b",
        0,
    )


# transaction=True: the injected error poisons an enclosing test transaction.
@pytest.mark.django_db(transaction=True)
def test_database_outage_while_claiming_propagates(limited_task, limited_attempt):
    """A dead database must not be mistaken for another worker holding the row."""
    before = attempt_record(limited_attempt.id)

    def unreachable_on_update(execute, sql, params, many, context):
        if sql.startswith("UPDATE"):
            raise OperationalError("server closed the connection unexpectedly")
        return execute(sql, params, many, context)

    with connection.execute_wrapper(unreachable_on_update), pytest.raises(OperationalError):
        _run(limited_task)

    assert attempt_record(limited_attempt.id) == before


@pytest.mark.django_db
@pytest.mark.parametrize("raises", [False, True], ids=["function returns", "function raises"])
def test_outcome_of_a_worker_that_lost_its_claim_is_discarded_with_its_writes(limited_task, limited_attempt, raises):
    """
    A worker whose claim was taken over records nothing, and its own writes roll back.

    The takeover is simulated from inside the function, so it rolls back as well; what matters
    is that neither the outcome nor the function's write survives, and no retry is sent.
    """

    def write_then_lose_the_claim(block_number: int, **kwargs: Any) -> str:
        get_user_model().objects.create(username="written-by-the-old-worker")
        TaskAttempt.objects.filter(id=limited_attempt.id).update(celery_task_id="worker-b")
        if raises:
            raise ValueError("boom")
        return "stale result"

    _BEHAVIOUR["limited"] = write_then_lose_the_claim

    with (
        structlog.testing.capture_logs() as logs,
        patch.object(limited_task, "apply_async") as mock_apply_async,
    ):
        assert _run(limited_task) is None

    mock_apply_async.assert_not_called()
    assert [log["event"] for log in logs if log["log_level"] == "warning"] == [
        "Discarding outcome of a task attempt taken over by a newer one"
    ]
    assert _usernames() == []
    assert attempt_record(limited_attempt.id) == {
        "status": TaskAttempt.Status.RUNNING,
        "attempt_count": 1,
        "celery_task_id": "worker-a",
        "celery_queue_override": None,
        "execution_result": None,
        "next_retry_at": None,
        "lease": timedelta(seconds=130),
    }


# transaction=True in the tests below: they check what the task's own transaction commits.
@pytest.mark.django_db(transaction=True)
@freeze_time(NOW)
def test_failure_commits_the_functions_writes_before_the_retry_is_sent(limited_task, limited_attempt):
    def store_progress_then_fail(block_number: int, **kwargs: Any) -> None:
        get_user_model().objects.create(username="progress")
        raise ValueError("boom")

    _BEHAVIOUR["limited"] = store_progress_then_fail
    seen_when_sent = []

    def send(**kwargs: Any) -> None:
        seen_when_sent.append({"in_transaction": connection.in_atomic_block, "usernames": _usernames()})

    with patch.object(limited_task, "apply_async", side_effect=send) as mock_apply_async:
        assert _run(limited_task) is None

    assert seen_when_sent == [{"in_transaction": False, "usernames": ["progress"]}]
    mock_apply_async.assert_called_once_with(kwargs={"block_number": BLOCK}, eta=RETRY_AT, queue=CLAIMS.name)
    assert attempt_record(limited_attempt.id) == WAITING_FOR_ITS_RETRY


@pytest.mark.django_db(transaction=True)
@freeze_time(NOW)
def test_writes_the_function_wraps_in_atomic_roll_back_when_it_raises_from_them(limited_task, limited_attempt):
    def store_progress_then_fail_inside_atomic(block_number: int, **kwargs: Any) -> None:
        get_user_model().objects.create(username="progress")
        with transaction.atomic():
            get_user_model().objects.create(username="undone")
            raise ValueError("boom")

    _BEHAVIOUR["limited"] = store_progress_then_fail_inside_atomic

    with patch.object(limited_task, "apply_async") as mock_apply_async:
        assert _run(limited_task) is None

    assert _usernames() == ["progress"]
    mock_apply_async.assert_called_once_with(kwargs={"block_number": BLOCK}, eta=RETRY_AT, queue=CLAIMS.name)
    assert attempt_record(limited_attempt.id) == WAITING_FOR_ITS_RETRY


def _fail_outside_a_savepoint(block_number: int, **kwargs: Any) -> None:
    """Raise IntegrityError from an atomic block without a savepoint, which marks the transaction for rollback."""
    get_user_model().objects.create(username="progress")
    with transaction.atomic(savepoint=False):
        get_user_model().objects.create(username="progress")


def _fail_after_a_write_its_foreign_key_rejects_at_commit(block_number: int, **kwargs: Any) -> None:
    """Add a user to a group that does not exist; the deferred foreign key fails the commit."""
    user = get_user_model().objects.create(username="progress")
    get_user_model().groups.through.objects.create(user=user, group_id=999_999)
    raise ValueError("boom")


@pytest.mark.django_db(transaction=True)
@freeze_time(NOW)
@pytest.mark.parametrize(
    ("func", "function_error", "commit_error"),
    [
        (_fail_outside_a_savepoint, "IntegrityError", "TransactionManagementError"),
        (_fail_after_a_write_its_foreign_key_rejects_at_commit, "ValueError", "IntegrityError"),
    ],
    ids=["database error outside a savepoint", "foreign key fails at commit"],
)
def test_failure_whose_transaction_cannot_commit_is_recorded_without_its_writes(
    limited_task, limited_attempt, func, function_error, commit_error
):
    _BEHAVIOUR["limited"] = func

    with (
        structlog.testing.capture_logs() as logs,
        patch.object(limited_task, "apply_async") as mock_apply_async,
    ):
        assert _run(limited_task) is None

    assert [(log["log_level"], log["event"], log["error_type"]) for log in logs if "error_type" in log] == [
        ("error", "Task execution failed", function_error),
        (
            "warning",
            "Discarding database writes of a failed task attempt whose transaction could not commit",
            commit_error,
        ),
    ]
    assert _usernames() == []
    mock_apply_async.assert_called_once_with(kwargs={"block_number": BLOCK}, eta=RETRY_AT, queue=CLAIMS.name)
    assert attempt_record(limited_attempt.id) == WAITING_FOR_ITS_RETRY


@pytest.mark.django_db(transaction=True)
@freeze_time(NOW)
def test_commit_callbacks_of_a_failing_function_run_once_failed_commits(limited_task, limited_attempt):
    seen_by_callbacks = []

    def register_callbacks_then_fail(block_number: int, **kwargs: Any) -> None:
        transaction.on_commit(lambda: seen_by_callbacks.append(("kept", attempt_record(limited_attempt.id))))
        with contextlib.suppress(ValueError), transaction.atomic():
            transaction.on_commit(lambda: seen_by_callbacks.append(("rolled back", None)))
            raise ValueError("undo")
        raise ValueError("boom")

    _BEHAVIOUR["limited"] = register_callbacks_then_fail

    with patch.object(limited_task, "apply_async"):
        assert _run(limited_task) is None

    assert seen_by_callbacks == [("kept", FAILED_WITH_A_RETRY_TIME)]
