from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from celery import Celery
from django.utils import timezone
from freezegun import freeze_time

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.queue_backfiller import QueueBackfiller
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.reconciler import queue_reconciler_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.dumper import BlockDumper, Queue, RetryPolicy
from tests.fatories import TaskAttemptFactory
from tests.sent_messages import sent_time_limits

LIVE = Queue("events-live", soft_time_limit=100, time_limit=120)
BACKFILL = Queue("events-backfill", soft_time_limit=200, time_limit=240)
NOW = datetime(2026, 1, 1, tzinfo=UTC)


# One function per registration: Celery's task registry is keyed by name.
def index_events(block_number: int) -> int:
    return block_number


def failing_events(block_number: int) -> None:
    raise ValueError("RPC node down")


def shared_queue_task(block_number: int) -> int:
    return block_number


@pytest.fixture
def dumper(celery_test_app) -> BlockDumper:
    return BlockDumper(celery_app=celery_test_app)


@pytest.fixture
def events_task(dumper):
    return dumper.block_task(live_queue=LIVE, backfill_queue=BACKFILL, backfilling_lookback=10)(index_events)


@pytest.fixture
def events_item(events_task):
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(index_events))
    assert registry_item is not None
    return registry_item


def _record(executable_path: str, block_number: int) -> dict[str, Any]:
    return (
        TaskAttempt.objects.filter(executable_path=executable_path, block_number=block_number)
        .values("status", "celery_queue_override", "dispatch_queue")
        .get()
    )


@pytest.mark.django_db
def test_task_is_registered_on_the_dumper_app_with_the_live_queue_as_its_default(
    celery_test_app, dumper, events_task, events_item
):
    assert celery_test_app.tasks[events_item.executable_path] is events_task._get_current_object()
    assert (events_task.queue, events_task.soft_time_limit, events_task.time_limit) == ("events-live", None, 1800)
    assert (events_item.live_queue, events_item.backfill_queue) == (LIVE, BACKFILL)
    assert dumper.queues == [LIVE, BACKFILL]


@pytest.fixture
def memory_broker_app() -> Celery:
    return Celery("limits", broker="memory://")


def test_manual_sends_carry_the_limits_of_the_live_queue(memory_broker_app):
    live_queue = Queue("manual-live", soft_time_limit=30, time_limit=40)
    celery_task = BlockDumper(celery_app=memory_broker_app).block_task(live_queue=live_queue)(shared_queue_task)

    celery_task.delay(block_number=1)
    celery_task.s(block_number=2).delay()
    celery_task.apply_async(kwargs={"block_number": 3}, time_limit=60)

    assert sent_time_limits(memory_broker_app, "manual-live") == [[40, 30], [40, 30], [60, 30]]


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("backfill_limits", "celery_kwargs", "expected", "expected_attributes"),
    [
        ({}, {}, [1800, None], (None, 1800)),
        ({"time_limit": 600}, {}, [600, None], (None, 1800)),
        ({"soft_time_limit": 500}, {}, [1800, 500], (None, 1800)),
        ({"soft_time_limit": 500, "time_limit": 600}, {}, [600, 500], (None, 1800)),
        ({"time_limit": 600}, {"soft_time_limit": 77, "time_limit": 88}, [600, 77], (77, 88)),
    ],
    ids=["no limits", "hard limit only", "soft limit only", "both limits", "celery_kwargs fill in"],
)
def test_backfill_messages_never_carry_the_limits_of_the_live_queue(
    memory_broker_app, backfill_limits, celery_kwargs, expected, expected_attributes
):
    backfill_queue = Queue("limited-backfill", **backfill_limits)
    celery_task = BlockDumper(celery_app=memory_broker_app).block_task(
        live_queue=Queue("limited-live", soft_time_limit=30, time_limit=40),
        backfill_queue=backfill_queue,
        celery_kwargs=celery_kwargs,
    )(shared_queue_task)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(shared_queue_task))

    QueueBackfiller(QueueExecutor()).submit_block(registry_item, 100, {}, executed_blocks=set(), head_block=110)

    assert sent_time_limits(memory_broker_app, "limited-backfill") == [expected]
    # The attributes are what a worker falls back to for a message that sets no limit.
    assert (celery_task.soft_time_limit, celery_task.time_limit) == expected_attributes


def test_task_keeps_a_base_class_from_celery_kwargs(celery_test_app, dumper):
    class AuditedTask(celery_test_app.Task):
        pass

    celery_task = dumper.block_task(live_queue=LIVE, celery_kwargs={"base": AuditedTask})(shared_queue_task)

    assert isinstance(celery_task._get_current_object(), AuditedTask)


def test_queue_settings_win_over_celery_kwargs(memory_broker_app):
    live_queue = Queue("winning-live", soft_time_limit=100, time_limit=120)
    celery_task = BlockDumper(celery_app=memory_broker_app).block_task(
        live_queue=live_queue, celery_kwargs={"queue": "elsewhere", "time_limit": 999, "acks_late": True}
    )(shared_queue_task)

    celery_task.delay(block_number=1)

    assert (celery_task.queue, celery_task.acks_late) == ("winning-live", True)
    assert sent_time_limits(memory_broker_app, "winning-live") == [[120, 100]]


def test_one_queue_may_serve_both_roles(dumper):
    dumper.block_task(live_queue=LIVE, backfill_queue=LIVE, backfilling_lookback=5)(shared_queue_task)

    assert dumper.queues == [LIVE]


@pytest.mark.parametrize(
    ("decorator_kwargs", "message"),
    [
        (
            {"live_queue": LIVE, "backfilling_lookback": 5},
            "backfilling_lookback requires backfill_queue; pass the live queue again to backfill on the same queue",
        ),
        ({"live_queue": "events-live"}, "BlockDumper queues must be Queue instances, got 'events-live'"),
        (
            {"live_queue": Queue("events-live", time_limit=60)},
            "Queue 'events-live' is already defined with different settings",
        ),
    ],
    ids=["lookback without a backfill queue", "queue name instead of a Queue", "one name, two policies"],
)
def test_misconfigured_task_fails_at_import(dumper, decorator_kwargs, message):
    dumper.block_task(live_queue=LIVE)(index_events)

    with pytest.raises(ValueError, match=message):
        dumper.block_task(**decorator_kwargs)(shared_queue_task)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"time_limit": 0}, "Queue 'invalid': time_limit must be a positive number or None"),
        ({"soft_time_limit": True}, "Queue 'invalid': soft_time_limit must be a positive number or None"),
        ({"message_ttl": "60"}, "Queue 'invalid': message_ttl must be a positive number or None"),
        ({"throttle": 0}, "Queue 'invalid': throttle must be a positive integer or None"),
        ({"throttle": 2.0}, "Queue 'invalid': throttle must be a positive integer or None"),
        ({"throttle": True}, "Queue 'invalid': throttle must be a positive integer or None"),
    ],
)
def test_invalid_queue_is_rejected(fields, message):
    with pytest.raises(ValueError) as raised:
        Queue("invalid", **fields)

    assert str(raised.value) == message


@pytest.mark.parametrize("block_time_seconds", [0, -1, True, "12"])
def test_invalid_block_time_is_rejected(celery_test_app, block_time_seconds):
    with pytest.raises(ValueError) as raised:
        BlockDumper(celery_app=celery_test_app, block_time_seconds=block_time_seconds)

    assert (str(raised.value), dumper_registry.dumpers()) == ("block_time_seconds must be a positive number", [])


def test_one_dumper_per_celery_app(celery_test_app, dumper):
    with pytest.raises(ValueError, match="A BlockDumper already exists for Celery app 'test_app'"):
        BlockDumper(celery_app=celery_test_app)


def test_queue_belongs_to_one_dumper(dumper, memory_broker_app):
    dumper.block_task(live_queue=LIVE)(index_events)

    with pytest.raises(ValueError) as raised:
        BlockDumper(celery_app=memory_broker_app).block_task(live_queue=LIVE)(shared_queue_task)

    assert str(raised.value) == (
        "Queue 'events-live' already belongs to the BlockDumper of Celery app 'test_app'; give this queue another name"
    )


def test_task_that_fails_at_import_registers_none_of_its_queues(dumper):
    dumper.block_task(live_queue=BACKFILL)(index_events)

    with pytest.raises(ValueError, match="Queue 'events-backfill' is already defined with different settings"):
        dumper.block_task(live_queue=LIVE, backfill_queue=Queue("events-backfill", throttle=5))(shared_queue_task)

    assert (dumper.queues, dumper_registry.queues()) == ([BACKFILL], [BACKFILL])


@pytest.mark.django_db
def test_live_submission_goes_to_the_live_queue_with_its_limits(events_task, events_item):
    with patch.object(events_task, "apply_async") as mock_apply_async:
        block_processor_factory().process_block(100, registry_items=[events_item])

    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 100, "_use_archive_network": False},
        queue="events-live",
        soft_time_limit=100,
        time_limit=120,
    )
    assert _record(events_item.executable_path, 100) == {
        "status": TaskAttempt.Status.PENDING,
        "celery_queue_override": "events-live",
        "dispatch_queue": "events-live",
    }


@pytest.mark.django_db
def test_backfill_submission_goes_to_the_backfill_queue_with_its_limits(events_task, events_item):
    with patch.object(events_task, "apply_async") as mock_apply_async:
        QueueBackfiller(QueueExecutor()).submit_block(events_item, 100, {}, executed_blocks=set(), head_block=110)

    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 100, "_use_archive_network": False},
        queue="events-backfill",
        soft_time_limit=200,
        time_limit=240,
    )
    assert _record(events_item.executable_path, 100) == {
        "status": TaskAttempt.Status.PENDING,
        "celery_queue_override": "events-backfill",
        "dispatch_queue": "events-backfill",
    }


@pytest.mark.django_db
def test_backfill_of_a_waiting_attempt_keeps_the_queue_it_was_first_sent_to(events_task, events_item):
    processor = block_processor_factory()

    with patch.object(events_task, "apply_async") as mock_apply_async:
        processor.process_block(100, registry_items=[events_item])
        QueueBackfiller(QueueExecutor()).submit_block(events_item, 100, {}, executed_blocks=set(), head_block=110)

    assert [call.kwargs["queue"] for call in mock_apply_async.call_args_list] == ["events-live", "events-live"]
    assert _record(events_item.executable_path, 100) == {
        "status": TaskAttempt.Status.PENDING,
        "celery_queue_override": "events-live",
        "dispatch_queue": "events-live",
    }


def tight_backfill_task(block_number: int) -> int:
    return block_number


@pytest.mark.django_db
def test_backfill_with_a_tighter_block_age_limit_does_not_give_up_a_waiting_attempt(dumper):
    tight_backfill = Queue("tight-backfill", retry=RetryPolicy(give_up_after_blocks=10))
    celery_task = dumper.block_task(live_queue=LIVE, backfill_queue=tight_backfill)(tight_backfill_task)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(tight_backfill_task))
    processor = block_processor_factory()

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        processor.process_block(100, registry_items=[registry_item])
        QueueBackfiller(QueueExecutor()).submit_block(registry_item, 100, {}, executed_blocks=set(), head_block=1000)

    assert mock_apply_async.call_count == 2
    assert _record(registry_item.executable_path, 100) == {
        "status": TaskAttempt.Status.PENDING,
        "celery_queue_override": "events-live",
        "dispatch_queue": "events-live",
    }


@pytest.mark.django_db
@freeze_time(NOW)
def test_retry_keeps_the_queue_and_limits_of_the_failed_submission(dumper):
    celery_task = dumper.block_task(live_queue=LIVE, backfill_queue=BACKFILL)(failing_events)
    TaskAttemptFactory(
        block_number=100,
        executable_path=abd_utils.get_executable_path(failing_events),
        status=TaskAttempt.Status.PENDING,
        celery_queue_override="events-backfill",
        dispatch_queue="events-backfill",
    )

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        celery_task(100)

    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 100},
        eta=NOW + timedelta(seconds=5),  # the default RetryPolicy's first fast retry
        queue="events-backfill",
        soft_time_limit=200,
        time_limit=240,
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("recorded_queue", "expected"),
    [("events-live", LIVE), ("renamed-queue", BACKFILL)],
    ids=["queue still defined", "queue no longer defined"],
)
def test_stuck_attempt_is_redispatched_to_its_recorded_queue_while_it_is_defined(
    events_task, events_item, recorded_queue, expected
):
    attempt = TaskAttemptFactory(
        block_number=100,
        executable_path=events_item.executable_path,
        status=TaskAttempt.Status.PENDING,
        celery_queue_override=recorded_queue,
        dispatch_queue=recorded_queue,
    )
    TaskAttempt.objects.filter(id=attempt.id).update(updated_at=timezone.now() - timedelta(hours=1))

    with patch.object(events_task, "apply_async") as mock_apply_async:
        recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 1
    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 100, "_use_archive_network": False},
        queue=expected.name,
        soft_time_limit=expected.soft_time_limit,
        time_limit=expected.time_limit,
    )
    assert _record(events_item.executable_path, 100) == {
        "status": TaskAttempt.Status.PENDING,
        "celery_queue_override": expected.name,
        "dispatch_queue": expected.name,
    }
