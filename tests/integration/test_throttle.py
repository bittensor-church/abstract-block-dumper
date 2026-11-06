from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import PropertyMock, patch

import pytest
import structlog
from django.db import connection
from django.db.models import DurationField, ExpressionWrapper, F
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from freezegun import freeze_time

import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.db_clock import db_now_offset
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services import metrics
from abstract_block_dumper._internal.services.backfill_scheduler import (
    HEAD_REFRESH_SECONDS,
    backfill_scheduler_factory,
)
from abstract_block_dumper._internal.services.block_processor import block_processor_factory
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.queue_monitor import QueueMonitor
from abstract_block_dumper._internal.services.window_backfiller import WindowBackfiller
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.dumper import BlockDumper, Queue, RetryPolicy
from tests.fatories import TaskAttemptFactory

PENDING = TaskAttempt.Status.PENDING
RUNNING = TaskAttempt.Status.RUNNING
SUCCESS = TaskAttempt.Status.SUCCESS
LIVE = Queue("events-live", time_limit=120)
BACKFILL = Queue("events-backfill", time_limit=120, throttle=3)
SHARED = Queue("rare", time_limit=120, throttle=2)
THROTTLED = "abstract_block_dumper._internal.services.throttle.record_throttled"
THROTTLE_SLEEP = "abstract_block_dumper._internal.services.throttle.time.sleep"
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def index_events(block_number: int) -> int:
    return block_number


def rare_task(block_number: int) -> int:
    return block_number


def _inflight_row(block_number: int, queue: str = "events-backfill", **fields) -> TaskAttempt:
    attempt = TaskAttemptFactory(
        block_number=block_number,
        executable_path=abd_utils.get_executable_path(index_events),
        status=fields.pop("status", PENDING),
        dispatch_queue=queue,
        **fields,
    )
    TaskAttempt.objects.filter(id=attempt.id).update(dispatched_at=db_now_offset(0))
    return attempt


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


@pytest.mark.django_db
def test_only_attempts_still_carried_by_a_message_or_a_worker_count_as_in_flight():
    counted = [
        _inflight_row(1, status=RUNNING, lease_expires_at=None),
        _inflight_row(2),
    ]
    live_lease = _inflight_row(3, status=RUNNING)
    TaskAttempt.objects.filter(id=live_lease.id).update(lease_expires_at=db_now_offset(60))
    counted.append(live_lease)
    expired_lease = _inflight_row(4, status=RUNNING)
    TaskAttempt.objects.filter(id=expired_lease.id).update(lease_expires_at=db_now_offset(-60))
    expired_message = _inflight_row(5)
    TaskAttempt.objects.filter(id=expired_message.id).update(dispatched_at=db_now_offset(-3600))
    _inflight_row(6, next_retry_at=timezone.now() + timedelta(minutes=5))
    _inflight_row(7, status=SUCCESS)
    _inflight_row(8, queue="other-queue")

    count = queue_attempts.count_inflight("events-backfill", ttl_window=360, stale_before=None)

    assert count == len(counted)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("ttl_window", "stale_after"),
    [(70, None), (None, timedelta(seconds=70))],
    ids=["queue with a message TTL", "queue without one, measured by the stuck timeout"],
)
def test_retry_counts_as_in_flight_for_one_window_after_its_eta(ttl_window, stale_after):
    now = timezone.now()
    just_due = _inflight_row(1, next_retry_at=now - timedelta(seconds=1))
    still_alive = _inflight_row(2, next_retry_at=now - timedelta(seconds=60))
    lost = _inflight_row(3, next_retry_at=now - timedelta(seconds=80))
    # A retry delay longer than the window: the message was sent long before its ETA.
    TaskAttempt.objects.filter(id__in=[just_due.id, still_alive.id, lost.id]).update(
        dispatched_at=db_now_offset(-1200), updated_at=now - timedelta(seconds=1200)
    )

    count = queue_attempts.count_inflight(
        "events-backfill", ttl_window=ttl_window, stale_before=now - stale_after if stale_after else None
    )

    assert count == 2


class _RecordingGauge:
    """A Prometheus gauge with a `queue` label that keeps the last value set per queue."""

    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def labels(self, *, queue: str) -> SimpleNamespace:
        return SimpleNamespace(set=lambda value: self.values.__setitem__(queue, value))


@pytest.fixture
def inflight_gauge():
    gauge = _RecordingGauge()
    with patch.object(metrics, "PROMETHEUS_AVAILABLE", True), patch.object(metrics, "INFLIGHT_TASKS", gauge):
        yield gauge


@pytest.mark.django_db
def test_live_scheduler_keeps_the_inflight_gauge_of_throttled_queues_current(events_task, inflight_gauge):
    inflight = [_inflight_row(block) for block in (1, 2, 3)]
    lost = _inflight_row(4)
    TaskAttempt.objects.filter(id=lost.id).update(updated_at=timezone.now() - timedelta(hours=1))

    QueueMonitor().check()
    while_running = dict(inflight_gauge.values)
    TaskAttempt.objects.filter(id__in=[attempt.id for attempt in inflight]).update(status=SUCCESS)
    QueueMonitor().check()

    assert (while_running, inflight_gauge.values) == ({"events-backfill": 3}, {"events-backfill": 0})


@pytest.mark.django_db
def test_lookback_publishes_the_inflight_count_it_reads(events_task, events_item, inflight_gauge):
    _inflight_row(50)

    with patch.object(events_task, "apply_async"):
        WindowBackfiller(block_processor_factory().executor).process_item_range(events_item, 100, 104, head_block=105)

    assert inflight_gauge.values == {"events-backfill": 1}


@pytest.mark.django_db
def test_lookback_submits_only_what_the_throttle_has_room_for_and_the_rest_later(events_task, events_item):
    _inflight_row(50)
    backfiller = WindowBackfiller(block_processor_factory().executor)

    with patch.object(events_task, "apply_async") as mock_apply_async, patch(THROTTLED) as mock_throttled:
        submitted = backfiller.process_item_range(events_item, 100, 104, head_block=105)

    assert submitted == 2
    assert [c.kwargs["kwargs"]["block_number"] for c in mock_apply_async.call_args_list] == [100, 101]
    assert [c.args for c in mock_throttled.call_args_list] == [("events-backfill", "lookback")] * 3

    TaskAttempt.objects.update(status=SUCCESS)
    with patch.object(events_task, "apply_async") as mock_apply_async:
        backfiller.process_item_range(events_item, 100, 104, head_block=106)

    assert [c.kwargs["kwargs"]["block_number"] for c in mock_apply_async.call_args_list] == [102, 103, 104]


@pytest.mark.django_db
def test_archive_usage_counts_only_messages_that_went_out(events_task, events_item):
    _inflight_row(50)
    backfiller = WindowBackfiller(block_processor_factory().executor)

    with (
        patch.object(events_task, "apply_async") as mock_apply_async,
        patch(f"{QueueExecutor.__module__}.increment_archive_network_usage") as archive_usage,
    ):
        submitted = backfiller.process_item_range(events_item, 100, 104, head_block=1000)

    assert submitted == 2
    assert mock_apply_async.call_count == 2
    assert archive_usage.call_count == 2


@pytest.mark.django_db
def test_live_submissions_ignore_a_full_queue_and_backfills_get_what_they_leave(dumper):
    celery_task = dumper.block_task(live_queue=SHARED, backfill_queue=SHARED, backfilling_lookback=5)(rare_task)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(rare_task))
    processor = block_processor_factory()

    with patch.object(celery_task, "apply_async") as mock_apply_async:
        for head in (200, 201, 202):
            processor.process_block(head, registry_items=[registry_item])
        WindowBackfiller(processor.executor).process_item_range(registry_item, 190, 194, head_block=202)

    # The throttle of 2 is full after two live submissions; the third still goes out, backfill waits.
    assert [c.kwargs["kwargs"]["block_number"] for c in mock_apply_async.call_args_list] == [200, 201, 202]


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_waits_for_room_then_continues(mock_get_bittensor_client, events_task):
    mock_get_bittensor_client.return_value.block = 500
    blockers = [_inflight_row(block) for block in (90, 91, 92)]

    def a_worker_finishes_one(_seconds):
        TaskAttempt.objects.filter(id=blockers.pop().id).update(status=SUCCESS)

    scheduler = backfill_scheduler_factory(from_block=100, to_block=101, rate_limit=0)
    with (
        patch.object(events_task, "apply_async") as mock_apply_async,
        patch(
            "abstract_block_dumper._internal.services.throttle.time.sleep", side_effect=a_worker_finishes_one
        ) as sleep,
    ):
        scheduler.start()

    assert [c.kwargs["kwargs"]["block_number"] for c in mock_apply_async.call_args_list] == [100, 101]
    assert sleep.call_count == 2


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_stopping_the_backfill_ends_the_wait_without_sending(mock_get_bittensor_client, events_task):
    mock_get_bittensor_client.return_value.block = 500
    for block in (90, 91, 92):
        _inflight_row(block)
    scheduler = backfill_scheduler_factory(from_block=100, to_block=100, rate_limit=0)

    with (
        patch.object(events_task, "apply_async") as mock_apply_async,
        patch("abstract_block_dumper._internal.services.throttle.time.sleep", side_effect=lambda _s: scheduler.stop()),
    ):
        scheduler.start()

    mock_apply_async.assert_not_called()


@pytest.fixture
def throttled_backfill_task(dumper):
    queue = Queue("slow-backfill", throttle=1, retry=RetryPolicy(give_up_after_blocks=1000))
    return dumper.block_task(live_queue=LIVE, backfill_queue=queue)(rare_task)


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_block_is_measured_against_the_head_after_the_throttle_wait(mock_get_bittensor_client, throttled_backfill_task):
    chain = mock_get_bittensor_client.return_value
    chain.block = 1100  # block 1000 is 100 behind when the backfill starts
    blocker = _inflight_row(90, queue="slow-backfill")
    scheduler = backfill_scheduler_factory(from_block=1000, to_block=1000, rate_limit=0)

    with freeze_time(NOW) as frozen:

        def the_chain_moves_while_a_worker_finishes(_seconds):
            TaskAttempt.objects.filter(id=blocker.id).update(status=SUCCESS)
            chain.block = 1500
            frozen.tick(timedelta(seconds=HEAD_REFRESH_SECONDS))

        with (
            patch.object(throttled_backfill_task, "apply_async") as mock_apply_async,
            patch(THROTTLE_SLEEP, side_effect=the_chain_moves_while_a_worker_finishes),
        ):
            scheduler.start()

    mock_apply_async.assert_called_once_with(
        kwargs={"block_number": 1000, "_use_archive_network": True}, queue="slow-backfill"
    )
    time_left = (
        TaskAttempt.objects.filter(block_number=1000)
        .annotate(left=ExpressionWrapper(F("give_up_at") - F("dispatched_at"), output_field=DurationField()))
        .values_list("left", flat=True)
        .get()
    )
    # 500 of the 1000 blocks are left at 12 s each; the deadline and the dispatch are two statements apart.
    assert timedelta(seconds=6000) - timedelta(seconds=1) < time_left <= timedelta(seconds=6000)


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_failed_head_read_keeps_the_last_known_head_and_the_backfill_going(
    mock_get_bittensor_client, throttled_backfill_task
):
    block = PropertyMock(side_effect=[1100, RuntimeError("rpc down")])
    type(mock_get_bittensor_client.return_value).block = block
    scheduler = backfill_scheduler_factory(from_block=1000, to_block=1000, rate_limit=0)
    blocker = _inflight_row(90, queue="slow-backfill")

    with freeze_time(NOW) as frozen:

        def a_worker_finishes(_seconds):
            TaskAttempt.objects.filter(id=blocker.id).update(status=SUCCESS)
            frozen.tick(timedelta(seconds=HEAD_REFRESH_SECONDS))

        with (
            structlog.testing.capture_logs() as logs,
            patch.object(throttled_backfill_task, "apply_async") as mock_apply_async,
            patch(THROTTLE_SLEEP, side_effect=a_worker_finishes),
        ):
            scheduler.start()

    assert mock_apply_async.call_args.kwargs["kwargs"] == {"block_number": 1000, "_use_archive_network": False}
    assert block.call_count == 2
    assert [log["last_known_head"] for log in logs if log["event"].startswith("Failed to read the chain head")] == [
        1100
    ]


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_backfill_does_not_read_the_head_for_every_block(mock_get_bittensor_client, events_task):
    block = PropertyMock(return_value=500)
    type(mock_get_bittensor_client.return_value).block = block
    scheduler = backfill_scheduler_factory(from_block=100, to_block=102, rate_limit=0)

    with patch.object(events_task, "apply_async") as mock_apply_async:
        scheduler.start()

    assert mock_apply_async.call_count == 3
    assert block.call_count == 1


@pytest.mark.django_db
def test_free_room_is_counted_once_per_batch(dumper):
    big_backfill = Queue("big-backfill", throttle=10)
    celery_task = dumper.block_task(live_queue=LIVE, backfill_queue=big_backfill, backfilling_lookback=5)(rare_task)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(rare_task))

    backfiller = WindowBackfiller(block_processor_factory().executor)

    with patch.object(celery_task, "apply_async"), CaptureQueriesContext(connection) as queries:
        backfiller.process_item_range(registry_item, 100, 104, head_block=105)

    assert sum("COUNT(" in query["sql"] for query in queries.captured_queries) == 1
