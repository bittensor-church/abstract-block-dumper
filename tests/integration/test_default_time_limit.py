from datetime import timedelta
from typing import Any

import pytest
from celery import Celery, Task
from django.test import override_settings
from django.utils import timezone

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.queue_backfiller import QueueBackfiller
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.reconciler import queue_reconciler_factory
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from abstract_block_dumper.v1.dumper import BlockDumper, Queue
from tests.fatories import TaskAttemptFactory
from tests.sent_messages import sent_time_limits

TIMEOUT = "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS"
MARGIN = "BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS"


class HardLimitedTask(Task):
    time_limit = 600


class SoftLimitedTask(Task):
    soft_time_limit = 3000


class LowHardLimitTask(Task):
    time_limit = 200


def plain_task(block_number: int) -> int:
    return block_number


def failing_task(block_number: int) -> None:
    raise ValueError("RPC node down")


def shared_task_without_limit(block_number: int) -> int:
    return block_number


@pytest.fixture
def app() -> Celery:
    return Celery("default-limits", broker="memory://")


@pytest.mark.parametrize(
    ("queue_limits", "celery_kwargs", "app_conf", "limit_settings", "expected_attributes", "expected_sent"),
    [
        pytest.param({}, {}, {}, {}, (None, 1800), [1800, None], id="default"),
        pytest.param({}, {}, {}, {TIMEOUT: 900}, (None, 900), [900, None], id="lower setting"),
        pytest.param({}, {}, {}, {TIMEOUT: None}, (None, None), [None, None], id="setting None"),
        pytest.param({}, {"time_limit": 600}, {}, {}, (None, 600), [600, None], id="celery_kwargs limit"),
        pytest.param({}, {"time_limit": None}, {}, {}, (None, None), [None, None], id="limit removed"),
        pytest.param({}, {"base": HardLimitedTask}, {}, {}, (None, 600), [600, None], id="base class limit"),
        pytest.param({}, {}, {"task_time_limit": 600}, {}, (None, None), [None, None], id="config limit"),
        pytest.param({"time_limit": 60}, {}, {}, {}, (None, 1800), [60, None], id="queue limit below"),
        pytest.param({"time_limit": 2400}, {}, {}, {}, (None, 1800), [2400, None], id="queue limit above"),
        pytest.param({"soft_time_limit": 100}, {}, {}, {}, (None, 1800), [1800, 100], id="queue soft limit below"),
        pytest.param({"soft_time_limit": 3000}, {}, {}, {}, (None, 3010), [3010, 3000], id="queue soft limit above"),
        pytest.param(
            {"soft_time_limit": 3000}, {}, {}, {MARGIN: 30}, (None, 3030), [3030, 3000], id="higher recovery margin"
        ),
        pytest.param(
            {"soft_time_limit": 3000, "time_limit": 3600},
            {},
            {},
            {},
            (None, 1800),
            [3600, 3000],
            id="queue with both limits",
        ),
        pytest.param({}, {"soft_time_limit": 3000}, {}, {}, (3000, 3010), [3010, 3000], id="celery_kwargs soft limit"),
        pytest.param({}, {"base": SoftLimitedTask}, {}, {}, (3000, 3010), [3010, 3000], id="base class soft limit"),
        pytest.param({}, {}, {"task_soft_time_limit": 3000}, {}, (None, 3010), [3010, None], id="config soft limit"),
        pytest.param(
            {"soft_time_limit": 300},
            {"time_limit": None},
            {},
            {},
            (None, None),
            [None, 300],
            id="queue soft limit without any hard limit",
        ),
    ],
)
def test_block_dumper_task_limits(
    app, queue_limits, celery_kwargs, app_conf, limit_settings, expected_attributes, expected_sent
):
    app.conf.update(app_conf)
    with override_settings(**limit_settings):
        celery_task = BlockDumper(celery_app=app).block_task(
            live_queue=Queue("default-limit", **queue_limits), celery_kwargs=celery_kwargs
        )(plain_task)

    celery_task.delay(block_number=1)

    assert (celery_task.soft_time_limit, celery_task.time_limit) == expected_attributes
    assert sent_time_limits(app, "default-limit") == [expected_sent]


@pytest.mark.parametrize(
    ("live_queue", "backfill_queue", "celery_kwargs", "app_conf", "limit_settings", "expected_error"),
    [
        pytest.param(
            Queue("limits", soft_time_limit=300),
            None,
            {"time_limit": 120},
            {},
            {},
            "Queue 'limits': soft_time_limit 300 s (Queue) must be lower than time_limit 120 s (celery_kwargs)",
            id="celery_kwargs hard limit",
        ),
        pytest.param(
            Queue("limits", soft_time_limit=300),
            None,
            {"base": LowHardLimitTask},
            {},
            {},
            "Queue 'limits': soft_time_limit 300 s (Queue) must be lower than "
            "time_limit 200 s (LowHardLimitTask.time_limit)",
            id="base class hard limit",
        ),
        pytest.param(
            Queue("limits", soft_time_limit=300),
            None,
            {},
            {"task_time_limit": 100},
            {},
            "Queue 'limits': soft_time_limit 300 s (Queue) must be lower than "
            "time_limit 100 s (Celery's task_time_limit)",
            id="config hard limit",
        ),
        pytest.param(
            Queue("limits", time_limit=600),
            None,
            {},
            {"task_soft_time_limit": 900},
            {},
            "Queue 'limits': soft_time_limit 900 s (Celery's task_soft_time_limit) must be lower than "
            "time_limit 600 s (Queue)",
            id="config soft limit",
        ),
        pytest.param(
            Queue("limits"),
            None,
            {"soft_time_limit": 600, "time_limit": 600},
            {},
            {},
            "Queue 'limits': soft_time_limit 600 s (celery_kwargs) must be lower than time_limit 600 s (celery_kwargs)",
            id="equal celery_kwargs limits",
        ),
        pytest.param(
            Queue("limits-live", time_limit=60),
            Queue("limits-backfill", soft_time_limit=300),
            {"time_limit": 120},
            {},
            {},
            "Queue 'limits-backfill': soft_time_limit 300 s (Queue) must be lower than "
            "time_limit 120 s (celery_kwargs)",
            id="backfill queue",
        ),
        pytest.param(
            Queue("limits", soft_time_limit=100, time_limit=120),
            None,
            {"soft_time_limit": 600, "time_limit": 300},
            {},
            {},
            "Messages without queue limits: soft_time_limit 600 s (celery_kwargs) must be lower than "
            "time_limit 300 s (celery_kwargs)",
            id="messages without queue limits",
        ),
        pytest.param(
            Queue("limits", soft_time_limit=2000),
            None,
            {},
            {},
            {MARGIN: 0},
            "Queue 'limits': soft_time_limit 2000 s (Queue) must be lower than "
            "time_limit 2000 s (the default hard limit)",
            id="no recovery margin",
        ),
    ],
)
def test_declaration_rejects_a_soft_limit_at_or_above_the_hard_limit(
    app, live_queue, backfill_queue, celery_kwargs, app_conf, limit_settings, expected_error
):
    app.conf.update(app_conf)
    dumper = BlockDumper(celery_app=app)

    with override_settings(**limit_settings), pytest.raises(ValueError) as raised:
        dumper.block_task(live_queue=live_queue, backfill_queue=backfill_queue, celery_kwargs=celery_kwargs)(plain_task)

    assert (str(raised.value), dumper.queues, task_registry.get_functions()) == (expected_error, [], [])


def test_block_task_gets_no_default_limit(app):
    celery_task = block_task(shared_task_without_limit)

    celery_task.delay(block_number=1)

    assert (celery_task.soft_time_limit, celery_task.time_limit) == (None, None)
    assert sent_time_limits(app, "celery") == [[None, None]]


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("live_limits", "expected_live", "expected_backfill"),
    [
        ({"soft_time_limit": 3000}, [[3010, 3000]], [[3010, None]]),
        ({"time_limit": 60}, [[60, None]], [[1800, None]]),
    ],
    ids=["live soft limit raises the default", "live hard limit leaves the default"],
)
def test_backfill_queue_without_limits_runs_on_the_default(app, live_limits, expected_live, expected_backfill):
    celery_task = BlockDumper(celery_app=app).block_task(
        live_queue=Queue("two-queues-live", **live_limits), backfill_queue=Queue("two-queues-backfill")
    )(plain_task)
    registry_item = task_registry.get_by_executable_path(abd_utils.get_executable_path(plain_task))

    celery_task.delay(block_number=1)
    QueueBackfiller(QueueExecutor()).submit_block(registry_item, 100, {}, executed_blocks=set(), head_block=110)

    assert sent_time_limits(app, "two-queues-live") == expected_live
    assert sent_time_limits(app, "two-queues-backfill") == expected_backfill


def _attempt(func: Any, **fields: Any) -> TaskAttempt:
    return TaskAttemptFactory(
        block_number=100,
        executable_path=abd_utils.get_executable_path(func),
        status=TaskAttempt.Status.PENDING,
        **fields,
    )


@pytest.mark.django_db
def test_retry_runs_on_the_default_limit(app):
    celery_task = BlockDumper(celery_app=app).block_task(live_queue=Queue("retrying"))(failing_task)
    _attempt(failing_task, celery_queue_override="retrying", dispatch_queue="retrying")

    celery_task(100)

    assert sent_time_limits(app, "retrying") == [[1800, None]]


@pytest.mark.django_db
def test_redispatch_of_an_attempt_whose_queue_was_removed_runs_on_the_default_limit(app):
    BlockDumper(celery_app=app).block_task(live_queue=Queue("current"))(plain_task)
    attempt = _attempt(plain_task, celery_queue_override="removed", dispatch_queue="removed")
    TaskAttempt.objects.filter(id=attempt.id).update(updated_at=timezone.now() - timedelta(hours=1))

    recovered = queue_reconciler_factory().reconcile(stale_after_seconds=1800, limit=10, head_block=None)

    assert recovered == 1
    assert sent_time_limits(app, "current") == [[1800, None]]


@override_settings(BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=-1)
def test_invalid_timeout_fails_when_the_task_is_declared(app):
    with pytest.raises(ValueError, match="BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS must be a positive number or None"):
        BlockDumper(celery_app=app).block_task(live_queue=Queue("invalid"))(plain_task)


@pytest.mark.parametrize("margin", [-5, "10"])
@pytest.mark.parametrize(
    ("celery_kwargs", "app_conf", "limit_settings"),
    [
        pytest.param({}, {}, {}, id="default limit"),
        pytest.param({"time_limit": 600}, {}, {}, id="celery_kwargs limit"),
        pytest.param({"base": HardLimitedTask}, {}, {}, id="base class limit"),
        pytest.param({}, {"task_time_limit": 600}, {}, id="config limit"),
        pytest.param({}, {}, {TIMEOUT: None}, id="no stuck-task timeout"),
    ],
)
def test_invalid_recovery_margin_fails_when_the_task_is_declared(app, celery_kwargs, app_conf, limit_settings, margin):
    app.conf.update(app_conf)
    dumper = BlockDumper(celery_app=app)

    with override_settings(**limit_settings, **{MARGIN: margin}), pytest.raises(ValueError) as raised:
        dumper.block_task(live_queue=Queue("invalid"), celery_kwargs=celery_kwargs)(plain_task)

    assert (str(raised.value), dumper.queues, task_registry.get_functions()) == (
        "BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS must be a non-negative number",
        [],
        [],
    )
