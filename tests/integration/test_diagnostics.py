import sys
from contextlib import contextmanager
from io import StringIO
from unittest.mock import patch

import pytest
import structlog
from celery import Celery
from django.core.checks import run_checks
from django.core.management import call_command
from django.test import modify_settings
from kombu import Queue as KombuQueue

import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.services.queue_monitor import QueueMonitor
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.dumper import BlockDumper, Queue, RetryPolicy
from abstract_block_dumper.v1.testing import BrokerCall, MockBrokerContact, QueueStats, use_broker_contact
from tests.fatories import TaskAttemptFactory

DISCOVERY = "abstract_block_dumper._internal.checks._import_task_modules"
COMMAND_DISCOVERY = "abstract_block_dumper.management.commands.block_dumper_queues_v1.ensure_modules_loaded"

LIVE = Queue("events-live", soft_time_limit=100, time_limit=120, message_ttl=60)
BACKFILL = Queue("events-backfill", soft_time_limit=100, time_limit=120, message_ttl=300, throttle=600)
RARE = Queue("rare")
THROTTLED_LIVE = Queue("throttled-live", time_limit=60, throttle=5)
SOLO = Queue("solo", time_limit=60, throttle=5)
UNTHROTTLED_SOLO = Queue("unthrottled-solo", time_limit=60)


def index_events(block_number: int) -> int:
    return block_number


def rare_task(block_number: int) -> int:
    return block_number


def live_only_task(block_number: int) -> int:
    return block_number


def solo_task(block_number: int) -> int:
    return block_number


def unthrottled_solo_task(block_number: int) -> int:
    return block_number


@pytest.fixture
def app() -> Celery:
    return Celery("indexer", broker="memory://")


@pytest.fixture
def broker(app):
    with use_broker_contact(MockBrokerContact(app)) as contact:
        yield contact


@pytest.fixture
def dumper(app) -> BlockDumper:
    dumper = BlockDumper(celery_app=app)
    dumper.block_task(live_queue=LIVE, backfill_queue=BACKFILL, backfilling_lookback=10)(index_events)
    dumper.block_task(live_queue=RARE, backfill_queue=RARE)(rare_task)
    dumper.block_task(live_queue=THROTTLED_LIVE, backfill_queue=BACKFILL)(live_only_task)
    # Without a backfill_queue, backfills go to the live queue.
    dumper.block_task(live_queue=SOLO)(solo_task)
    dumper.block_task(live_queue=UNTHROTTLED_SOLO)(unthrottled_solo_task)
    return dumper


def _warnings() -> list[tuple[str, str]]:
    with patch(DISCOVERY):
        return [(message.id, message.msg) for message in run_checks(tags=["abstract_block_dumper"])]


@pytest.mark.parametrize(
    "project_queues",
    [
        pytest.param([KombuQueue("events-live")], id="list of queues"),
        pytest.param({"events-live": KombuQueue("events-live")}, id="mapping of queues"),
        pytest.param(
            {"events-live": {"exchange": "events-live", "routing_key": "events-live"}}, id="mapping of options"
        ),
    ],
)
def test_checks_warn_about_configurations_that_usually_mean_a_mistake(app, dumper, broker, project_queues):
    app.conf.task_queues = project_queues

    assert _warnings() == [
        (
            "abstract_block_dumper.W001",
            "Queue 'events-live' is in CELERY_TASK_QUEUES with arguments {}, "
            "but its BlockDumper policy needs {'x-message-ttl': 60000}.",
        ),
        ("abstract_block_dumper.W002", "Queue 'rare' serves live and backfill submissions without a throttle."),
        ("abstract_block_dumper.W005", "Queue 'rare' has no time_limit."),
        (
            "abstract_block_dumper.W003",
            "Queue 'throttled-live' has a throttle but no backfill or lookback submissions go to it.",
        ),
    ]


def test_checks_accept_a_project_queue_matching_the_policy_under_a_max_priority(app, broker):
    app.conf.task_queue_max_priority = 10
    app.conf.task_queues = [KombuQueue("events-live", queue_arguments={"x-message-ttl": 60_000})]
    BlockDumper(celery_app=app).block_task(live_queue=LIVE)(index_events)

    assert _warnings() == []


@pytest.mark.parametrize(
    ("project_queue", "expected"),
    [
        pytest.param(KombuQueue("events-live", message_ttl=60), [], id="message_ttl option"),
        pytest.param(
            KombuQueue("events-live", queue_arguments={"x-message-ttl": 60_000}, durable=False), [], id="not durable"
        ),
        pytest.param(
            KombuQueue("events-live", message_ttl=60, max_priority=10),
            [
                (
                    "abstract_block_dumper.W001",
                    "Queue 'events-live' is in CELERY_TASK_QUEUES with arguments "
                    "{'x-message-ttl': 60000, 'x-max-priority': 10}, but its BlockDumper policy needs "
                    "{'x-message-ttl': 60000}.",
                )
            ],
            id="max_priority option",
        ),
    ],
)
def test_checks_compare_project_queues_by_the_arguments_the_broker_receives(app, broker, project_queue, expected):
    app.conf.task_queues = [project_queue]
    BlockDumper(celery_app=app).block_task(live_queue=LIVE)(index_events)

    assert _warnings() == expected


@pytest.mark.parametrize("broker_url", ["amqp://guest@localhost//", "memory://"])
def test_checks_warn_about_broker_limits(broker_url):
    BlockDumper(celery_app=Celery("indexer", broker=broker_url)).block_task(
        live_queue=Queue("slow-retries", time_limit=60, retry=RetryPolicy(max_delay=2400))
    )(live_only_task)

    assert _warnings() == [
        (
            "abstract_block_dumper.W006",
            "Queue 'slow-retries' retries up to 2400 s apart, but the broker takes back messages left "
            "unacknowledged for 1800 s.",
        ),
    ]


def test_checks_warn_about_a_broker_other_than_rabbitmq():
    other_app = Celery("indexer", broker="filesystem://")
    BlockDumper(celery_app=other_app).block_task(live_queue=Queue("elsewhere", time_limit=60))(live_only_task)

    assert _warnings() == [
        (
            "abstract_block_dumper.W004",
            "BlockDumper needs RabbitMQ, but Celery app 'indexer' uses the 'filesystem' broker. Point the broker URL "
            "at RabbitMQ (amqp://), or declare these tasks with @block_task, which runs on any broker.",
        ),
    ]


def _w005(queue_name: str, default_limit: str) -> tuple[str, str, str]:
    return (
        "abstract_block_dumper.W005",
        f"Queue {queue_name!r} has no time_limit.",
        f"Tasks on it without a hard limit of their own are killed at the default hard limit, {default_limit} s. "
        "Set Queue(time_limit=...) to the longest run you expect.",
    )


@pytest.mark.parametrize(
    ("queue", "celery_kwargs", "app_conf", "expected"),
    [
        (RARE, {}, {}, [_w005("rare", "1800")]),
        (Queue("soft-only", soft_time_limit=3000), {}, {}, [_w005("soft-only", "3010")]),
        (RARE, {"time_limit": 600}, {}, []),
        (RARE, {}, {"task_time_limit": 600}, []),
    ],
    ids=["default hard limit", "default above a soft limit", "celery_kwargs hard limit", "config hard limit"],
)
def test_time_limit_check_warns_only_when_a_task_runs_on_the_default_hard_limit(
    app, broker, queue, celery_kwargs, app_conf, expected
):
    app.conf.update(app_conf)
    BlockDumper(celery_app=app).block_task(live_queue=queue, celery_kwargs=celery_kwargs)(rare_task)

    with patch(DISCOVERY):
        messages = run_checks(tags=["abstract_block_dumper"])

    assert [(message.id, message.msg, message.hint) for message in messages] == expected


def test_checks_never_fail_the_command_that_runs_them(dumper):
    with patch(DISCOVERY, side_effect=ValueError("backfilling_lookback requires backfill_queue")):
        messages = run_checks(tags=["abstract_block_dumper"])

    assert [(message.id, message.level, message.msg) for message in messages] == [
        (
            "abstract_block_dumper.W000",
            30,
            "Could not check BlockDumper queues: ValueError: backfilling_lookback requires backfill_queue",
        )
    ]


@pytest.fixture
def installed_app(tmp_path, monkeypatch):
    """Installs a Django app of the given name whose `block_tasks` module has the given source, if any."""
    monkeypatch.syspath_prepend(tmp_path)

    @contextmanager
    def install(name: str, block_tasks_source: str | None):
        package = tmp_path / name
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "helpers.py").write_text("")
        if block_tasks_source is not None:
            (package / "block_tasks.py").write_text(block_tasks_source)
        with modify_settings(INSTALLED_APPS={"append": name}):
            yield package

    return install


def test_checks_report_a_block_task_module_importing_a_missing_package(dumper, installed_app):
    with installed_app("missing_package_app", "import a_package_that_is_not_installed\n"):
        messages = run_checks(tags=["abstract_block_dumper"])

    assert [(message.id, message.msg, message.hint) for message in messages] == [
        (
            "abstract_block_dumper.W000",
            "Could not check BlockDumper queues: ModuleNotFoundError: "
            "No module named 'a_package_that_is_not_installed'",
            "Import the block task modules in `manage.py shell` to see the full traceback.",
        )
    ]


def test_checks_report_a_block_task_module_importing_a_missing_name(dumper, installed_app):
    with installed_app("missing_name_app", "from missing_name_app.helpers import no_such_name\n") as package:
        messages = run_checks(tags=["abstract_block_dumper"])

    assert [(message.id, message.msg) for message in messages] == [
        (
            "abstract_block_dumper.W000",
            "Could not check BlockDumper queues: ImportError: cannot import name 'no_such_name' "
            f"from 'missing_name_app.helpers' ({package / 'helpers.py'})",
        )
    ]


def test_checks_accept_an_app_without_a_block_task_module(app, installed_app):
    BlockDumper(celery_app=app).block_task(live_queue=UNTHROTTLED_SOLO)(unthrottled_solo_task)

    with installed_app("no_block_tasks_app", None):
        assert run_checks(tags=["abstract_block_dumper"]) == []


def test_checks_leave_a_project_without_a_block_dumper_alone(installed_app):
    """Without a BlockDumper the checks import nothing, so they cannot run a module's import side effects."""
    with installed_app("plain_app", "IMPORTED = True\n"):
        messages = run_checks(tags=["abstract_block_dumper"])

    assert (messages, "plain_app.block_tasks" in sys.modules) == ([], False)


HALF_IMPORTED_MODULE = """
from abstract_block_dumper.v1.decorators import block_task

@block_task
def registered_before_the_error(block_number):
    return block_number

import a_package_that_is_not_installed
"""


def test_checks_take_back_tasks_of_a_module_that_failed_half_way(dumper, installed_app):
    registered = list(task_registry.get_functions())

    with installed_app("half_app", HALF_IMPORTED_MODULE):
        messages = run_checks(tags=["abstract_block_dumper"])

    assert [message.id for message in messages] == ["abstract_block_dumper.W000"]
    assert task_registry.get_functions() == registered


HEALTHY_MODULE = """
from abstract_block_dumper.v1.decorators import block_task

@block_task
def healthy_task(block_number):
    return block_number
"""


def _executable_paths() -> list[str]:
    return [item.executable_path for item in task_registry.get_functions()]


def test_checks_keep_the_tasks_of_modules_imported_before_a_failing_one(dumper, installed_app):
    registered = _executable_paths()

    with installed_app("healthy_app", HEALTHY_MODULE), installed_app("late_half_app", HALF_IMPORTED_MODULE):
        messages = run_checks(tags=["abstract_block_dumper"])
        after_checks = _executable_paths()
        ensure_modules_loaded()
        after_discovery = _executable_paths()

    assert ([message.id for message in messages], after_checks, after_discovery) == (
        ["abstract_block_dumper.W000"],
        [*registered, "healthy_app.block_tasks.healthy_task"],
        [
            *registered,
            "healthy_app.block_tasks.healthy_task",
            "late_half_app.block_tasks.registered_before_the_error",
        ],
    )


@pytest.fixture
def inflight_backfill():
    for block_number in (1, 2):
        TaskAttemptFactory(
            block_number=block_number,
            executable_path=abd_utils.get_executable_path(index_events),
            status=TaskAttempt.Status.PENDING,
            dispatch_queue="events-backfill",
        )


@pytest.mark.django_db
def test_queues_command_lists_policies_and_broker_state(dumper, broker, inflight_backfill):
    broker.stats["events-live"] = QueueStats(messages=3, consumers=2)
    broker.stats["events-backfill"] = QueueStats(messages=2, consumers=0)
    out = StringIO()

    with patch(COMMAND_DISCOVERY):
        call_command("block_dumper_queues_v1", stdout=out)

    assert out.getvalue() == (
        "QUEUE             ROLE           MESSAGE_TTL  SOFT/HARD  THROTTLE  IN_FLIGHT  WAITING  CONSUMERS\n"
        "events-live       live           60 s         100/120 s  -         0          3        2\n"
        "events-backfill   backfill       300 s        100/120 s  600       2          2        0  no consumers\n"
        "rare              live+backfill  -            -/-        -         0          0        0\n"
        "throttled-live    live           -            -/60 s     5         0          0        0\n"
        "solo              live+backfill  -            -/60 s     5         0          0        0\n"
        "unthrottled-solo  live+backfill  -            -/60 s     -         0          0        0\n"
    )


@pytest.mark.django_db
def test_monitor_warns_about_a_queue_with_work_and_no_consumers(dumper, broker, inflight_backfill):
    broker.stats["events-backfill"] = QueueStats(messages=2, consumers=0)
    broker.stats["events-live"] = QueueStats(messages=0, consumers=0)

    with (
        structlog.testing.capture_logs() as logs,
        patch("abstract_block_dumper._internal.services.queue_monitor.set_queue_consumers") as consumers,
    ):
        QueueMonitor().check()

    assert [log for log in logs if log["log_level"] == "warning"] == [
        {
            "event": "Block task queue has no consumers",
            "log_level": "warning",
            "queue": "events-backfill",
            "waiting_messages": 2,
            "inflight_attempts": 2,
        }
    ]
    assert [c.args for c in consumers.call_args_list] == [
        ("events-live", 0),
        ("events-backfill", 0),
        ("rare", 0),
        ("throttled-live", 0),
        ("solo", 0),
        ("unthrottled-solo", 0),
    ]
    assert broker.calls == [
        BrokerCall(
            "queue_stats",
            ("events-live", "events-backfill", "rare", "throttled-live", "solo", "unthrottled-solo"),
            {},
        )
    ]
