from io import StringIO
from unittest.mock import patch

import pytest
import structlog
from celery import Celery
from celery.signals import celeryd_init, task_revoked
from django.apps import apps as django_apps
from django.core.management import CommandError, call_command
from django.test import override_settings
from kombu import Exchange
from kombu import Queue as KombuQueue

from abstract_block_dumper._internal.exceptions import QueueConfigurationError
from abstract_block_dumper._internal.services.celery_config import install_queues, prepare_block_dumpers
from abstract_block_dumper.v1.celery import setup_celery_tasks
from abstract_block_dumper.v1.dumper import BlockDumper, Queue
from abstract_block_dumper.v1.testing import (
    BrokerCall,
    BrokerUnreachableError,
    MockBrokerContact,
    QueueArgumentsMismatchError,
    use_broker_contact,
)

LIVE = Queue("events-live", time_limit=120)
BACKFILL = Queue("events-backfill", time_limit=240)
FACTORY = "abstract_block_dumper._internal.services.celery_config.broker_contact_factory"
THROTTLE_POLL_ERROR = "BLOCK_DUMPER_THROTTLE_POLL_SECONDS must be a positive number"
MAX_PRIORITY = {"x-max-priority": 10}
UNSEEN_DUMPER_WARNING = {
    "event": "BlockDumper was created by a block task module, so manage.py check does not check its queues",
    "log_level": "warning",
    "celery_app": "queue_config",
    "hint": "Create the BlockDumper next to the Celery app, e.g. in your_project/celery.py.",
}


def _table(celery_app: Celery) -> dict[str, dict | None]:
    return {name: queue.queue_arguments for name, queue in celery_app.amqp.queues.items()}


def _setup_calls(arguments: dict) -> list[BrokerCall]:
    """The broker calls of one setup of the `dumper` fixture's queues, each declared with `arguments`."""
    return [
        BrokerCall("queue_definition", ("events-live",), arguments),
        BrokerCall("queue_definition", ("events-backfill",), arguments),
        BrokerCall("declare_queue", ("events-live",), arguments),
        BrokerCall("declare_queue", ("events-backfill",), arguments),
    ]


def index_events(block_number: int) -> int:
    return block_number


@pytest.fixture
def app() -> Celery:
    return Celery("queue_config", broker="memory://")


@pytest.fixture
def dumper(app) -> BlockDumper:
    """A dumper created next to the Celery app, in a module Django imports while it loads."""
    with patch.object(django_apps, "ready", False):
        dumper = BlockDumper(celery_app=app)
    dumper.block_task(live_queue=LIVE, backfill_queue=BACKFILL)(index_events)
    return dumper


@pytest.fixture
def broker(app) -> MockBrokerContact:
    with use_broker_contact(MockBrokerContact(app)) as contact:
        yield contact


@pytest.fixture
def prioritized_app(app) -> Celery:
    """The app with Celery's `task_queue_max_priority`, which adds `x-max-priority` to every queue in its table."""
    app.conf.task_queue_max_priority = 10
    return app


def test_installed_queues_join_the_default_queue_when_the_project_defines_none(app):
    install_queues(MockBrokerContact(app), [KombuQueue("events-live", Exchange("events-live"), "events-live")])

    assert _table(app) == {"celery": None, "events-live": None}


def test_project_queue_with_the_same_arguments_is_kept(app):
    project_queue = KombuQueue("events-live", Exchange("events-live"), "events-live", queue_arguments={"x": 1})
    app.conf.task_queues = [project_queue]
    policy_queue = KombuQueue("events-live", Exchange("events-live"), "events-live", queue_arguments={"x": 1})

    table_queues = install_queues(MockBrokerContact(app), [policy_queue])

    assert (table_queues, app.amqp.queues["events-live"] is project_queue) == ([project_queue], True)


def test_project_queue_with_other_arguments_is_rejected(app):
    app.conf.task_queues = [KombuQueue("events-live", queue_arguments={"x-message-ttl": 1000})]

    with pytest.raises(QueueConfigurationError, match="Queue 'events-live' is defined in the Celery config"):
        install_queues(MockBrokerContact(app), [KombuQueue("events-live")])


@override_settings(BLOCK_DUMPER_THROTTLE_POLL_SECONDS=0)
def test_projects_without_a_block_dumper_never_touch_the_broker():
    with patch(FACTORY) as factory:
        prepare_block_dumpers(verify_broker=True)

    factory.assert_not_called()


def test_prepare_installs_every_queue_and_checks_it_on_the_broker(app, dumper, broker):
    prepare_block_dumpers(verify_broker=True)

    assert _table(app) == {"celery": None, "events-live": None, "events-backfill": None}
    assert broker.calls == [
        BrokerCall("queue_definition", ("events-live",), {}),
        BrokerCall("queue_definition", ("events-backfill",), {}),
        BrokerCall("declare_queue", ("events-live",), {}),
        BrokerCall("declare_queue", ("events-backfill",), {}),
    ]


def test_queue_existing_with_other_arguments_on_the_broker_fails_the_check(dumper, broker):
    broker.mismatches["events-backfill"] = "PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'"

    with pytest.raises(QueueArgumentsMismatchError, match="Queue 'events-backfill' exists on the broker"):
        prepare_block_dumpers(verify_broker=True)


def test_worker_setup_installs_and_checks_the_queues(app, dumper, broker):
    with patch("abstract_block_dumper.v1.celery.ensure_modules_loaded"):
        setup_celery_tasks()

    assert (_table(app), broker.calls) == (
        {"celery": None, "events-live": None, "events-backfill": None},
        [
            BrokerCall("queue_definition", ("events-live",), {}),
            BrokerCall("queue_definition", ("events-backfill",), {}),
            BrokerCall("declare_queue", ("events-live",), {}),
            BrokerCall("declare_queue", ("events-backfill",), {}),
        ],
    )


def test_project_queue_matching_the_policy_is_kept_under_a_max_priority(prioritized_app, dumper, broker):
    project_queue = KombuQueue("events-live")
    prioritized_app.conf.task_queues = [project_queue]

    prepare_block_dumpers(verify_broker=True)

    assert (_table(prioritized_app), prioritized_app.amqp.queues["events-live"] is project_queue, broker.calls) == (
        {"events-live": MAX_PRIORITY, "events-backfill": MAX_PRIORITY},
        True,
        _setup_calls(MAX_PRIORITY),
    )


@pytest.mark.parametrize(
    ("project_queue", "live_queue", "arguments"),
    [
        pytest.param(KombuQueue("events-live", durable=False), LIVE, {}, id="not durable"),
        pytest.param(
            KombuQueue("events-live", Exchange("blocks", type="topic"), "events.#"), LIVE, {}, id="topic exchange"
        ),
        pytest.param(
            KombuQueue("events-live", message_ttl=30),
            Queue("events-live", time_limit=120, message_ttl=30),
            {"x-message-ttl": 30_000},
            id="message_ttl option",
        ),
    ],
)
def test_broker_check_declares_the_project_queue_producers_and_workers_use(
    app, broker, project_queue, live_queue, arguments
):
    app.conf.task_queues = [project_queue]
    BlockDumper(celery_app=app).block_task(live_queue=live_queue)(index_events)

    prepare_block_dumpers(verify_broker=True)

    assert (app.amqp.queues["events-live"] is project_queue, broker.declared, broker.calls) == (
        True,
        {"events-live": project_queue},
        [
            BrokerCall("queue_definition", ("events-live",), arguments),
            BrokerCall("declare_queue", ("events-live",), arguments),
        ],
    )


def test_project_queue_whose_options_add_arguments_the_policy_lacks_is_rejected(app, dumper, broker):
    app.conf.task_queues = [KombuQueue("events-live", max_priority=10)]

    with pytest.raises(QueueConfigurationError) as raised:
        prepare_block_dumpers(verify_broker=True)

    assert (str(raised.value), broker.calls) == (
        "Queue 'events-live' is defined in the Celery config with arguments {'x-max-priority': 10}, "
        "but its BlockDumper policy needs {}; remove it from task_queues or rename one of them",
        [
            BrokerCall("queue_definition", ("events-live",), {}),
            BrokerCall("queue_definition", ("events-backfill",), {}),
        ],
    )


def test_worker_setup_is_safe_to_repeat_under_a_max_priority(prioritized_app, dumper, broker):
    with patch("abstract_block_dumper.v1.celery.ensure_modules_loaded"):
        setup_celery_tasks()
        setup_celery_tasks()

    assert (_table(prioritized_app), broker.calls) == (
        {"celery": MAX_PRIORITY, "events-live": MAX_PRIORITY, "events-backfill": MAX_PRIORITY},
        [*_setup_calls(MAX_PRIORITY), *_setup_calls(MAX_PRIORITY)],
    )


def test_worker_setup_without_a_block_dumper_only_loads_the_task_modules():
    with (
        patch("abstract_block_dumper.v1.celery.ensure_modules_loaded") as load_modules,
        patch(FACTORY) as contact_factory,
    ):
        setup_celery_tasks()

    load_modules.assert_called_once_with()
    contact_factory.assert_not_called()
    assert task_revoked.has_listeners() is False


def test_worker_warns_about_a_block_dumper_created_by_a_block_task_module(app, broker):
    def create_dumper():
        BlockDumper(celery_app=app).block_task(live_queue=LIVE)(index_events)

    with (
        patch("abstract_block_dumper.v1.celery.ensure_modules_loaded", side_effect=create_dumper),
        structlog.testing.capture_logs() as logs,
    ):
        setup_celery_tasks()

    assert [log for log in logs if log["log_level"] == "warning"] == [UNSEEN_DUMPER_WARNING]


@pytest.mark.parametrize(
    ("django_loaded", "expected_warnings"),
    [(False, []), (True, [UNSEEN_DUMPER_WARNING])],
    ids=["created while Django loads", "created by a tasks module before celeryd_init"],
)
def test_worker_warns_about_a_block_dumper_created_after_django_loaded(app, broker, django_loaded, expected_warnings):
    # Celery's autodiscovery imports every app's `tasks` module after Django loads and before `celeryd_init`.
    with patch.object(django_apps, "ready", django_loaded):
        BlockDumper(celery_app=app).block_task(live_queue=LIVE)(index_events)

    with (
        patch("abstract_block_dumper.v1.celery.ensure_modules_loaded"),
        structlog.testing.capture_logs() as logs,
    ):
        setup_celery_tasks()

    assert [log for log in logs if log["log_level"] == "warning"] == expected_warnings


@pytest.fixture
def worker_init_handler():
    """A project's `celeryd_init` handler, as the README shows it, with module discovery stubbed."""

    def on_worker_init(**_kwargs):
        setup_celery_tasks()

    celeryd_init.connect(on_worker_init, weak=False)
    with patch("abstract_block_dumper.v1.celery.ensure_modules_loaded"):
        yield
    celeryd_init.disconnect(on_worker_init)


def test_worker_does_not_start_on_a_queue_mismatch(dumper, broker, worker_init_handler):
    broker.mismatches["events-live"] = "PRECONDITION_FAILED"

    with pytest.raises(SystemExit) as raised:
        celeryd_init.send(sender="worker@host", instance=None, conf=None, options=None)

    assert raised.value.code == (
        "Queue 'events-live' exists on the broker with arguments other than {}: PRECONDITION_FAILED "
        "Queue arguments cannot change on an existing queue: drain the queue and delete it on the broker, "
        "or give the Queue a new name (for example 'events-live.v2')."
    )


def test_worker_does_not_start_on_a_broker_other_than_rabbitmq(worker_init_handler):
    BlockDumper(celery_app=Celery("elsewhere", broker="filesystem://")).block_task(live_queue=LIVE)(index_events)

    with pytest.raises(SystemExit) as raised:
        celeryd_init.send(sender="worker@host", instance=None, conf=None, options=None)

    assert raised.value.code == (
        "BlockDumper needs RabbitMQ, but Celery app 'elsewhere' uses the 'filesystem' broker. Point the broker URL "
        "at RabbitMQ (amqp://), or declare these tasks with @block_task, which runs on any broker."
    )


def test_worker_does_not_start_on_a_conflict_with_the_project_queues(app, dumper, broker, worker_init_handler):
    app.conf.task_queues = [KombuQueue("events-live", queue_arguments={"x-message-ttl": 1000})]

    with pytest.raises(SystemExit) as raised:
        celeryd_init.send(sender="worker@host", instance=None, conf=None, options=None)

    assert raised.value.code == (
        "Queue 'events-live' is defined in the Celery config with arguments {'x-message-ttl': 1000}, "
        "but its BlockDumper policy needs {}; remove it from task_queues or rename one of them"
    )


def _other_index_events(block_number: int) -> int:
    return block_number


def test_unreachable_broker_leaves_the_worker_starting_with_every_queue_installed(
    app, dumper, broker, worker_init_handler
):
    other_app = Celery("other_project", broker="memory://")
    BlockDumper(celery_app=other_app).block_task(live_queue=Queue("other-live", message_ttl=60))(_other_index_events)
    broker.unreachable = True

    with use_broker_contact(MockBrokerContact(other_app)):
        results = celeryd_init.send(sender="worker@host", instance=None, conf=None, options=None)

    assert [type(response) for _receiver, response in results] == [BrokerUnreachableError]
    assert _table(app) == {"celery": None, "events-live": None, "events-backfill": None}
    assert _table(other_app) == {"celery": None, "other-live": {"x-message-ttl": 60_000}}
    assert task_revoked.has_listeners()


def test_scheduler_refuses_to_start_on_a_queue_mismatch(dumper, broker):
    broker.mismatches["events-live"] = "PRECONDITION_FAILED"

    with (
        patch("abstract_block_dumper.management.commands.block_tasks_v1.ensure_modules_loaded"),
        patch("abstract_block_dumper.management.commands.block_tasks_v1.task_scheduler_factory") as scheduler_factory,
        pytest.raises(CommandError) as raised,
    ):
        call_command("block_tasks_v1")

    assert str(raised.value) == (
        "Queue 'events-live' exists on the broker with arguments other than {}: PRECONDITION_FAILED "
        "Queue arguments cannot change on an existing queue: drain the queue and delete it on the broker, "
        "or give the Queue a new name (for example 'events-live.v2')."
    )
    scheduler_factory.assert_not_called()


def test_scheduler_starts_with_every_queue_installed_when_the_broker_is_unreachable(app, dumper, broker):
    broker.unreachable = True

    with (
        patch("abstract_block_dumper.management.commands.block_tasks_v1.ensure_modules_loaded"),
        patch("abstract_block_dumper.management.commands.block_tasks_v1.task_scheduler_factory") as scheduler_factory,
        structlog.testing.capture_logs() as logs,
    ):
        call_command("block_tasks_v1", stdout=StringIO())

    scheduler_factory.return_value.start.assert_called_once_with()
    assert _table(app) == {"celery": None, "events-live": None, "events-backfill": None}
    assert [log for log in logs if log["log_level"] == "warning"] == [
        {
            "event": "Broker unreachable; block scheduler starts without checking the BlockDumper queues on it",
            "log_level": "warning",
            "celery_app": "queue_config",
            "error": "[Errno 111] Connection refused",
        }
    ]


@pytest.fixture
def throttled_dumper(app) -> BlockDumper:
    dumper = BlockDumper(celery_app=app)
    throttled_backfill = Queue("events-throttled-backfill", time_limit=240, throttle=10)
    dumper.block_task(live_queue=LIVE, backfill_queue=throttled_backfill)(index_events)
    return dumper


@pytest.mark.parametrize("poll_seconds", [-1, 0, "1", None, True])
def test_invalid_throttle_poll_interval_stops_setup_before_any_queue_is_touched(throttled_dumper, broker, poll_seconds):
    with (
        override_settings(BLOCK_DUMPER_THROTTLE_POLL_SECONDS=poll_seconds),
        pytest.raises(QueueConfigurationError) as raised,
    ):
        prepare_block_dumpers(verify_broker=True)

    assert (str(raised.value), broker.calls) == (THROTTLE_POLL_ERROR, [])


@override_settings(BLOCK_DUMPER_THROTTLE_POLL_SECONDS=0)
def test_commands_and_workers_refuse_an_invalid_throttle_poll_interval(throttled_dumper, broker):
    with (
        patch("abstract_block_dumper.management.commands.block_tasks_v1.ensure_modules_loaded"),
        patch("abstract_block_dumper.management.commands.block_tasks_v1.task_scheduler_factory"),
        pytest.raises(CommandError) as command_error,
    ):
        call_command("block_tasks_v1")
    with (
        patch("abstract_block_dumper.v1.celery.ensure_modules_loaded"),
        pytest.raises(SystemExit) as worker_exit,
    ):
        setup_celery_tasks()

    assert (str(command_error.value), str(worker_exit.value)) == (THROTTLE_POLL_ERROR, THROTTLE_POLL_ERROR)


@override_settings(BLOCK_DUMPER_THROTTLE_POLL_SECONDS=0)
def test_invalid_throttle_poll_interval_is_ignored_without_a_throttled_queue(dumper, broker):
    prepare_block_dumpers(verify_broker=True)

    assert broker.calls == [
        BrokerCall("queue_definition", ("events-live",), {}),
        BrokerCall("queue_definition", ("events-backfill",), {}),
        BrokerCall("declare_queue", ("events-live",), {}),
        BrokerCall("declare_queue", ("events-backfill",), {}),
    ]
