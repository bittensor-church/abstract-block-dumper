"""
Django system checks for BlockDumper queues (`manage.py check`, also run by `migrate` and `runserver`).

They run only in projects whose `BlockDumper` exists when Django has loaded, which is why the
dumper is created next to the Celery app; projects with `@block_task` tasks only are not
touched. Every finding is a Warning, never an Error, so these checks cannot stop a command
from running; mistakes that are always wrong already fail at import or at the dumper's startup.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

import structlog
from django.core.checks import CheckMessage, Tags, Warning, register

from abstract_block_dumper._internal.contacts.broker import BrokerContact, broker_contact_factory
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper._internal.exceptions import UnsupportedBrokerError
from abstract_block_dumper._internal.numeric import format_number
from abstract_block_dumper._internal.services.queue_inventory import BACKFILL, LIVE, QueueUsage, queue_usages
from abstract_block_dumper._internal.services.queue_policy import Queue
from abstract_block_dumper._internal.services.time_limits import default_time_limit_options, has_default_hard_limit

if TYPE_CHECKING:
    from celery import Celery
    from kombu import Queue as KombuQueue

    from abstract_block_dumper._internal.services.block_dumper import BlockDumper

logger = structlog.get_logger(__name__)


@register(Tags.compatibility, "abstract_block_dumper")
def check_block_dumper_queues(app_configs: Any = None, **kwargs: Any) -> list[CheckMessage]:
    if not dumper_registry.dumpers():
        return []
    try:
        _import_task_modules()
        contacts: dict[BlockDumper, BrokerContact] = {}
        messages: list[CheckMessage] = []
        for dumper in dumper_registry.dumpers():
            try:
                contacts[dumper] = broker_contact_factory(dumper.celery_app)
            except UnsupportedBrokerError as exc:
                messages.append(Warning(str(exc), id="abstract_block_dumper.W004"))
        for usage in queue_usages():
            contact = contacts.get(usage.dumper)
            if contact is not None:
                messages.extend(_check_queue(usage, contact))
        return messages
    except Exception as exc:
        return [
            Warning(
                f"Could not check BlockDumper queues: {type(exc).__name__}: {exc}",
                hint="Import the block task modules in `manage.py shell` to see the full traceback.",
                id="abstract_block_dumper.W000",
            )
        ]


def warn_about_dumpers_unseen_by_checks() -> None:
    """Log every BlockDumper created after Django had loaded, which these checks never see."""
    for dumper in dumper_registry.dumpers_created_after_django_loaded():
        logger.warning(
            "BlockDumper was created by a block task module, so manage.py check does not check its queues",
            celery_app=dumper.celery_app.main,
            hint="Create the BlockDumper next to the Celery app, e.g. in your_project/celery.py.",
        )


def _import_task_modules() -> None:
    """Import every app's `tasks` and `block_tasks` module, raising the first import error."""
    from django.apps import apps  # noqa: PLC0415

    for app_config in apps.get_app_configs():
        for module_suffix in ["tasks", "block_tasks"]:
            _import_task_module(f"{app_config.name}.{module_suffix}")


def _import_task_module(module_name: str) -> None:
    """
    Import one task module, taking its tasks back out of the registry if it fails.

    The command or worker that runs the checks imports the task modules again later. Python
    drops a module that fails half-way from `sys.modules`, so that second import would register
    the tasks above its failing line twice. Modules that imported cleanly are not run again, so
    their tasks stay.
    """
    registered = list(task_registry.get_functions())
    try:
        importlib.import_module(module_name)
    except Exception as e:
        if isinstance(e, ModuleNotFoundError) and e.name == module_name:
            return
        task_registry.replace_functions(registered)
        raise


def _check_queue(usage: QueueUsage, contact: BrokerContact) -> list[CheckMessage]:
    queue = usage.queue
    messages: list[CheckMessage] = []

    expected = contact.declared_arguments(contact.queue_definition(queue))
    project_queue = _project_queue(usage.dumper.celery_app, queue.name)
    project_arguments = None if project_queue is None else contact.declared_arguments(project_queue)
    if project_arguments is not None and project_arguments != expected:
        messages.append(
            Warning(
                f"Queue {queue.name!r} is in CELERY_TASK_QUEUES with arguments "
                f"{project_arguments!r}, but its BlockDumper policy needs {expected!r}.",
                hint="Remove it from CELERY_TASK_QUEUES; BlockDumper adds its queues itself.",
                id="abstract_block_dumper.W001",
            )
        )
    # A task without a backfill_queue is the simplest setup there is, and its backfills only run
    # when an operator starts them, so W002 waits for a queue someone chose for backfills.
    if LIVE in usage.roles and usage.named_as_backfill_queue and queue.throttle is None:
        messages.append(
            Warning(
                f"Queue {queue.name!r} serves live and backfill submissions without a throttle.",
                hint="A large backfill can delay live tasks; set Queue(throttle=...).",
                id="abstract_block_dumper.W002",
            )
        )
    if queue.throttle is not None and BACKFILL not in usage.roles:
        messages.append(
            Warning(
                f"Queue {queue.name!r} has a throttle but no backfill or lookback submissions go to it.",
                hint="The throttle only holds back backfill and lookback submissions, so it has no effect here.",
                id="abstract_block_dumper.W003",
            )
        )
    default_limits = _default_hard_limits(usage) if queue.time_limit is None else []
    if default_limits:
        limits = ", ".join(format_number(limit) for limit in default_limits)
        messages.append(
            Warning(
                f"Queue {queue.name!r} has no time_limit.",
                hint=(
                    "Tasks on it without a hard limit of their own are killed at the default hard limit, "
                    f"{limits} s. Set Queue(time_limit=...) to the longest run you expect."
                ),
                id="abstract_block_dumper.W005",
            )
        )
    unacked_timeout = contact.unacked_timeout_seconds()
    if unacked_timeout is not None and queue.retry.max_delay > unacked_timeout:
        messages.append(
            Warning(
                f"Queue {queue.name!r} retries up to {queue.retry.max_delay} s apart, but the broker takes back "
                f"messages left unacknowledged for {unacked_timeout} s.",
                hint=(
                    "A retry waits for its ETA unacknowledged in the worker. Lower RetryPolicy.max_delay, or raise "
                    "RabbitMQ's consumer_timeout."
                ),
                id="abstract_block_dumper.W006",
            )
        )
    return messages


def _project_queue(celery_app: Celery, name: str) -> KombuQueue | None:
    """
    The queue the project's `task_queues` defines under `name`, as Celery's queue table holds it.

    The table is what `install_queues` compares against: it accepts every form of the setting
    (a list, a mapping of queues or of their options) and carries what Celery adds, such as
    `x-max-priority`. With the setting empty, the table holds only Celery's default queue, which
    the project did not define.
    """
    if not celery_app.conf.task_queues:
        return None
    return celery_app.amqp.queues.get(name)


def _default_hard_limits(usage: QueueUsage) -> list[float]:
    """The distinct default hard limits of the queue's tasks that run on one, lowest first."""
    app = usage.dumper.celery_app
    return sorted(
        {
            default_time_limit_options(app, item.celery_kwargs, _queues_of(item))["time_limit"]
            for item in usage.tasks
            if has_default_hard_limit(app, item.celery_kwargs)
        }
    )


def _queues_of(item: RegistryItem) -> list[Queue]:
    return [queue for queue in (item.live_queue, item.backfill_queue) if isinstance(queue, Queue)]
