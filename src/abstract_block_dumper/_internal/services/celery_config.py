"""
Make Celery and the broker agree on the queues BlockDumper tasks use.

Celery declares a queue both before a producer first sends to it and when a worker starts
consuming it, and builds that declaration from its own queue table. A queue missing from
the table is declared without arguments, which RabbitMQ rejects with PRECONDITION_FAILED
once the queue exists with arguments: the producer's `apply_async` raises and the worker
exits. Every process that sends or consumes BlockDumper tasks therefore installs the same
definitions first: the management commands at startup, workers from `celeryd_init` through
`setup_celery_tasks()`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

import structlog

from abstract_block_dumper._internal.contacts.broker import QueueArgumentsMismatchError, broker_contact_factory
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from kombu import Queue as KombuQueue

    from abstract_block_dumper._internal.contacts.broker import BrokerContact

logger = structlog.get_logger(__name__)


def install_queues(contact: BrokerContact, definitions: Iterable[KombuQueue]) -> list[KombuQueue]:
    """
    Add `definitions` to the queue table producers and workers of `contact`'s Celery app declare from.

    A queue the project already defines under the same name is kept as long as the broker
    receives the same arguments for it, counting those Kombu derives from options such as
    `max_priority`; different arguments raise `QueueConfigurationError`. The table starts from
    `task_queues`, or from the default queue when that setting is empty, so a worker started
    without `-Q` keeps consuming the default queue next to the added ones.

    Returns:
        The table's queue for each definition, in order: the project's own queue where it was kept.

    """
    queues = contact.celery_app.amqp.queues
    installed = []
    table_queues = []
    for definition in definitions:
        existing = queues.get(definition.name)
        if existing is None:
            queues.add(definition)
            installed.append(definition.name)
            table_queues.append(definition)
            continue
        project_arguments = contact.declared_arguments(existing)
        policy_arguments = contact.declared_arguments(definition)
        if project_arguments != policy_arguments:
            msg = (
                f"Queue {definition.name!r} is defined in the Celery config with arguments "
                f"{project_arguments!r}, but its BlockDumper policy needs "
                f"{policy_arguments!r}; remove it from task_queues or rename one of them"
            )
            raise QueueConfigurationError(msg)
        table_queues.append(existing)
    if installed:
        logger.info("Installed block dumper queues into Celery config", queues=installed)
    return table_queues


def prepare_block_dumpers(*, verify_broker: bool) -> None:
    """
    Install every BlockDumper's queues into its Celery app and, optionally, check the broker.

    With `verify_broker`, each queue is declared on the broker once, as the Celery queue table
    holds it, so a queue that exists there with other arguments or flags fails here with
    `QueueArgumentsMismatchError` instead of breaking the first send or the worker. Every queue
    is installed before the first declaration, so an unreachable broker leaves none without its
    arguments. Projects with no BlockDumper never touch the broker here.

    Raises:
        QueueConfigurationError: The broker is not RabbitMQ, the Celery config defines a queue
            with other arguments, or, with a throttled queue,
            `BLOCK_DUMPER_THROTTLE_POLL_SECONDS` is invalid. Nothing is installed in the last
            case.
        QueueArgumentsMismatchError: With `verify_broker`, the broker has a queue with other
            arguments or flags.
        BrokerUnreachableError: With `verify_broker`, the broker cannot be reached. Every
            queue is installed by then.

    """
    if any(queue.throttle is not None for dumper in dumper_registry.dumpers() for queue in dumper.queues):
        # Imported here: `throttle` loads Django models, and `abstract_block_dumper.v1.celery`,
        # which imports this module, must stay importable while Django loads its settings.
        from abstract_block_dumper._internal.services.throttle import throttle_poll_seconds  # noqa: PLC0415

        throttle_poll_seconds()
    prepared = []
    for dumper in dumper_registry.dumpers():
        contact = broker_contact_factory(dumper.celery_app)
        definitions = [contact.queue_definition(queue) for queue in dumper.queues]
        prepared.append((contact, install_queues(contact, definitions)))
    if verify_broker:
        for contact, table_queues in prepared:
            for table_queue in table_queues:
                contact.declare_queue(table_queue)


def queue_setup_error_message(exc: QueueArgumentsMismatchError | QueueConfigurationError) -> str:
    """What an operator reads when `prepare_block_dumpers` fails, including what to do."""
    if isinstance(exc, QueueArgumentsMismatchError):
        return (
            f"{exc} Queue arguments cannot change on an existing queue: drain the queue and delete it "
            f"on the broker, or give the Queue a new name (for example {exc.queue + '.v2'!r})."
        )
    return str(exc)
