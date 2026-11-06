"""
Queue management on the message broker.

Celery sends and consumes the messages; this contact covers the broker operations Celery
does not expose, such as declaring a queue with arguments and detecting that an existing
queue was declared with different ones. Policy decisions stay with the callers.
"""

from __future__ import annotations

import abc
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from kombu import Queue as KombuQueue

from abstract_block_dumper._internal.exceptions import AbstractBlockDumperError, UnsupportedBrokerError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from celery import Celery

    from abstract_block_dumper._internal.services.queue_policy import Queue


class QueueArgumentsMismatchError(AbstractBlockDumperError):
    """The broker already has the queue, declared with different arguments."""

    def __init__(self, queue: str, expected_arguments: dict[str, Any], detail: str) -> None:
        super().__init__(
            f"Queue {queue!r} exists on the broker with arguments other than {expected_arguments!r}: {detail}"
        )
        self.queue = queue
        self.expected_arguments = expected_arguments
        self.detail = detail


class BrokerUnreachableError(AbstractBlockDumperError):
    """The broker of a Celery app refused the connection or did not answer in time."""

    def __init__(self, app_name: str, detail: str) -> None:
        super().__init__(f"Cannot connect to the broker of Celery app {app_name!r}: {detail}")
        self.app_name = app_name
        self.detail = detail


@dataclass(frozen=True)
class QueueStats:
    """Messages waiting in a queue, and how many consumers read it."""

    messages: int
    consumers: int


class BrokerContact(abc.ABC):
    def __init__(self, celery_app: Celery) -> None:
        self.celery_app = celery_app

    @abc.abstractmethod
    def queue_definition(self, queue: Queue) -> KombuQueue:
        """The declaration of `queue` that every producer and worker must use."""

    @abc.abstractmethod
    def declared_arguments(self, definition: KombuQueue) -> dict[str, Any]:
        """
        The arguments the broker receives when `definition` is declared.

        Kombu adds arguments for some `Queue` options, such as `message_ttl` and `max_priority`,
        only at declaration time, so `definition.queue_arguments` alone can miss them.
        """

    @abc.abstractmethod
    def declare_queue(self, definition: KombuQueue) -> None:
        """
        Declare `definition` on the broker.

        Raises `QueueArgumentsMismatchError` when the queue exists with other arguments, and
        `BrokerUnreachableError` when the broker cannot be reached.
        """

    @abc.abstractmethod
    def unacked_timeout_seconds(self) -> float | None:
        """
        How long the broker lets a delivered message stay unacknowledged; None if unlimited.

        Workers hold a retry unacknowledged until its ETA, so a longer retry delay makes the
        broker take the message back.
        """

    @abc.abstractmethod
    def queue_stats(self, names: Sequence[str]) -> dict[str, QueueStats]:
        """
        Current state of each queue in `names`, read over one connection to the broker.

        A queue that does not exist is empty with no consumers. Raises `BrokerUnreachableError`
        when the broker cannot be reached.
        """

    def _celery_definition(self, name: str, arguments: dict[str, Any]) -> KombuQueue:
        """
        The queue Celery itself would create for `name`, with `arguments` added.

        Starting from Celery's own definition keeps the exchange, routing key, queue type and
        max priority identical to queues Celery created on its own before, so redeclaring them
        succeeds.
        """
        table = self.celery_app.amqp.queues
        base = table.new_missing(name)
        queue_arguments = {**(base.queue_arguments or {}), **arguments}
        if table.max_priority is not None:
            # Celery adds it to every queue it puts in its table (`Queues._add`), but not in `new_missing`.
            queue_arguments.setdefault("x-max-priority", table.max_priority)
        return KombuQueue(name, base.exchange, base.routing_key, queue_arguments=queue_arguments or None)


_injected_contacts: dict[Celery, BrokerContact] = {}


@contextmanager
def use_broker_contact(contact: BrokerContact) -> Iterator[BrokerContact]:
    """
    Make `broker_contact_factory` return `contact` for its Celery app inside the `with` block.

    The contact replaces the one the app's broker URL selects, so tests can run the startup
    checks and the scheduler against a `MockBrokerContact` whatever broker their settings name.
    Leaving the block restores the contact injected before it, if any.
    """
    celery_app = contact.celery_app
    previous = _injected_contacts.get(celery_app)
    _injected_contacts[celery_app] = contact
    try:
        yield contact
    finally:
        if previous is None:
            del _injected_contacts[celery_app]
        else:
            _injected_contacts[celery_app] = previous


def broker_contact_factory(celery_app: Celery) -> BrokerContact:
    """
    The contact for the broker `celery_app` sends to, chosen by its transport.

    BlockDumper queues rely on RabbitMQ: its per-queue message TTL, consumer counts and
    `consumer_timeout`. The in-memory transport gets the mock contact, for tests. Any other
    broker raises `UnsupportedBrokerError`. A contact injected with `use_broker_contact` is
    returned whatever the transport.
    """
    from abstract_block_dumper._internal.contacts.mock import MockBrokerContact  # noqa: PLC0415
    from abstract_block_dumper._internal.contacts.rabbitmq import RabbitMQContact  # noqa: PLC0415

    injected = _injected_contacts.get(celery_app)
    if injected is not None:
        return injected
    # The transport class tells the broker type without connecting to it.
    driver_type = celery_app.connection_for_write().get_transport_cls().driver_type
    contacts: dict[str, type[BrokerContact]] = {
        "amqp": RabbitMQContact,
        "memory": MockBrokerContact,
    }
    if driver_type not in contacts:
        raise UnsupportedBrokerError(celery_app.main or "default", driver_type)
    return contacts[driver_type](celery_app)
