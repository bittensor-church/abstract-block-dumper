from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from amqp.exceptions import NotFound, PreconditionFailed
from kombu import Queue as KombuQueue
from kombu.exceptions import OperationalError
from kombu.transport.base import to_rabbitmq_queue_arguments

from abstract_block_dumper._internal.contacts.broker import (
    BrokerContact,
    BrokerUnreachableError,
    QueueArgumentsMismatchError,
    QueueStats,
)

if TYPE_CHECKING:
    from kombu import Connection

    from abstract_block_dumper._internal.services.queue_policy import Queue


# RabbitMQ's default `consumer_timeout`; the server's actual value is not visible to clients.
RABBITMQ_DEFAULT_CONSUMER_TIMEOUT_SECONDS = 1800


def rabbitmq_queue_arguments(queue: Queue) -> dict[str, Any]:
    """The RabbitMQ arguments `queue` is declared with: `x-message-ttl`, in milliseconds, for a `message_ttl`."""
    if queue.message_ttl is None:
        return {}
    return {"x-message-ttl": int(queue.message_ttl * 1000)}


def rabbitmq_declared_arguments(definition: KombuQueue) -> dict[str, Any]:
    """The arguments RabbitMQ receives for `definition`, as Kombu's AMQP transports prepare them."""
    return dict(
        to_rabbitmq_queue_arguments(
            definition.queue_arguments or {},
            expires=definition.expires,
            message_ttl=definition.message_ttl,
            max_length=definition.max_length,
            max_length_bytes=definition.max_length_bytes,
            max_priority=definition.max_priority,
        )
    )


class RabbitMQContact(BrokerContact):
    def unacked_timeout_seconds(self) -> float | None:
        return RABBITMQ_DEFAULT_CONSUMER_TIMEOUT_SECONDS

    def queue_definition(self, queue: Queue) -> KombuQueue:
        return self._celery_definition(queue.name, rabbitmq_queue_arguments(queue))

    def declared_arguments(self, definition: KombuQueue) -> dict[str, Any]:
        return rabbitmq_declared_arguments(definition)

    def declare_queue(self, definition: KombuQueue) -> None:
        with self._connection() as connection:
            try:
                definition.bind(connection.default_channel).declare()
            except PreconditionFailed as exc:
                expected_arguments = self.declared_arguments(definition)
                raise QueueArgumentsMismatchError(definition.name, expected_arguments, str(exc)) from exc

    def queue_stats(self, names: Sequence[str]) -> dict[str, QueueStats]:
        with self._connection() as connection:
            return {name: _passive_queue_stats(connection, name) for name in names}

    @contextmanager
    def _connection(self) -> Iterator[Connection]:
        with self.celery_app.connection_for_write() as connection:
            try:
                # Opening the default channel connects, retrying as the broker transport options say.
                connection.default_channel  # noqa: B018
            except OperationalError as exc:
                raise BrokerUnreachableError(self.celery_app.main or "default", str(exc)) from exc
            yield connection


def _passive_queue_stats(connection: Connection, name: str) -> QueueStats:
    """Read queue `name` on a channel of its own: a passive declare that finds no queue closes its channel."""
    with connection.channel() as channel:
        try:
            _name, messages, consumers = channel.queue_declare(name, passive=True)
        except NotFound:
            return QueueStats(messages=0, consumers=0)
    return QueueStats(messages=messages, consumers=consumers)
