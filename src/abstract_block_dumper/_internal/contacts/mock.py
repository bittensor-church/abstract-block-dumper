from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from kombu import Queue as KombuQueue

from abstract_block_dumper._internal.contacts.broker import (
    BrokerContact,
    BrokerUnreachableError,
    QueueArgumentsMismatchError,
    QueueStats,
)
from abstract_block_dumper._internal.contacts.rabbitmq import (
    RABBITMQ_DEFAULT_CONSUMER_TIMEOUT_SECONDS,
    rabbitmq_declared_arguments,
    rabbitmq_queue_arguments,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from celery import Celery

    from abstract_block_dumper._internal.services.queue_policy import Queue


@dataclass(frozen=True)
class BrokerCall:
    """One call to the broker contact, with the queues it covered."""

    operation: str
    queues: tuple[str, ...]
    arguments: dict[str, Any]


class MockBrokerContact(BrokerContact):
    """
    Broker contact for tests: records every call and reports the mismatches it is told to.

    It declares queues the way RabbitMQ does, with `x-message-ttl` for a `message_ttl`, and
    reports RabbitMQ's default `consumer_timeout`. Inject it with `use_broker_contact` so the
    library uses it for its Celery app. Configure it by mutating `mismatches`, a map from queue
    name to the broker's error text; declaring such a queue raises `QueueArgumentsMismatchError`.
    `stats` maps queue names to the `QueueStats` to report; other queues are empty with no
    consumers. With `unreachable` set, declaring a queue and reading stats raise
    `BrokerUnreachableError`. `calls` lists every call but `declared_arguments`, in order, one
    `BrokerCall` per call however many queues it covers. `declared` maps each queue declared
    without an error to the `kombu.Queue` it was last declared with.
    """

    def __init__(
        self,
        celery_app: Celery,
        *,
        mismatches: dict[str, str] | None = None,
        stats: dict[str, QueueStats] | None = None,
        unreachable: bool = False,
    ) -> None:
        super().__init__(celery_app)
        self.mismatches: dict[str, str] = dict(mismatches or {})
        self.stats: dict[str, QueueStats] = dict(stats or {})
        self.unreachable = unreachable
        self.calls: list[BrokerCall] = []
        self.declared: dict[str, KombuQueue] = {}

    def unacked_timeout_seconds(self) -> float | None:
        return RABBITMQ_DEFAULT_CONSUMER_TIMEOUT_SECONDS

    def queue_definition(self, queue: Queue) -> KombuQueue:
        definition = self._celery_definition(queue.name, rabbitmq_queue_arguments(queue))
        self.calls.append(BrokerCall("queue_definition", (queue.name,), self.declared_arguments(definition)))
        return definition

    def declared_arguments(self, definition: KombuQueue) -> dict[str, Any]:
        return rabbitmq_declared_arguments(definition)

    def declare_queue(self, definition: KombuQueue) -> None:
        arguments = self.declared_arguments(definition)
        self.calls.append(BrokerCall("declare_queue", (definition.name,), arguments))
        self._connect()
        if definition.name in self.mismatches:
            raise QueueArgumentsMismatchError(definition.name, arguments, self.mismatches[definition.name])
        self.declared[definition.name] = definition

    def queue_stats(self, names: Sequence[str]) -> dict[str, QueueStats]:
        self.calls.append(BrokerCall("queue_stats", tuple(names), {}))
        self._connect()
        return {name: self.stats.get(name, QueueStats(messages=0, consumers=0)) for name in names}

    def reset(self) -> None:
        self.mismatches = {}
        self.stats = {}
        self.unreachable = False
        self.calls = []
        self.declared = {}

    def _connect(self) -> None:
        if self.unreachable:
            raise BrokerUnreachableError(self.celery_app.main or "default", "[Errno 111] Connection refused")
