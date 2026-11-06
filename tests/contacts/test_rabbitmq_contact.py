"""
Tests marked `requires_rabbitmq` run against a real RabbitMQ when BLOCK_DUMPER_TEST_RABBITMQ_URL
is set, e.g. amqp://guest:guest@localhost:5672//.
"""

import os
import uuid
from unittest.mock import patch

import pytest
from celery import Celery
from kombu import Connection
from kombu import Queue as KombuQueue

from abstract_block_dumper._internal.contacts.broker import (
    BrokerUnreachableError,
    QueueArgumentsMismatchError,
    QueueStats,
)
from abstract_block_dumper._internal.contacts.rabbitmq import RabbitMQContact
from abstract_block_dumper.v1.dumper import Queue

RABBITMQ_URL = os.environ.get("BLOCK_DUMPER_TEST_RABBITMQ_URL")
requires_rabbitmq = pytest.mark.skipif(not RABBITMQ_URL, reason="BLOCK_DUMPER_TEST_RABBITMQ_URL is not set")


@pytest.fixture
def contact() -> RabbitMQContact:
    return RabbitMQContact(Celery("rabbitmq_contact", broker=RABBITMQ_URL))


@pytest.fixture
def queue_names():
    """Unique queue names, deleted from the broker after the test."""
    names: list[str] = []

    def new_name() -> str:
        names.append(f"abd-test-{uuid.uuid4().hex[:12]}")
        return names[-1]

    yield new_name
    with Connection(RABBITMQ_URL) as connection:
        for name in names:
            connection.default_channel.queue_delete(name)


def test_unreachable_broker_fails_every_operation_with_a_contact_error():
    app = Celery("unreachable", broker="amqp://guest:guest@127.0.0.1:1//")
    # Without retries the connection fails at once instead of after Kombu's backoff.
    app.conf.broker_transport_options = {"max_retries": 0}
    contact = RabbitMQContact(app)

    with pytest.raises(BrokerUnreachableError) as declare_error:
        contact.declare_queue(contact.queue_definition(Queue("events-live")))
    with pytest.raises(BrokerUnreachableError) as stats_error:
        contact.queue_stats(["events-live"])

    errors = [declare_error.value, stats_error.value]
    assert [error.app_name for error in errors] == ["unreachable", "unreachable"]
    assert all("Connection refused" in error.detail for error in errors)


@requires_rabbitmq
def test_declares_new_queues_and_reports_ones_declared_with_other_arguments(contact, queue_names):
    fresh, foreign = queue_names(), queue_names()
    with Connection(RABBITMQ_URL) as connection:
        KombuQueue(foreign, queue_arguments={"x-message-ttl": 1000})(connection.default_channel).declare()

    contact.declare_queue(contact.queue_definition(Queue(fresh)))
    with pytest.raises(QueueArgumentsMismatchError) as raised:
        contact.declare_queue(contact.queue_definition(Queue(foreign)))

    assert (raised.value.queue, raised.value.expected_arguments) == (foreign, {})
    assert "PRECONDITION_FAILED" in raised.value.detail


@requires_rabbitmq
def test_queue_with_a_message_ttl_is_declared_with_it(contact, queue_names):
    name = queue_names()
    contact.declare_queue(contact.queue_definition(Queue(name, message_ttl=300)))

    with pytest.raises(QueueArgumentsMismatchError) as raised:
        contact.declare_queue(contact.queue_definition(Queue(name, message_ttl=60)))

    assert raised.value.expected_arguments == {"x-message-ttl": 60_000}


def test_declared_arguments_include_those_kombu_derives_from_queue_options(contact):
    definition = KombuQueue(
        "events-live", queue_arguments={"x-message-ttl": 1, "x-queue-type": "classic"}, message_ttl=2, max_priority=10
    )

    assert contact.declared_arguments(definition) == {
        "x-message-ttl": 2000,
        "x-queue-type": "classic",
        "x-max-priority": 10,
    }


@requires_rabbitmq
def test_queue_options_are_compared_with_the_arguments_kombu_derives_from_them(contact, queue_names):
    name = queue_names()
    contact.declare_queue(contact.queue_definition(Queue(name, message_ttl=30)))

    contact.declare_queue(KombuQueue(name, message_ttl=30))
    with pytest.raises(QueueArgumentsMismatchError) as raised:
        contact.declare_queue(KombuQueue(name, max_priority=10))

    assert raised.value.expected_arguments == {"x-max-priority": 10}


@requires_rabbitmq
def test_queue_stats_read_every_queue_over_one_connection(contact, queue_names):
    waiting, missing, consumed = queue_names(), queue_names(), queue_names()
    app = contact.celery_app
    with Connection(RABBITMQ_URL) as connection:
        producer = connection.Producer()
        KombuQueue(waiting)(connection.default_channel).declare()
        for number in range(3):
            producer.publish({"n": number}, routing_key=waiting)
        # The missing queue sits between two others: its passive declare must not break the next read.
        with (
            connection.Consumer(KombuQueue(consumed)),
            patch.object(app, "connection_for_write", wraps=app.connection_for_write) as connect,
        ):
            stats = contact.queue_stats([waiting, missing, consumed])

    assert (stats, connect.call_count) == (
        {
            waiting: QueueStats(messages=3, consumers=0),
            missing: QueueStats(messages=0, consumers=0),
            consumed: QueueStats(messages=0, consumers=1),
        },
        1,
    )
