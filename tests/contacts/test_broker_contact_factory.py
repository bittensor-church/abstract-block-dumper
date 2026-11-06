import pytest
from celery import Celery

from abstract_block_dumper._internal.contacts.broker import broker_contact_factory
from abstract_block_dumper._internal.contacts.mock import MockBrokerContact
from abstract_block_dumper._internal.contacts.rabbitmq import RabbitMQContact
from abstract_block_dumper._internal.exceptions import UnsupportedBrokerError
from abstract_block_dumper.v1.testing import use_broker_contact


@pytest.mark.parametrize(
    ("broker_url", "contact_class"),
    [
        ("amqp://guest@localhost//", RabbitMQContact),
        ("pyamqp://guest@localhost//", RabbitMQContact),
        ("amqps://guest@localhost//", RabbitMQContact),
        ("memory://", MockBrokerContact),
    ],
)
def test_contact_follows_the_broker_transport(broker_url, contact_class):
    contact = broker_contact_factory(Celery("factory", broker=broker_url))

    assert type(contact) is contact_class


def test_broker_other_than_rabbitmq_is_rejected():
    # Any transport kombu loads without an extra client library; Redis is refused the same way.
    with pytest.raises(UnsupportedBrokerError) as error:
        broker_contact_factory(Celery("factory", broker="filesystem://"))

    assert (error.value.app_name, error.value.driver_type) == ("factory", "filesystem")


@pytest.mark.parametrize(
    ("broker_url", "contact_class"),
    [("amqp://guest@localhost//", RabbitMQContact), ("memory://", MockBrokerContact)],
)
def test_injected_contact_serves_its_app_only_inside_the_block(broker_url, contact_class):
    app, other_app = Celery("injected", broker=broker_url), Celery("other", broker=broker_url)
    injected = MockBrokerContact(app)

    with use_broker_contact(injected):
        inside = (broker_contact_factory(app), type(broker_contact_factory(other_app)))
    after = broker_contact_factory(app)

    assert (inside, type(after), after is injected) == ((injected, contact_class), contact_class, False)


def test_leaving_a_block_restores_the_contact_injected_before_it_even_on_an_error():
    app = Celery("injected", broker="memory://")
    outer, inner = MockBrokerContact(app), MockBrokerContact(app)

    with use_broker_contact(outer):
        with pytest.raises(RuntimeError), use_broker_contact(inner):
            raise RuntimeError
        restored = broker_contact_factory(app)

    assert restored is outer
