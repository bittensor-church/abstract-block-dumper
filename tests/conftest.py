import django
import pytest
from celery import Celery
from django.conf import settings

from abstract_block_dumper.dal.memory_registry import task_registry

from .django_fixtures import *  # noqa: F401, F403

# Ensure Django is set up
if not settings.configured:
    django.setup()


@pytest.fixture(autouse=True)
def celery_test_app():
    """Configure Celery for testing with eager mode."""
    app = Celery("test_app")
    app.config_from_object(settings, namespace="CELERY")

    yield app


def every_block_task_func(block_number: int):
    """
    Test function for every block execution.
    """
    return f"Processed block {block_number}"


def modulo_task_func(block_number: int, netuid: int):
    """
    Test function for modulo condition execution.
    """
    return f"Modulo task processed block {block_number} for netuid {netuid}"


def failing_task_func(block_number: int):
    """
    Test function that always fails.
    """
    raise ValueError("Test error")


@pytest.fixture
def setup_test_tasks():
    # Register test tasks using decorators
    from abstract_block_dumper.decorators import block_task

    # every block
    block_task(condition=lambda bn: True)(every_block_task_func)

    # every 5 blocks
    block_task(condition=lambda bn, netuid: bn % 5 == 0, args=[{"netuid": 1}, {"netuid": 2}])(modulo_task_func)

    yield


@pytest.fixture(autouse=True)
def cleanup_memory_registry():
    task_registry.clear()

    yield

    task_registry.clear()
