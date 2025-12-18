from collections.abc import Generator
from typing import Any, NoReturn

import django
import pytest
from celery import Celery
from django.conf import settings

from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper.v1.decorators import block_task

from .django_fixtures import *  # noqa: F401, F403

# Ensure Django is set up
if not settings.configured:
    django.setup()


class MockedBlockProcessor:
    """Mock implementation of BaseBlockProcessor for testing."""

    def __init__(self, executor: Any = None, registry: Any = None) -> None:
        self.executor = executor
        self.registry = registry or task_registry
        self.processed_blocks: list[int] = []
        self.processed_registry_items: list[tuple[RegistryItem, int]] = []
        self.recover_failed_retries_calls: list[tuple[int, int | None]] = []

    def process_block(self, block_number: int) -> None:
        self.processed_blocks.append(block_number)

    def process_registry_item(self, registry_item: RegistryItem, block_number: int) -> None:
        self.processed_registry_items.append((registry_item, block_number))

    def recover_failed_retries(self, poll_interval: int, batch_size: int | None = None) -> None:
        self.recover_failed_retries_calls.append((poll_interval, batch_size))


class MockedSubtensor:
    """Mock implementation of bt.Subtensor for testing."""

    def __init__(self, current_block: int = 1000) -> None:
        self._current_block = current_block

    def get_current_block(self) -> int:
        return self._current_block

    def set_current_block(self, block: int) -> None:
        self._current_block = block


@pytest.fixture(autouse=True)
def celery_test_app() -> Generator[Celery, Any, None]:
    """Configure Celery for testing with eager mode."""
    app = Celery("test_app")
    app.config_from_object(settings, namespace="CELERY")
    return app


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


def failing_task_func(block_number: int) -> NoReturn:
    """
    Test function that always fails.
    """
    raise ValueError("Test error")


@pytest.fixture
def setup_test_tasks():
    # Register test tasks using decorators

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


@pytest.fixture
def mock_block_processor() -> MockedBlockProcessor:
    return MockedBlockProcessor()


@pytest.fixture
def mock_subtensor() -> MockedSubtensor:
    return MockedSubtensor(current_block=1000)


@pytest.fixture
def mock_archive_subtensor() -> MockedSubtensor:
    return MockedSubtensor(current_block=1000)
