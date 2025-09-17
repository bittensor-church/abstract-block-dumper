"""Unit tests for discovery module."""

from unittest.mock import patch

import pytest

from abstract_block_dumper.decorators import BlockDumperRegistry
from abstract_block_dumper.discovery import sync_block_task_functions
from abstract_block_dumper.models import BlockDumperConfig, ConditionType, NetuidType


@pytest.fixture(autouse=True)
def clear_registry():
    BlockDumperRegistry.clear_pendings()
    yield
    BlockDumperRegistry.clear_pendings()


@pytest.fixture
def sample_registration():
    return {
        "name": "test_task",
        "description": "Test task",
        "function_path": "test.module.test_function",
        "condition_type": ConditionType.EVERY_BLOCK,
        "condition_params": {},
        "netuid_type": NetuidType.NONE,
        "netuid_values": [],
        "queue": "celery",
        "max_retries": 3,
        "retry_backoff": 2,
        "timeout": None,
        "is_active": True,
    }


@pytest.mark.django_db
def test_sync_block_task_functions_multiple(sample_registration):
    """Test syncing multiple registrations."""
    # Register multiple tasks
    BlockDumperRegistry.register(sample_registration)

    second_registration = sample_registration.copy()
    second_registration["name"] = "second_task"
    BlockDumperRegistry.register(second_registration)

    count = sync_block_task_functions(ensure_loaded=False)

    assert count == 2
    assert BlockDumperConfig.objects.count() == 2

    names = list(BlockDumperConfig.objects.values_list("name", flat=True))
    assert "test_task" in names
    assert "second_task" in names


@pytest.mark.django_db
def test_sync_block_task_functions_clears_registry(sample_registration):
    """Test that sync clears the pending registry."""
    BlockDumperRegistry.register(sample_registration)

    assert len(BlockDumperRegistry.get_pending_registrations()) == 1

    sync_block_task_functions(ensure_loaded=False)

    assert len(BlockDumperRegistry.get_pending_registrations()) == 0


def test_sync_block_task_functions_ensure_loaded_true():
    """Test sync with ensure_loaded=True calls module loading."""
    with patch("abstract_block_dumper.discovery.ensure_modules_loaded") as mock_ensure:
        sync_block_task_functions(ensure_loaded=True)

    mock_ensure.assert_called_once()


def test_sync_block_task_functions_ensure_loaded_false():
    """Test sync with ensure_loaded=False skips module loading."""
    with patch("abstract_block_dumper.discovery.ensure_modules_loaded") as mock_ensure:
        sync_block_task_functions(ensure_loaded=False)

    mock_ensure.assert_not_called()


def test_sync_block_task_functions_ensure_loaded_default():
    """Test sync default behavior calls module loading."""
    with patch("abstract_block_dumper.discovery.ensure_modules_loaded") as mock_ensure:
        sync_block_task_functions()  # Default ensure_loaded=True

    mock_ensure.assert_called_once()
