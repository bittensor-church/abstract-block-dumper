"""Unit tests for decorators module."""

import pytest

from abstract_block_dumper.decorators import (
    BlockDumperRegistry,
    _build_condition_params,
    _build_netuid_values,
    _determine_netuid_type,
    block_task,
)
from abstract_block_dumper.models import ConditionType, NetuidType


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear registry before each test."""
    BlockDumperRegistry.clear_pendings()
    yield
    BlockDumperRegistry.clear_pendings()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "name": "test_task",
        "description": "Test task",
        "function_path": "test.function",
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


# Registry tests
def test_registry_register_and_get_pending(sample_config):
    """Test registering and retrieving pending registrations."""
    BlockDumperRegistry.register(sample_config)
    registrations = BlockDumperRegistry.get_pending_registrations()

    assert len(registrations) == 1
    assert registrations[0] == sample_config


def test_registry_clear_pendings(sample_config):
    """Test clearing pending registrations."""
    BlockDumperRegistry.register(sample_config)
    assert len(BlockDumperRegistry.get_pending_registrations()) == 1

    BlockDumperRegistry.clear_pendings()
    assert len(BlockDumperRegistry.get_pending_registrations()) == 0


def test_build_condition_params_modulo():
    """Test building condition params for modulo condition."""
    params = _build_condition_params(ConditionType.MODULO, {"modulo": 10})
    assert params == {"modulo": 10}


def test_build_condition_params_callable():
    """Test building condition params with callable condition."""

    def condition_func():
        return True

    params = _build_condition_params(condition_func, {})
    assert "function_bytes" in params


@pytest.mark.parametrize(
    "netuid,expected",
    [
        (None, NetuidType.NONE),
        ([1, 2, 3], NetuidType.MULTIPLE),
        (42, NetuidType.SINGLE),
        ("all", NetuidType.ALL),
    ],
)
def test_determine_netuid_type(netuid, expected):
    """Test determining netuid type for various inputs."""
    assert _determine_netuid_type(netuid) == expected


@pytest.mark.parametrize(
    "netuid,expected",
    [
        (None, []),
        ("all", []),
        ([1, 2, 3], [1, 2, 3]),
        (42, [42]),
    ],
)
def test_build_netuid_values(netuid, expected):
    assert _build_netuid_values(netuid) == expected


def test_block_task_all_params():
    """Test block_task decorator with all parameters."""

    @block_task(
        name="complex_task",
        condition=ConditionType.MODULO,
        modulo=10,
        netuid=[1, 2],
        queue="custom_queue",
        max_retries=5,
        retry_backoff=3,
        timeout=600,
        description="Complex test task",
    )
    def complex_func(block_number: int, netuid: int | None = None):
        return f"processed {block_number} for {netuid}"

    # Function works
    assert complex_func(100, 1) == "processed 100 for 1"

    # Configuration is correct
    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["name"] == "complex_task"
    assert config["description"] == "Complex test task"
    assert config["condition_type"] == ConditionType.MODULO
    assert config["condition_params"]["modulo"] == 10
    assert config["netuid_type"] == NetuidType.MULTIPLE
    assert config["netuid_values"] == [1, 2]
    assert config["queue"] == "custom_queue"
    assert config["max_retries"] == 5
    assert config["retry_backoff"] == 3
    assert config["timeout"] == 600


def test_block_task_auto_name():
    """Test block_task decorator auto-generates name from function."""

    @block_task()
    def my_auto_named_task(block_number: int):
        pass

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert "my_auto_named_task" in config["name"]


def test_block_task_invalid_signature():
    """Test block_task decorator with invalid function signature raises error."""
    with pytest.raises(ValueError, match="must have 'block_number' parameter"):

        @block_task()
        def invalid_func(some_param: int):
            pass


def test_block_task_preserves_metadata():
    """Test block_task decorator preserves original function metadata."""

    @block_task(name="preserve_test")
    def documented_func(block_number: int):
        return "test"

    # Config attached
    assert hasattr(documented_func, "_block_dumper_config")
    config = getattr(documented_func, "_block_dumper_config")
    assert config["name"] == "preserve_test"
