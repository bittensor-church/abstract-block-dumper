"""Unit tests for shortcuts module."""

from collections.abc import Generator
from typing import Any, Literal

import pytest

from abstract_block_dumper.decorators import BlockDumperRegistry
from abstract_block_dumper.models import ConditionType
from abstract_block_dumper.shortcuts import every_block, every_n_blocks, on_epoch


@pytest.fixture(autouse=True)
def clear_registry() -> Generator[None, Any, None]:
    """Clear registry before each test."""
    BlockDumperRegistry.clear_pendings()
    yield
    BlockDumperRegistry.clear_pendings()


def test_every_block_basic() -> None:
    @every_block(name="test_every_block")
    def test_func(block_number: int):
        return f"processed {block_number}"

    # Function should work
    assert test_func(100) == "processed 100"

    # Should register with correct parameters
    registrations = BlockDumperRegistry.get_pending_registrations()
    assert len(registrations) == 1

    config = registrations[0]
    assert config["name"] == "test_every_block"
    assert config["condition_type"] == ConditionType.EVERY_BLOCK


def test_every_block_without_name() -> None:
    @every_block()
    def my_test_function(block_number: int) -> Literal["processed"]:
        return "processed"

    registrations = BlockDumperRegistry.get_pending_registrations()
    config = registrations[0]
    assert "my_test_function" in config["name"]


def test_every_block_with_extra_kwargs() -> None:
    @every_block(name="test_with_extras", queue="custom_queue", max_retries=5, description="Test description")
    def test_func(block_number: int) -> Literal["processed"]:
        return "processed"

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["name"] == "test_with_extras"
    assert config["queue"] == "custom_queue"
    assert config["max_retries"] == 5
    assert config["description"] == "Test description"
    assert config["condition_type"] == ConditionType.EVERY_BLOCK


def test_every_n_blocks_basic() -> None:
    @every_n_blocks(name="test_every_n", n=10)
    def test_func(block_number: int):
        return f"processed {block_number}"

    assert test_func(100) == "processed 100"

    registrations = BlockDumperRegistry.get_pending_registrations()
    config = registrations[0]
    assert config["name"] == "test_every_n"
    assert config["condition_type"] == ConditionType.MODULO
    assert config["condition_params"]["modulo"] == 10
    assert config["condition_params"]["offset"] == 0


def test_every_n_blocks_with_extra_kwargs() -> None:
    """Test every_n_blocks shortcut with additional parameters."""

    @every_n_blocks(name="test_n_extras", n=15, offset=5, queue="priority_queue", timeout=600)
    def test_func(block_number: int) -> Literal["processed"]:
        return "processed"

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["name"] == "test_n_extras"
    assert config["condition_params"]["modulo"] == 15
    assert config["condition_params"]["offset"] == 5
    assert config["queue"] == "priority_queue"
    assert config["timeout"] == 600


@pytest.mark.parametrize(
    "condition,expected_condition",
    [
        (ConditionType.EPOCH_START, ConditionType.EPOCH_START),
        (ConditionType.EPOCH_MIDDLE, ConditionType.EPOCH_MIDDLE),
        (ConditionType.EPOCH_END, ConditionType.EPOCH_END),
    ],
)
def test_on_epoch_positions(condition, expected_condition) -> None:
    @on_epoch(condition=condition, netuids=[1, 22], name="test_epoch")
    def test_func(block_number: int, netuid: int | None = None) -> Literal["processed"]:
        return "processed"

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["name"] == "test_epoch"
    assert config["condition_type"] == expected_condition
    assert config["netuid_values"] == [1, 22]

    # default netuid_offset
    assert config["condition_params"]["netuid_offset"] is True


def test_on_epoch_with_netuids() -> None:
    @on_epoch(condition=ConditionType.EPOCH_START, netuids=[1, 2, 3], name="test_with_netuids")
    def test_func(block_number: int, netuid: int | None = None) -> str:
        return f"processed {block_number} for {netuid}"

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["netuid_values"] == [1, 2, 3]


def test_on_epoch_with_single_netuid() -> None:
    @on_epoch(condition=ConditionType.EPOCH_START, netuids=22, name="test_with_single_netuid")
    def test_with_single_netuid(block_number: int, netuid: int | None = None) -> str:
        return f"processed {block_number} for {netuid}"

    config = BlockDumperRegistry.get_pending_registrations()[0]
    assert config["netuid_values"] == [22]


def test_shortcut_invalid_function_signature():
    """Test that shortcuts validate function signatures."""
    with pytest.raises(ValueError, match="must have 'block_number' parameter"):

        @every_block(name="invalid")
        def invalid_func(some_param: int) -> Literal["invalid"]:
            return "invalid"


def test_on_epoch_rejects_empty_netuids():
    """Test that on_epoch rejects empty netuid lists."""

    with pytest.raises(ValueError, match="requires netuid\\(s\\) to be specified"):

        @on_epoch(condition=ConditionType.EPOCH_START, netuids=[], name="test_empty")
        def test_func(block_number: int, netuid: int | None = None) -> Literal["processed"]:
            return "processed"


def test_on_epoch_requires_netuid_parameter():
    """Test that on_epoch requires netuid parameter in function signature."""

    with pytest.raises(ValueError, match="with epoch condition must have 'netuid' parameter"):

        @on_epoch(condition=ConditionType.EPOCH_START, netuids=[1, 22], name="test_missing_netuid")
        def test_func(block_number: int) -> Literal["processed"]:
            return "processed"


def test_on_epoch_requires_netuid_default_value():
    """Test that on_epoch requires netuid parameter to have default value."""

    with pytest.raises(ValueError, match="netuid parameter should have default value of None"):

        @on_epoch(condition=ConditionType.EPOCH_START, netuids=[1, 22], name="test_no_default")
        def test_func(block_number: int, netuid: int) -> Literal["processed"]:
            return "processed"
