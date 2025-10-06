import pytest

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.memory_registry import MemoryRegistry

from .django_fixtures import *  # noqa: F401, F403


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
    # Clear any existing registrations
    MemoryRegistry.clear()
    
    # Register test tasks using decorators

    # every block
    block_task(
        condition=lambda bn: True
    )(every_block_task_func)

    # every 5 blocks
    block_task(
        condition=lambda bn, netuid: bn % 5 == 0,
        args=[{"netuid": 1}, {"netuid": 2}]
    )(modulo_task_func)
    
    yield

    # Cleanup
    MemoryRegistry.clear()


@pytest.fixture(autouse=True)
def cleanup_memory_registry():
    MemoryRegistry.clear()
    yield
    MemoryRegistry.clear()
