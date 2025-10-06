import pytest

from abstract_block_dumper.discovery import ensure_modules_loaded
from abstract_block_dumper.memory_registry import MemoryRegistry
from abstract_block_dumper.models import TaskAttempt


@pytest.mark.django_db
def test_task_registraion_worfklow(setup_test_tasks):
    registry_items = MemoryRegistry.get_functions()
    assert len(registry_items) == 2

    ensure_modules_loaded()

    # Verify TaskAttempt records can be created
    task_attempt, created = TaskAttempt.create_or_get_pending(
        block_number=100,
        executable_path="tests.test_e2e.every_block_task_func",
        args={}
    )
    assert created
    assert task_attempt.status == TaskAttempt.Status.PENDING