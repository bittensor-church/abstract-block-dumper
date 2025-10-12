import pytest

import abstract_block_dumper.dal.django_dal as abd_dal
import abstract_block_dumper.services.utils as abd_utils
from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.discovery import ensure_modules_loaded
from abstract_block_dumper.models import TaskAttempt
from tests.conftest import every_block_task_func


@pytest.mark.django_db
def test_task_registration_workflow(setup_test_tasks):
    current_block_number = 100
    registry_items = task_registry.get_functions()
    assert len(registry_items) == 2

    ensure_modules_loaded()

    # Verify TaskAttempt records can be created
    task_attempt, created = abd_dal.task_create_or_get_pending(
        current_block_number, abd_utils.get_executable_path(every_block_task_func)
    )
    assert created
    assert task_attempt.status == TaskAttempt.Status.PENDING
