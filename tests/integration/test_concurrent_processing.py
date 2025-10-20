import threading
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any

import pytest
from celery.result import EagerResult
from django.conf import settings

import abstract_block_dumper.dal.django_dal as abd_dal
from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.exceptions import CeleryTaskLocked
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.services.utils import get_executable_path


def simple_task(block_number: int) -> str:
    return f"Block number {block_number}"


@pytest.mark.skipif("sqlite" in settings.DATABASES["default"]["ENGINE"], reason="SQLite lacks proper row-level locking")
@pytest.mark.django_db(transaction=True)
def test_concurrent_celery_task_call() -> None:
    current_block = 6000
    block_task(condition=lambda bn: True)(simple_task)

    task_attempt, _ = abd_dal.task_create_or_get_pending(
        block_number=current_block, executable_path=get_executable_path(simple_task)
    )

    N = 20
    barrier = threading.Barrier(N)

    def celery_task_registry_call(i: int, task: TaskAttempt) -> Any | EagerResult | Any:
        barrier.wait()

        registry_item = task_registry.get_by_executable_path(task.executable_path)
        try:
            output = registry_item.function.delay(task.block_number)
        except CeleryTaskLocked:
            return None
        return output.result

    with ThreadPoolExecutor(max_workers=N) as exe:
        thread_jobs = [exe.submit(celery_task_registry_call, i, task_attempt) for i in range(N)]
        wait(thread_jobs)

    passed_jobs = sum(1 for job in thread_jobs if job.result())
    assert passed_jobs == 1, f"Expected exactly 1 passed job, got {passed_jobs}"

    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.SUCCESS
