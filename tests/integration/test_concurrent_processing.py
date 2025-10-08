import threading
from concurrent.futures import ThreadPoolExecutor, wait

import pytest
from django.conf import settings

from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.memory_registry import RegistryItem, task_registry
from abstract_block_dumper.models import TaskAttempt


def simple_task(block_number: int) -> str:
    return f"Block number {block_number}"


@pytest.mark.skipif("sqlite" in settings.DATABASES["default"]["ENGINE"], reason="SQLite lacks proper row-level locking")
@pytest.mark.django_db(transaction=True)
def test_concurrent_celery_task_call() -> None:
    registry_item = RegistryItem(
        function=simple_task,
        condition=lambda _bn: True,
        args=None,
        celery_kwargs=None,
        backfilling_lookback=None,
    )
    block_number = 500
    task_registry.register_item(registry_item)
    task_attempt = TaskAttempt.objects.create(
        block_number=block_number,
        executable_path=registry_item.executable_path,
    )

    block_number = block_number

    N = 20

    barrier = threading.Barrier(N)

    def celery_unit_call(i: int, task: TaskAttempt) -> None:
        barrier.wait()
        return celery_unit(task.block_number, task.args_dict, task.executable_path)

    with ThreadPoolExecutor(max_workers=N) as exe:
        thread_jobs = [exe.submit(celery_unit_call, i, task_attempt) for i in range(N)]

        wait(thread_jobs)

    passed_jobs = sum(1 for job in thread_jobs if job.result())
    assert passed_jobs == 1, f"Expected exactly 1 passed job, got {passed_jobs}"

    task_attempt.refresh_from_db()
    assert task_attempt.status == TaskAttempt.Status.SUCCESS
    assert task_attempt.celery_task_id is None
