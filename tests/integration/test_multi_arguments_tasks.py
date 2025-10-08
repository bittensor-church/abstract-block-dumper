import pytest

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.executor import celery_unit
from abstract_block_dumper.memory_registry import task_registry
from abstract_block_dumper.models import TaskAttempt


def multi_arg_task(block_number: int, netuid: int, custom_param: str) -> str:
    return f"Block {block_number}, netuid {netuid}, custom_param {custom_param}"


@pytest.mark.django_db
def test_multi_arguments_tasks():
    task_registry.clear()

    multi_args = [
        {"netuid": 1, "custom_param": "test1"},
        {"netuid": 2, "custom_param": "test2"},
    ]

    block_task(condition=lambda bn, **kwargs: bn % 10 == 0, args=multi_args)(multi_arg_task)

    block_number = 100

    for args in multi_args:
        task_attempt, _ = TaskAttempt.create_or_get_pending(
            block_number=block_number,
            executable_path=f"{multi_arg_task.__module__}.{multi_arg_task.__name__}",
            args=args,
        )

        result = celery_unit(block_number, args, task_attempt.executable_path)

        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.SUCCESS

        expected_result = f"Block {block_number}, netuid {args['netuid']}, custom_param {args['custom_param']}"
        assert result == expected_result
        assert task_attempt.execution_result == expected_result

    task_registry.clear()
