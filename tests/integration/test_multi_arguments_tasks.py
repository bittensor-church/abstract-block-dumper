import pytest

import abstract_block_dumper.dal.django_dal as abd_dal
from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.services.utils import get_executable_path


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
    executable_path = get_executable_path(multi_arg_task)
    registry_item = task_registry.get_by_executable_path(executable_path)
    assert registry_item is not None
    assert callable(registry_item.function)

    for args in multi_args:
        task_attempt, _ = abd_dal.task_create_or_get_pending(
            block_number=block_number,
            executable_path=executable_path,
            args=args,
        )

        eager_output = registry_item.function.delay(block_number, **args)
        output = eager_output.result

        assert isinstance(output, dict)
        assert "result" in output

        result = output.get("result")
        expected_result = f"Block {block_number}, netuid {args['netuid']}, custom_param {args['custom_param']}"
        assert result == expected_result

        task_attempt.refresh_from_db()
        assert task_attempt.status == TaskAttempt.Status.SUCCESS
        assert task_attempt.execution_result == expected_result

    task_registry.clear()
