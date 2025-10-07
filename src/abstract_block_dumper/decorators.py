from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from abstract_block_dumper.memory_registry import MemoryRegistry, RegistryItem

P = ParamSpec("P")
R = TypeVar("R")


def block_task(
    condition: Callable[..., bool],
    args: list[dict[str, Any]] | None = None,
    backfilling_lookback: int | None = None,
    celery_kwargs: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for registering block tasks.

    Args:
        condition: Lambda function that determines when to execute
        args: List of argument dictioneries for multi-execution
        backfilling_lookback: Number of blocks to backfill
        celery_kwargs: Additional Celeray task parameters

    Examples:
        @block_task(
            condition=lambda bn: bn % 100 == 0
        )
        def simple_task(block_number: int):
            pass

        @block_task(
            condition=lambda bn, netuid: bn + netuid % 100 == 0,
            args=[{"netuid": 3}, {"netuid": 22}],
            backfilling_loockback=300,
            celery_kwargs={"queue": "high-priority"}
        )
        def multi_netuid_task(block_number: int, netuid: int):
            pass

    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if not callable(condition):
            raise ValueError("condition must be a callable.")

        if args is not None and not isinstance(args, list):
            raise ValueError("args must be a list of dictionaries")

        registry_item = RegistryItem(
            condition=condition,
            function=func,
            args=args,
            backfilling_lookback=backfilling_lookback,
            celery_kwargs=celery_kwargs or {},
        )

        MemoryRegistry.register(registry_item)

        @wraps(func)
        def wrapper(*f_args, **f_kwargs) -> Any:
            return func(*f_args, **f_kwargs)

        wrapper._abd_registry_item = registry_item  # type: ignore

        return wrapper

    return decorator
