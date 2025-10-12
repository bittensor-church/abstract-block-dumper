import abc
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import structlog
from celery import Task

from abstract_block_dumper.exceptions import ConditionEvaluationError

logger = structlog.getLogger(__name__)


@dataclass
class RegistryItem:
    condition: Callable[..., bool]
    function: Task
    args: list[dict[str, Any]] | None = None
    backfilling_lookback: int | None = None
    celery_kwargs: dict[str, Any] = field(default_factory=dict)

    def match_condition(self, block_number: int, **kwargs) -> bool:
        """
        Check if condition matches for given block and arguments
        """
        try:
            return self.condition(block_number, **kwargs)
        except Exception as e:
            logger.error(
                "Condition evaluation failed",
                condition=self.function.__name__,
                block_number=block_number,
                exc_info=True,
            )
            raise ConditionEvaluationError(f"Failed to evaluate condition: {e}") from e

    def get_execution_args(self) -> list[dict[str, Any]]:
        """
        Get list of argument sets for execution
        """
        return self.args or [{}]

    @property
    def executable_path(self) -> str:
        """
        Get the importable path to the function.
        """
        if hasattr(self.function, "name") and self.function.name is not None:
            return self.function.name

        return ".".join([self.function.__module__, self.function.__name__])

    def requires_backfilling(self) -> bool:
        """
        Check if this item requires backfilling.
        """
        return self.backfilling_lookback is not None


class BaseRegistry(abc.ABC):
    @abc.abstractmethod
    def register_item(self, item: RegistryItem) -> None:
        pass

    @abc.abstractmethod
    def get_functions(self) -> list[RegistryItem]:
        pass

    @abc.abstractmethod
    def clear(self) -> None:
        pass

    @abc.abstractmethod
    def get_by_executable_path(self, executable_path: str) -> RegistryItem | None:
        pass


class MemoryRegistry(BaseRegistry):
    _functions: list[RegistryItem] = []

    def register_item(self, item: RegistryItem) -> None:
        self._functions.append(item)
        logger.info(
            "Registered function",
            function_name=item.function.__name__,
            executable_path=item.executable_path,
            args=item.args,
            backfilling_lookback=item.backfilling_lookback,
        )

    def get_functions(self) -> list[RegistryItem]:
        return self._functions

    def clear(self) -> None:
        self._functions = []

    def get_by_executable_path(self, executable_path: str) -> RegistryItem:
        for registry_item in self.get_functions():
            if registry_item.executable_path == executable_path:
                return registry_item
        # TODO: Improve this
        raise Exception("Function Not Found")


task_registry = MemoryRegistry()
