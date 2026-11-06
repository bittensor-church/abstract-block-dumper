from __future__ import annotations

import abc
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from celery import Task

from abstract_block_dumper._internal.exceptions import ConditionEvaluationError
from abstract_block_dumper._internal.services.block_source import LATEST_BLOCK_SOURCE, BlockSource

if TYPE_CHECKING:
    from abstract_block_dumper._internal.services.queue_policy import Queue

logger = structlog.getLogger(__name__)


@dataclass
class RegistryItem:
    condition: Callable[..., bool]
    function: Task
    args: list[dict[str, Any]] | None = None
    block_source: BlockSource = LATEST_BLOCK_SOURCE
    backfilling_lookback: int | None = None
    # A queue name for tasks declared with `@block_task`, a `Queue` for `BlockDumper` tasks.
    backfill_queue: Queue | str | None = None
    celery_kwargs: dict[str, Any] = field(default_factory=dict)
    # Set for `BlockDumper` tasks only; live submissions then go to this queue.
    live_queue: Queue | None = None
    # Seconds per block, to turn a queue's `give_up_after_blocks` into a deadline.
    block_time_seconds: float = 12

    def match_condition(self, block_number: int, **kwargs: dict[str, Any]) -> bool:
        """Check if condition matches for given block and arguments."""
        try:
            return self.condition(block_number, **kwargs)
        except Exception as exc:
            logger.exception(
                "Condition evaluation failed",
                condition=self.function.__name__,
                block_number=block_number,
            )
            msg = "Failed to evaluate condition"
            raise ConditionEvaluationError(msg) from exc

    def get_execution_args(self) -> list[dict[str, Any]]:
        """Get list of argument sets for execution."""
        return self.args or [{}]

    @property
    def executable_path(self) -> str:
        """Get the importable path to the function."""
        if hasattr(self.function, "name") and self.function.name is not None:
            return self.function.name

        return f"{self.function.__module__}.{self.function.__name__}"

    def requires_backfilling(self) -> bool:
        """Check if this item requires backfilling."""
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
        fields: dict[str, Any] = {
            "function_name": item.function.__name__,
            "executable_path": item.executable_path,
            "args_counter": len(item.args or []),
            "block_source": item.block_source.name,
            "backfilling_lookback": item.backfilling_lookback,
            "backfill_queue": item.backfill_queue,
        }
        if item.live_queue is not None:
            fields.update(backfill_queue=getattr(item.backfill_queue, "name", None), live_queue=item.live_queue.name)
        logger.info("Registered function", **fields)

    def replace_functions(self, items: list[RegistryItem]) -> None:
        """Make `items` the whole registry, e.g. to undo the registrations of a module that failed to import."""
        self._functions = list(items)

    def get_functions(self) -> list[RegistryItem]:
        return self._functions

    def clear(self) -> None:
        self._functions = []

    def get_by_executable_path(self, executable_path: str) -> RegistryItem | None:
        for registry_item in self.get_functions():
            if registry_item.executable_path == executable_path:
                return registry_item
        return None


task_registry = MemoryRegistry()
