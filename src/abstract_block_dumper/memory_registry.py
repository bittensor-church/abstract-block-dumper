from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.getLogger(__name__)


@dataclass
class RegistryItem:
    condition: Callable
    function: Callable
    args: list[dict[str, Any]] | None = None
    backfilling_lookback: int | None = None
    celery_kwargs: dict[str, Any] | None = field(default_factory=dict)

    def match_condition(self, block_number: int, **kwargs) -> bool:
        """
        Check if condition matches for given block and arguments
        """
        try:
            return self.condition(block_number, **kwargs)
        except Exception:
            logger.error("Error calling conditional match:", exec_info=True)
        return False

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
        return ".".join([self.function.__module__, self.function.__name__])

    def requires_backfilling(self) -> bool:
        """
        Check if this item requires backfilling.
        """
        return self.backfilling_lookback is not None


class MemoryRegistry:

    _functions: list[RegistryItem] = []

    @classmethod
    def register(cls, item: RegistryItem) -> None:
        cls._functions.append(item)
        logger.info(
            "Registered function",
            function_name=item.function.__name__,
            executable_path=item.executable_path,
            args=item.args,
            backfilling_loockback=item.backfilling_lookback
        )

    @classmethod
    def get_functions(cls) -> list[RegistryItem]:
        return cls._functions

    @classmethod
    def clear(cls) -> None:
        cls._functions = []
