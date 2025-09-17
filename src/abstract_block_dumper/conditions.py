from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import cloudpickle

from abstract_block_dumper.models import ConditionType, EpochPosition


class BlockCondition(ABC):
    """
    Abstract base class for block conditions.
    """

    @abstractmethod
    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        """
        Determine if the block dumper should execute based on the block number and additional parameters.

        :param block_number: The current block number.
        :param netuid: The network ID (optional).
        :param kwargs: Additional parameters for condition evaluation.
        :return: True if the condition is met, False otherwise.
        """
        pass


class EveryBlockCondition(BlockCondition):
    """
    Condition to trigger on every block.
    """

    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        return True


class ModuloCondition(BlockCondition):
    """
    Condition to trigger every N blocks with an optional offset.
    """

    def __init__(self, modulo: int, offset: int = 0):
        self.modulo = modulo
        self.offset = offset

    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        return (block_number - self.offset) % self.modulo == 0


class EpochBoundaryCondition(BlockCondition):
    """
    Condition to trigger on epoch boundaries: start, middle, or end.
    """

    def __init__(self, tempo: int, position: EpochPosition, netuid_offset: bool = True):
        self.tempo = tempo
        self.position = position
        self.netuid_offset = netuid_offset

    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        from abstract_block_dumper.models import EpochPosition

        # Assuming tempo represents epoch length for simplicity
        if self.netuid_offset and netuid is not None:
            adjusted_block = block_number + netuid + 2
        else:
            adjusted_block = block_number

        block_in_epoch = adjusted_block % self.tempo

        if self.position == EpochPosition.START:
            return block_in_epoch == 0
        elif self.position == EpochPosition.MIDDLE:
            return block_in_epoch == self.tempo // 2
        elif self.position == EpochPosition.END:
            return block_in_epoch == self.tempo - 1
        return False


class CustomCondition(BlockCondition):
    """
    Condition to trigger based on a user-defined function.
    """

    def __init__(self, condition_func: Callable) -> None:
        self.condition_func = condition_func

    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        try:
            if netuid is not None:
                return self.condition_func(block_number, netuid=netuid)
            return self.condition_func(block_number)
        except Exception:
            return False


def get_condition_instance(condition_type: ConditionType, params: dict[str, Any]) -> BlockCondition:
    """
    Factory function to get the appropriate BlockCondition instance based on the condition type and parameters.

    :param condition_type: The type of condition.
    :param params: Parameters required to initialize the condition.
    :return: An instance of a BlockCondition subclass.
    """
    if condition_type == ConditionType.EVERY_BLOCK:
        return EveryBlockCondition()

    elif condition_type == ConditionType.MODULO:
        return ModuloCondition(
            modulo=params["modulo"],
            offset=params.get("offset", 0),
        )

    elif condition_type in [ConditionType.EPOCH_START, ConditionType.EPOCH_MIDDLE, ConditionType.EPOCH_END]:
        # map conditiontype with epoch position
        epoch_position = {
            ConditionType.EPOCH_START: EpochPosition.START,
            ConditionType.EPOCH_MIDDLE: EpochPosition.MIDDLE,
            ConditionType.EPOCH_END: EpochPosition.END,
        }.get(condition_type, EpochPosition.START)

        return EpochBoundaryCondition(
            tempo=params["tempo"],
            position=epoch_position,
            netuid_offset=params.get("netuid_offset", True),
        )

    elif condition_type == ConditionType.CUSTOM:
        if "function_bytes" in params:
            condition_func = cloudpickle.loads(params["function_bytes"])
            if not callable(condition_func):
                raise ValueError("Deserialized function is not callable.")
            return CustomCondition(condition_func)

    # Default to every block if unknown condition type
    return EveryBlockCondition()
