from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import cloudpickle

from abstract_block_dumper.models import ConditionType


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

    def __init__(self, position: ConditionType, tempo: int | None = None, netuid_offset: bool = True):
        self.tempo = tempo
        self.position = position
        self.netuid_offset = netuid_offset

    def should_execute(self, block_number: int, netuid: int | None = None) -> bool:
        if netuid is None:
            raise ValueError("netuid must be provided for EpochBoundaryCondition")

        tempo = self.get_tempo(netuid)

        if self.netuid_offset:
            BITTENSOR_BUG_OFFSET = 1  # Adjust for known bittensor bug
            BLOCK_INDEX_OFFSET = 1  # Offset to align with epoch start
            block_index = (block_number + netuid + BLOCK_INDEX_OFFSET + BITTENSOR_BUG_OFFSET) % tempo
        else:
            block_index = block_number % tempo

        if self.position == ConditionType.EPOCH_START:
            return block_index == 0

        elif self.position == ConditionType.EPOCH_MIDDLE:
            if tempo % 2 != 0:
                return False
            return block_index == (tempo // 2)

        elif self.position == ConditionType.EPOCH_END:
            return block_index == (tempo - 1)

        return False

    def get_tempo(self, netuid: int) -> int:
        tempo = {
            # Handle known netuids with specific tempos
            0: 100,
            1: 99,
        }.get(netuid, 360)  # Default tempo
        return self.get_epoch_duration(tempo)

    @staticmethod
    def get_epoch_duration(tempo: int) -> int:
        # Because of bittensor bug epoch is really one block longer than tempo
        return tempo + 1


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
        return EpochBoundaryCondition(
            tempo=params["tempo"],
            position=condition_type,
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
