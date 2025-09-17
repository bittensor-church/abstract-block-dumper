from collections.abc import Callable

from abstract_block_dumper.decorators import block_task
from abstract_block_dumper.models import ConditionType, EpochPosition


def every_block(name: str | None = None, **kwargs) -> Callable:
    """
    Shortcut for block dumper that runs every block.

    Example:
    ```python

    @every_block(name="my_every_block_dumper")
    def my_dumper(block_number: int):
        pass
    ```

    :param name: Optional name for the block dumper task.
    :param kwargs: Additional keyword arguments for the block dumper decorator.
    :return: Decorator for the block dumper function.

    """
    return block_task(name=name, condition=ConditionType.EVERY_BLOCK, **kwargs)


def every_n_blocks(name: str | None = None, n: int = 120, offset: int = 0, **kwargs) -> Callable:
    """
    Shortcut for block dumper that runs every N blocks.

    Example:
    ```python

    @every_n_blocks(name="my_n_block_dumper", n=50, offset=10)
    def my_dumper(block_number: int):
        pass
    ```

    :param name: Optional name for the block dumper task.
    :param n: Interval of blocks to trigger the dumper.
    :param offset: Offset to adjust the starting block for triggering.
    :param kwargs: Additional keyword arguments for the block dumper decorator.
    :return: Decorator for the block dumper function.

    """
    return block_task(name=name, condition=ConditionType.MODULO, modulo=n, offset=offset, **kwargs)


def on_epoch(
    name: str | None = None,
    position: EpochPosition = EpochPosition.START,
    netuids: list[int] | None = None,
) -> Callable:
    """
    Shortcut for block dumper that runs on epoch boundaries.

    Example:
    ```python

    @on_epoch(name="my_epoch_dumper", position=EpochPosition.START, netuids=[1, 2])
    def my_dumper(block_number: int):
        pass
    ```

    :param name: Optional name for the block dumper task.
    :param position: Position within the epoch to trigger the dumper (START, MIDDLE, END).
    :param netuids: List of netuids to apply the dumper to. If None, applies to all netuids.
    :return: Decorator for the block dumper function.

    """
    condition_map = {
        EpochPosition.START: ConditionType.EPOCH_START,
        EpochPosition.MIDDLE: ConditionType.EPOCH_MIDDLE,
        EpochPosition.END: ConditionType.EPOCH_END,
    }.get(position, ConditionType.EPOCH_START)

    return block_task(
        name=name,
        condition=condition_map,
        tempo=300,
        netuid_offset=True,
        netuid=netuids,
    )
