import cloudpickle
import pytest

from abstract_block_dumper.conditions import (
    CustomCondition,
    EpochBoundaryCondition,
    EveryBlockCondition,
    ModuloCondition,
    get_condition_instance,
)
from abstract_block_dumper.models import ConditionType, EpochPosition


def test_every_block_condition() -> None:
    condition = EveryBlockCondition()
    for block_number in range(360):
        assert condition.should_execute(block_number) is True


def test_modulo_condition() -> None:
    condition = ModuloCondition(modulo=50, offset=10)
    test_cases = [
        (0, False),
        (10, True),
        (59, False),
        (60, True),
        (110, True),
        (160, True),
        (210, True),
        (259, False),
        (260, True),
        (310, True),
        (359, False),
    ]
    for block_number, expected in test_cases:
        assert condition.should_execute(block_number) is expected


def test_epoch_boundary_condition() -> None:
    """
    Test the epoch boundary condition for block dumper.

    The test checks the condition at different block numbers for start, middle, and end of an epoch.
    """
    # Assuming tempo is 100 for testing
    tempo = 100
    condition_start = EpochBoundaryCondition(tempo=tempo, position=EpochPosition.START)
    condition_middle = EpochBoundaryCondition(tempo=tempo, position=EpochPosition.MIDDLE)
    condition_end = EpochBoundaryCondition(tempo=tempo, position=EpochPosition.END)

    for block_number, start_expected, middle_expected, end_expected in [
        (0, True, False, False),
        (49, False, False, False),
        (50, False, True, False),
        (99, False, False, True),
        (100, True, False, False),
        (149, False, False, False),
        (150, False, True, False),
        (199, False, False, True),
        (200, True, False, False),
    ]:
        assert condition_start.should_execute(block_number) is start_expected
        assert condition_middle.should_execute(block_number) is middle_expected
        assert condition_end.should_execute(block_number) is end_expected


@pytest.mark.parametrize(
    ("condition_type", "expected_condition"),
    [
        (ConditionType.EVERY_BLOCK, EveryBlockCondition),
        (ConditionType.MODULO, ModuloCondition),
        (ConditionType.EPOCH_START, EpochBoundaryCondition),
        (ConditionType.EPOCH_MIDDLE, EpochBoundaryCondition),
        (ConditionType.EPOCH_END, EpochBoundaryCondition),
        (ConditionType.CUSTOM, CustomCondition),
    ],
)
def test_get_condition_instance(condition_type, expected_condition):
    params = {
        "modulo": 50,
        "offset": 10,
        "tempo": 100,
        "netuid_offset": True,
        "function_bytes": cloudpickle.dumps(lambda x: x % 10 == 0),
    }
    condition = get_condition_instance(condition_type, params)

    assert isinstance(condition, expected_condition)
    if condition_type == ConditionType.MODULO:
        assert getattr(condition, "modulo") == 50
        assert getattr(condition, "offset") == 10

    elif condition_type in [ConditionType.EPOCH_START, ConditionType.EPOCH_MIDDLE, ConditionType.EPOCH_END]:
        assert getattr(condition, "tempo") == 100
        assert getattr(condition, "netuid_offset") is True
    elif condition_type == ConditionType.CUSTOM:
        assert callable(getattr(condition, "condition_func"))
        assert condition.should_execute(20) is True
        assert condition.should_execute(25) is False
