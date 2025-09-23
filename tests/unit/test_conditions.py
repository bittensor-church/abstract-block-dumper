import cloudpickle
import pytest

from abstract_block_dumper.conditions import (
    CustomCondition,
    EpochBoundaryCondition,
    EveryBlockCondition,
    ModuloCondition,
    get_condition_instance,
)
from abstract_block_dumper.models import ConditionType


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
    condition_start = EpochBoundaryCondition(position=ConditionType.EPOCH_START, netuid_offset=False)
    condition_middle = EpochBoundaryCondition(position=ConditionType.EPOCH_MIDDLE, netuid_offset=False)
    condition_end = EpochBoundaryCondition(position=ConditionType.EPOCH_END, netuid_offset=False)

    # netuid=0 uses tempo=100, epoch_duration=101 (tempo + 1)
    # With netuid_offset=False: block_index = block % 101
    # Start: block % 101 == 0
    # Middle: returns False for odd epoch_duration (101 is odd)
    # End: block % 101 == 100

    for block_number, start_expected, middle_expected, end_expected in [
        (0, True, False, False),  # 0 % 101 == 0 (start), 101 is odd (no middle)
        (50, False, False, False),  # 50 % 101 == 50 (neither start nor end)
        (100, False, False, True),  # 100 % 101 == 100 (end)
        (101, True, False, False),  # 101 % 101 == 0 (start)
        (150, False, False, False),  # 150 % 101 == 49 (neither)
        (201, False, False, True),  # 201 % 101 == 100 (end)
        (202, True, False, False),  # 202 % 101 == 0 (start)
    ]:
        assert condition_start.should_execute(block_number, netuid=0) is start_expected
        assert condition_middle.should_execute(block_number, netuid=0) is middle_expected
        assert condition_end.should_execute(block_number, netuid=0) is end_expected


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


@pytest.mark.parametrize(
    "netuid,block_number,expected",
    [
        # netuid=0: epoch_duration=101, block_index = (block + 0 + 2) % 101 = (block + 2) % 101
        (0, 99, True),  # (99 + 2) % 101 = 0 (epoch start)
        (0, 200, True),  # (200 + 2) % 101 = 0 (epoch start)
        (0, 301, True),  # (301 + 2) % 101 = 0 (epoch start)
        (0, 0, False),  # (0 + 2) % 101 = 2 (not epoch start)
        (0, 50, False),  # (50 + 2) % 101 = 52 (not epoch start)
        # netuid=1: tempo=99, epoch_duration=100, block_index = (block + 1 + 2) % 100 = (block + 3) % 100
        (1, 97, True),  # (97 + 3) % 100 = 0 (epoch start)
        (1, 197, True),  # (197 + 3) % 100 = 0 (epoch start)
        (1, 297, True),  # (297 + 3) % 100 = 0 (epoch start)
        (1, 0, False),  # (0 + 3) % 100 = 3 (not epoch start)
        (1, 50, False),  # (50 + 3) % 100 = 53 (not epoch start)
        # netuid=22: default tempo=360, epoch_duration=361, block_index = (block + 22 + 2) % 361 = (block + 24) % 361
        (22, 337, True),  # (337 + 24) % 361 = 0 (epoch start)
        (22, 698, True),  # (698 + 24) % 361 = 0 (epoch start)
        (22, 1059, True),  # (1059 + 24) % 361 = 0 (epoch start)
        (22, 100, False),  # (100 + 24) % 361 = 124 (not epoch start)
    ],
)
def test_epoch_boundary_condition_with_netuid_offset(netuid, block_number, expected) -> None:
    """Test epoch boundary condition with netuid offset enabled for various netuids."""
    condition = EpochBoundaryCondition(position=ConditionType.EPOCH_START, netuid_offset=True)
    assert condition.should_execute(block_number, netuid=netuid) is expected


@pytest.mark.parametrize(
    "position,netuid,block_number,expected",
    [
        # netuid=22 with different epoch positions
        # epoch_duration=361, block_index = (block + 24) % 361
        # EPOCH_START: block_index == 0
        (ConditionType.EPOCH_START, 22, 337, True),  # (337 + 24) % 361 = 0
        (ConditionType.EPOCH_START, 22, 698, True),  # (698 + 24) % 361 = 0
        (ConditionType.EPOCH_START, 22, 1059, True),  # (1059 + 24) % 361 = 0
        (ConditionType.EPOCH_START, 22, 100, False),  # (100 + 24) % 361 = 124
        # EPOCH_MIDDLE: returns False for odd epoch durations (361 is odd)
        (ConditionType.EPOCH_MIDDLE, 22, 156, False),  # any block returns False
        (ConditionType.EPOCH_MIDDLE, 22, 517, False),  # any block returns False
        # EPOCH_END: block_index == 360
        (ConditionType.EPOCH_END, 22, 336, True),  # (336 + 24) % 361 = 360
        (ConditionType.EPOCH_END, 22, 697, True),  # (697 + 24) % 361 = 360
        (ConditionType.EPOCH_END, 22, 1058, True),  # (1058 + 24) % 361 = 360
        (ConditionType.EPOCH_END, 22, 100, False),  # (100 + 24) % 361 = 124
    ],
)
def test_epoch_boundary_condition_netuid_22(position, netuid, block_number, expected) -> None:
    """Test epoch boundary condition for netuid 22 with different positions."""
    condition = EpochBoundaryCondition(position=position, netuid_offset=True)
    assert condition.should_execute(block_number, netuid=netuid) is expected
