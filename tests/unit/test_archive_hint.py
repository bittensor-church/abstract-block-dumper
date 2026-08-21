from unittest.mock import patch

import pytest
from django.test import override_settings

from abstract_block_dumper._internal.services.archive_hint import (
    ARCHIVE_BLOCK_THRESHOLD,
    reset_head_cache,
    resolve_use_archive_network,
)
from tests.conftest import MockedSubtensor


@pytest.fixture(autouse=True)
def clear_head_cache():
    """The head cache is process-global; keep it from leaking between tests."""
    reset_head_cache()
    yield
    reset_head_cache()


def patched_head(head: int):
    """Patch the worker's head read to report ``head``."""
    return patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=MockedSubtensor(current_block=head),
    )


def test_recomputes_archive_for_a_block_that_fell_behind_since_dispatch():
    """The bug: a retry arrives with no hint, and the block now needs the archive node."""
    with patched_head(1000):
        assert resolve_use_archive_network(100, hint=False) is True


def test_stays_on_the_plain_node_for_a_recent_block():
    with patched_head(1000):
        assert resolve_use_archive_network(900, hint=False) is False


def test_never_downgrades_a_true_hint():
    """The head only moves forward, so a True hint can never become stale."""
    with patched_head(1000) as get_client:
        assert resolve_use_archive_network(999, hint=True) is True

    get_client.assert_not_called()


@pytest.mark.parametrize(
    ("blocks_behind", "expected"),
    [
        (ARCHIVE_BLOCK_THRESHOLD - 1, False),
        (ARCHIVE_BLOCK_THRESHOLD, False),
        (ARCHIVE_BLOCK_THRESHOLD + 1, True),
    ],
)
def test_threshold_boundary_is_strictly_greater_than(blocks_behind, expected):
    head = 1_000_000
    with patched_head(head):
        assert resolve_use_archive_network(head - blocks_behind, hint=False) is expected


@pytest.mark.parametrize("hint", [False, True])
def test_falls_back_to_the_hint_when_the_head_cannot_be_read(hint):
    """A dead endpoint must not turn every task into an error."""
    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        side_effect=ConnectionError("endpoint down"),
    ):
        assert resolve_use_archive_network(100, hint=hint) is hint


def test_head_is_read_once_per_ttl_across_many_tasks():
    subtensor = MockedSubtensor(current_block=1000)

    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=subtensor,
    ):
        for _ in range(5):
            assert resolve_use_archive_network(100, hint=False) is True

    assert subtensor.block_reads == 1


@override_settings(BLOCK_DUMPER_HEAD_CACHE_TTL=0)
def test_head_is_re_read_when_the_ttl_expires():
    subtensor = MockedSubtensor(current_block=1000)

    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=subtensor,
    ):
        resolve_use_archive_network(100, hint=False)
        resolve_use_archive_network(100, hint=False)

    assert subtensor.block_reads == 2


def test_a_failed_head_read_does_not_poison_the_cache():
    subtensor = MockedSubtensor(current_block=1000)

    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        side_effect=ConnectionError("endpoint down"),
    ):
        assert resolve_use_archive_network(100, hint=False) is False

    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=subtensor,
    ):
        assert resolve_use_archive_network(100, hint=False) is True
