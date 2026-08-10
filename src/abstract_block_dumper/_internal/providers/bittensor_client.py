from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import bittensor as bt
import structlog

import abstract_block_dumper._internal.services.utils as abd_utils

if TYPE_CHECKING:
    import types

logger = structlog.get_logger(__name__)


# Blocks older than this threshold from current head require archive network
ARCHIVE_BLOCK_THRESHOLD = 300


class BittensorConnectionClient:
    """
    Manages connections to regular and archive Bittensor subtensor networks.

    Supports context manager protocol for safe connection cleanup:
        with BittensorConnectionClient(network="finney") as client:
            block = client.subtensor.block
    """

    def __init__(self, network: str) -> None:
        self.network = network
        self._subtensor: bt.Subtensor | None = None
        self._archive_subtensor: bt.Subtensor | None = None
        self._current_block_cache: int | None = None
        self._block_streams: dict[bool, Iterator[bt.BlockHeader]] = {}

    def __enter__(self) -> BittensorConnectionClient:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: types.TracebackType | None,
    ) -> None:
        """Context manager exit - ensures connections are closed."""
        self.close()

    def close(self) -> None:
        """Close all subtensor connections to prevent memory leaks."""
        self._block_streams.clear()

        if self._subtensor is not None:
            try:
                self._subtensor.close()
            except Exception:
                logger.warning("Error closing subtensor connection", exc_info=True)
            self._subtensor = None

        if self._archive_subtensor is not None:
            try:
                self._archive_subtensor.close()
            except Exception:
                logger.warning("Error closing archive subtensor connection", exc_info=True)
            self._archive_subtensor = None

        self._current_block_cache = None
        logger.debug("Subtensor connections closed")

    def get_block(self, *, finalized: bool = False) -> int:
        """Return the next block number from the selected chain subscription."""
        if finalized not in self._block_streams:
            self._block_streams[finalized] = self.subtensor.blocks(finalized=finalized)

        try:
            return next(self._block_streams[finalized]).number
        except Exception:
            self._block_streams.pop(finalized, None)
            raise

    def get_for_block(self, block_number: int) -> bt.Subtensor:
        """Get the appropriate subtensor client for the given block number."""
        raise NotImplementedError

    @property
    def subtensor(self) -> bt.Subtensor:
        """Get the regular subtensor connection, creating it if needed."""
        if self._subtensor is None:
            self._subtensor = abd_utils.get_bittensor_client(self.network)
        return self._subtensor

    @subtensor.setter
    def subtensor(self, value: bt.Subtensor | None) -> None:
        """Set or reset the subtensor connection."""
        self._subtensor = value

    @property
    def archive_subtensor(self) -> bt.Subtensor:
        """Get the archive subtensor connection, creating it if needed."""
        if self._archive_subtensor is None:
            self._archive_subtensor = abd_utils.get_bittensor_client("archive")
        return self._archive_subtensor

    @archive_subtensor.setter
    def archive_subtensor(self, value: bt.Subtensor | None) -> None:
        """Set or reset the archive subtensor connection."""
        self._archive_subtensor = value

    def get_subtensor_for_block(self, block_number: int) -> bt.Subtensor:
        """
        Get the appropriate subtensor for the given block number.

        Uses archive network for blocks older than ARCHIVE_BLOCK_THRESHOLD
        from the current head.
        """
        if self._current_block_cache is None:
            self._current_block_cache = self.subtensor.block

        blocks_behind = self._current_block_cache - block_number

        if blocks_behind > ARCHIVE_BLOCK_THRESHOLD:
            logger.debug(
                "Using archive network for old block",
                block_number=block_number,
                blocks_behind=blocks_behind,
            )
            return self.archive_subtensor
        return self.subtensor

    def refresh_connections(self) -> None:
        """Close and reset all subtensor connections to force re-establishment."""
        self.close()
        logger.info("Subtensor connections refreshed")
