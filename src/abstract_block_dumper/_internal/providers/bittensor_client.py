import bittensor as bt
import structlog

import abstract_block_dumper._internal.services.utils as abd_utils

logger = structlog.get_logger(__name__)


# Blocks older than this threshold from current head require archive network
ARCHIVE_BLOCK_THRESHOLD = 300


class BittensorConnectionClient:
    """
    Manages connections to regular and archive Bittensor subtensor networks.
    """

    def __init__(self, network: str) -> None:
        self.network = network
        self._subtensor: bt.Subtensor | None = None
        self._archive_subtensor: bt.Subtensor | None = None
        self._current_block_cache: int | None = None

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
            self._current_block_cache = self.subtensor.get_current_block()

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
        """Reset all subtensor connections to force re-establishment."""
        self._subtensor = None
        self._archive_subtensor = None
        self._current_block_cache = None
        logger.info("Subtensor connections reset")
