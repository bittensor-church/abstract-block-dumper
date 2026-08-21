from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, TypeVar

import bittensor as bt
import structlog

import abstract_block_dumper._internal.services.utils as abd_utils

# Blocks older than this threshold from current head require the archive network.
# Defined in archive_hint so the scheduler, the workers and this client agree.
from abstract_block_dumper._internal.services.archive_hint import ARCHIVE_BLOCK_THRESHOLD, head_cache_ttl

if TYPE_CHECKING:
    import types
    from collections.abc import Coroutine

logger = structlog.get_logger(__name__)

T = TypeVar("T")

# A raw RPC call that has not answered by now is treated as a stalled connection. The
# scheduler drives these reads from its single thread, so an unbounded wait on one
# chain head would park every other head with it.
DEFAULT_RPC_TIMEOUT_SECONDS = 30.0


class BittensorConnectionClient:
    """
    Manages connections to regular and archive Bittensor subtensor networks.

    Supports context manager protocol for safe connection cleanup:
        with BittensorConnectionClient(network="finney") as client:
            block = client.subtensor.block
    """

    def __init__(self, network: str, rpc_timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS) -> None:
        self.network = network
        self.rpc_timeout = rpc_timeout
        self._subtensor: bt.Subtensor | None = None
        self._archive_subtensor: bt.Subtensor | None = None
        self._current_block_cache: int | None = None
        self._current_block_cached_at: float | None = None
        self._rpc_substrate: bt.RpcSubstrate | None = None
        self._rpc_loop: asyncio.AbstractEventLoop | None = None

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
        self._close_rpc_substrate()
        if self._rpc_loop is not None:
            self._rpc_loop.close()
            self._rpc_loop = None

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
        self._current_block_cached_at = None
        logger.debug("Subtensor connections closed")

    def get_block(self, *, finalized: bool = False) -> int:
        """
        Return the current block number of the selected chain head.

        Each call reads the chain directly, so the caller always sees where the
        chain is now rather than the next entry of a buffered stream.
        """
        if not finalized:
            return self.subtensor.block

        try:
            return self._run_rpc(self._read_finalized_block_number())
        except Exception:
            # The connection may be the reason this failed - including a timeout, where
            # the socket accepted the request and never answered. Drop it so the next
            # read reconnects instead of retrying on a dead socket.
            self._close_rpc_substrate()
            raise

    async def _read_finalized_block_number(self) -> int:
        """Resolve the finalized head hash and read its block number."""
        substrate = await self._connected_rpc_substrate()
        block_hash = await substrate.raw.get_chain_finalised_head()
        return await substrate.raw.get_block_number(block_hash=block_hash)

    async def _connected_rpc_substrate(self) -> bt.RpcSubstrate:
        """Get the raw RPC connection, opening it if needed."""
        if self._rpc_substrate is None:
            logger.info("Creating new RPC substrate connection", network=self.network)
            substrate = bt.RpcSubstrate(self.subtensor.endpoint)
            await substrate.connect()
            self._rpc_substrate = substrate
        return self._rpc_substrate

    def _run_rpc(self, coro: Coroutine[Any, Any, T]) -> T:
        """
        Run an async bittensor call on this client's own event loop.

        The loop is kept between calls so the RPC connection survives; the
        scheduler that drives this is synchronous and single-threaded. Every call is
        bounded by ``rpc_timeout``, so a websocket that goes quiet raises
        ``TimeoutError`` instead of parking the caller forever.
        """
        if self._rpc_loop is None or self._rpc_loop.is_closed():
            self._rpc_loop = asyncio.new_event_loop()
        return self._rpc_loop.run_until_complete(asyncio.wait_for(coro, self.rpc_timeout))

    def _close_rpc_substrate(self) -> None:
        """Close the raw RPC connection, if one is open."""
        if self._rpc_substrate is None:
            return

        try:
            self._run_rpc(self._rpc_substrate.close())
        except Exception:
            logger.warning("Error closing RPC substrate connection", exc_info=True)
        self._rpc_substrate = None

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

    def _current_head(self) -> int:
        """
        Return the chain head, re-reading it once the cached value ages out.

        This client outlives any single block: the scheduler holds one for its whole
        run. A head cached once and never refreshed only ever understates how far
        behind a block is, which sends historical blocks to the pruned node.
        """
        now = time.monotonic()
        if (
            self._current_block_cache is not None
            and self._current_block_cached_at is not None
            and (now - self._current_block_cached_at) < head_cache_ttl()
        ):
            return self._current_block_cache

        self._current_block_cache = self.subtensor.block
        self._current_block_cached_at = now
        return self._current_block_cache

    def get_subtensor_for_block(self, block_number: int) -> bt.Subtensor:
        """
        Get the appropriate subtensor for the given block number.

        Uses archive network for blocks older than ARCHIVE_BLOCK_THRESHOLD
        from the current head.
        """
        blocks_behind = self._current_head() - block_number

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
