from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from abstract_block_dumper._internal.providers.bittensor_client import BittensorConnectionClient


@dataclass(frozen=True)
class BlockSource:
    """Read blocks with one finalization policy."""

    name: str
    finalized: bool

    def get_block(self, bittensor_client: BittensorConnectionClient) -> int:
        """Return the next block snapshot for this source."""
        return bittensor_client.get_block(finalized=self.finalized)


LATEST_BLOCK_SOURCE = BlockSource(name="latest", finalized=False)
FINALIZED_BLOCK_SOURCE = BlockSource(name="finalized", finalized=True)
