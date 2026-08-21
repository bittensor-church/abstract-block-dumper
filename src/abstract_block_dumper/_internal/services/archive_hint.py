"""
Resolve the archive-network hint at execution time.

``_use_archive_network`` is computed when a task is dispatched but consumed when a
worker runs it, and an ETA sits between the two. Retries rebuild their kwargs from
the ``TaskAttempt`` row, which never held the hint -- it is stripped from
``args_json`` and travels in the Celery message alone -- so a retried historical
block would otherwise fall back to ``False`` and query a pruned node.

The chain head only moves forward, so ``use_archive`` is monotone: once true, always
true. Recomputing at execution can therefore only correct ``False`` to ``True``,
never the harmful direction, which is why nothing needs persisting on the row.
"""

import time

import structlog
from django.conf import settings

import abstract_block_dumper._internal.services.utils as abd_utils

logger = structlog.get_logger(__name__)

# Blocks older than this threshold from the current head require the archive network.
ARCHIVE_BLOCK_THRESHOLD = 300

# Roughly one block time, so this costs about one head read per block per worker
# process rather than one per task execution.
DEFAULT_HEAD_CACHE_TTL_SECONDS = 12.0

_subtensor = None
_head_cache: tuple[int, float] | None = None


def reset_head_cache() -> None:
    """Drop the cached head and connection. Used by tests and after a read failure."""
    global _subtensor, _head_cache  # noqa: PLW0603
    _subtensor = None
    _head_cache = None


def head_cache_ttl() -> float:
    """Seconds a cached chain head may be reused before it must be read again."""
    return getattr(settings, "BLOCK_DUMPER_HEAD_CACHE_TTL", DEFAULT_HEAD_CACHE_TTL_SECONDS)


def _worker_subtensor():  # noqa: ANN202
    """Get this process's subtensor, creating it on first use."""
    global _subtensor  # noqa: PLW0603
    if _subtensor is None:
        network = getattr(settings, "BITTENSOR_NETWORK", "finney")
        _subtensor = abd_utils.get_bittensor_client(network)
    return _subtensor


def current_head() -> int | None:
    """
    Return the current chain head, cached for ``BLOCK_DUMPER_HEAD_CACHE_TTL`` seconds.

    Returns ``None`` when the head cannot be read, so callers can fall back rather
    than fail. The connection is dropped on failure so the next read reconnects
    instead of retrying on a dead socket.
    """
    global _head_cache  # noqa: PLW0603

    now = time.monotonic()
    if _head_cache is not None and (now - _head_cache[1]) < head_cache_ttl():
        return _head_cache[0]

    try:
        head = _worker_subtensor().block
    except Exception:
        logger.warning("Could not read chain head for archive hint", exc_info=True)
        reset_head_cache()
        return None

    _head_cache = (head, now)
    return head


def resolve_use_archive_network(
    block_number: int,
    *,
    hint: bool = False,
    threshold: int = ARCHIVE_BLOCK_THRESHOLD,
) -> bool:
    """
    Decide whether ``block_number`` needs the archive network, as of now.

    ``hint`` is the value that arrived in the Celery message, which is absent on
    retries. A true hint is never downgraded: the head has only moved further away
    since it was computed, so the block can only need the archive network more.
    """
    if hint:
        return True

    head = current_head()
    if head is None:
        return hint

    return (head - block_number) > threshold
