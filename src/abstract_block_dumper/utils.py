import time
from collections.abc import Callable
from functools import cache, lru_cache

import bittensor as bt
import structlog
from django.conf import settings

logger = structlog.get_logger(__name__)


@cache
def get_bittensor_client() -> bt.Subtensor:
    """
    Get a cached bittensor client.

    The client is cached indefinitely since network configuration
    doesn't change during runtime.
    """
    DEFAULT_BITTENSOR_NETWORK = "finney"
    network = settings.BITTENSOR_NETWORK or DEFAULT_BITTENSOR_NETWORK
    logger.info(f"Creating new bittensor client for network: {network}")
    return bt.subtensor(network=network)


@lru_cache(maxsize=1)
def _get_all_active_netuids_cached(cache_key: int) -> list[int]:
    """
    Internal cached function for fetching active netuids.

    :cache_key: An integer key to control cache invalidation timing.
    :return: List of active netuid integers
    """
    # cache_key is used for cache invalidation timing - not directly in function body
    try:
        subtensor = get_bittensor_client()
        netuids = [x.netuid for x in subtensor.get_all_subnets_info()]
        logger.debug(f"Fetched {len(netuids)} active netuids: {netuids} (cache_key: {cache_key})")
        return netuids
    except Exception as e:
        logger.error(f"Error fetching active netuids: {e}")
        return []


def get_all_active_netuids(cache_duration: int = 300) -> list[int]:
    """
    Get all active netuids with caching.

    :cache_duration: Cache duration in seconds (default: 5 minutes)
    :return: List of active netuid integers
    """
    # Create time-based cache key that expires every cache_duration seconds
    cache_key = int(time.time() // cache_duration)
    return _get_all_active_netuids_cached(cache_key)


def clear_caches() -> None:
    """
    Clear all cached data.

    Useful for testing or manual cache invalidation.
    """
    get_bittensor_client.cache_clear()
    _get_all_active_netuids_cached.cache_clear()
    logger.info("Cleared all bittensor client and netuids caches")


def load_function_from_path(function_path: str) -> Callable:
    """
    Load a function from a module path.
    """
    module_path, func_name = function_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[func_name])
    return getattr(module, func_name)
