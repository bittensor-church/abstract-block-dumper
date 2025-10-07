import importlib
from collections.abc import Callable
from functools import cache
from typing import Any

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
    network = getattr(settings, "BITTENSOR_NETWORK", DEFAULT_BITTENSOR_NETWORK)
    logger.info(f"Creating new bittensor client for network: {network}")
    return bt.subtensor(network=network)


def clear_caches() -> None:
    """
    Clear all cached data.

    Useful for testing or manual cache invalidation.
    """
    get_bittensor_client.cache_clear()
    logger.info("Cleared all bittensor client and netuids caches")


def load_function_from_path(function_path: str) -> Callable[..., Any]:
    """
    Load a function from a module path.
    """
    module_path, func_name = function_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
