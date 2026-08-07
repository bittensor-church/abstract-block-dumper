import json
from collections.abc import Callable

import bittensor as bt
import structlog
from celery import current_task
from django.conf import settings

logger = structlog.get_logger(__name__)


def get_bittensor_client(network: str = "finney") -> bt.Subtensor:
    """
    Get a cached bittensor client.

    The client is cached indefinitely since network configuration
    doesn't change during runtime.
    """
    logger.info("Creating new bittensor client for network", network=network)
    return bt.Subtensor(network=network)


def get_current_celery_task_id() -> str:
    """Get current celery task id."""
    try:
        celery_task_id = current_task.id
    except Exception:
        celery_task_id = ""
    return str(celery_task_id)


def get_executable_path(func: Callable) -> str:
    """Get executable path for the callable `func`."""
    return ".".join([func.__module__, func.__name__])


def get_max_attempt_limit() -> int:
    default_max_attempts = 3
    return getattr(settings, "BLOCK_DUMPER_MAX_ATTEMPTS", default_max_attempts)


def serialize_args(args: dict) -> str:
    return json.dumps(args, sort_keys=True)
