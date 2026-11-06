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


def get_current_message_id() -> str | None:
    """Id of the Celery task message being executed, or None outside a Celery task."""
    try:
        return current_task.request.id
    except AttributeError:
        return None


def get_current_time_limit() -> float | None:
    """
    Hard time limit the worker enforces on the Celery task being executed.

    Mirrors the worker's own precedence: the limit sent with the message, the task's
    `time_limit`, then the app's `task_time_limit`, which the worker's pool applies to tasks
    without a limit of their own. None when no hard limit applies or outside a Celery task.
    """
    try:
        request = current_task.request
        task_time_limit = current_task.time_limit
        app_time_limit = current_task.app.conf.task_time_limit
    except AttributeError:
        return None
    message_time_limit, _soft_time_limit = request.timelimit or (None, None)
    return message_time_limit or task_time_limit or app_time_limit


def get_executable_path(func: Callable) -> str:
    """Get executable path for the callable `func`."""
    return ".".join([func.__module__, func.__name__])


def get_max_attempt_limit() -> int:
    default_max_attempts = 3
    return getattr(settings, "BLOCK_DUMPER_MAX_ATTEMPTS", default_max_attempts)


def serialize_args(args: dict) -> str:
    return json.dumps(args, sort_keys=True)
