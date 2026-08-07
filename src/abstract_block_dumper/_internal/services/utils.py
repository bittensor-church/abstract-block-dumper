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


def get_backfill_queue() -> str | None:
    """Return the optional Celery queue reserved for backfill submissions."""
    queue = getattr(settings, "BLOCK_DUMPER_BACKFILL_QUEUE", None)
    if queue is None:
        return None
    if not isinstance(queue, str) or not queue.strip():
        msg = "BLOCK_DUMPER_BACKFILL_QUEUE must be a non-empty string or None"
        raise ValueError(msg)
    return queue.strip()


def resolve_retry_queue(stored_queue: str | None) -> str | None:
    """
    Resolve the queue recorded on a TaskAttempt into the queue to retry it on.

    The backfill queue is the only override the dumper ever records, so a recorded
    queue means "this was a backfill submission" and the current
    ``BLOCK_DUMPER_BACKFILL_QUEUE`` decides where backfill work goes now:

    - Setting unchanged: retry on the same queue.
    - Setting renamed: follow the rename, so the retry keeps its capacity isolation
      instead of landing on a queue no worker consumes.
    - Setting removed: backfill routing is gone, so fall back to None — the task's own
      ``celery_kwargs`` queue, or Celery's default live queue when it declares none.

    Live submissions record no queue and always resolve to None.
    """
    if not stored_queue:
        return None

    backfill_queue = get_backfill_queue()
    if backfill_queue is None:
        logger.info(
            "Backfill queue is no longer configured, retrying on the default queue",
            stored_queue=stored_queue,
        )
        return None

    if backfill_queue != stored_queue:
        logger.info(
            "Backfill queue was renamed, retrying on the current one",
            stored_queue=stored_queue,
            backfill_queue=backfill_queue,
        )
    return backfill_queue


def serialize_args(args: dict) -> str:
    return json.dumps(args, sort_keys=True)
