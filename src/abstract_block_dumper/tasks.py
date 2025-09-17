import traceback

import structlog
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from abstract_block_dumper.models import ScheduledTask
from abstract_block_dumper.utils import load_function_from_path

logger = structlog.wrap_logger(get_task_logger(__name__))


# Default to 5 minutes
DEFAULT_BLOCK_DUMPER_TASK_TIMEOUT = 300


@shared_task(
    bind=True, autoretry_for=(), time_limit=settings.BLOCK_DUMPER_TASK_TIMEOUT or DEFAULT_BLOCK_DUMPER_TASK_TIMEOUT
)
def execute_block_task(self, task_id: int) -> None:
    """
    Execute a single block task.
    """
    try:
        task = ScheduledTask.objects.get(
            id=task_id,
            status=ScheduledTask.Status.PENDING,
        )
        task.mark_as_started(self.request.id)
    except ScheduledTask.DoesNotExist:
        logger.debug(f"Task {task_id} not available for processing.")
        return

    started_at = timezone.now()

    try:
        func = load_function_from_path(task.config.function_path)
        if func is None:
            raise ImportError(f"Could not load function from path: {task.config.function_path}")

        if task.netuid is not None:
            result = func(task.block_number, task.netuid)
        else:
            result = func(task.block_number)

        duration = timezone.now() - started_at

        task.mark_as_success(result_data={"result": str(result)} if result is not None else None, duration=duration)
        logger.info(
            "Successfully executed block task",
            task_id=task.id,
            config=task.config.name,
            block_number=task.block_number,
            netuid=task.netuid,
            duration=duration.total_seconds(),
            result=result,
        )
    except Exception as e:
        error_message = str(e)
        error_traceback = traceback.format_exc()

        task.mark_as_failed(error_message, error_traceback)

        logger.error(
            "Task failed",
            task_id=task.id,
            config=task.config.name,
            block_number=task.block_number,
            netuid=task.netuid,
            error=str(e),
        )
        if task.can_retry():
            execute_block_task.apply_async(
                args=[task.id],
                queue=task.config.queue,
                eta=task.next_retry_at,
            )
            logger.info(f"Scheduled retry {task.retry_count} for task {task.id}")
