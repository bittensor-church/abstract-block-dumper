import json
from typing import Any

import structlog
from celery import shared_task
from django.db import OperationalError, transaction

from abstract_block_dumper.memory_registry import RegistryItem
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.utils import load_function_from_path

logger = structlog.get_logger(__name__)


def execute_from_path(executable_path: str, block_number: int, args: dict[str, Any]) -> Any:
    function = load_function_from_path(executable_path)

    # Merge block number with args
    execution_args = {"block_number": block_number, **args}

    logger.info(
        "Executing function",
        executable_path=executable_path,
        block_number=block_number,
        args=args,
    )
    return function(**execution_args)


@shared_task(bind=True)
def celery_unit(self, block_number, args: dict[str, Any], executable_path: str) -> None:
    """
    Celery task unit with atomic execution and locking.

    Sample input:
    block_number=1000;args="netuid=1";celery_task_name="project.app.foo3"
    """
    args_json = json.dumps(args, sort_keys=True)

    with transaction.atomic():
        try:
            task_attempt = TaskAttempt.objects.select_for_update(nowait=True).get(
                block_number=block_number, executable_path=executable_path, args_json=args_json
            )
        except TaskAttempt.DoesNotExist:
            logger.warning(
                "TaskAttempt not found - possible race condition",
                block_number=block_number,
                executable_path=executable_path,
                args=args,
            )
            return
        except OperationalError:
            logger.info(
                "Task already being processed by another worker",
                block_number=block_number,
                executable_path=executable_path,
            )
            return

        if task_attempt.status != TaskAttempt.Status.PENDING:
            logger.info(
                "Task already processed",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return

        task_attempt.mark_started(self.request.id)
        try:
            logger.info(
                "Starting task execution",
                task_id=task_attempt.id,
                block_number=block_number,
                executable_path=executable_path,
                celery_task_id=self.request.id,
            )

            result = execute_from_path(executable_path, block_number, args)
            task_attempt.mark_success(result)

            logger.info("Task completed successfully", task_id=task_attempt.id, result=str(result) if result else None)
            return result
        except Exception as e:
            logger.error("Task execution failed", task_id=task_attempt.id, error_type=type(e).__name__, exc_info=True)
            task_attempt.mark_failed()

    # Schedule retry after transaction commits
    if task_attempt.status == TaskAttempt.Status.FAILED and task_attempt.can_retry():
        try:
            schedule_retry(task_attempt)
        except Exception:
            logger.error(
                "Failed to schedule retry, will be picked up by recovery",
                task_id=task_attempt.id,
                next_retry_at=task_attempt.next_retry_at,
                exc_info=True,
            )


def schedule_retry(task_attempt: TaskAttempt) -> None:
    """
    Schedule a retry for a failed task.
    Task must already be in FAILED state with next_retry_at set by mark_failed().
    This function only schedules the Celery task - the TaskAttempt will be reset to PENDING
    when the retry executes via create_or_get_pending().
    """
    if not task_attempt.next_retry_at:
        logger.error(
            "Cannot schedule retry without next_retry_at",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
        )
        return

    if task_attempt.status != TaskAttempt.Status.FAILED:
        logger.warning(
            "Attempted to schedule retry for non-failed task",
            task_id=task_attempt.id,
            status=task_attempt.status,
        )
        return

    logger.info(
        "Scheduling retry",
        task_id=task_attempt.id,
        attempt_count=task_attempt.attempt_count,
        next_retry_at=task_attempt.next_retry_at,
    )

    celery_unit.apply_async(
        args=[
            task_attempt.block_number,
            task_attempt.args_dict,
            task_attempt.executable_path,
        ],
        eta=task_attempt.next_retry_at,
    )


class CeleryExecutor:
    def execute(self, registry_item: RegistryItem, block_number: int, args: dict[str, Any]) -> None:
        task_attempt, created = TaskAttempt.create_or_get_pending(
            block_number=block_number,
            executable_path=registry_item.executable_path,
            args=args,
        )
        if not created and task_attempt.status != TaskAttempt.Status.PENDING:
            logger.debug(
                "Task already exists",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return

        celery_kwargs = {
            **(registry_item.celery_kwargs or {}),
            "args": [block_number, args, registry_item.executable_path],
        }

        if task_attempt.next_retry_at:
            celery_kwargs["eta"] = task_attempt.next_retry_at

        logger.info(
            "Scheduling Celery task",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            args=args,
            celery_kwargs=celery_kwargs,
        )

        celery_task = celery_unit.apply_async(**celery_kwargs)

        logger.debug("Celery task scheduled", task_id=task_attempt.id, celery_task_id=celery_task.id)
