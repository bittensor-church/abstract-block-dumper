from collections.abc import Callable
from typing import Any, cast

import structlog
from celery import Task, shared_task
from django.db import OperationalError, transaction

import abstract_block_dumper.dal.django_dal as abd_dal
import abstract_block_dumper.services.utils as abd_utils
from abstract_block_dumper.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper.exceptions import CeleryTaskLocked
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


def schedule_retry(task_attempt: TaskAttempt) -> None:
    """
    Schedule a retry for a failed task by calling the decorated Celery task directly.

    Task must already be in FAILED state with next_retry_at set by mark_failed()
    """

    if not task_attempt.next_retry_at:
        logger.error(
            "Cannot schedule retry without next_retry_at",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
        )

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

    abd_dal.task_schedule_to_retry(task_attempt)

    celery_task = task_registry.get_by_executable_path(task_attempt.executable_path)
    if not celery_task:
        logger.error(
            "Cannot schedule retry - task not found in registry",
            executable_path=task_attempt.executable_path,
        )
        return

    celery_task.function.apply_async(
        kwargs={
            "block_number": task_attempt.block_number,
            **task_attempt.args_dict,
        },
        eta=task_attempt.next_retry_at,
    )


def _celery_task_wrapper(func, block_number: int, **kwargs) -> dict[str, Any] | None:
    executable_path = abd_utils.get_executable_path(func)

    with transaction.atomic():
        try:
            task_attempt = TaskAttempt.objects.select_for_update(nowait=True).get(
                block_number=block_number,
                executable_path=executable_path,
                args_json=abd_utils.serialize_args(kwargs),
            )
        except TaskAttempt.DoesNotExist:
            logger.warning(
                "TaskAttempt not found - task may have been canceled directly",
                block_number=block_number,
                executable_path=executable_path,
            )
            raise CeleryTaskLocked("TaskAttempt not found - task may have been canceled directly")
        except OperationalError as e:
            logger.info(
                "Task already being processed by another worker",
                block_number=block_number,
                executable_path=executable_path,
                operational_error=str(e),
            )
            raise CeleryTaskLocked("Task already being processed by another worker")

        if task_attempt.status != TaskAttempt.Status.PENDING:
            logger.info(
                "Task already processed",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return None

        abd_dal.task_mark_as_started(task_attempt, abd_utils.get_current_celery_task_id())

        # Start task execution
        try:
            execution_kwargs = {"block_number": block_number, **kwargs}
            logger.info(
                "Starting task execution",
                task_id=task_attempt.id,
                block_number=block_number,
                executable_path=executable_path,
                celery_task_id=task_attempt.celery_task_id,
                execution_kwargs=execution_kwargs,
            )

            result = func(**execution_kwargs)

            abd_dal.task_mark_as_success(task_attempt, result)

            logger.info("Task completed successfully", task_id=task_attempt.id)
            return {"result": result}
        except Exception as e:
            logger.error(
                "Task execution failed",
                task_id=task_attempt.id,
                error_type=type(e).__name__,
                exc_info=True,
            )
            abd_dal.task_mark_as_failed(task_attempt)

    # Schedule retry after transaction commits:
    if abd_dal.task_can_retry(task_attempt):
        try:
            schedule_retry(task_attempt)
        except Exception:
            logger.error(
                "Failed to schedule retry",
                task_id=task_attempt.id,
                exc_info=True,
            )
    return None


def block_task(
    condition: Callable[..., bool],
    args: list[dict[str, Any]] | None = None,
    backfilling_lookback: int | None = None,
    celery_kwargs: dict[str, Any] | None = None,
) -> Callable[..., Any]:
    """
    Decorator for registering block tasks.

    Args:
        condition: Lambda function that determines when to execute
        args: List of argument dictionaries for multi-execution
        backfilling_lookback: Number of blocks to backfill
        celery_kwargs: Additional Celery task parameters

    Examples:
        @block_task(
            condition=lambda bn: bn % 100 == 0
        )
        def simple_task(block_number: int):
            pass

        @block_task(
            condition=lambda bn, netuid: bn + netuid % 100 == 0,
            args=[{"netuid": 3}, {"netuid": 22}],
            backfilling_lookback=300,
            celery_kwargs={"queue": "high-priority"}
        )
        def multi_netuid_task(block_number: int, netuid: int):
            pass

    """

    def decorator(func: Callable[..., Any]) -> Any:
        if not callable(condition):
            raise ValueError("condition must be a callable.")

        # Celery task wrapper
        def shared_celery_task(block_number: int, **kwargs) -> None | Any:
            """
            Wrapper that handles TaskAttempt tracking and executed the original
            function

            This entire wrapper becomes a Celery task.
            """
            return _celery_task_wrapper(func, block_number, **kwargs)

        # Wrap with celery shared_task
        celery_task = shared_task(
            name=abd_utils.get_executable_path(func),
            bind=False,
            **celery_kwargs or {},
        )(shared_celery_task)

        # Store original function referefence for introspection
        celery_task._original_func = func

        # Register the Celery task
        task_registry.register_item(
            RegistryItem(
                condition=condition,
                function=cast(Task, celery_task),
                args=args,
                backfilling_lookback=backfilling_lookback,
                celery_kwargs=celery_kwargs or {},
            )
        )
        return celery_task

    return decorator
