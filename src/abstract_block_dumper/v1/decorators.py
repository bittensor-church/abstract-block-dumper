import inspect
import time
from collections.abc import Callable
from typing import Any, cast

import structlog
from celery import Task, shared_task
from django.db import OperationalError, transaction

import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper._internal.exceptions import CeleryTaskLockedError
from abstract_block_dumper._internal.services.metrics import (
    observe_task_execution_time,
    record_task_execution,
    record_task_retry,
)
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

    # Record retry metric
    task_name = task_attempt.executable_path.split(".")[-1]
    record_task_retry(task_name)


def _celery_task_wrapper(
    func: Callable[..., Any],
    block_number: int,
    **kwargs: dict[str, Any],
) -> dict[str, Any] | None:
    executable_path = abd_utils.get_executable_path(func)

    # Extract runtime hints that shouldn't be stored in DB
    use_archive_network = kwargs.pop("_use_archive_network", False)

    # Create db_kwargs without runtime hints for DB lookup
    db_kwargs = {k: v for k, v in kwargs.items() if not k.startswith("_")}

    with transaction.atomic():
        try:
            task_attempt = TaskAttempt.objects.select_for_update(nowait=True).get(
                block_number=block_number,
                executable_path=executable_path,
                args_json=abd_utils.serialize_args(db_kwargs),
            )
        except TaskAttempt.DoesNotExist as exc:
            msg = "TaskAttempt not found - task may have been canceled directly"
            logger.warning(msg, block_number=block_number, executable_path=executable_path)
            raise CeleryTaskLockedError(msg) from exc

        except OperationalError as e:
            msg = "Task already being processed by another worker"
            logger.info(msg, block_number=block_number, executable_path=executable_path, operational_error=str(e))
            raise CeleryTaskLockedError(msg) from e

        if task_attempt.status != TaskAttempt.Status.PENDING:
            logger.info(
                "Task already processed",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return None

        abd_dal.task_mark_as_started(task_attempt, abd_utils.get_current_celery_task_id())

        # Start task execution
        # Pass _use_archive_network only if the function accepts **kwargs
        # Otherwise, strip it to avoid TypeError
        execution_kwargs = {"block_number": block_number, **kwargs}

        # Check if function accepts **kwargs before adding _use_archive_network
        sig = inspect.signature(func)
        has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        if has_var_keyword:
            execution_kwargs["_use_archive_network"] = use_archive_network

        task_name = executable_path.split(".")[-1]  # Get short task name
        start_time = time.perf_counter()

        try:
            logger.info(
                "Starting task execution",
                task_id=task_attempt.id,
                block_number=block_number,
                executable_path=executable_path,
                celery_task_id=task_attempt.celery_task_id,
                use_archive_network=use_archive_network,
            )

            result = func(**execution_kwargs)
            execution_duration = time.perf_counter() - start_time

            abd_dal.task_mark_as_success(task_attempt, result)

            # Record success metrics
            record_task_execution(task_name, "success")
            observe_task_execution_time(task_name, execution_duration)

            logger.info("Task completed successfully", task_id=task_attempt.id, duration=execution_duration)
            return {"result": result}
        except Exception as e:
            execution_duration = time.perf_counter() - start_time
            logger.exception(
                "Task execution failed",
                task_id=task_attempt.id,
                error_type=type(e).__name__,
                error_message=str(e),
                duration=execution_duration,
            )
            abd_dal.task_mark_as_failed(task_attempt)

            # Record failure metrics
            record_task_execution(task_name, "failed")
            observe_task_execution_time(task_name, execution_duration)

    # Schedule retry after transaction commits:
    if abd_dal.task_can_retry(task_attempt):
        try:
            schedule_retry(task_attempt)
        except Exception:
            logger.exception(
                "Failed to schedule retry",
                task_id=task_attempt.id,
            )
    return None


def block_task(
    func: Callable[..., Any] | None = None,
    *,
    condition: Callable[..., bool] | None = None,
    args: list[dict[str, Any]] | None = None,
    backfilling_lookback: int | None = None,
    celery_kwargs: dict[str, Any] | None = None,
) -> Callable[..., Any]:
    """
    Decorator to register a function as a block task.

    Block task is a function that will be executed conditionally on each new block.
    The condition is a callable that takes the block number and any additional arguments,
    and returns a boolean indicating whether to execute the task.

    Args:
        func: The function to decorate (used when decorator is applied without parentheses)
        condition: Lambda function that determines when to execute the task. It should accept
                   block_number and any additional args as parameters and return a boolean.
                   Defaults to always True (run on every block).
        args: List of argument dictionaries for multi-execution
        backfilling_lookback: Number of blocks to backfill
        celery_kwargs: Additional Celery task parameters

    Examples:
        @block_task
        def run_on_every_block(block_number: int):
            pass

        @block_task()
        def also_runs_on_every_block(block_number: int):
            pass

        @block_task(condition=lambda bn: bn % 100 == 0)
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
    # Default condition: always run
    effective_condition = condition if condition is not None else (lambda *_args, **_kwargs: True)

    def decorator(fn: Callable[..., Any]) -> Any:
        if not callable(effective_condition):
            msg = "condition must be a callable."
            raise TypeError(msg)

        # Celery task wrapper
        def shared_celery_task(block_number: int, **kwargs: dict[str, Any]) -> None | Any:
            """
            Wrapper that handles TaskAttempt tracking and executed the original
            function

            This entire wrapper becomes a Celery task.
            """
            return _celery_task_wrapper(fn, block_number, **kwargs)

        # Wrap with celery shared_task
        celery_task = shared_task(
            name=abd_utils.get_executable_path(fn),
            bind=False,
            **celery_kwargs or {},
        )(shared_celery_task)

        # Store original function referefence for introspection
        celery_task._original_func = fn  # noqa: SLF001

        # Register the Celery task
        task_registry.register_item(
            RegistryItem(
                condition=effective_condition,
                function=cast("Task", celery_task),
                args=args,
                backfilling_lookback=backfilling_lookback,
                celery_kwargs=celery_kwargs or {},
            )
        )
        return celery_task

    # If func is provided, decorator was used without parentheses: @block_task
    if func is not None:
        return decorator(func)

    # Otherwise, decorator was used with parentheses: @block_task() or @block_task(condition=...)
    return decorator
