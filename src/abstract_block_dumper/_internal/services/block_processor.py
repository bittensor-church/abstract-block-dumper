import itertools
import time
from typing import Protocol

import structlog
from django.db import transaction

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import BaseRegistry, RegistryItem, task_registry
from abstract_block_dumper._internal.exceptions import ConditionEvaluationError
from abstract_block_dumper._internal.services.executor import CeleryExecutor
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class BaseBlockProcessor(Protocol):
    """Protocol defining the interface for block processors."""

    executor: CeleryExecutor
    registry: BaseRegistry

    def process_block(self, block_number: int) -> None:
        """Process a single block - executes registered tasks for this block only."""
        ...

    def process_registry_item(self, registry_item: RegistryItem, block_number: int) -> None:
        """Process a single registry item for a given block."""
        ...

    def recover_failed_retries(self, poll_interval: int, batch_size: int | None = None) -> None:
        """Recover failed tasks that are ready to be retried."""
        ...


class BlockProcessor:
    def __init__(self, executor: CeleryExecutor, registry: BaseRegistry) -> None:
        self.executor = executor
        self.registry = registry
        self._cleanup_phantom_tasks()

    def process_block(self, block_number: int) -> None:
        """Process a single block - executes registered tasks for this block only."""
        for registry_item in self.registry.get_functions():
            try:
                self.process_registry_item(registry_item, block_number)
            except Exception:
                logger.exception(
                    "Error processing registry item",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                )

    def process_registry_item(self, registry_item: RegistryItem, block_number: int) -> None:
        for args in registry_item.get_execution_args():
            try:
                if registry_item.match_condition(block_number, **args):
                    self.executor.execute(registry_item, block_number, args)
            except ConditionEvaluationError as e:
                logger.warning(
                    "Condition evaluation failed, skipping task",
                    function_name=registry_item.function.__name__,
                    error=str(e),
                )
                # Continue with other tasks
            except Exception:
                logger.exception("Unexpected error processing task")

    def recover_failed_retries(self, poll_interval: int, batch_size: int | None = None) -> None:
        """
        Recover failed tasks that are ready to be retried.

        This handles tasks that may have been lost due to scheduler restarts.

        Args:
            poll_interval: Seconds to sleep between processing each retry.
            batch_size: Maximum number of retries to process. If None, process all.

        """
        retry_count = 0
        retry_attempts = abd_dal.get_ready_to_retry_attempts()

        # Apply batch size limit if specified (use islice for iterator compatibility)
        if batch_size is not None:
            retry_attempts = itertools.islice(retry_attempts, batch_size)

        for retry_attempt in retry_attempts:
            time.sleep(poll_interval)
            try:
                # Find the registry item to get celery_kwargs
                registry_item = self.registry.get_by_executable_path(retry_attempt.executable_path)
                if not registry_item:
                    logger.warning(
                        "Registry item not found for failed task, skipping retry recovery",
                        task_id=retry_attempt.id,
                        executable_path=retry_attempt.executable_path,
                    )
                    continue

                # Use atomic transaction to prevent race conditions
                with transaction.atomic():
                    # Re-fetch with select_for_update to prevent concurrent modifications
                    task_attempt = TaskAttempt.objects.select_for_update(nowait=True).get(id=retry_attempt.id)

                    # Verify task is still in FAILED state and ready for retry
                    if task_attempt.status == TaskAttempt.Status.SUCCESS:
                        logger.info(
                            "Task was already recovered",
                            task_id=task_attempt.id,
                            current_status=task_attempt.status,
                        )
                        continue

                    if not abd_dal.task_can_retry(task_attempt):
                        logger.info(
                            "Task cannot be retried, skipping recovery",
                            task_id=task_attempt.id,
                            attempt_count=task_attempt.attempt_count,
                        )
                        continue

                    # Reset to PENDING and clear celery_task_id
                    abd_dal.reset_to_pending(task_attempt)

                # Execute outside of transaction to avoid holding locks too long
                self.executor.execute(registry_item, task_attempt.block_number, task_attempt.args_dict)
                retry_count += 1

                logger.info(
                    "Recovered orphaned retry",
                    task_id=task_attempt.id,
                    block_number=task_attempt.block_number,
                    attempt_count=task_attempt.attempt_count,
                )
            except Exception:
                logger.exception(
                    "Failed to recover retry",
                    task_id=retry_attempt.id,
                )
                # Reload task to see current state after potential execution failure
                try:
                    retry_attempt.refresh_from_db()
                    # If task is still PENDING after error, revert to FAILED
                    # (execution may have failed before celery task could mark it)
                    if retry_attempt.status == TaskAttempt.Status.PENDING:
                        abd_dal.revert_to_failed(retry_attempt)
                except TaskAttempt.DoesNotExist:
                    # Task was deleted during recovery, nothing to revert
                    pass

        if retry_count > 0:
            logger.info("Retry recovery completed", recovered_count=retry_count)

    def _cleanup_phantom_tasks(self) -> None:
        """
        Clean up tasks marked as SUCCESS but never actually started.

        Only removes tasks that were created recently (within last hour) to avoid
        deleting legitimate tasks marked as success by external processes.
        """
        recent_phantom_tasks = abd_dal.get_recent_phantom_tasks()
        count = recent_phantom_tasks.count()
        if count > 0:
            recent_phantom_tasks.delete()
            logger.info("Cleaned up recent phantom tasks on initialization", count=count)


def block_processor_factory(
    executor: CeleryExecutor | None = None,
    registry: BaseRegistry | None = None,
) -> BaseBlockProcessor:
    return BlockProcessor(
        executor=executor or CeleryExecutor(),
        registry=registry or task_registry,
    )
