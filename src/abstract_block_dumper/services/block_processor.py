import structlog
from django.db import transaction

import abstract_block_dumper.dal.django_dal as abd_dal
from abstract_block_dumper.dal.memory_registry import BaseRegistry, RegistryItem, task_registry
from abstract_block_dumper.exceptions import ConditionEvaluationError
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.services.executor import CeleryExecutor
from abstract_block_dumper.services.utils import serialize_args

logger = structlog.get_logger(__name__)


class BlockProcessor:
    def __init__(self, executor: CeleryExecutor, registry: BaseRegistry) -> None:
        self.executor = executor
        self.registry = registry
        self._cleanup_phantom_tasks()

    def process_block(self, block_number: int) -> None:
        for registry_item in self.registry.get_functions():
            try:
                self.process_backfill(registry_item, block_number)
                self.process_registry_item(registry_item, block_number)
            except Exception:
                logger.error(
                    "Error processing registry item",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                    exc_info=True,
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
                logger.error("Unexpected error processing task", exc_info=True)

    def process_backfill(self, registry_item: RegistryItem, current_block: int) -> None:
        if not registry_item.backfilling_lookback:
            return None

        start_block = max(0, current_block - registry_item.backfilling_lookback)

        logger.info(
            "Processing backfill",
            function_name=registry_item.function.__name__,
            start_block=start_block,
            current_block=current_block,
            lookback=registry_item.backfilling_lookback,
        )

        execution_args_list = registry_item.get_execution_args()

        for args in execution_args_list:
            args_json = serialize_args(args)

            executed_blocks = abd_dal.executed_block_numbers(
                registry_item.executable_path,
                args_json,
                start_block,
                current_block,
            )

            for block_number in range(start_block, current_block):
                if block_number in executed_blocks:
                    continue

                try:
                    if registry_item.match_condition(block_number, **args):
                        logger.debug(
                            "Backfilling block",
                            function_name=registry_item.function.__name__,
                            block_number=block_number,
                            args=args,
                        )
                        self.executor.execute(registry_item, block_number, args)
                except Exception:
                    logger.error(
                        "Error during backfill",
                        function_name=registry_item.function.__name__,
                        block_number=block_number,
                        args=args,
                        exc_info=True,
                    )

    def recover_failed_retries(self) -> None:
        """
        Recover failed tasks that are ready to be retried.

        This handles tasks that may have been lost due to scheduler restarts.
        """
        retry_count = 0
        for task_attempt in abd_dal.get_ready_to_retry_attempts():
            try:
                # Find the registry item to get celery_kwargs
                registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
                if not registry_item:
                    logger.warning(
                        "Registry item not found for failed task, skipping retry recovery",
                        task_id=task_attempt.id,
                        executable_path=task_attempt.executable_path,
                    )
                    continue

                # Use atomic transaction to prevent race conditions
                with transaction.atomic():
                    # Re-fetch with select_for_update to prevent concurrent modifications
                    task_attempt = TaskAttempt.objects.select_for_update(nowait=True).get(id=task_attempt.id)

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
                logger.error(
                    "Failed to recover retry",
                    task_id=task_attempt.id,
                    exc_info=True,
                )
                # Reload task to see current state after potential execution failure
                try:
                    task_attempt.refresh_from_db()
                    # If task is still PENDING after error, revert to FAILED
                    # (execution may have failed before celery task could mark it)
                    if task_attempt.status == TaskAttempt.Status.PENDING:
                        abd_dal.revert_to_failed(task_attempt)
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
) -> BlockProcessor:
    return BlockProcessor(
        executor=executor or CeleryExecutor(),
        registry=registry or task_registry,
    )
