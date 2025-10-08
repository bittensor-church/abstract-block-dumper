import json

import structlog
from django.db import transaction
from django.utils import timezone

from abstract_block_dumper.exceptions import ConditionEvaluationError
from abstract_block_dumper.executor import CeleryExecutor
from abstract_block_dumper.memory_registry import BaseRegistry, RegistryItem, task_registry
from abstract_block_dumper.models import TaskAttempt

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
        execution_args_list = registry_item.get_execution_args()

        for args in execution_args_list:
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
            args_json = self._serialize_args(args)

            executed_blocks = set(
                TaskAttempt.objects.filter(
                    executable_path=registry_item.executable_path,
                    args_json=args_json,
                    block_number__gte=start_block,
                    block_number__lt=current_block,
                    status=TaskAttempt.Status.SUCCESS,
                ).values_list("block_number", flat=True)
            )

            for block_num in range(start_block, current_block):
                if block_num not in executed_blocks:
                    try:
                        if registry_item.match_condition(block_num, **args):
                            logger.debug(
                                "Backfilling block",
                                function_name=registry_item.function.__name__,
                                block_number=block_num,
                                args=args,
                            )
                            self.executor.execute(registry_item, block_num, args)
                    except Exception:
                        logger.error(
                            "Error during backfill",
                            function_name=registry_item.function.__name__,
                            block_number=block_num,
                            args=args,
                            exc_info=True,
                        )

    def recover_failed_retries(self) -> None:
        """
        Recover failed tasks that are ready to be retried.

        This handles tasks that may have been lost due to scheduler restarts.
        """
        ready_to_retry = TaskAttempt.objects.filter(
            status=TaskAttempt.Status.FAILED,
            next_retry_at__isnull=False,
            next_retry_at__lte=timezone.now(),
        )

        retry_count = 0
        for task_attempt in ready_to_retry:
            try:
                # Find the registry item to get celery_kwargs
                registry_item = None
                for item in self.registry.get_functions():
                    if item.executable_path == task_attempt.executable_path:
                        registry_item = item
                        break

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
                    task_attempt = TaskAttempt.objects.select_for_update().get(id=task_attempt.id)

                    # Verify task is still in FAILED state and ready for retry
                    if task_attempt.status != TaskAttempt.Status.FAILED:
                        logger.info(
                            "Task no longer in FAILED state, skipping recovery",
                            task_id=task_attempt.id,
                            current_status=task_attempt.status,
                        )
                        continue

                    if not task_attempt.can_retry():
                        logger.info(
                            "Task cannot be retried, skipping recovery",
                            task_id=task_attempt.id,
                            attempt_count=task_attempt.attempt_count,
                        )
                        continue

                    # Reset to PENDING and clear celery_task_id
                    task_attempt.celery_task_id = None
                    task_attempt.status = TaskAttempt.Status.PENDING
                    task_attempt.save(update_fields=["status", "celery_task_id"])

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
                    # (execution may have failed before celery_unit could mark it)
                    if task_attempt.status == TaskAttempt.Status.PENDING:
                        task_attempt.status = TaskAttempt.Status.FAILED
                        task_attempt.save(update_fields=["status"])
                except TaskAttempt.DoesNotExist:
                    # Task was deleted during recovery, nothing to revert
                    pass

        if retry_count > 0:
            logger.info("Retry recovery completed", recovered_count=retry_count)

    def _serialize_args(self, args) -> str:
        return json.dumps(args, sort_keys=True)

    def _cleanup_phantom_tasks(self) -> None:
        """
        Clean up tasks marked as SUCCESS but never actually started.
        Only removes tasks that were created recently (within last hour) to avoid
        deleting legitimate tasks marked as success by external processes.
        """
        from datetime import timedelta

        # Only clean up recent phantom tasks to avoid deleting legitimate external successes
        recent_phantom_tasks = TaskAttempt.objects.filter(
            status=TaskAttempt.Status.SUCCESS,
            last_attempted_at__isnull=True,
            celery_task_id__isnull=True,  # Additional safety check
            created_at__gte=timezone.now() - timedelta(hours=1),  # Only recent tasks
        )

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
