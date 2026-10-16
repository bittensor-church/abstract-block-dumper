from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Protocol

import structlog
from django.db import transaction
from django.utils import timezone

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import BaseRegistry, RegistryItem, task_registry
from abstract_block_dumper._internal.exceptions import ConditionEvaluationError
from abstract_block_dumper._internal.services.executor import CeleryExecutor
from abstract_block_dumper._internal.services.metrics import increment_archive_network_usage, record_task_recovered
from abstract_block_dumper._internal.services.window_backfiller import ARCHIVE_BLOCK_THRESHOLD
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class BaseBlockProcessor(Protocol):
    """Protocol defining the interface for block processors."""

    executor: CeleryExecutor
    registry: BaseRegistry

    def process_block(
        self,
        block_number: int,
        *,
        registry_items: Iterable[RegistryItem] | None = None,
    ) -> None:
        """Process a single block - executes registered tasks for this block only."""
        ...

    def process_registry_item(self, registry_item: RegistryItem, block_number: int) -> None:
        """Process a single registry item for a given block."""
        ...

    def reconcile_stuck_tasks(self, *, stale_after_seconds: float, limit: int, head_block: int | None) -> int:
        """Re-dispatch attempts the database says should run but nothing is carrying."""
        ...


class BlockProcessor:
    def __init__(self, executor: CeleryExecutor, registry: BaseRegistry) -> None:
        self.executor = executor
        self.registry = registry
        self._cleanup_phantom_tasks()

    def process_block(
        self,
        block_number: int,
        *,
        registry_items: Iterable[RegistryItem] | None = None,
    ) -> None:
        """Process a single block - executes registered tasks for this block only."""
        items = registry_items if registry_items is not None else self.registry.get_functions()
        for registry_item in items:
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

    def reconcile_stuck_tasks(self, *, stale_after_seconds: float, limit: int, head_block: int | None) -> int:
        """
        Re-dispatch attempts the database says should run but nothing is carrying.

        An attempt is stuck when its Celery message was lost after the row was committed:
        the database or broker was unreachable, a worker died, or a FAILED row's retry was
        never enqueued. See `abd_dal.get_stuck_task_attempt_ids` for the exact predicate.
        Each row is claimed under its row lock, so an attempt a worker is executing right
        now is skipped, and handed back to the executor on the queue it was originally
        submitted to. A re-dispatch does not count as an attempt.

        Args:
            stale_after_seconds: How long an attempt may sit without progress before it
                counts as stuck.
            limit: Maximum number of attempts to re-dispatch in this call.
            head_block: Current chain head, used to decide whether the re-dispatched block
                needs the archive network. None means unknown, i.e. never archive.

        Returns:
            The number of attempts re-dispatched.

        """
        stale_before = timezone.now() - timedelta(seconds=stale_after_seconds)
        recovered = 0
        for task_id in abd_dal.get_stuck_task_attempt_ids(stale_before, limit):
            try:
                if self._reconcile_stuck_task(task_id, stale_before, head_block):
                    recovered += 1
            except Exception:
                logger.exception("Failed to reconcile stuck task", task_id=task_id)
        return recovered

    def _reconcile_stuck_task(self, task_id: int, stale_before: datetime, head_block: int | None) -> bool:
        with transaction.atomic():
            # skip_locked: a row whose worker is mid-execution is locked by that worker's
            # transaction, so it comes back empty here and is left alone.
            task_attempt = TaskAttempt.objects.select_for_update(skip_locked=True).filter(id=task_id).first()
            if task_attempt is None or not abd_dal.is_stuck(task_attempt, stale_before):
                return False

            registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
            if registry_item is None:
                logger.warning(
                    "Registry item not found for stuck task, leaving it as is",
                    task_id=task_attempt.id,
                    executable_path=task_attempt.executable_path,
                )
                return False

            previous_status = task_attempt.status
            # The claim: bumps updated_at, so if the dispatch below fails the row is picked
            # up again one timeout later rather than on every sweep.
            abd_dal.reset_to_pending(task_attempt)

        use_archive = head_block is not None and (head_block - task_attempt.block_number) > ARCHIVE_BLOCK_THRESHOLD
        if use_archive:
            increment_archive_network_usage()
        queue = task_attempt.celery_queue_override
        self.executor.execute(
            registry_item,
            task_attempt.block_number,
            task_attempt.args_dict,
            use_archive=use_archive,
            queue=queue,
        )

        record_task_recovered(task_attempt.executable_path.split(".")[-1], previous_status)
        logger.warning(
            "Recovered stuck task",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            previous_status=previous_status,
            attempt_count=task_attempt.attempt_count,
            queue=queue,
            use_archive=use_archive,
        )
        return True

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
