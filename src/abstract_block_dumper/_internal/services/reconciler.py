"""
Recovery of task attempts the database says should run but nothing is carrying.

Three kinds of attempts are recovered, in this order:

- RUNNING attempts whose lease has passed. Their worker was killed or timed out after
  claiming them; the claim already counted the attempt, so they are failed and retried on
  the normal backoff, or failed permanently once no tries remain.
- PENDING attempts on a queue with a message TTL whose message expired unexecuted. They are
  sent again without consuming an attempt.
- Attempts matching the stuck-task predicate (see `queue_attempts.stuck_attempt_ids`):
  PENDING attempts whose message was lost and FAILED attempts whose retry was never
  enqueued. They are re-dispatched without consuming an attempt. Only their age gives them
  away, so without a stuck-task timeout they are not recovered.

Only attempts of `BlockDumper` tasks are recovered here; `BlockProcessor` recovers the
attempts of `@block_task` tasks with the rules in `django_dal`.
"""

from datetime import datetime, timedelta

import structlog
from django.db import transaction
from django.utils import timezone

import abstract_block_dumper._internal.dal.attempt_transitions as transitions
import abstract_block_dumper._internal.dal.django_dal as abd_dal
import abstract_block_dumper._internal.dal.queue_attempts as queue_attempts
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import BaseRegistry, task_registry
from abstract_block_dumper._internal.services.archive_network import ARCHIVE_BLOCK_THRESHOLD
from abstract_block_dumper._internal.services.metrics import record_task_lease_expired, record_task_recovered
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor, redispatch_queue
from abstract_block_dumper._internal.services.recovery_settings import recovery_margin_seconds
from abstract_block_dumper._internal.services.retry_rules import report_given_up, retry_delay
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class QueueReconciler:
    def __init__(self, executor: QueueExecutor, registry: BaseRegistry) -> None:
        self.executor = executor
        self.registry = registry

    def reconcile(self, *, stale_after_seconds: float | None, limit: int, head_block: int | None) -> int:
        """
        Recover at most `limit` attempts, in the order the module docstring lists.

        Args:
            stale_after_seconds: How long an attempt without a lease may sit without
                progress before it counts as stuck. None skips stuck attempts.
            limit: Maximum number of attempts to handle in this call.
            head_block: Current chain head, used to decide whether a re-dispatched block
                needs the archive network. None means unknown, i.e. never archive.

        Returns:
            The number of attempts re-dispatched.

        """
        dumper_paths = {item.executable_path for item in self.registry.get_functions() if item.live_queue is not None}
        if not dumper_paths:
            return 0
        recovered = 0
        expired_ids = transitions.expired_lease_ids(limit, dumper_paths)
        for task_id in expired_ids:
            try:
                if self._recover_expired_lease(task_id, head_block):
                    recovered += 1
            except Exception:
                logger.exception("Failed to recover task attempt with an expired lease", task_id=task_id)

        ttl_windows = {
            queue.name: queue.message_ttl + recovery_margin_seconds()
            for queue in dumper_registry.queues()
            if queue.message_ttl is not None
        }
        expired_message_ids = queue_attempts.expired_message_ids(ttl_windows, limit - len(expired_ids), dumper_paths)
        for task_id in expired_message_ids:
            try:
                if self._recover_expired_message(task_id, ttl_windows, head_block):
                    recovered += 1
            except Exception:
                logger.exception("Failed to re-dispatch task attempt whose message expired", task_id=task_id)

        if stale_after_seconds is None:
            return recovered
        stale_before = timezone.now() - timedelta(seconds=stale_after_seconds)
        remaining = limit - len(expired_ids) - len(expired_message_ids)
        stuck_ids = queue_attempts.stuck_attempt_ids(stale_before, remaining, dumper_paths, ttl_queues=ttl_windows)
        for task_id in stuck_ids:
            try:
                if self._recover_stuck_task(task_id, stale_before, ttl_windows, head_block):
                    recovered += 1
            except Exception:
                logger.exception("Failed to reconcile stuck task", task_id=task_id)
        return recovered

    def _recover_expired_lease(self, task_id: int, head_block: int | None) -> bool:
        task_attempt = TaskAttempt.objects.filter(id=task_id).first()
        if task_attempt is None:
            return False
        if not transitions.fail_expired_lease(task_id, retry_delay=retry_delay(task_attempt)):
            # The worker recorded an outcome after all.
            return False

        task_attempt.refresh_from_db()
        record_task_lease_expired(task_attempt.executable_path.split(".")[-1])
        logger.warning(
            "Task attempt lease expired",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            celery_task_id=task_attempt.celery_task_id,
            attempt_count=task_attempt.attempt_count,
            will_retry=task_attempt.next_retry_at is not None,
        )
        if task_attempt.give_up_reason is not None:
            report_given_up(task_attempt)
            return False

        registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
        if registry_item is None:
            logger.warning(
                "Registry item not found for task attempt with an expired lease, leaving it failed",
                task_id=task_attempt.id,
                executable_path=task_attempt.executable_path,
            )
            return False
        if not self.executor.dispatch_retry(
            registry_item, task_attempt, use_archive=self._needs_archive(task_attempt, head_block)
        ):
            return False
        record_task_recovered(task_attempt.executable_path.split(".")[-1], TaskAttempt.Status.RUNNING)
        return True

    def _recover_expired_message(self, task_id: int, ttl_windows: dict[str, float], head_block: int | None) -> bool:
        task_attempt = TaskAttempt.objects.filter(id=task_id).first()
        if task_attempt is None:
            return False
        registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
        if registry_item is None or not transitions.claim_expired_message(task_id, ttl_windows):
            return False

        use_archive = self._needs_archive(task_attempt, head_block)
        sent = self.executor.execute(
            registry_item,
            task_attempt.block_number,
            task_attempt.args_dict,
            use_archive=use_archive,
            queue=redispatch_queue(registry_item, task_attempt),
            head_block=head_block,
        )
        if not sent:
            return False
        record_task_recovered(task_attempt.executable_path.split(".")[-1], TaskAttempt.Status.PENDING)
        logger.warning(
            "Re-dispatched task attempt whose message expired",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            attempt_count=task_attempt.attempt_count,
            queue=task_attempt.dispatch_queue,
            use_archive=use_archive,
        )
        return True

    def _recover_stuck_task(
        self, task_id: int, stale_before: datetime, ttl_queues: dict[str, float], head_block: int | None
    ) -> bool:
        with transaction.atomic():
            # skip_locked: a row another sweep is recovering right now is left to it.
            task_attempt = TaskAttempt.objects.select_for_update(skip_locked=True).filter(id=task_id).first()
            if task_attempt is None or not queue_attempts.is_stuck(task_attempt, stale_before, ttl_queues):
                return False

            registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
            if registry_item is None:
                return False

            previous_status = task_attempt.status
            # The claim: bumps updated_at, so if the dispatch below fails the row is picked
            # up again one timeout later rather than on every sweep.
            abd_dal.reset_to_pending(task_attempt)

        use_archive = self._needs_archive(task_attempt, head_block)
        queue = redispatch_queue(registry_item, task_attempt)
        sent = self.executor.execute(
            registry_item,
            task_attempt.block_number,
            task_attempt.args_dict,
            use_archive=use_archive,
            queue=queue,
            head_block=head_block,
        )
        if not sent:
            return False

        record_task_recovered(task_attempt.executable_path.split(".")[-1], previous_status)
        logger.warning(
            "Recovered stuck task",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            previous_status=previous_status,
            attempt_count=task_attempt.attempt_count,
            queue=queue.name,
            use_archive=use_archive,
        )
        return True

    @staticmethod
    def _needs_archive(task_attempt: TaskAttempt, head_block: int | None) -> bool:
        return head_block is not None and (head_block - task_attempt.block_number) > ARCHIVE_BLOCK_THRESHOLD


def queue_reconciler_factory(registry: BaseRegistry | None = None) -> QueueReconciler:
    return QueueReconciler(QueueExecutor(), registry or task_registry)
