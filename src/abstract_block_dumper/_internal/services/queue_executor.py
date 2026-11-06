"""Sending the messages of `BlockDumper` tasks: first submissions, re-dispatches and retries under a queue policy."""

from typing import Any, cast

import structlog
from django.utils import timezone

import abstract_block_dumper._internal.dal.attempt_transitions as transitions
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem
from abstract_block_dumper._internal.services.archive_network import ARCHIVE_BLOCK_THRESHOLD
from abstract_block_dumper._internal.services.metrics import (
    increment_archive_network_usage,
    increment_tasks_submitted,
    record_task_retry,
)
from abstract_block_dumper._internal.services.queue_policy import Queue, message_expiry, message_options
from abstract_block_dumper._internal.services.retry_rules import report_given_up
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class QueueExecutor:
    def execute(
        self,
        registry_item: RegistryItem,
        block_number: int,
        args: dict[str, Any],
        *,
        use_archive: bool = False,
        queue: Queue | None = None,
        head_block: int | None = None,
    ) -> bool:
        """
        Record the attempt for `block_number` and send it to Celery, unless it already ran or runs.

        `queue` is the `Queue` whose policy the submission follows; without it the task's live
        queue. An existing attempt keeps the policy it was first submitted under, if that
        still exists, whichever queue this submission names. `head_block`, the chain head the
        submission is made against, sets the give-up deadline of a queue with
        `give_up_after_blocks`; an attempt already past it is given up instead of sent.
        Returns whether a message was sent.
        """
        task_attempt, created = transitions.create_or_reopen(block_number, registry_item.executable_path, args)
        if not created and task_attempt.status != TaskAttempt.Status.PENDING:
            logger.debug(
                "Task already exists",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return False

        policy = queue if queue is not None else cast("Queue", registry_item.live_queue)
        if not created:
            policy = dumper_registry.get_queue(task_attempt.dispatch_queue) or policy

        if self._gave_up(registry_item, task_attempt, policy, head_block):
            return False

        # Persist the routing so retries — including ones recovered from the database
        # by a different process — can be re-dispatched to the same queue, and stamp
        # the row as dispatched now.
        transitions.record_dispatch(task_attempt, policy.name, dispatch_queue=policy.name)

        task_kwargs = {
            "block_number": block_number,
            "_use_archive_network": use_archive,  # Runtime hint, not stored in DB
            **args,
        }

        apply_async_kwargs: dict[str, Any] = {"kwargs": task_kwargs}

        if task_attempt.next_retry_at:
            apply_async_kwargs["eta"] = task_attempt.next_retry_at

        celery_options = {
            k: v for k, v in (registry_item.celery_kwargs or {}).items() if k not in ("kwargs", "eta", "args")
        }
        celery_options["queue"] = policy.name
        celery_options.update(message_options(policy))
        expires = message_expiry(policy, task_attempt.next_retry_at, timezone.now())
        if expires is not None:
            celery_options["expires"] = expires

        apply_async_kwargs.update(celery_options)

        logger.info(
            "Scheduling Celery task",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            args=args,
            celery_kwargs=apply_async_kwargs,
        )

        # Don't store AsyncResult to avoid memory accumulation during long runs
        # The task ID can be retrieved from the task_attempt if needed
        registry_item.function.apply_async(**apply_async_kwargs)

        # Track task submission metric
        task_name = registry_item.executable_path.split(".")[-1]
        increment_tasks_submitted(task_name)
        if use_archive:
            increment_archive_network_usage()

        logger.debug("Celery task scheduled", task_id=task_attempt.id)
        return True

    def dispatch_retry(
        self, registry_item: RegistryItem, task_attempt: TaskAttempt, *, use_archive: bool = False
    ) -> bool:
        """
        Send the retry of a FAILED attempt, due at its `next_retry_at`.

        The retry goes to the queue `redispatch_queue` picks. It carries the archive hint when
        `use_archive` asks for it or when the attempt has aged past the archive threshold while
        retrying. Returns False, and sends nothing, when the attempt has no retry time, is no
        longer FAILED or was given up.
        """
        if not task_attempt.next_retry_at:
            logger.error(
                "Cannot schedule retry without next_retry_at",
                task_id=task_attempt.id,
                block_number=task_attempt.block_number,
                executable_path=task_attempt.executable_path,
            )
            return False

        policy = redispatch_queue(registry_item, task_attempt)
        if not transitions.requeue_for_retry(task_attempt.id, queue=policy.name):
            logger.warning("Not sending retry of a task attempt no longer waiting for one", task_id=task_attempt.id)
            return False
        use_archive = use_archive or _aged_past_archive_threshold(registry_item, task_attempt)

        task_kwargs: dict[str, Any] = {"block_number": task_attempt.block_number, **task_attempt.args_dict}
        if use_archive:
            task_kwargs["_use_archive_network"] = True
        apply_async_kwargs: dict[str, Any] = {
            "kwargs": task_kwargs,
            "eta": task_attempt.next_retry_at,
            "queue": policy.name,
            **message_options(policy),
        }
        expires = message_expiry(policy, task_attempt.next_retry_at, timezone.now())
        if expires is not None:
            apply_async_kwargs["expires"] = expires

        logger.info(
            "Scheduling retry",
            task_id=task_attempt.id,
            attempt_count=task_attempt.attempt_count,
            next_retry_at=task_attempt.next_retry_at,
        )
        registry_item.function.apply_async(**apply_async_kwargs)
        record_task_retry(registry_item.executable_path.split(".")[-1])
        if use_archive:
            increment_archive_network_usage()
        return True

    @staticmethod
    def _gave_up(registry_item: RegistryItem, task_attempt: TaskAttempt, policy: Queue, head_block: int | None) -> bool:
        give_up_after_blocks = policy.retry.give_up_after_blocks
        if give_up_after_blocks is not None and head_block is not None:
            blocks_left = give_up_after_blocks - (head_block - task_attempt.block_number)
            transitions.set_give_up_deadline(task_attempt.id, blocks_left * registry_item.block_time_seconds)
        if not transitions.give_up_if_past_deadline(task_attempt.id):
            return False
        task_attempt.refresh_from_db()
        report_given_up(task_attempt)
        return True


def _aged_past_archive_threshold(registry_item: RegistryItem, task_attempt: TaskAttempt) -> bool:
    """
    Whether more blocks than the archive threshold were produced since the attempt was created.

    The worker sending a retry knows no chain head, only the hint of the failed message, which
    goes stale while an attempt keeps retrying. Counting from the attempt's creation errs on the
    late side for a block that was already behind the head then.
    """
    age_seconds = (timezone.now() - task_attempt.created_at).total_seconds()
    return age_seconds / registry_item.block_time_seconds > ARCHIVE_BLOCK_THRESHOLD


def redispatch_queue(registry_item: RegistryItem, task_attempt: TaskAttempt) -> Queue:
    """
    The queue a retry or re-dispatch of `task_attempt` goes to.

    That is the queue the attempt was last sent to. Once that queue is no longer defined, for
    example after a rename for a new `message_ttl`, it is the task's backfill queue, or its live
    queue without one, where `backfill_blocks_v1` sends the block too.
    """
    recorded = dumper_registry.get_queue(task_attempt.dispatch_queue)
    if recorded is not None:
        return recorded
    current = cast("Queue", registry_item.backfill_queue or registry_item.live_queue)
    logger.warning(
        "Recorded queue of task attempt is not defined; sending it to the task's current queue",
        task_id=task_attempt.id,
        recorded_queue=task_attempt.dispatch_queue,
        queue=current.name,
    )
    return current
