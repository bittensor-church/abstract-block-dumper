"""
Worker side of a `BlockDumper` task: claim the TaskAttempt, run the function, record the outcome.

The claim is committed before the function runs, so a worker killed mid-execution (hard
time limit, OOM, lost host) leaves a RUNNING row with a lease behind, which the sweep
recovers once the lease passes. Every later write is fenced on the claim token, so a worker
whose attempt was taken over after its lease expired cannot overwrite the newer one.

The function runs in a transaction that also records SUCCESS or FAILED, so its own database
writes commit whether it returns or raises; a task that needs writes undone when it fails
wraps them in `transaction.atomic()`. When that transaction cannot commit, for example
because the function left it unusable with a database error outside a savepoint, the
function's writes are discarded and FAILED is recorded in a transaction of its own.
"""

import inspect
import time
import uuid
from collections.abc import Callable
from typing import Any

import structlog
from django.db import OperationalError, transaction

import abstract_block_dumper._internal.dal.attempt_transitions as transitions
import abstract_block_dumper._internal.services.utils as abd_utils
from abstract_block_dumper._internal.dal.memory_registry import BaseRegistry, task_registry
from abstract_block_dumper._internal.exceptions import CeleryTaskLockedError
from abstract_block_dumper._internal.services.metrics import observe_task_execution_time, record_task_execution
from abstract_block_dumper._internal.services.queue_executor import QueueExecutor
from abstract_block_dumper._internal.services.recovery_settings import lease_seconds
from abstract_block_dumper._internal.services.retry_rules import report_given_up, retry_delay
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class _AttemptTakenOverError(Exception):
    """The claim was lost while the function ran; its writes must be rolled back."""


class TaskRunner:
    def __init__(self, executor: QueueExecutor | None = None, registry: BaseRegistry | None = None) -> None:
        self.executor = executor or QueueExecutor()
        self.registry = registry or task_registry

    def run(self, func: Callable[..., Any], block_number: int, kwargs: dict[str, Any]) -> dict[str, Any] | None:
        """
        Execute one message for `func` and `block_number`.

        Returns the Celery result `{"result": ...}` on success and None when the function did
        not run to success here. Raises `CeleryTaskLockedError` when another worker holds the
        attempt or the attempt no longer exists, and `OperationalError` when the database is
        unreachable while claiming.
        """
        executable_path = abd_utils.get_executable_path(func)
        use_archive_network = kwargs.pop("_use_archive_network", False)
        # Runtime hints start with an underscore and are not part of the attempt's identity.
        db_kwargs = {k: v for k, v in kwargs.items() if not k.startswith("_")}
        claim_token = abd_utils.get_current_message_id() or f"local-{uuid.uuid4().hex}"

        task_attempt = self._claim(block_number, executable_path, abd_utils.serialize_args(db_kwargs), claim_token)
        if task_attempt is None:
            return None

        execution_kwargs = {"block_number": block_number, **kwargs}
        if _accepts_var_keyword(func):
            execution_kwargs["_use_archive_network"] = use_archive_network

        task_name = executable_path.split(".")[-1]
        logger.info(
            "Starting task execution",
            task_id=task_attempt.id,
            block_number=block_number,
            executable_path=executable_path,
            celery_task_id=claim_token,
            use_archive_network=use_archive_network,
        )
        start_time = time.perf_counter()
        failed = False
        try:
            with transaction.atomic():
                try:
                    result = func(**execution_kwargs)
                except Exception as e:
                    failed = True
                    self._report_failure(task_name, task_attempt, e, start_time)
                    finished = self._finish_failure(task_attempt, claim_token)
                else:
                    finished = transitions.finish_success(task_attempt.id, claim_token=claim_token, result=result)
                if not finished:
                    raise _AttemptTakenOverError
        except _AttemptTakenOverError:
            self._log_taken_over(task_attempt, claim_token)
            return None
        except Exception as e:
            # Nothing committed: the function left the transaction unusable, or recording the outcome failed.
            if failed:
                self._log_writes_discarded(task_attempt, e)
            else:
                self._report_failure(task_name, task_attempt, e, start_time)
            self._record_failure(task_attempt, claim_token, use_archive_network)
            return None

        if failed:
            self._retry_or_report_given_up(task_attempt, use_archive_network)
            return None

        execution_duration = time.perf_counter() - start_time
        record_task_execution(task_name, "success")
        observe_task_execution_time(task_name, execution_duration)
        logger.info("Task completed successfully", task_id=task_attempt.id, duration=execution_duration)
        return {"result": result}

    def _claim(self, block_number: int, executable_path: str, args_json: str, claim_token: str) -> TaskAttempt | None:
        try:
            task_attempt = transitions.find(block_number, executable_path, args_json)
            if task_attempt is not None and task_attempt.status == TaskAttempt.Status.PENDING:
                lease = lease_seconds(abd_utils.get_current_time_limit())
                if transitions.claim(task_attempt.id, claim_token=claim_token, lease_seconds=lease):
                    task_attempt.refresh_from_db()
                    return task_attempt
                if transitions.give_up_if_past_deadline(task_attempt.id):
                    task_attempt.refresh_from_db()
                    report_given_up(task_attempt)
                    return None
                # Another worker claimed it between the read and the update.
                task_attempt = transitions.find(block_number, executable_path, args_json)
        except OperationalError:
            # The sweep re-sends the attempt once its message expires or it counts as stuck.
            logger.error(
                "Database unavailable while claiming task",
                block_number=block_number,
                executable_path=executable_path,
                exc_info=True,
            )
            raise

        if task_attempt is None:
            msg = "TaskAttempt not found - task may have been canceled directly"
            logger.warning(msg, block_number=block_number, executable_path=executable_path)
            raise CeleryTaskLockedError(msg)
        if task_attempt.status == TaskAttempt.Status.RUNNING:
            msg = "Task already being processed by another worker"
            logger.info(msg, task_id=task_attempt.id, block_number=block_number, executable_path=executable_path)
            raise CeleryTaskLockedError(msg)
        logger.info("Task already processed", task_id=task_attempt.id, status=task_attempt.status)
        return None

    @staticmethod
    def _report_failure(task_name: str, task_attempt: TaskAttempt, error: Exception, start_time: float) -> None:
        execution_duration = time.perf_counter() - start_time
        logger.error(
            "Task execution failed",
            task_id=task_attempt.id,
            error_type=type(error).__name__,
            error_message=str(error),
            duration=execution_duration,
            exc_info=error,
        )
        record_task_execution(task_name, "failed")
        observe_task_execution_time(task_name, execution_duration)

    @staticmethod
    def _finish_failure(task_attempt: TaskAttempt, claim_token: str) -> bool:
        return transitions.finish_failure(
            task_attempt.id, claim_token=claim_token, retry_delay=retry_delay(task_attempt)
        )

    def _record_failure(self, task_attempt: TaskAttempt, claim_token: str, use_archive_network: bool) -> None:
        """Record FAILED in a transaction of its own, then retry the attempt or report it given up."""
        if not self._finish_failure(task_attempt, claim_token):
            self._log_taken_over(task_attempt, claim_token)
            return
        self._retry_or_report_given_up(task_attempt, use_archive_network)

    def _retry_or_report_given_up(self, task_attempt: TaskAttempt, use_archive_network: bool) -> None:
        """Send the retry of an attempt whose FAILED status has committed, or report it given up."""
        task_attempt.refresh_from_db()
        if task_attempt.give_up_reason is not None:
            report_given_up(task_attempt)
            return

        registry_item = self.registry.get_by_executable_path(task_attempt.executable_path)
        if registry_item is None:
            logger.error(
                "Cannot schedule retry - task not found in registry",
                executable_path=task_attempt.executable_path,
            )
            return
        try:
            self.executor.dispatch_retry(registry_item, task_attempt, use_archive=use_archive_network)
        except Exception:
            logger.exception("Failed to schedule retry", task_id=task_attempt.id)

    @staticmethod
    def _log_writes_discarded(task_attempt: TaskAttempt, error: Exception) -> None:
        logger.warning(
            "Discarding database writes of a failed task attempt whose transaction could not commit",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            error_type=type(error).__name__,
            error_message=str(error),
        )

    @staticmethod
    def _log_taken_over(task_attempt: TaskAttempt, claim_token: str) -> None:
        logger.warning(
            "Discarding outcome of a task attempt taken over by a newer one",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            celery_task_id=claim_token,
        )


def _accepts_var_keyword(func: Callable[..., Any]) -> bool:
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in inspect.signature(func).parameters.values())
