import json
from typing import Any

from django.db import models

import abstract_block_dumper._internal.services.utils as abd_utils


class TaskAttempt(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    class GiveUpReason(models.TextChoices):
        ATTEMPTS = "attempts", "No attempts left"
        BLOCK_AGE = "block_age", "Block too old"
        MANUAL = "manual", "Given up by hand"

    # Execution
    block_number = models.PositiveIntegerField(db_index=True)
    executable_path = models.CharField(max_length=255)
    args_json = models.TextField(default="{}")

    # Execution state
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    celery_task_id = models.CharField(max_length=50, blank=True, null=True)
    execution_result = models.JSONField(null=True)

    # Routing: the queue override used for the original submission, so retries and
    # recovered retries can be re-dispatched to the same queue. NULL means "no
    # override", i.e. the task's own `celery_kwargs` queue or Celery's default.
    celery_queue_override = models.CharField(max_length=255, blank=True, null=True)
    # The BlockDumper queue whose policy (time limits, ...) the attempt follows; NULL for
    # tasks declared with `@block_task`, which have no queue policy.
    dispatch_queue = models.CharField(max_length=255, blank=True, null=True)
    # When the current message was sent, on the database clock; with the queue's message TTL
    # it tells when that message can no longer run.
    dispatched_at = models.DateTimeField(null=True, blank=True)

    # Execution lease: set when a worker claims the attempt, on the database clock. A RUNNING
    # attempt whose lease has passed belongs to a worker that was killed or timed out.
    lease_expires_at = models.DateTimeField(null=True, blank=True)

    # Retry Management
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    attempt_count = models.PositiveIntegerField(default=0)
    next_retry_at = models.DateTimeField(null=True, blank=True)
    # Deadline from the queue's `give_up_after_blocks`, on the database clock.
    give_up_at = models.DateTimeField(null=True, blank=True)
    # Why a FAILED attempt is never retried again; NULL while it may still be.
    give_up_reason = models.CharField(max_length=20, choices=GiveUpReason.choices, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Task Attempt"
        verbose_name_plural = "Task Attempts"
        indexes = [
            models.Index(fields=["status", "next_retry_at"]),
            models.Index(fields=["block_number", "executable_path"]),
            models.Index(fields=["status", "lease_expires_at"], name="abd_attempt_status_lease_idx"),
            models.Index(fields=["dispatch_queue", "status"], name="abd_attempt_queue_status_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["block_number", "executable_path", "args_json"], name="unique_task_attempt"
            ),
        ]

    def __str__(self) -> str:
        return f"TaskAttempt(block={self.block_number}, path={self.executable_path}, status={self.status})"

    @property
    def args_dict(self) -> dict[str, Any]:
        try:
            return json.loads(self.args_json)
        except (json.JSONDecodeError, TypeError):
            return {}

    @args_dict.setter
    def args_dict(self, value: dict[str, Any]) -> None:
        self.args_json = abd_utils.serialize_args(value)
