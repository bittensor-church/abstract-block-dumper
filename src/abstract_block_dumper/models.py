import json
from typing import Any

from django.db import models

import abstract_block_dumper.services.utils as abd_utils


class TaskAttempt(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    # Execution
    block_number = models.PositiveIntegerField(db_index=True)
    executable_path = models.CharField(max_length=255)
    args_json = models.TextField(default="{}")

    # Execution state
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    celery_task_id = models.CharField(max_length=50, blank=True, null=True)
    execution_result = models.JSONField(null=True)

    # Retry Management
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    attempt_count = models.PositiveIntegerField(default=0)
    next_retry_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Task Attempt"
        verbose_name_plural = "Task Attempts"
        indexes = [
            models.Index(fields=["status", "next_retry_at"]),
            models.Index(fields=["block_number", "executable_path"]),
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
