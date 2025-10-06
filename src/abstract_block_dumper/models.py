import json
from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db import models, transaction
from django.utils import timezone


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
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING
    )
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
                fields=['block_number', 'executable_path', 'args_json'],
                name='unique_task_attempt'
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
        self.args_json = json.dumps(value, sort_keys=True)

    def can_retry(self) -> bool:
        DEFAULT_BLOCK_DUMPER_MAX_ATTEMPTS = 3
        return (
            self.status == self.Status.FAILED
            and self.attempt_count <= getattr(settings, "BLOCK_DUMPER_MAX_ATTEMPTS", DEFAULT_BLOCK_DUMPER_MAX_ATTEMPTS)
        )

    def mark_started(self, celery_task_id: str) -> None:
        self.celery_task_id = celery_task_id
        self.status = self.Status.RUNNING
        self.last_attempted_at = timezone.now()
        self.save()

    def mark_success(self, result_data: dict) -> None:
        self.status = self.Status.SUCCESS
        self.execution_result = result_data
        self.last_attempted_at = timezone.now()
        self.next_retry_at = None
        self.save()

    def mark_failed(self) -> None:
        DEFAULT_BLOCK_TASK_RETRY_BACKOFF = 1
        self.status = self.Status.FAILED
        self.finished_at = timezone.now()
        self.last_attempted_at = timezone.now()
        self.attempt_count += 1

        if self.can_retry():
            base_retry_backoff = getattr(settings, "BLOCK_TASK_RETRY_BACKOFF", DEFAULT_BLOCK_TASK_RETRY_BACKOFF)
            backoff_minutes = base_retry_backoff**self.attempt_count
            self.next_retry_at = timezone.now() + timedelta(minutes=backoff_minutes)
        else:
            self.next_retry_at = None
        self.save()

    @classmethod
    def create_or_get_pending(
        cls,
        block_number: int,
        executable_path: str,
        args: dict[str, Any],
    ) -> tuple['TaskAttempt', bool]:
        """
        Create or get a pending task attempt.
        """
        args_json = json.dumps(args, sort_keys=True)

        with transaction.atomic():
            task_attempt, created = cls.objects.get_or_create(
                block_number=block_number,
                executable_path=executable_path,
                args_json=args_json,
                defaults={"status": cls.Status.PENDING}
            )

            if not created and task_attempt.status == cls.Status.FAILED and task_attempt.can_retry():
                task_attempt.status = cls.Status.PENDING
                task_attempt.save(update_fields=['status'])
        return task_attempt, created
