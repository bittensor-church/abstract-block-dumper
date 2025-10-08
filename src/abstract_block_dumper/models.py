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
        self.args_json = json.dumps(value, sort_keys=True)

    def can_retry(self) -> bool:
        default_max_attempts = 3
        max_attempts = getattr(settings, "BLOCK_DUMPER_MAX_ATTEMPTS", default_max_attempts)
        return self.status == self.Status.FAILED and self.attempt_count < max_attempts

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
        MAX_RETRY_DELAY_MINUTES = 1440  # 24 hours max delay

        self.status = self.Status.FAILED
        self.last_attempted_at = timezone.now()
        self.attempt_count += 1

        if self.can_retry():
            base_retry_backoff = getattr(settings, "BLOCK_TASK_RETRY_BACKOFF", DEFAULT_BLOCK_TASK_RETRY_BACKOFF)
            max_delay_minutes = getattr(settings, "BLOCK_TASK_MAX_RETRY_DELAY_MINUTES", MAX_RETRY_DELAY_MINUTES)

            # Calculate exponential backoff with bounds checking
            backoff_minutes = base_retry_backoff**self.attempt_count
            backoff_minutes = min(backoff_minutes, max_delay_minutes)

            self.next_retry_at = timezone.now() + timedelta(minutes=backoff_minutes)
        else:
            self.next_retry_at = None
        self.save()

    @classmethod
    def create_or_get_pending(
        cls,
        block_number: int,
        executable_path: str,
        args: dict[str, Any] | None = None,
    ) -> tuple["TaskAttempt", bool]:
        """
        Create or get a pending task attempt.
        Returns (task_attempt, created) where created indicates if a new task was created.

        For failed tasks that can retry:
        - If next_retry_at is in the future, leave task as FAILED (will be picked up by scheduler)
        - If next_retry_at is in the past or None, reset to PENDING for immediate execution
        """
        if args is None:
            args = {}
        args_json = json.dumps(args, sort_keys=True)

        with transaction.atomic():
            task_attempt, created = cls.objects.get_or_create(
                block_number=block_number,
                executable_path=executable_path,
                args_json=args_json,
                defaults={"status": cls.Status.PENDING},
            )

            # Don't modify tasks that are already in a terminal or active state
            if created or task_attempt.status in [cls.Status.SUCCESS, cls.Status.RUNNING]:
                return task_attempt, created

            # For failed tasks that can retry, only reset to PENDING if retry time has passed
            if task_attempt.status == cls.Status.FAILED and task_attempt.can_retry():
                now = timezone.now()
                if task_attempt.next_retry_at is None or task_attempt.next_retry_at <= now:
                    task_attempt.status = cls.Status.PENDING
                    task_attempt.save(update_fields=["status"])
        return task_attempt, created
