from datetime import timedelta

import structlog
from django.db import models
from django.utils import timezone

from abstract_block_dumper.utils import get_all_active_netuids

logger = structlog.get_logger(__name__)


DEFAULT_QUEUE = "celery"
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF = 2


class ConditionType(models.TextChoices):
    EVERY_BLOCK = "every_block", "Every Block"
    MODULO = "modulo", "Modulo Condition"
    CUSTOM = "custom", "Custom Function"
    EPOCH_START = "epoch_start", "Epoch Start"
    EPOCH_MIDDLE = "epoch_middle", "Epoch Middle"
    EPOCH_END = "epoch_end", "Epoch End"


class EpochPosition(models.TextChoices):
    START = "start", "Start of Epoch"
    MIDDLE = "middle", "Middle of Epoch"
    END = "end", "End of Epoch"


class NetuidType(models.TextChoices):
    NONE = "none", "No Netuid"
    ALL = "all", "All Netuids"
    SINGLE = "single", "Single Netuid"
    MULTIPLE = "multiple", "Multiple Netuids"


class BlockDumperConfig(models.Model):
    """
    Configure block triggered tasks execution.

    Decorated function will create a record for this model.

    Example:
    ```python
        @block_task
        def func_name():
            pass
    ```

    """

    name = models.CharField(max_length=255, unique=True, db_index=True)
    description = models.TextField(blank=True)
    function_path = models.CharField(max_length=255, help_text="Path to the executable function: myapp.task.function")

    # Condition configuration
    condition_type = models.CharField(max_length=20, choices=ConditionType.choices, default=ConditionType.EVERY_BLOCK)
    condition_params = models.JSONField(default=dict, blank=True)

    # netuid configuration
    netuid_type = models.CharField(max_length=15, choices=NetuidType.choices, default=NetuidType.NONE)
    netuid_values = models.JSONField(default=list, blank=True)

    # Execution
    queue = models.CharField(max_length=70, default=DEFAULT_QUEUE)
    max_retries = models.PositiveIntegerField(default=DEFAULT_MAX_RETRIES)
    retry_backoff = models.PositiveIntegerField(default=DEFAULT_RETRY_BACKOFF)
    timeout = models.PositiveIntegerField(null=True, blank=True)

    # Metadata
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Stats
    total_executions = models.PositiveIntegerField(default=0)
    successful_executions = models.PositiveIntegerField(default=0)
    failed_executions = models.PositiveIntegerField(default=0)
    last_execution_at = models.DateTimeField(null=True, blank=True)
    last_success_at = models.DateTimeField(null=True, blank=True)
    last_failure_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        """Meta configuration for BlockDumperConfig model."""

        verbose_name = "Configuration"
        verbose_name_plural = "Configurations"
        indexes = [
            models.Index(fields=["is_active", "condition_type"]),
            models.Index(fields=["queue", "is_active"]),
            models.Index(fields=["last_execution_at"]),
        ]

    def __str__(self):
        return f"{self.name} ({self.get_condition_type_display()})"  # type: ignore (TODO: install django-stubs)

    def should_execute_at_block(
        self,
        block_number: int,
        netuid: int | None = None,
    ) -> bool:
        """
        Determine if the task should execute at the given block number and optional netuid.

        :param block_number: The current block number.
        :param netuid: Optional netuid for conditions that depend on it.
        :return: True if the task should execute, False otherwise.
        """
        from abstract_block_dumper.conditions import get_condition_instance

        condition = get_condition_instance(ConditionType(self.condition_type), self.condition_params)
        return condition.should_execute(block_number, netuid)

    def get_netuids(self) -> list[None] | list[int]:
        """
        Get the list of netuids based on the netuid_type and netuid_values.

        :return: List of netuids or [None] if no netuid is applicable.
        """
        if self.netuid_type == NetuidType.NONE:
            return [None]
        elif self.netuid_type == NetuidType.ALL:
            return get_all_active_netuids()
        elif self.netuid_type in {NetuidType.SINGLE, NetuidType.MULTIPLE}:
            return self.netuid_values
        else:
            return [None]

    def update_stats(self, success: bool) -> None:
        self.total_executions += 1
        self.last_execution_at = timezone.now()

        if success:
            self.successful_executions += 1
            self.last_success_at = timezone.now()
        else:
            self.failed_executions += 1
            self.last_failure_at = timezone.now()

        self.save(
            update_fields=[
                "total_executions",
                "successful_executions",
                "failed_executions",
                "last_execution_at",
                "last_success_at",
                "last_failure_at",
            ]
        )


class ScheduledTask(models.Model):
    class Status(models.TextChoices):
        """Status choices for scheduled tasks."""

        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    config = models.ForeignKey(BlockDumperConfig, on_delete=models.CASCADE, related_name="scheduled_tasks")

    block_number = models.IntegerField(db_index=True)
    netuid = models.PositiveIntegerField(null=True, blank=True, db_index=True)

    # Execution information
    celery_task_id = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(
        max_length=15,
        choices=Status.choices,
        default=Status.PENDING,
    )
    scheduled_time = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)

    # Retry
    retry_count = models.PositiveIntegerField(default=0)
    max_retries_reached = models.BooleanField(default=False)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    next_retry_at = models.DateTimeField(null=True, blank=True)

    # Results
    # TODO: proper field type
    execution_duration = models.DurationField(null=True, blank=True)

    result_data = models.JSONField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    error_traceback = models.TextField(blank=True)

    class Meta:
        """Meta configuration for ScheduledTask model."""

        verbose_name = "Scheduled Task"
        verbose_name_plural = "Scheduled Tasks"

    def can_retry(self) -> bool:
        return (
            self.status == self.Status.FAILED
            and self.retry_count < self.config.max_retries
            and not self.max_retries_reached
        )

    def mark_as_started(self, celery_task_id: str) -> None:
        self.status = self.Status.RUNNING
        self.started_at = timezone.now()
        self.celery_task_id = celery_task_id
        self.last_attempted_at = timezone.now()
        self.save(update_fields=["status", "started_at", "celery_task_id", "last_attempted_at"])

    def mark_as_success(self, result_data: dict | None = None, duration: timedelta | None = None) -> None:
        self.status = self.Status.SUCCESS
        self.finished_at = timezone.now()
        self.result_data = result_data
        if duration:
            self.execution_duration = duration
        self.last_attempted_at = timezone.now()
        self.next_retry_at = None
        self.max_retries_reached = False
        self.save(
            update_fields=[
                "status",
                "finished_at",
                "result_data",
                "execution_duration",
                "last_attempted_at",
                "next_retry_at",
                "max_retries_reached",
            ]
        )
        self.config.update_stats(success=True)

        # Update BlockDumperExecution stats
        self._update_block_execution_stats()

    def mark_as_failed(self, error_message: str, error_traceback: str, schedule_retry: bool = True) -> None:
        self.status = self.Status.FAILED
        self.error_message = error_message
        self.error_traceback = error_traceback
        self.finished_at = timezone.now()
        self.last_attempted_at = timezone.now()

        if schedule_retry and self.can_retry():
            self.retry_count += 1

            backoff_minutes = (self.config.retry_backoff**self.retry_count) * 1
            self.next_retry_at = timezone.now() + timedelta(minutes=backoff_minutes)
        else:
            self.next_retry_at = None
            self.max_retries_reached = True

        self.save(
            update_fields=[
                "status",
                "error_message",
                "error_traceback",
                "finished_at",
                "last_attempted_at",
                "retry_count",
                "next_retry_at",
                "max_retries_reached",
            ]
        )
        self.config.update_stats(success=False)

        # Update BlockDumperExecution stats
        self._update_block_execution_stats()

    def _update_block_execution_stats(self) -> None:
        """
        Update BlockDumperExecution stats for this task's block.
        This method handles race conditions by using atomic updates.
        """
        try:
            execution = BlockDumperExecution.objects.get(block_number=self.block_number)

            # Count actual task statuses for this block to avoid race conditions
            block_tasks = ScheduledTask.objects.filter(block_number=self.block_number)
            completed_count = block_tasks.filter(status=self.Status.SUCCESS).count()
            failed_count = block_tasks.filter(status=self.Status.FAILED).count()
            pending_count = block_tasks.filter(status=self.Status.PENDING).count()

            # Update with actual counts
            execution.tasks_completed = completed_count
            execution.tasks_failed = failed_count
            execution.tasks_pending = pending_count
            execution.all_completed = pending_count == 0 and execution.total_tasks_scheduled > 0
            execution.has_failures = failed_count > 0
            if execution.all_completed:
                execution.completed_at = timezone.now()

            execution.save(
                update_fields=[
                    "tasks_completed",
                    "tasks_failed",
                    "tasks_pending",
                    "all_completed",
                    "has_failures",
                    "completed_at",
                ]
            )
            logger.debug(
                f"[STATS_UPDATE] Updated block {self.block_number}: completed={completed_count}, "
                f"failed={failed_count}, pending={pending_count}, all_completed={execution.all_completed}"
            )

        except BlockDumperExecution.DoesNotExist:
            logger.warning(f"[STATS_UPDATE] BlockDumperExecution not found for block {self.block_number}")
        except Exception as e:
            logger.error(f"[STATS_UPDATE] Error updating block {self.block_number} stats: {e}")
            # This shouldn't happen, but handle gracefully
            pass


class BlockDumperExecution(models.Model):
    block_number = models.PositiveIntegerField(unique=True, db_index=True)
    total_tasks_scheduled = models.PositiveBigIntegerField(default=0)
    tasks_completed = models.PositiveBigIntegerField(default=0)
    tasks_failed = models.PositiveBigIntegerField(default=0)
    tasks_pending = models.PositiveBigIntegerField(default=0)

    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    all_completed = models.BooleanField(default=False)
    has_failures = models.BooleanField(default=False)

    def __str__(self):
        return f"Block {self.block_number} ({self.tasks_completed}/{self.total_tasks_scheduled} completed)"

    class Meta:
        """Meta configuration for BlockDumperExecution model."""

        verbose_name = "Execution"
        verbose_name_plural = "Executions"
