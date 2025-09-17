from django.contrib import admin
from django.utils.html import format_html

from abstract_block_dumper.models import BlockDumperConfig, BlockDumperExecution, ScheduledTask


@admin.register(BlockDumperConfig)
class BlockDumperConfigAdmin(admin.ModelAdmin):
    list_display = ["name", "condition_type", "netuid_type", "queue", "is_active", "success_rate", "last_execution_at"]
    list_filter = [
        "condition_type",
        "netuid_type",
        "queue",
        "is_active",
    ]
    search_fields = ["name", "description"]
    readonly_fields = [
        "function_path",
        "last_execution_at",
        "last_success_at",
        "last_failure_at",
        "total_executions",
        "successful_executions",
        "failed_executions",
        "created_at",
        "updated_at",
    ]

    @admin.display(description="Success Rate")
    def success_rate(self, obj: BlockDumperConfig) -> str:
        if obj.total_executions == 0:
            return "N/A"
        rate = (obj.successful_executions / obj.total_executions) * 100
        color = "green" if rate > 95 else "orange" if rate > 80 else "red"
        return format_html('<span style="color: {};">{}%</span>', color, f"{rate:.2f}")


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "config",
        "block_number",
        "netuid",
        "status",
        "celery_task_id",
        "started_at",
        "retry_count",
        "execution_duration",
    ]
    list_filter = [
        "status",
        "config__name",
        "started_at",
    ]
    search_fields = ["config__name", "celery_task_id", "block_number"]
    readonly_fields = [
        "started_at",
        "finished_at",
        "execution_duration",
        "result_data",
        "error_message",
        "error_traceback",
        "block_number",
        "netuid",
        "celery_task_id",
        "config",
        "last_attempted_at",
    ]
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "config",
                    "block_number",
                    "netuid",
                    "status",
                    "celery_task_id",
                )
            },
        ),
        (
            "Results",
            {
                "fields": (
                    "result_data",
                    "error_message",
                    "error_traceback",
                )
            },
        ),
        (
            "Retry Information",
            {
                "fields": (
                    "next_retry_at",
                    "retry_count",
                    "max_retries_reached",
                )
            },
        ),
        (
            "Timestamps",
            {
                "fields": (
                    "started_at",
                    "finished_at",
                    "execution_duration",
                    "last_attempted_at",
                )
            },
        ),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("config")


@admin.register(BlockDumperExecution)
class BlockDumperExecutionAdmin(admin.ModelAdmin):
    list_display = [
        "block_number",
        "total_tasks_scheduled",
        "tasks_completed",
        "tasks_failed",
        "all_completed",
        "has_failures",
        "scheduled_tasks_link",
    ]
    list_filter = ["all_completed", "has_failures"]
    ordering = ["-block_number"]
    search_fields = ["block_number"]
    readonly_fields = [
        "block_number",
        "total_tasks_scheduled",
        "tasks_pending",
        "tasks_completed",
        "tasks_failed",
        "all_completed",
        "started_at",
        "completed_at",
        "has_failures",
    ]

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "block_number",
                    "all_completed",
                    "has_failures",
                )
            },
        ),
        (
            "Task Summary",
            {
                "fields": (
                    "total_tasks_scheduled",
                    "tasks_pending",
                    "tasks_completed",
                    "tasks_failed",
                )
            },
        ),
        (
            "Timestamps",
            {
                "fields": (
                    "started_at",
                    "completed_at",
                )
            },
        ),
    )

    @admin.display(description="Scheduled Tasks")
    def scheduled_tasks_link(self, obj: BlockDumperExecution) -> str:
        url = f"/admin/abstract_block_dumper/scheduledtask/?block_number__exact={obj.block_number}"
        return format_html('<a href="{}">{} Scheduled Tasks</a>', url, obj.total_tasks_scheduled)
