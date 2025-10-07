from django.contrib import admin

from abstract_block_dumper.models import TaskAttempt


@admin.register(TaskAttempt)
class TaskAttemptAdmin(admin.ModelAdmin):
    list_display = [
        "executable_path",
        "block_number",
        "status",
    ]
    list_filter = [
        "status",
        "executable_path",
    ]
    search_fields = ["celery_task_id", "block_number"]
    readonly_fields = [
        "block_number",
        "executable_path",
        "args_json",
        "status",
        # Execution fields
        "celery_task_id",
        "execution_result",
        # Attempts & Retry fields
        "last_attempted_at",
        "attempt_count",
        "next_retry_at",
        "created_at",
        "updated_at",
    ]
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "block_number",
                    "executable_path",
                    "args_json",
                )
            },
        ),
        (
            "Task Execution",
            {
                "fields": (
                    "status",
                    "celery_task_id",
                    "execution_result",
                )
            },
        ),
        (
            "Retry Information",
            {
                "fields": (
                    "last_attempted_at",
                    "attempt_count",
                    "next_retry_at",
                )
            },
        ),
        (
            "Timestamps",
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )
