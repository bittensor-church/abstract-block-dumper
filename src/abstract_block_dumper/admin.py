from typing import Any

from django.contrib import admin
from django.http import HttpRequest

from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper.models import TaskAttempt

# Columns only `BlockDumper` attempts fill; projects without a BlockDumper do not see them.
BLOCK_DUMPER_FILTERS = ["give_up_reason", "dispatch_queue"]
BLOCK_DUMPER_EXECUTION_FIELDS = ("dispatch_queue", "dispatched_at", "lease_expires_at")
BLOCK_DUMPER_RETRY_FIELDS = ("give_up_at", "give_up_reason")


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
        "celery_queue_override",
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
                    "celery_queue_override",
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

    def get_list_filter(self, request: HttpRequest) -> Any:
        if not dumper_registry.dumpers():
            return self.list_filter
        return ["status", *BLOCK_DUMPER_FILTERS, "executable_path"]

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> Any:
        if not dumper_registry.dumpers():
            return self.readonly_fields
        return [*self.readonly_fields, *BLOCK_DUMPER_EXECUTION_FIELDS, *BLOCK_DUMPER_RETRY_FIELDS]

    def get_fieldsets(self, request: HttpRequest, obj: Any = None) -> Any:
        if not dumper_registry.dumpers():
            return self.fieldsets
        extra = {"Task Execution": BLOCK_DUMPER_EXECUTION_FIELDS, "Retry Information": BLOCK_DUMPER_RETRY_FIELDS}
        return tuple(
            (name, {**options, "fields": (*options["fields"], *extra.get(name or "", ()))})
            for name, options in self.fieldsets
        )
