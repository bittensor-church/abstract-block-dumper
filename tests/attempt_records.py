from typing import Any

from django.db.models import DurationField, ExpressionWrapper, F

from abstract_block_dumper.models import TaskAttempt


def attempt_record(attempt_id: int) -> dict[str, Any]:
    """
    The externally visible state of one attempt, as a dict to compare whole.

    Columns on the database clock are reported relative to each other (`lease` is the lease
    length the claim granted), so tests do not depend on the database's absolute time.
    """
    return (
        TaskAttempt.objects.filter(id=attempt_id)
        .annotate(lease=ExpressionWrapper(F("lease_expires_at") - F("last_attempted_at"), output_field=DurationField()))
        .values(
            "status",
            "attempt_count",
            "celery_task_id",
            "celery_queue_override",
            "execution_result",
            "next_retry_at",
            "lease",
        )
        .get()
    )
