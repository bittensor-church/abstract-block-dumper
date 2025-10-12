"""
Maintenance tasks for Abstract Block Dumper.

This module contains utility tasks for maintaining the TaskAttempt database,
such as cleaning up old completed or failed tasks.
"""

from datetime import timedelta

from celery import shared_task
from django.db.models import Q
from django.utils import timezone

from abstract_block_dumper.models import TaskAttempt


@shared_task(name="abstract_block_dumper.cleanup_old_tasks")
def cleanup_old_tasks(days: int = 7) -> dict[str, int | str]:
    """
    Delete all succeeded or unrecoverable failed tasks older than the specified number of days.

    This task helps maintain database performance by removing old task records that are
    no longer needed. It targets:
    - Tasks with SUCCESS status
    - Tasks with FAILED status (which are unrecoverable/exhausted retries)

    Tasks with PENDING or RUNNING status are never deleted to ensure ongoing work is preserved.

    Args:
        days: Number of days to retain. Tasks older than this will be deleted. Default is 7.

    Returns:
        A dictionary containing:
            - deleted_count: Number of task attempts deleted
            - cutoff_date: ISO formatted datetime string of the cutoff date used

    Example:
        # Delete tasks older than 7 days (default)
        cleanup_old_tasks()

        # Delete tasks older than 30 days
        cleanup_old_tasks(days=30)

    Recommended Usage:
        Run this task daily via cron or Celery beat to maintain optimal database performance.
        For production systems with high task volumes, consider running it more frequently.

        Example cron (daily at 2 AM):
        0 2 * * * python manage.py shell -c \
            "from abstract_block_dumper.tasks import cleanup_old_tasks; cleanup_old_tasks.delay()"

        Example Celery beat schedule (in settings.py):
        CELERY_BEAT_SCHEDULE = {
            'cleanup-old-tasks': {
                'task': 'abstract_block_dumper.cleanup_old_tasks',
                'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
                'kwargs': {'days': 7},
            },
        }

    """
    cutoff_date = timezone.now() - timedelta(days=days)

    # Query for tasks that are either succeeded or failed (unrecoverable)
    # We only delete completed work, never pending or running tasks
    tasks_to_delete = TaskAttempt.objects.filter(
        Q(status=TaskAttempt.Status.SUCCESS) | Q(status=TaskAttempt.Status.FAILED), updated_at__lt=cutoff_date
    )

    deleted_count, _ = tasks_to_delete.delete()

    return {
        "deleted_count": deleted_count,
        "cutoff_date": cutoff_date.isoformat(),
    }
