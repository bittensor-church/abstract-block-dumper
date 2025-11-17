"""Tests for Celery integration helpers."""

from abstract_block_dumper.v1.celery import setup_celery_tasks


def test_setup_celery_tasks_can_be_called():
    """Test that setup_celery_tasks can be called without errors."""
    # This should not raise any exceptions
    setup_celery_tasks()


def test_setup_celery_tasks_is_idempotent():
    """Test that calling setup_celery_tasks multiple times is safe."""
    # Should be safe to call multiple times (e.g., if worker restarts)
    setup_celery_tasks()
    setup_celery_tasks()
    setup_celery_tasks()
