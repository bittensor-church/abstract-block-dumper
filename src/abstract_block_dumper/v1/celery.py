"""
Celery integration helpers for abstract-block-dumper.

This module provides utilities to integrate @block_task decorated functions
with Celery workers.
"""

from abstract_block_dumper._internal.discovery import ensure_modules_loaded


def setup_celery_tasks() -> None:
    """
    Discover and register all @block_task decorated functions for Celery.

    This function MUST be called when Celery workers start to ensure that
    all @block_task decorated functions are registered and available to
    receive tasks from the message broker.

    Usage in your project's celery.py:

        from celery import Celery
        from celery.signals import worker_ready

        app = Celery('your_project')
        app.config_from_object('django.conf:settings', namespace='CELERY')
        app.autodiscover_tasks()

        @worker_ready.connect
        def on_worker_ready(**kwargs):
            '''Load block tasks when worker is ready.'''
            from abstract_block_dumper.v1.celery import setup_celery_tasks
            setup_celery_tasks()

    Why is this needed?
    -------------------
    The @block_task decorator uses Celery's @shared_task, which requires
    the decorated functions to be imported before workers can receive
    messages for those tasks. Without calling this function, you'll see
    errors like:

        "Received unregistered task of type 'your_app.block_tasks.task_name'"

    What does it do?
    ----------------
    - Automatically imports all 'tasks.py' and 'block_tasks.py' modules
      from your INSTALLED_APPS
    - Triggers @block_task decorator registration
    - Makes tasks available to Celery workers
    """
    ensure_modules_loaded()


__all__ = ["setup_celery_tasks"]
