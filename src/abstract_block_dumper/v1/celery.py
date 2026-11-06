"""
Celery integration helpers for abstract-block-dumper.

This module provides utilities to integrate @block_task decorated functions
with Celery workers.
"""

from abstract_block_dumper._internal.checks import warn_about_dumpers_unseen_by_checks
from abstract_block_dumper._internal.contacts.broker import QueueArgumentsMismatchError
from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.exceptions import QueueConfigurationError
from abstract_block_dumper._internal.services.celery_config import prepare_block_dumpers, queue_setup_error_message
from abstract_block_dumper._internal.services.celery_signals import connect_worker_signals


def setup_celery_tasks() -> None:
    """
    Discover and register all @block_task decorated functions for Celery.

    This function MUST be called when Celery workers start to ensure that
    all @block_task decorated functions are registered and available to
    receive tasks from the message broker.

    Usage in your project's celery.py:

        from celery import Celery
        from celery.signals import celeryd_init

        app = Celery('your_project')
        app.config_from_object('django.conf:settings', namespace='CELERY')
        app.autodiscover_tasks()

        @celeryd_init.connect
        def on_worker_init(**kwargs):
            '''Load block tasks before the worker starts consuming.'''
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

    With a `BlockDumper`
    --------------------
    It also adds the queues of BlockDumper tasks to the worker's Celery config and checks
    that the broker (RabbitMQ) has them with the same arguments. The config change only takes
    effect before the worker declares the queues it consumes, so projects that create a
    `BlockDumper` must call it from `celeryd_init`; later signals such as `worker_ready` are
    enough only without one.

    Raises:
        SystemExit: Only with a `BlockDumper`: when the broker is not RabbitMQ, a queue is
            defined with other arguments in the Celery config or on the broker, or
            `BLOCK_DUMPER_THROTTLE_POLL_SECONDS` is invalid while a queue has a throttle, with
            a message that says what to do. Celery's signal dispatch swallows every
            `Exception`, so anything less would leave the worker running. This also ends any
            other process that calls this function.
        BrokerUnreachableError: Only with a `BlockDumper`, when the broker is unreachable.
            The queues are already installed by then, and `celeryd_init` swallows this
            error, so a worker starts and Celery keeps retrying the connection.

    """
    ensure_modules_loaded()
    if not dumper_registry.dumpers():
        return
    warn_about_dumpers_unseen_by_checks()
    connect_worker_signals()
    try:
        prepare_block_dumpers(verify_broker=True)
    except (QueueArgumentsMismatchError, QueueConfigurationError) as exc:
        raise SystemExit(queue_setup_error_message(exc)) from exc


__all__ = ["setup_celery_tasks"]
