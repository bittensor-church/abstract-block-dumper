"""
Block tasks bound to queues with a policy, on RabbitMQ with Celery's prefork pool.

Example:
    # your_project/celery.py, next to the Celery app, so that `manage.py check` sees the dumper
    from abstract_block_dumper.v1.dumper import BlockDumper, Queue

    dumper = BlockDumper(celery_app=app)
    EVENTS = Queue("events", soft_time_limit=100, time_limit=120)

    # your_app/block_tasks.py
    @dumper.block_task(live_queue=EVENTS, backfill_queue=EVENTS, backfilling_lookback=300)
    def index_events(block_number: int): ...

"""

from abstract_block_dumper._internal.services.block_dumper import BlockDumper
from abstract_block_dumper._internal.services.queue_policy import Queue
from abstract_block_dumper._internal.services.retry_policy import RetryPolicy

__all__ = ["BlockDumper", "Queue", "RetryPolicy"]
