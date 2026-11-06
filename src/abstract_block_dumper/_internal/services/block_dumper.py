from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from abstract_block_dumper._internal.dal.dumper_registry import dumper_registry
from abstract_block_dumper._internal.dal.memory_registry import RegistryItem, task_registry
from abstract_block_dumper._internal.numeric import is_positive_number
from abstract_block_dumper._internal.services.block_source import FINALIZED_BLOCK_SOURCE, LATEST_BLOCK_SOURCE
from abstract_block_dumper._internal.services.queue_policy import Queue, message_options
from abstract_block_dumper._internal.services.recovery_settings import check_recovery_settings
from abstract_block_dumper._internal.services.time_limits import check_time_limits, default_time_limit_options

if TYPE_CHECKING:
    from celery import Celery, Task


def _sending_live_limits(base: type[Task], live_queue: Queue) -> type[Task]:
    """
    A task class whose messages to `live_queue` carry that queue's time limits.

    The limits cannot be attributes of the task: a worker falls back to those for a message
    without limits, so they would also bind the messages of another queue that sets none. The
    default hard limit is an attribute because it is meant to bind every such message.
    `.delay()` calls `apply_async`, so manual sends get the limits too.
    """

    class LiveLimitsTask(base):  # type: ignore[misc, valid-type]
        def apply_async(self, args: Any = None, kwargs: Any = None, **options: Any) -> Any:
            if options.get("queue", live_queue.name) == live_queue.name:
                options = {**message_options(live_queue), **options}
            return super().apply_async(args, kwargs, **options)

    return LiveLimitsTask


class BlockDumper:
    """
    Registers block tasks on one Celery app, each bound to queues with a policy.

    Create one per Celery app, next to the app so that `manage.py check` sees it, then
    decorate block functions with its `block_task`. The app's broker must be RabbitMQ. The
    queues are added to the app's Celery config by the management commands and, in workers,
    by `setup_celery_tasks()` called from `celeryd_init`.
    """

    def __init__(self, celery_app: Celery, *, block_time_seconds: float = 12) -> None:
        """
        Args:
            celery_app: The app tasks are registered on and whose config gets the queues.
            block_time_seconds: Average seconds between blocks, used to turn a queue's
                `RetryPolicy.give_up_after_blocks` into a deadline in time.

        """
        if not is_positive_number(block_time_seconds):
            msg = "block_time_seconds must be a positive number"
            raise ValueError(msg)
        self.celery_app = celery_app
        self.block_time_seconds = block_time_seconds
        self._queues: dict[str, Queue] = {}
        dumper_registry.register_dumper(self)

    @property
    def queues(self) -> list[Queue]:
        """Every queue a task of this dumper uses, live or backfill."""
        return list(self._queues.values())

    def block_task(
        self,
        func: Callable[..., Any] | None = None,
        *,
        live_queue: Queue,
        backfill_queue: Queue | None = None,
        condition: Callable[..., bool] | None = None,
        args: list[dict[str, Any]] | None = None,
        finalized: bool = False,
        backfilling_lookback: int | None = None,
        celery_kwargs: dict[str, Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Register a function as a block task with queue policies.

        A manual `.delay()` of the returned task runs only an attempt that is already recorded;
        `backfill_blocks_v1 --no-gap-detection` over the block submits one by hand.

        Args:
            func: The function, when the decorator is used without parentheses.
            live_queue: Queue for submissions of new head blocks.
            backfill_queue: Queue for lookback and `backfill_blocks_v1` submissions. May be
                the same object as `live_queue`. Without it, backfills use `live_queue`.
            condition: See `abstract_block_dumper.v1.decorators.block_task`.
            args: See `abstract_block_dumper.v1.decorators.block_task`.
            finalized: See `abstract_block_dumper.v1.decorators.block_task`.
            backfilling_lookback: See `abstract_block_dumper.v1.decorators.block_task`.
                Requires `backfill_queue`.
            celery_kwargs: Additional Celery task options. Queue settings win over the same
                keys here. A `time_limit` replaces the default hard limit, and `None` removes it.

        Raises:
            ValueError: When a queue is not a `Queue`, `backfilling_lookback` is set without
                `backfill_queue`, a queue name is already defined with other settings or
                belongs to another `BlockDumper`, a soft time limit is not below the hard
                limit it runs under, or a
                `BLOCK_DUMPER_STALE_TASK_*` or `BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS` setting
                is invalid.
            TypeError: When `condition` is not callable.

        """
        for queue in (live_queue, backfill_queue):
            if queue is not None and not isinstance(queue, Queue):
                msg = f"BlockDumper queues must be Queue instances, got {queue!r}"
                raise ValueError(msg)
        if backfilling_lookback is not None and backfill_queue is None:
            msg = (
                "backfilling_lookback requires backfill_queue; pass the live queue again to backfill on the same queue"
            )
            raise ValueError(msg)
        if condition is not None and not callable(condition):
            msg = "condition must be a callable."
            raise TypeError(msg)
        effective_condition = condition if condition is not None else (lambda *_args, **_kwargs: True)
        task_options = celery_kwargs or {}
        check_recovery_settings()
        default_options = default_time_limit_options(self.celery_app, task_options, (live_queue, backfill_queue))
        queues = [queue for queue in (live_queue, backfill_queue) if queue is not None]
        check_time_limits(self.celery_app, task_options, queues, default_options.get("time_limit"))

        dumper_registry.register_queues(self, queues)
        self._queues.update({queue.name: queue for queue in queues})

        def decorator(fn: Callable[..., Any]) -> Any:
            # Imported here because both load Django models or bittensor, and a dumper is created
            # next to the Celery app, which Django imports while it loads its settings.
            from abstract_block_dumper._internal.services.task_runner import TaskRunner  # noqa: PLC0415
            from abstract_block_dumper._internal.services.utils import get_executable_path  # noqa: PLC0415

            def run_block_task(block_number: int, **kwargs: Any) -> Any:
                return TaskRunner().run(fn, block_number, kwargs)

            celery_task = self.celery_app.task(
                name=get_executable_path(fn),
                bind=False,
                shared=False,
                **{
                    **default_options,
                    **task_options,
                    "queue": live_queue.name,
                    "base": _sending_live_limits(task_options.get("base") or self.celery_app.Task, live_queue),
                },
            )(run_block_task)
            celery_task._original_func = fn  # noqa: SLF001

            task_registry.register_item(
                RegistryItem(
                    condition=effective_condition,
                    function=cast("Task", celery_task),
                    args=args,
                    block_source=FINALIZED_BLOCK_SOURCE if finalized else LATEST_BLOCK_SOURCE,
                    backfilling_lookback=backfilling_lookback,
                    backfill_queue=backfill_queue,
                    celery_kwargs=celery_kwargs or {},
                    live_queue=live_queue,
                    block_time_seconds=self.block_time_seconds,
                )
            )
            return celery_task

        if func is not None:
            return decorator(func)
        return decorator
