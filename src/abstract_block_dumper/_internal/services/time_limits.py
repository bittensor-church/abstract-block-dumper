"""
Time limits of `BlockDumper` tasks: where a limit comes from, and the default hard limit.

A worker takes each limit of a message from the message itself, else from the task's
attribute (`celery_kwargs` or its base class), else from the Celery config.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from abstract_block_dumper._internal.numeric import format_number
from abstract_block_dumper._internal.services.recovery_settings import recovery_margin_seconds, stale_task_settings

if TYPE_CHECKING:
    from celery import Celery

    from abstract_block_dumper._internal.services.queue_policy import Queue


@dataclass(frozen=True)
class TimeLimit:
    """
    A time limit and where it is set.

    Attributes:
        seconds: The limit.
        source: Where it is set, as an operator would look for it, e.g. "celery_kwargs" or
            "Celery's task_time_limit".

    """

    seconds: float
    source: str


def task_attribute_limit(app: Celery, celery_kwargs: Mapping[str, Any], name: str) -> TimeLimit | None:
    """
    The `name` limit (`time_limit` or `soft_time_limit`) the task sets as an attribute, if any.

    `celery_kwargs` wins over the base class: a None there leaves the task without the limit.
    """
    if name in celery_kwargs:
        return TimeLimit(celery_kwargs[name], "celery_kwargs") if celery_kwargs[name] else None
    base = celery_kwargs.get("base") or app.Task
    if getattr(base, name):
        return TimeLimit(getattr(base, name), f"{base.__name__}.{name}")
    return None


def config_limit(app: Celery, name: str) -> TimeLimit | None:
    """The `name` limit (`time_limit` or `soft_time_limit`) of the Celery config, if any."""
    seconds = getattr(app.conf, f"task_{name}")
    return TimeLimit(seconds, f"Celery's task_{name}") if seconds else None


def has_default_hard_limit(app: Celery, celery_kwargs: Mapping[str, Any]) -> bool:
    """
    Whether the task gets the default hard limit: it sets no hard limit, and neither does the config.

    `time_limit: None` in `celery_kwargs` removes the default. A limit in the config gets no
    default because a task attribute would beat it. Without a stuck-task timeout there is no
    default.
    """
    return (
        stale_task_settings()[0] is not None
        and "time_limit" not in celery_kwargs
        and task_attribute_limit(app, celery_kwargs, "time_limit") is None
        and config_limit(app, "time_limit") is None
    )


def default_time_limit_options(
    app: Celery, celery_kwargs: Mapping[str, Any], queues: Iterable[Queue | None] = ()
) -> dict[str, float]:
    """
    The `time_limit` option of a `BlockDumper` task with no hard limit of its own, else nothing.

    Celery then kills the task before its lease passes, so the sweep never takes over a task
    that is still running. The result stays above every soft limit the task can run under:
    `apply_async` rejects a lower one on the task, and the prefork pool on RabbitMQ only
    applies a hard limit after the soft limit fires.
    """
    timeout = stale_task_settings()[0]
    if timeout is None or not has_default_hard_limit(app, celery_kwargs):
        return {}
    margin = recovery_margin_seconds()
    soft_limits = [
        # A queue's own hard limit wins in its messages, so its soft limit needs no room here.
        *(queue.soft_time_limit for queue in queues if queue is not None and queue.time_limit is None),
        *(
            limit.seconds
            for limit in (
                task_attribute_limit(app, celery_kwargs, "soft_time_limit"),
                config_limit(app, "soft_time_limit"),
            )
            if limit is not None
        ),
    ]
    return {"time_limit": max([timeout, *(limit + margin for limit in soft_limits if limit)])}


def check_time_limits(
    app: Celery, celery_kwargs: Mapping[str, Any], queues: Iterable[Queue], default_time_limit: float | None
) -> None:
    """
    Raise `ValueError` when a soft limit of the task is not below the hard limit it runs under.

    Each of `queues`, and messages without queue limits, is checked with the limits a worker
    would apply. The prefork pool on RabbitMQ kills a task only at the later of its two
    limits, so with the soft limit at or above the hard one the task outlives its lease and
    the sweep retries it while it still runs.
    """
    default = TimeLimit(default_time_limit, "the default hard limit") if default_time_limit else None
    task_soft = _first_set(
        task_attribute_limit(app, celery_kwargs, "soft_time_limit"), config_limit(app, "soft_time_limit")
    )
    task_hard = _first_set(
        task_attribute_limit(app, celery_kwargs, "time_limit"), default, config_limit(app, "time_limit")
    )
    checks = [
        (f"Queue {queue.name!r}", _queue_limit(queue.soft_time_limit), _queue_limit(queue.time_limit))
        for queue in {queue.name: queue for queue in queues}.values()
    ]
    checks.append(("Messages without queue limits", None, None))
    for subject, queue_soft, queue_hard in checks:
        soft = queue_soft or task_soft
        hard = queue_hard or task_hard
        if soft is not None and hard is not None and soft.seconds >= hard.seconds:
            msg = (
                f"{subject}: soft_time_limit {format_number(soft.seconds)} s ({soft.source}) must be lower than "
                f"time_limit {format_number(hard.seconds)} s ({hard.source})"
            )
            raise ValueError(msg)


def _queue_limit(seconds: float | None) -> TimeLimit | None:
    return TimeLimit(seconds, "Queue") if seconds is not None else None


def _first_set(*limits: TimeLimit | None) -> TimeLimit | None:
    return next((limit for limit in limits if limit is not None), None)
