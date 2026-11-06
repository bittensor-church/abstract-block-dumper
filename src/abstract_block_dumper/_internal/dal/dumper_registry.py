"""In-memory record of the BlockDumper instances and the queues their tasks use."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.apps import apps

if TYPE_CHECKING:
    from abstract_block_dumper._internal.services.block_dumper import BlockDumper
    from abstract_block_dumper._internal.services.queue_policy import Queue


class DumperRegistry:
    def __init__(self) -> None:
        self._dumpers: list[BlockDumper] = []
        self._created_after_django_loaded: list[BlockDumper] = []
        self._queues: dict[str, Queue] = {}
        self._queue_owners: dict[str, BlockDumper] = {}

    def register_dumper(self, dumper: BlockDumper) -> None:
        if any(existing.celery_app is dumper.celery_app for existing in self._dumpers):
            msg = f"A BlockDumper already exists for Celery app {dumper.celery_app.main!r}; create one per app"
            raise ValueError(msg)
        self._dumpers.append(dumper)
        if apps.ready:
            self._created_after_django_loaded.append(dumper)

    def dumpers(self) -> list[BlockDumper]:
        return list(self._dumpers)

    def dumpers_created_after_django_loaded(self) -> list[BlockDumper]:
        """
        Dumpers created once Django had loaded, which `manage.py check` does not see.

        Task modules create them: the management commands import those modules after Django
        has loaded, and so does Celery's autodiscovery in workers, before `celeryd_init`.
        """
        return list(self._created_after_django_loaded)

    def register_queues(self, dumper: BlockDumper, queues: list[Queue]) -> None:
        """
        Record `queues` under their names as queues of `dumper`: all of them, or none on a conflict.

        Attempts store only the queue name, so one name must mean one policy and one Celery app
        across the whole process. Registering an equal queue again for the same dumper is a
        no-op, which keeps re-imports harmless.

        Raises:
            ValueError: When a name is already defined with different settings or belongs to
                another dumper.

        """
        accepted: dict[str, Queue] = {}
        for queue in queues:
            existing = accepted.get(queue.name) or self._queues.get(queue.name)
            if existing is not None and existing != queue:
                msg = f"Queue {queue.name!r} is already defined with different settings: {existing!r}"
                raise ValueError(msg)
            owner = self._queue_owners.get(queue.name)
            if owner is not None and owner is not dumper:
                msg = (
                    f"Queue {queue.name!r} already belongs to the BlockDumper of Celery app "
                    f"{owner.celery_app.main!r}; give this queue another name"
                )
                raise ValueError(msg)
            accepted[queue.name] = queue
        self._queues.update(accepted)
        self._queue_owners.update(dict.fromkeys(accepted, dumper))

    def queues(self) -> list[Queue]:
        return list(self._queues.values())

    def get_queue(self, name: str | None) -> Queue | None:
        return self._queues.get(name) if name is not None else None

    def clear(self) -> None:
        self._dumpers = []
        self._created_after_django_loaded = []
        self._queues = {}
        self._queue_owners = {}


dumper_registry = DumperRegistry()
