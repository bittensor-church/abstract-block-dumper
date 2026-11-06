"""Worker-side Celery signal handlers for `BlockDumper` tasks."""

from typing import Any

from celery.signals import task_revoked

from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.services.metrics import record_message_expired

EXPIRED_MESSAGES_DISPATCH_UID = "abstract_block_dumper.count_expired_messages"


def _count_expired_messages(sender: Any = None, request: Any = None, expired: bool = False, **_kwargs: Any) -> None:
    if not expired or sender is None:
        return
    registry_item = task_registry.get_by_executable_path(sender.name)
    if registry_item is None or registry_item.live_queue is None:
        return
    delivery_info = getattr(request, "delivery_info", None) or {}
    record_message_expired(delivery_info.get("routing_key") or "unknown")


def connect_worker_signals() -> None:
    """Connect the handlers; safe to call more than once."""
    task_revoked.connect(_count_expired_messages, dispatch_uid=EXPIRED_MESSAGES_DISPATCH_UID)
