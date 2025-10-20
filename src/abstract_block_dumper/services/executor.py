from typing import Any

import structlog

import abstract_block_dumper.dal.django_dal as abd_dal
from abstract_block_dumper.dal.memory_registry import RegistryItem
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class CeleryExecutor:
    def execute(self, registry_item: RegistryItem, block_number: int, args: dict[str, Any]) -> None:
        task_attempt, created = abd_dal.task_create_or_get_pending(
            block_number=block_number,
            executable_path=registry_item.executable_path,
            args=args,
        )
        if not created and task_attempt.status != TaskAttempt.Status.PENDING:
            logger.debug(
                "Task already exists",
                task_id=task_attempt.id,
                status=task_attempt.status,
            )
            return

        task_kwargs = {
            "block_number": block_number,
            **args,
        }

        apply_async_kwargs: dict[str, Any] = {"kwargs": task_kwargs}

        if task_attempt.next_retry_at:
            apply_async_kwargs["eta"] = task_attempt.next_retry_at

        celery_options = {
            k: v for k, v in (registry_item.celery_kwargs or {}).items() if k not in ("kwargs", "eta", "args")
        }

        apply_async_kwargs.update(celery_options)

        logger.info(
            "Scheduling Celery task",
            task_id=task_attempt.id,
            block_number=task_attempt.block_number,
            executable_path=task_attempt.executable_path,
            args=args,
            celery_kwargs=apply_async_kwargs,
        )

        celery_task = registry_item.function.apply_async(**apply_async_kwargs)

        logger.debug("Celery task scheduled", task_id=task_attempt.id, celery_task_id=celery_task.id)
