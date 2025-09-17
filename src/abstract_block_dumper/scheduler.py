import time

import structlog
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from abstract_block_dumper.models import BlockDumperConfig, BlockDumperExecution, ScheduledTask
from abstract_block_dumper.tasks import execute_block_task
from abstract_block_dumper.utils import get_bittensor_client

logger = structlog.get_logger(__name__)


DEFAULT_BLOCK_DUMPER_POLL_INTERVAL = 5  # seconds
DEFAULT_BLOCK_DUMPER_MAX_BLOCKS_BEHIND = 10


class BlockScheduler:
    def __init__(self) -> None:
        self.subtensor = get_bittensor_client()
        self.poll_interval = settings.BLOCK_DUMPER_POLL_INTERVAL or DEFAULT_BLOCK_DUMPER_POLL_INTERVAL
        self.max_blocks_behind = settings.BLOCK_DUMPER_MAX_BLOCKS_BEHIND or DEFAULT_BLOCK_DUMPER_MAX_BLOCKS_BEHIND
        self.last_processed_block = -1
        self.is_running = False

    def start(self) -> None:
        self.is_running = True
        logger.info("BlockScheduler started.")

        self._initialize_last_block()

        while self.is_running:
            try:
                current_block = self.subtensor.get_current_block()
                self._process_block_range(current_block)
                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("BlockScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception as e:
                logger.error(f"Error in BlockScheduler: {e}")
                time.sleep(self.poll_interval)

    def stop(self) -> None:
        self.is_running = False
        logger.info("BlockScheduler stopped.")

    def _initialize_last_block(self) -> None:
        start_from_block_setting = settings.BLOCK_DUMPER_START_FROM_BLOCK or None

        if start_from_block_setting is not None:
            if start_from_block_setting == "current":
                self.last_processed_block = self.subtensor.get_current_block()
                logger.info(f"Starting from current blockchain block {self.last_processed_block}")

            elif isinstance(start_from_block_setting, int):
                self.last_processed_block = start_from_block_setting
                logger.info(f"Starting from configured block {self.last_processed_block}")

            else:
                raise ValueError(f"Invalid BLOCK_DUMPER_START_FROM_BLOCK value: {start_from_block_setting}")
        else:
            # Default behavior - resume from database
            latest_execution = BlockDumperExecution.objects.order_by("-block_number").first()
            if latest_execution:
                self.last_processed_block = latest_execution.block_number
                logger.info(f"Resuming from database block {self.last_processed_block}")

            else:
                self.last_processed_block = self.subtensor.get_current_block()
                logger.info(f"No database history, starting from current block {self.last_processed_block}")

    def _process_block_range(self, current_block_number: int) -> None:
        if current_block_number <= self.last_processed_block:
            return

        start_block = self.last_processed_block + 1
        end_block = min(current_block_number, start_block + self.max_blocks_behind - 1)

        blocks_to_process = list(range(start_block, end_block + 1))

        if blocks_to_process:
            logger.info("Processing block range", start=start_block, end=end_block, total=len(blocks_to_process))

            for block_number in blocks_to_process:
                self._schedule_task_for_block(block_number)

            self.last_processed_block = end_block

    def _schedule_task_for_block(self, block_number: int):
        """
        Find all applicable tasks for the given block number and schedule them.
        """
        logger.debug("Scheduling tasks for block", block_number=block_number)

        with transaction.atomic():
            execution, _ = BlockDumperExecution.objects.get_or_create(
                block_number=block_number, defaults={"started_at": timezone.now()}
            )

        # Store tasks to schedule after transaction commits
        tasks_to_schedule = []

        active_configs = BlockDumperConfig.objects.filter(is_active=True)
        for config in active_configs:
            netuids = config.get_netuids()

            for netuid in netuids:
                should_execute = config.should_execute_at_block(block_number, netuid)
                if should_execute:
                    with transaction.atomic():
                        task, task_created = ScheduledTask.objects.get_or_create(
                            config=config, block_number=block_number, netuid=netuid
                        )
                        logger.info(
                            "Created ScheduledTask",
                            config=config.name,
                            block=block_number,
                            netuid=netuid,
                            task_id=task.id,
                            created=task_created,
                        )

                    if task_created:
                        tasks_to_schedule.append((task, config))

        scheduled_tasks = self.schedule_celery_tasks(tasks_to_schedule)

        if len(tasks_to_schedule) > 0:
            execution.total_tasks_scheduled += scheduled_tasks
            execution.save(update_fields=["total_tasks_scheduled"])

            logger.info(f"Scheduled {len(tasks_to_schedule)} tasks for block {block_number}")

        if len(tasks_to_schedule) != scheduled_tasks:
            logger.warning(
                "Some tasks were not scheduled successfully",
                block=block_number,
                attempted=len(tasks_to_schedule),
                successful=scheduled_tasks,
            )

    def schedule_celery_tasks(self, tasks_to_schedule: list[tuple[ScheduledTask, BlockDumperConfig]]) -> int:
        scheduled_tasks = 0
        for task, config in tasks_to_schedule:
            try:
                celery_result = execute_block_task.apply_async(args=[task.id], queue=config.queue)
                task.celery_task_id = celery_result.id
                task.save(update_fields=["celery_task_id"])

                logger.debug(
                    "Scheduled new task",
                    config=config.name,
                    block=task.block_number,
                    netuid=task.netuid,
                    task_id=task.id,
                    celery_task_id=celery_result.id,
                )
                scheduled_tasks += 1
            except Exception as e:
                logger.error(
                    "Failed to schedule Celery task",
                    config=config.name,
                    block=task.block_number,
                    netuid=task.netuid,
                    task_id=task.id,
                    error=str(e),
                )
        return scheduled_tasks


def block_scheduler_factory() -> BlockScheduler:
    return BlockScheduler()
