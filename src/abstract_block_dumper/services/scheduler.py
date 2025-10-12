import time

import bittensor as bt
import structlog
from django.conf import settings

import abstract_block_dumper.dal.django_dal as abd_dal
import abstract_block_dumper.services.utils as abd_utils
from abstract_block_dumper.services.block_processor import BlockProcessor, block_processor_factory

logger = structlog.get_logger(__name__)


class TaskScheduler:
    def __init__(
        self,
        block_processor: BlockProcessor,
        subtensor: bt.Subtensor,
        poll_interval: int,
    ) -> None:
        self.block_processor = block_processor
        self.subtensor = subtensor
        self.poll_interval = poll_interval
        self.last_processed_block = -1
        self.is_running = False

    def start(self) -> None:
        self.is_running = True

        self.initialize_last_block()

        logger.info(
            "TaskScheduler started",
            last_processed_block=self.last_processed_block,
            registry_functions=len(self.block_processor.registry.get_functions()),
        )

        while self.is_running:
            try:
                # Process lost retries first
                self.block_processor.recover_failed_retries()

                current_block = self.subtensor.get_current_block()

                for block_number in range(self.last_processed_block + 1, current_block + 1):
                    self.block_processor.process_block(block_number)
                    self.last_processed_block = block_number

                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.error("Fatal scheduler error", exc_info=True)
                # resume the loop even if task failed
                time.sleep(self.poll_interval)

    def stop(self) -> None:
        self.is_running = False
        logger.info("TaskScheduler stopped.")

    def initialize_last_block(self) -> None:
        start_from_block_setting = getattr(settings, "BLOCK_DUMPER_START_FROM_BLOCK")

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
            last_block_number = abd_dal.get_the_latest_executed_block_number()

            self.last_processed_block = last_block_number or self.subtensor.get_current_block()
            logger.info(
                "Resume from the last database block or start from the current block",
                last_processed_block=self.last_processed_block,
            )


def task_scheduler_factory() -> TaskScheduler:
    return TaskScheduler(
        block_processor=block_processor_factory(),
        subtensor=abd_utils.get_bittensor_client(),
        poll_interval=getattr(settings, "BLOCK_DUMPER_POLL_INTERVAL", 1),
    )
