import json
import time

import bittensor as bt
import structlog
from django.conf import settings

from abstract_block_dumper.executor import CeleryExecutor
from abstract_block_dumper.memory_registry import MemoryRegistry, RegistryItem
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.utils import get_bittensor_client

logger = structlog.get_logger(__name__)


class TaskScheduler:

    def __init__(
        self,
        registry: MemoryRegistry,
        subtensor: bt.Subtensor,
        executor: CeleryExecutor,
        poll_interval: int,
    ) -> None:
        self.registry = registry
        self.subtensor = subtensor
        self.poll_interval = poll_interval
        self.executor = executor
        self.last_processed_block = -1
        self.is_running = False

    def start(self) -> None:
        self.is_running = True

        self.initialize_last_block()

        logger.info(
            "TaskScheduler started",
            last_processed_block=self.last_processed_block,
            registry_functions=len(self.registry.get_functions())
        )

        while self.is_running:
            try:
                current_block = self.subtensor.get_current_block()

                for block_number in range(self.last_processed_block + 1, current_block + 1):
                    self.process_block(block_number)
                    self.last_processed_block = block_number

                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("TaskScheduler stopping due to KeyboardInterrupt.")
                self.stop()
                break
            except Exception:
                logger.error("Error in TaskScheduler:", exc_info=True)
                time.sleep(self.poll_interval)

    def stop(self) -> None:
        self.is_running = False
        logger.info("TaskScheduler stopped.")

    def process_block(self, block_number: int) -> None:
        for registry_item in self.registry.get_functions():
            try:
                if registry_item.requires_backfilling():
                    self.process_backfill(registry_item, block_number)

                self.process_registry_item(registry_item, block_number)
            except Exception:
                logger.error(
                    "Error processing registry item",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                    exc_info=True,
                )

    def process_registry_item(self, registry_item: RegistryItem, block_number: int) -> None:
        execution_args_list = registry_item.get_execution_args()

        for args in execution_args_list:
            try:
                if registry_item.match_condition(block_number, **args):
                    self.executor.execute(registry_item, block_number, args)
            except Exception:
                logger.error(
                    "Error evaluating condition or scheduling task",
                    function_name=registry_item.function.__name__,
                    block_number=block_number,
                    args=args,
                    exc_info=True,
                )

    def process_backfill(self, registry_item: RegistryItem, current_block: int):
        if not registry_item.backfilling_lookback:
            return

        start_block = max(0, current_block - registry_item.backfilling_lookback)

        logger.info(
            "Processing backfill",
            function_name=registry_item.function.__name__,
            start_block=start_block,
            current_block=current_block,
            lookback=registry_item.backfilling_lookback,
        )

        execution_args_list = registry_item.get_execution_args()

        for args in execution_args_list:
            args_json = self._serialize_args(args)

            executed_blocks = set(
                TaskAttempt.objects.filter(
                    executable_path=registry_item.executable_path,
                    args_json=args_json,
                    block_number__gte=start_block,
                    block_number__lte=current_block,
                    status=TaskAttempt.Status.SUCCESS,
                ).values_list("block_number", flat=True)
            )

            for block_num in range(start_block, current_block):
                if block_num not in executed_blocks:
                    try:
                        if registry_item.match_condition(block_num, **args):
                            logger.debug(
                                "Backfilling block",
                                function_name=registry_item.function.__name__,
                                block_number=block_num,
                                args=args,
                            )
                            self.executor.execute(registry_item, block_num, args)
                    except Exception:
                        logger.error(
                            "Error during backfill",
                            function_name=registry_item.function.__name__,
                            block_number=block_num,
                            args=args,
                            exc_info=True,
                        )


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
            latest_execution = TaskAttempt.objects.order_by("-block_number").first()
            if latest_execution:
                self.last_processed_block = latest_execution.block_number
                logger.info(f"Resuming from database block {self.last_processed_block}")

            else:
                self.last_processed_block = self.subtensor.get_current_block()
                logger.info(f"No database history, starting from current block {self.last_processed_block}")

    def _serialize_args(self, args) -> str:
        return json.dumps(args, sort_keys=True)


def task_scheduler_factory() -> TaskScheduler:
    return TaskScheduler(
        registry=MemoryRegistry(),
        executor=CeleryExecutor(),
        subtensor=get_bittensor_client(),
        poll_interval=getattr(settings, "BLOCK_DUMPER_POLL_INTERVAL", 1),
    )
