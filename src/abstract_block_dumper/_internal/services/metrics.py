"""
Optional Prometheus metrics for block dumper.

Metrics are only available if prometheus_client is installed.
Install with: pip install abstract-block-dumper[prometheus]
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import structlog

if TYPE_CHECKING:
    from types import TracebackType

logger = structlog.get_logger(__name__)

# Conditional import - metrics only work if prometheus_client is installed
try:
    from prometheus_client import Counter, Gauge, Histogram

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


# Define no-op placeholders when prometheus is not available
BLOCKS_PROCESSED = None
TASKS_SUBMITTED = None
CURRENT_BLOCK = None
BACKFILL_PROGRESS = None
BACKFILL_FROM_BLOCK = None
BACKFILL_TO_BLOCK = None
BLOCK_PROCESSING_TIME = None
# Task-level metrics
TASK_EXECUTIONS = None
TASK_EXECUTION_TIME = None
TASK_RETRIES = None
# Business/observability metrics
BLOCK_LAG = None  # How far behind the chain head
PENDING_TASKS = None  # Current pending tasks count
REGISTERED_TASKS = None  # Number of registered task types
ARCHIVE_NETWORK_USAGE = None  # Counter for archive network fallback

if PROMETHEUS_AVAILABLE:
    BLOCKS_PROCESSED = Counter(  # type: ignore
        "block_dumper_blocks_processed_total",
        "Total blocks processed",
        ["mode"],  # 'realtime' or 'backfill'
    )
    TASKS_SUBMITTED = Counter(  # type: ignore
        "block_dumper_tasks_submitted_total",
        "Total tasks submitted to Celery",
        ["task_name"],
    )
    CURRENT_BLOCK = Gauge(  # type: ignore
        "block_dumper_current_block",
        "Current block being processed",
        ["mode"],
    )
    BACKFILL_PROGRESS = Gauge(  # type: ignore
        "block_dumper_backfill_progress_percent",
        "Backfill progress percentage",
    )
    BACKFILL_FROM_BLOCK = Gauge(  # type: ignore
        "block_dumper_backfill_from_block",
        "Backfill starting block number",
    )
    BACKFILL_TO_BLOCK = Gauge(  # type: ignore
        "block_dumper_backfill_to_block",
        "Backfill target block number",
    )
    BLOCK_PROCESSING_TIME = Histogram(  # type: ignore
        "block_dumper_block_processing_seconds",
        "Time spent processing each block",
        ["mode"],
    )
    # Task-level metrics
    TASK_EXECUTIONS = Counter(  # type: ignore
        "block_dumper_task_executions_total",
        "Total task executions by status",
        ["task_name", "status"],  # status: 'success', 'failed'
    )
    TASK_EXECUTION_TIME = Histogram(  # type: ignore
        "block_dumper_task_execution_seconds",
        "Time spent executing each task",
        ["task_name"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
    )
    TASK_RETRIES = Counter(  # type: ignore
        "block_dumper_task_retries_total",
        "Total task retry attempts",
        ["task_name"],
    )
    # Business/observability metrics
    BLOCK_LAG = Gauge(  # type: ignore
        "block_dumper_block_lag",
        "Number of blocks behind the chain head",
        ["mode"],  # 'realtime' or 'backfill'
    )
    PENDING_TASKS = Gauge(  # type: ignore
        "block_dumper_pending_tasks",
        "Current number of pending tasks in queue",
    )
    REGISTERED_TASKS = Gauge(  # type: ignore
        "block_dumper_registered_tasks",
        "Number of registered task types",
    )
    ARCHIVE_NETWORK_USAGE = Counter(  # type: ignore
        "block_dumper_archive_network_requests_total",
        "Total requests using archive network",
    )


def increment_blocks_processed(mode: str) -> None:
    """Increment the blocks processed counter."""
    if PROMETHEUS_AVAILABLE and BLOCKS_PROCESSED is not None:
        BLOCKS_PROCESSED.labels(mode=mode).inc()


def increment_tasks_submitted(task_name: str) -> None:
    """Increment the tasks submitted counter."""
    if PROMETHEUS_AVAILABLE and TASKS_SUBMITTED is not None:
        TASKS_SUBMITTED.labels(task_name=task_name).inc()


def set_current_block(mode: str, block_number: int) -> None:
    """Set the current block being processed."""
    if PROMETHEUS_AVAILABLE and CURRENT_BLOCK is not None:
        CURRENT_BLOCK.labels(mode=mode).set(block_number)


def set_backfill_progress(from_block: int, to_block: int, current_block: int) -> None:
    """Set backfill progress metrics."""
    if not PROMETHEUS_AVAILABLE:
        return

    if BACKFILL_FROM_BLOCK is not None:
        BACKFILL_FROM_BLOCK.set(from_block)
    if BACKFILL_TO_BLOCK is not None:
        BACKFILL_TO_BLOCK.set(to_block)

    if BACKFILL_PROGRESS is not None:
        total_blocks = to_block - from_block
        if total_blocks > 0:
            processed = current_block - from_block
            progress = (processed / total_blocks) * 100
            BACKFILL_PROGRESS.set(progress)


def set_block_lag(mode: str, lag: int) -> None:
    """Set the current block lag (distance from chain head)."""
    if PROMETHEUS_AVAILABLE and BLOCK_LAG is not None:
        BLOCK_LAG.labels(mode=mode).set(lag)


def set_pending_tasks(count: int) -> None:
    """Set the current number of pending tasks."""
    if PROMETHEUS_AVAILABLE and PENDING_TASKS is not None:
        PENDING_TASKS.set(count)


def set_registered_tasks(count: int) -> None:
    """Set the number of registered task types."""
    if PROMETHEUS_AVAILABLE and REGISTERED_TASKS is not None:
        REGISTERED_TASKS.set(count)


def increment_archive_network_usage() -> None:
    """Increment the archive network usage counter."""
    if PROMETHEUS_AVAILABLE and ARCHIVE_NETWORK_USAGE is not None:
        ARCHIVE_NETWORK_USAGE.inc()


def record_task_execution(task_name: str, status: str) -> None:
    """Record a task execution with status (success/failed)."""
    if PROMETHEUS_AVAILABLE and TASK_EXECUTIONS is not None:
        TASK_EXECUTIONS.labels(task_name=task_name, status=status).inc()


def record_task_retry(task_name: str) -> None:
    """Record a task retry attempt."""
    if PROMETHEUS_AVAILABLE and TASK_RETRIES is not None:
        TASK_RETRIES.labels(task_name=task_name).inc()


def observe_task_execution_time(task_name: str, duration: float) -> None:
    """Record task execution duration in seconds."""
    if PROMETHEUS_AVAILABLE and TASK_EXECUTION_TIME is not None:
        TASK_EXECUTION_TIME.labels(task_name=task_name).observe(duration)


class TaskExecutionTimer:
    """Context manager for timing task execution."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        self._timer: Any = None

    def __enter__(self) -> Self:
        if PROMETHEUS_AVAILABLE and TASK_EXECUTION_TIME is not None:
            self._timer = TASK_EXECUTION_TIME.labels(task_name=self.task_name).time()
            self._timer.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._timer is not None:
            self._timer.__exit__(exc_type, exc_val, exc_tb)


class BlockProcessingTimer:
    """Context manager for timing block processing."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self._timer: Any = None

    def __enter__(self) -> Self:
        if PROMETHEUS_AVAILABLE and BLOCK_PROCESSING_TIME is not None:
            self._timer = BLOCK_PROCESSING_TIME.labels(mode=self.mode).time()
            self._timer.__enter__()  # Start the timer
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._timer is not None:
            self._timer.__exit__(exc_type, exc_val, exc_tb)
