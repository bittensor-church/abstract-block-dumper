import argparse
import time
from dataclasses import dataclass
from typing import TypedDict, Unpack

from django.core.management.base import BaseCommand

import abstract_block_dumper._internal.dal.django_dal as abd_dal
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.services.backfill_scheduler import (
    ARCHIVE_BLOCK_THRESHOLD,
    BackfillScheduler,
    backfill_scheduler_factory,
)

SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60


class BackfillOptions(TypedDict):
    """Type definition for command options."""

    from_block: int | None
    to_block: int | None
    rate_limit: float
    network: str
    dry_run: bool
    no_gap_detection: bool
    function: str | None


@dataclass
class Gap:
    """Represents a gap of missing blocks."""

    start: int
    end: int

    @property
    def size(self) -> int:
        """
        Get the size of the gap (number of missing blocks).
        """
        return self.end - self.start + 1


def find_gaps(from_block: int, to_block: int, processed_blocks: set[int]) -> list[Gap]:
    """Find gaps (missing blocks) in the given range."""
    gaps = []
    gap_start = None

    for block_num in range(from_block, to_block + 1):
        if block_num not in processed_blocks:
            if gap_start is None:
                gap_start = block_num
        elif gap_start is not None:
            gaps.append(Gap(start=gap_start, end=block_num - 1))
            gap_start = None

    # Handle gap at the end
    if gap_start is not None:
        gaps.append(Gap(start=gap_start, end=to_block))

    return gaps


class Command(BaseCommand):
    help = "Backfill historical blocks with rate limiting. Discovers gaps by default."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Add command-line arguments to the parser.
        """
        parser.add_argument(
            "--from-block",
            type=int,
            required=False,
            help="Starting block number (inclusive). If not provided, uses min block from database.",
        )
        parser.add_argument(
            "--to-block",
            type=int,
            required=False,
            help="Ending block number (inclusive). If not provided, uses max block from database.",
        )
        parser.add_argument(
            "--rate-limit",
            type=float,
            default=1.0,
            help="Seconds to sleep between processing each block (default: 1.0)",
        )
        parser.add_argument(
            "--network",
            type=str,
            default="finney",
            help="Bittensor network name (default: finney)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview blocks to backfill without executing tasks",
        )
        parser.add_argument(
            "--no-gap-detection",
            action="store_true",
            help="Process all blocks in range instead of only gaps (original behavior)",
        )
        parser.add_argument(
            "--function",
            type=str,
            required=False,
            help="Only backfill a specific function by executable path.",
        )

    def handle(self, **options: Unpack[BackfillOptions]) -> None:  # type: ignore[override]
        """
        Main command handler.
        """
        from_block = options["from_block"]
        to_block = options["to_block"]
        rate_limit = options["rate_limit"]
        network = options["network"]
        dry_run = options["dry_run"]
        no_gap_detection = options["no_gap_detection"]
        function = options["function"]

        # Load registered functions
        self.stdout.write("Syncing decorated functions...")
        ensure_modules_loaded()
        registered_functions = task_registry.get_functions()
        functions_counter = len(registered_functions)
        self.stdout.write(self.style.SUCCESS(f"Synced {functions_counter} functions"))

        if functions_counter == 0:
            self.stderr.write(self.style.WARNING("No functions registered. Nothing to backfill."))
            return

        # Validate function filter if provided
        if function:
            matching_function = task_registry.get_by_executable_path(function)
            if matching_function is None:
                self.stderr.write(self.style.ERROR(f"Function '{function}' not found in registry."))
                self.stdout.write("Available functions:")
                for item in registered_functions:
                    self.stdout.write(f"  - {item.executable_path}")
                return
            self.stdout.write(f"Filtering to function: {function}")

        # Determine block range
        if from_block is None or to_block is None:
            min_block, max_block = abd_dal.get_block_range()
            if min_block is None or max_block is None:
                self.stderr.write(
                    self.style.ERROR("No blocks found in database. Provide --from-block and --to-block."),
                )
                return

            from_block = from_block if from_block is not None else min_block
            to_block = to_block if to_block is not None else max_block

            self.stdout.write(f"Using block range from database: {from_block} -> {to_block}")

        # Validate arguments
        if from_block > to_block:
            self.stderr.write(self.style.ERROR(f"--from-block ({from_block}) must be <= --to-block ({to_block})"))
            return

        if rate_limit < 0:
            self.stderr.write(self.style.ERROR("--rate-limit must be >= 0"))
            return

        # Use gap detection by default
        if no_gap_detection:
            self._handle_range_backfill(from_block, to_block, rate_limit, network, dry_run=dry_run, function=function)
        else:
            self._handle_gap_backfill(from_block, to_block, rate_limit, network, dry_run=dry_run, function=function)

    def _handle_gap_backfill(
        self,
        from_block: int,
        to_block: int,
        rate_limit: float,
        network: str,
        *,
        dry_run: bool,
        function: str | None,
    ) -> None:
        """Discover and process gaps in the block range."""
        self.stdout.write("")
        self.stdout.write("Discovering gaps in block range...")

        # Get all successfully processed blocks in the range
        processed_blocks = abd_dal.get_successful_block_numbers(from_block, to_block, function)
        total_blocks = to_block - from_block + 1

        self.stdout.write(f"Block range: {from_block} -> {to_block} ({total_blocks} blocks)")
        self.stdout.write(f"Successfully processed blocks: {len(processed_blocks)}")

        # Find gaps
        gaps = find_gaps(from_block, to_block, processed_blocks)

        if not gaps:
            self.stdout.write(self.style.SUCCESS("No gaps found! All blocks have been processed."))
            return

        total_missing = sum(gap.size for gap in gaps)
        self.stdout.write(f"Found {len(gaps)} gap(s) with {total_missing} missing blocks:")
        self.stdout.write("")

        # Display gaps
        for i, gap in enumerate(gaps, 1):
            if gap.size == 1:
                self.stdout.write(f"  Gap {i}: block {gap.start}")
            else:
                self.stdout.write(f"  Gap {i}: blocks {gap.start} -> {gap.end} ({gap.size} blocks)")

        self.stdout.write("")

        if dry_run:
            self._handle_gap_dry_run(gaps, total_missing, rate_limit, network, function)
        else:
            self._handle_gap_execution(gaps, total_missing, rate_limit, network, function)

    def _handle_gap_dry_run(
        self,
        gaps: list[Gap],
        total_missing: int,
        rate_limit: float,
        network: str,
        function: str | None,
    ) -> None:
        """Handle dry-run mode for gap backfill."""
        self.stdout.write(self.style.WARNING("Dry-run mode: previewing gaps to backfill"))
        self.stdout.write("")

        estimated_tasks = 0
        for gap in gaps:
            scheduler = backfill_scheduler_factory(
                from_block=gap.start,
                to_block=gap.end,
                network=network,
                rate_limit=rate_limit,
                dry_run=True,
                executable_path=function,
            )
            stats = scheduler.start()
            if stats:
                estimated_tasks += stats.estimated_tasks

        self.stdout.write(self.style.SUCCESS("Summary:"))
        self.stdout.write(f"  Total gaps: {len(gaps)}")
        self.stdout.write(f"  Total missing blocks: {total_missing}")
        self.stdout.write(f"  Estimated tasks to submit: {estimated_tasks}")

        if rate_limit > 0 and total_missing > 0:
            estimated_seconds = total_missing * rate_limit
            self._print_time_estimate(estimated_seconds, rate_limit)

    def _handle_gap_execution(
        self,
        gaps: list[Gap],
        total_missing: int,
        rate_limit: float,
        network: str,
        function: str | None,
    ) -> None:
        """Execute backfill for all gaps."""
        self.stdout.write(f"Starting backfill of {len(gaps)} gap(s) with {total_missing} missing blocks")
        self.stdout.write(f"Rate limit: {rate_limit} seconds between blocks")

        if rate_limit > 0:
            estimated_seconds = total_missing * rate_limit
            self._print_time_estimate(estimated_seconds, rate_limit)

        self.stdout.write("")
        self.stdout.write("Press Ctrl+C to stop gracefully...")
        self.stdout.write("")

        blocks_processed = 0
        try:
            for i, gap in enumerate(gaps, 1):
                self.stdout.write(f"Processing gap {i}/{len(gaps)}: {gap.start} -> {gap.end} ({gap.size} blocks)")

                scheduler = backfill_scheduler_factory(
                    from_block=gap.start,
                    to_block=gap.end,
                    network=network,
                    rate_limit=rate_limit,
                    dry_run=False,
                    executable_path=function,
                )
                scheduler.start()
                blocks_processed += gap.size

                # Small pause between gaps
                if i < len(gaps) and rate_limit > 0:
                    time.sleep(rate_limit)

        except KeyboardInterrupt:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING(f"Interrupted. Processed {blocks_processed}/{total_missing} blocks."))
            return

        self.stdout.write(self.style.SUCCESS(f"Gap backfill completed. Processed {blocks_processed} blocks."))

    def _handle_range_backfill(
        self,
        from_block: int,
        to_block: int,
        rate_limit: float,
        network: str,
        *,
        dry_run: bool,
        function: str | None,
    ) -> None:
        """Handle backfill for the entire range (original behavior)."""
        scheduler = backfill_scheduler_factory(
            from_block=from_block,
            to_block=to_block,
            network=network,
            rate_limit=rate_limit,
            dry_run=dry_run,
            executable_path=function,
        )

        total_blocks = to_block - from_block + 1

        if dry_run:
            self._handle_dry_run(scheduler, from_block, to_block, total_blocks, rate_limit)
        else:
            self._handle_backfill(scheduler, from_block, to_block, total_blocks, rate_limit)

    def _handle_dry_run(
        self,
        scheduler: BackfillScheduler,
        from_block: int,
        to_block: int,
        total_blocks: int,
        rate_limit: float,
    ) -> None:
        """Handle dry-run mode output."""
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("Dry-run mode: previewing blocks to backfill (no tasks will be executed)"))
        self.stdout.write("")

        # Get network type
        scheduler._current_head_cache = scheduler.subtensor.get_current_block()  # noqa: SLF001
        network_type = scheduler._get_network_type_for_block(from_block)  # noqa: SLF001

        self.stdout.write(f"Block range: {from_block} -> {to_block} ({total_blocks} blocks)")
        operator = ">" if network_type == "archive" else "<="
        self.stdout.write(f"Network: {network_type} (blocks {operator}{ARCHIVE_BLOCK_THRESHOLD} behind head)")
        self.stdout.write(f"Current head: {scheduler._current_head_cache}")  # noqa: SLF001
        self.stdout.write("")

        # Show registry items
        self.stdout.write("Registry items:")
        for registry_item in scheduler.block_processor.registry.get_functions():
            self.stdout.write(f"  - {registry_item.executable_path}")
        self.stdout.write("")

        # Run dry-run
        self.stdout.write("Analyzing blocks...")
        stats = scheduler.start()

        if stats is None:
            self.stderr.write(self.style.ERROR("Dry-run failed"))
            return

        # Output summary
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Summary:"))
        self.stdout.write(f"  Total blocks in range: {stats.total_blocks}")
        self.stdout.write(f"  Already processed (all tasks done): {stats.already_processed}")
        self.stdout.write(f"  Blocks needing tasks: {stats.blocks_needing_tasks}")
        self.stdout.write(f"  Estimated tasks to submit: {stats.estimated_tasks}")

        if rate_limit > 0 and stats.blocks_needing_tasks > 0:
            estimated_seconds = stats.blocks_needing_tasks * rate_limit
            self._print_time_estimate(estimated_seconds, rate_limit)

    def _handle_backfill(
        self,
        scheduler: BackfillScheduler,
        from_block: int,
        to_block: int,
        total_blocks: int,
        rate_limit: float,
    ) -> None:
        """Handle actual backfill execution."""
        self.stdout.write("")
        self.stdout.write(f"Starting backfill: {from_block} -> {to_block} ({total_blocks} blocks)")
        self.stdout.write(f"Rate limit: {rate_limit} seconds between blocks")

        if rate_limit > 0:
            estimated_seconds = total_blocks * rate_limit
            self._print_time_estimate(estimated_seconds, rate_limit)

        self.stdout.write("")
        self.stdout.write("Press Ctrl+C to stop gracefully...")
        self.stdout.write("")

        scheduler.start()

        self.stdout.write(self.style.SUCCESS("Backfill completed"))

    def _print_time_estimate(self, estimated_seconds: float, rate_limit: float) -> None:
        """Print estimated time."""
        if estimated_seconds < SECONDS_PER_MINUTE:
            time_str = f"~{estimated_seconds:.0f} seconds"
        elif estimated_seconds < SECONDS_PER_HOUR:
            time_str = f"~{estimated_seconds / SECONDS_PER_MINUTE:.1f} minutes"
        else:
            time_str = f"~{estimated_seconds / SECONDS_PER_HOUR:.1f} hours"
        self.stdout.write(f"Estimated time at {rate_limit}s rate limit: {time_str}")
