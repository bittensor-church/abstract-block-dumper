"""Management command for backfilling historical blocks."""

from django.core.management.base import BaseCommand

from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.services.backfill_scheduler import (
    ARCHIVE_BLOCK_THRESHOLD,
    BackfillScheduler,
    backfill_scheduler_factory,
)


class Command(BaseCommand):
    help = "Backfill historical blocks with rate limiting."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--from-block",
            type=int,
            required=True,
            help="Starting block number (inclusive)",
        )
        parser.add_argument(
            "--to-block",
            type=int,
            required=True,
            help="Ending block number (inclusive)",
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

    def handle(self, *args, **options) -> None:
        from_block = options["from_block"]
        to_block = options["to_block"]
        rate_limit = options["rate_limit"]
        network = options["network"]
        dry_run = options["dry_run"]

        # Validate arguments
        if from_block > to_block:
            self.stderr.write(self.style.ERROR(f"--from-block ({from_block}) must be <= --to-block ({to_block})"))
            return

        if rate_limit < 0:
            self.stderr.write(self.style.ERROR("--rate-limit must be >= 0"))
            return

        # Load registered functions
        self.stdout.write("Syncing decorated functions...")
        ensure_modules_loaded()
        functions_counter = len(task_registry.get_functions())
        self.stdout.write(self.style.SUCCESS(f"Synced {functions_counter} functions"))

        if functions_counter == 0:
            self.stderr.write(self.style.WARNING("No functions registered. Nothing to backfill."))
            return

        # Create scheduler
        scheduler = backfill_scheduler_factory(
            from_block=from_block,
            to_block=to_block,
            network=network,
            rate_limit=rate_limit,
            dry_run=dry_run,
        )

        total_blocks = to_block - from_block + 1

        if dry_run:
            self._handle_dry_run(scheduler, from_block, to_block, total_blocks, rate_limit)
        else:
            self._handle_backfill(scheduler, from_block, to_block, total_blocks, rate_limit)

    def _handle_dry_run(
        self, scheduler: BackfillScheduler, from_block: int, to_block: int, total_blocks: int, rate_limit: float
    ) -> None:
        """Handle dry-run mode output."""
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("Dry-run mode: previewing blocks to backfill (no tasks will be executed)"))
        self.stdout.write("")

        # Get network type
        scheduler._current_head_cache = scheduler.subtensor.get_current_block()
        network_type = scheduler._get_network_type_for_block(from_block)

        self.stdout.write(f"Block range: {from_block} -> {to_block} ({total_blocks} blocks)")
        operator = ">" if network_type == "archive" else "<="
        self.stdout.write(f"Network: {network_type} (blocks {operator}{ARCHIVE_BLOCK_THRESHOLD} behind head)")
        self.stdout.write(f"Current head: {scheduler._current_head_cache}")
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
            if estimated_seconds < 60:
                time_str = f"~{estimated_seconds:.0f} seconds"
            elif estimated_seconds < 3600:
                time_str = f"~{estimated_seconds / 60:.1f} minutes"
            else:
                time_str = f"~{estimated_seconds / 3600:.1f} hours"
            self.stdout.write(f"  Estimated time at {rate_limit}s rate limit: {time_str}")

    def _handle_backfill(self, scheduler, from_block: int, to_block: int, total_blocks: int, rate_limit: float) -> None:
        """Handle actual backfill execution."""
        self.stdout.write("")
        self.stdout.write(f"Starting backfill: {from_block} -> {to_block} ({total_blocks} blocks)")
        self.stdout.write(f"Rate limit: {rate_limit} seconds between blocks")

        if rate_limit > 0:
            estimated_seconds = total_blocks * rate_limit
            if estimated_seconds < 60:
                time_str = f"~{estimated_seconds:.0f} seconds"
            elif estimated_seconds < 3600:
                time_str = f"~{estimated_seconds / 60:.1f} minutes"
            else:
                time_str = f"~{estimated_seconds / 3600:.1f} hours"
            self.stdout.write(f"Estimated max time: {time_str}")

        self.stdout.write("")
        self.stdout.write("Press Ctrl+C to stop gracefully...")
        self.stdout.write("")

        scheduler.start()

        self.stdout.write(self.style.SUCCESS("Backfill completed"))
