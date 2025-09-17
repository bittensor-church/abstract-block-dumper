from django.core.management.base import BaseCommand, CommandParser

from abstract_block_dumper.discovery import sync_block_task_functions
from abstract_block_dumper.scheduler import BlockScheduler


class Command(BaseCommand):
    help = "Run the block scheduler daemon."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scheduler = None

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--sync-functions",
            action="store_true",
            help="Sync block dumper functions before starting the scheduler.",
        )

    def handle(self, *args, **options) -> None:
        if options["sync_functions"]:
            self.stdout.write("Syncing decorated functions...")
            count = sync_block_task_functions()
            self.stdout.write(self.style.SUCCESS(f"Synced {count} functions"))

        self.scheduler = BlockScheduler()
        self.stdout.write("Starting block scheduler...")
        self.scheduler.start()
