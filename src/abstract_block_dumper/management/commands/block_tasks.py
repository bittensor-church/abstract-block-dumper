from django.core.management.base import BaseCommand

from abstract_block_dumper.discovery import ensure_modules_loaded
from abstract_block_dumper.memory_registry import MemoryRegistry
from abstract_block_dumper.scheduler import task_scheduler_factory


class Command(BaseCommand):
    help = "Run the block scheduler daemon."

    def handle(self, *args, **options) -> None:
        self.stdout.write("Syncing decorated functions...")
        ensure_modules_loaded()
        functions_counter = len(MemoryRegistry.get_functions())
        self.stdout.write(self.style.SUCCESS(f"Synced {functions_counter} functions"))

        scheduler = task_scheduler_factory()
        self.stdout.write("Starting block scheduler...")
        scheduler.start()
