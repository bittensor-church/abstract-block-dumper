from django.core.management.base import BaseCommand

from abstract_block_dumper.dal.memory_registry import task_registry
from abstract_block_dumper.discovery import ensure_modules_loaded
from abstract_block_dumper.services.scheduler import task_scheduler_factory


class Command(BaseCommand):
    help = "Run the block scheduler daemon."

    def handle(self, *args, **options) -> None:
        self.stdout.write("Syncing decorated functions...")
        ensure_modules_loaded()
        functions_counter = len(task_registry.get_functions())
        self.stdout.write(self.style.SUCCESS(f"Synced {functions_counter} functions"))

        scheduler = task_scheduler_factory()
        self.stdout.write("Starting block scheduler...")
        scheduler.start()
