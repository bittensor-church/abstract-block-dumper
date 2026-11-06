import structlog
from django.core.management.base import BaseCommand, CommandError

from abstract_block_dumper._internal.checks import warn_about_dumpers_unseen_by_checks
from abstract_block_dumper._internal.contacts.broker import BrokerUnreachableError, QueueArgumentsMismatchError
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper._internal.exceptions import QueueConfigurationError
from abstract_block_dumper._internal.services.celery_config import prepare_block_dumpers, queue_setup_error_message
from abstract_block_dumper._internal.services.scheduler import task_scheduler_factory

logger = structlog.get_logger(__name__)


class Command(BaseCommand):
    help = "Run the block scheduler daemon."

    def handle(self, *args, **options) -> None:
        """
        Handle the management command to start the block scheduler.
        """
        self.stdout.write("Syncing decorated functions...")
        ensure_modules_loaded()
        functions_counter = len(task_registry.get_functions())
        self.stdout.write(self.style.SUCCESS(f"Synced {functions_counter} functions"))
        warn_about_dumpers_unseen_by_checks()
        try:
            prepare_block_dumpers(verify_broker=True)
        except (QueueArgumentsMismatchError, QueueConfigurationError) as exc:
            raise CommandError(queue_setup_error_message(exc)) from exc
        except BrokerUnreachableError as exc:
            # Every queue is in the Celery queue table by now, so sends declare it with its arguments.
            logger.warning(
                "Broker unreachable; block scheduler starts without checking the BlockDumper queues on it",
                celery_app=exc.app_name,
                error=exc.detail,
            )

        scheduler = task_scheduler_factory()
        self.stdout.write("Starting block scheduler...")
        scheduler.start()
