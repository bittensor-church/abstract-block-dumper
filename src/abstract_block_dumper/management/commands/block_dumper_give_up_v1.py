import structlog
from django.core.management.base import BaseCommand, CommandError, CommandParser

import abstract_block_dumper._internal.dal.attempt_transitions as transitions
from abstract_block_dumper._internal.dal.memory_registry import task_registry
from abstract_block_dumper._internal.discovery import ensure_modules_loaded
from abstract_block_dumper.models import TaskAttempt

logger = structlog.get_logger(__name__)


class Command(BaseCommand):
    help = (
        "Stop retrying the attempts of one BlockDumper task: PENDING and FAILED attempts that could still run "
        "are marked FAILED with give_up_reason 'manual'. RUNNING attempts are left alone."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--task",
            required=True,
            help="Executable path of the task (module.function), or its function name if that is unique.",
        )
        parser.add_argument("--from-block", type=int, help="Lowest block number to give up (inclusive).")
        parser.add_argument("--to-block", type=int, help="Highest block number to give up (inclusive).")
        parser.add_argument("--dry-run", action="store_true", help="Report how many attempts would be given up.")

    def handle(self, *args, **options) -> None:
        executable_path = self._resolve_task(options["task"])
        ensure_modules_loaded()
        registry_item = task_registry.get_by_executable_path(executable_path)
        if registry_item is None or registry_item.live_queue is None:
            msg = f"{executable_path} is not a BlockDumper task; this command only gives up BlockDumper attempts"
            raise CommandError(msg)
        attempts = TaskAttempt.objects.filter(executable_path=executable_path)
        if options["from_block"] is not None:
            attempts = attempts.filter(block_number__gte=options["from_block"])
        if options["to_block"] is not None:
            attempts = attempts.filter(block_number__lte=options["to_block"])

        if options["dry_run"]:
            count = transitions.could_still_run(attempts).count()
            self.stdout.write(f"Would give up {count} attempt(s) of {executable_path}.")
            return

        count = transitions.give_up_by_hand(attempts)
        if count:
            logger.warning(
                "Task attempts given up by hand",
                executable_path=executable_path,
                attempts=count,
                from_block=options["from_block"],
                to_block=options["to_block"],
            )
        self.stdout.write(self.style.SUCCESS(f"Gave up {count} attempt(s) of {executable_path}."))

    @staticmethod
    def _resolve_task(task: str) -> str:
        if "." in task:
            return task
        paths = sorted(
            {
                path
                for path in TaskAttempt.objects.values_list("executable_path", flat=True).distinct()
                if path.rsplit(".", 1)[-1] == task
            }
        )
        if len(paths) != 1:
            msg = (
                f"Task name {task!r} matches {len(paths)} recorded tasks{': ' + ', '.join(paths) if paths else ''}; "
                "pass the full executable path"
            )
            raise CommandError(msg)
        return paths[0]
