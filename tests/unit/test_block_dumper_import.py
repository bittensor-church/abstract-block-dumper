import subprocess
import sys

DECLARE_A_DUMPER_BEFORE_DJANGO_LOADS = """
import sys
from celery import Celery
from abstract_block_dumper.v1.dumper import BlockDumper, Queue

BlockDumper(celery_app=Celery("project", broker="memory://"))
Queue("events", time_limit=120)
print(sorted(module for module in ("abstract_block_dumper.models", "bittensor") if module in sys.modules))
"""


def test_block_dumper_can_be_created_while_django_loads_its_settings():
    """A project creates its BlockDumper next to the Celery app, which Django imports before its apps are ready."""
    result = subprocess.run(
        [sys.executable, "-B", "-c", DECLARE_A_DUMPER_BEFORE_DJANGO_LOADS],
        capture_output=True,
        text=True,
        check=True,
        env={"PATH": "", "PYTHONDONTWRITEBYTECODE": "1"},
    )

    assert result.stdout == "[]\n"


IMPORT_THE_WORKER_SETUP_BEFORE_DJANGO_LOADS = """
import sys
import abstract_block_dumper.v1.celery

print(sorted(module for module in ("abstract_block_dumper.models", "bittensor") if module in sys.modules))
"""


def test_worker_setup_can_be_imported_while_django_loads_its_settings():
    """A project's Celery module may import `setup_celery_tasks` at the top, before Django's apps are ready."""
    result = subprocess.run(
        [sys.executable, "-B", "-c", IMPORT_THE_WORKER_SETUP_BEFORE_DJANGO_LOADS],
        capture_output=True,
        text=True,
        check=True,
        env={"PATH": "", "PYTHONDONTWRITEBYTECODE": "1"},
    )

    assert result.stdout == "[]\n"
