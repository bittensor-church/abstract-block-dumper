import os

from celery import Celery
from celery.signals import worker_ready
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "example_project.settings")

app = Celery("example_project")

app.config_from_object(settings, namespace="CELERY")

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@worker_ready.connect
def on_worker_ready(**kwargs):
    """
    Load block tasks when Celery worker starts.

    This is required for abstract-block-dumper to register @block_task
    decorated functions so they can receive messages from the broker.
    """
    from abstract_block_dumper.v1.celery import setup_celery_tasks

    setup_celery_tasks()
