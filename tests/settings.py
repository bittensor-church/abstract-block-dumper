"""
Django settings used in tests.
"""
# cookiecutter-rt-pkg macro: requires cookiecutter.is_django_package

import os

DEBUG = True
SECRET_KEY = "DUMMY"

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "abstract_block_dumper",
]

# Use PostgreSQL if DATABASE_URL is provided, otherwise SQLite
if os.environ.get("COMPOSE_DB"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "block_dumper",
            "USER": "block_dumper",
            "PASSWORD": "block_dumper123",
            "HOST": "postgres",
            "PORT": "5432",
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    }

ROOT_URLCONF = __name__
urlpatterns = []  # type: ignore

# Celery settings for testing
CELERY_TASK_ALWAYS_EAGER = True
CELERY_TASK_EAGER_PROPAGATES = True
CELERY_BROKER_URL = "memory://"
CELERY_RESULT_BACKEND = "cache+memory://"

# Abstract Block Dumper specific settings
# ----------------------------------
BITTENSOR_NETWORK = "finney"  # or 'local', 'mainnet',
BLOCK_DUMPER_POLL_INTERVAL = 1  # seconds - ultra-fast polling for real-time processing
BLOCK_DUMPER_START_FROM_BLOCK = "current"  # None = resume from DB, 'current' = current block, or block number
BLOCK_TASK_RETRY_BACKOFF = 2
BLOCK_DUMPER_MAX_ATTEMPTS = 3
# -----------------------------------
