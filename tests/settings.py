"""
Django settings used in tests.
"""
# cookiecutter-rt-pkg macro: requires cookiecutter.is_django_package

DEBUG = True
SECRET_KEY = "DUMMY"

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "abstract_block_dumper",
]

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

# Abstract Block Dumper specific settings
# ----------------------------------
BITTENSOR_NETWORK = "finney"  # or 'local', 'mainnet',
BLOCK_DUMPER_POLL_INTERVAL = 1  # seconds - ultra-fast polling for real-time processing
BLOCK_DUMPER_MAX_BLOCKS_BEHIND = 1  # process only 1 block per cycle - no jumps, real-time only
BLOCK_DUMPER_TASK_TIMEOUT = 300  # seconds
BLOCK_DUMPER_START_FROM_BLOCK = "current"  # None = resume from DB, 'current' = current block, or block number
# -----------------------------------
