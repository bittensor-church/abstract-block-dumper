# Abstract Block Dumper
&nbsp;[![Continuous Integration](https://github.com/bactensor/abstract-block-dumper/workflows/Continuous%20Integration/badge.svg)](https://github.com/bactensor/abstract-block-dumper/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/abstract_block_dumper.svg?label=License)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/abstract_block_dumper.svg?label=python%20versions)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/abstract_block_dumper.svg?label=PyPI%20version)](https://pypi.python.org/pypi/abstract_block_dumper)

This package provides a simplified framework for creating block processing tasks in Django applications.
Define tasks with lambda conditions using the @block_task decorator and run them asynchronously with Celery.

## Usage

> [!IMPORTANT]
> This package uses [ApiVer](#versioning), make sure to import `abstract_block_dumper.v1`.


## Versioning

This package uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
TL;DR you are safe to use [compatible release version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release) `~=MAJOR.MINOR` in your `pyproject.toml` or `requirements.txt`.

Additionally, this package uses [ApiVer](https://www.youtube.com/watch?v=FgcoAKchPjk) to further reduce the risk of breaking changes.
This means, the public API of this package is explicitly versioned, e.g. `abstract_block_dumper.v1`, and will not change in a backwards-incompatible way even when `abstract_block_dumper.v2` is released.

Internal packages, i.e. prefixed by `abstract_block_dumper._` do not share these guarantees and may change in a backwards-incompatible way at any time even in patch releases.

## Implementation Details

### General Workflow:
Register functions -> detect new blocks -> evaluate conditions -> send to Celery -> execute -> track results -> handle retries.


### WorkflowSteps
1. Register
- Functions are automatically discovered when the scheduler starts
- Functions must be located in installed apps in tasks.py or block_tasks.py
- Functions marked with @block_task decorators are stored in memory registry

2. Detect Blocks
- Scheduler is running by management command `block_tasks_v1`
- Scheduler polls the latest chain head and, when requested by a task, the finalized head

3. Plan Tasks
- For each block, lambda conditions are evaluated against registered functions
- Tasks are created for matching conditions (with optional multiple argument sets)

4. Queue
Tasks are sent to Celery with queue and timeout settings from `celery_kwargs`. Backfill
submissions can be routed per task with the `backfill_queue` decorator argument.

5. Execute
Celery runs the function with block info, capturing results and errors

6. Track
Task attempts are stored in TaskAttempt model with retry logic and state tracking


## Prerequisites
- Django
- Celery
- Redis (for Celery broker and result backend)
- PostgreSQL (recommended for production)

## Installation

1. Install the package:
```bash
pip install abstract_block_dumper
```

2. Add to your Django `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    # ... other apps
    "abstract_block_dumper",
]
```

3. Run migrations:
```bash
python manage.py migrate
```

4. **Configure Celery to discover block tasks:**

In your project's `celery.py` file, add the following to ensure Celery workers can discover your `@block_task` decorated functions:

```python
from celery import Celery
from celery.signals import celeryd_init
from django.conf import settings

app = Celery("your_project")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@celeryd_init.connect
def on_worker_init(**kwargs) -> None:
    """Load block tasks when worker initializes."""
    from abstract_block_dumper.v1.celery import setup_celery_tasks

    setup_celery_tasks()
```

> **Important:** Without this step, Celery workers will not recognize your `@block_task` decorated functions, and you'll see "Received unregistered task" errors.

## Usage

### 1. Define Block Processing Tasks
Create block processing tasks in `tasks.py` or `block_tasks.py` file inside any of your installed Django apps.

### 2. Use Decorators to Register Tasks
- Use `@block_task` with lambda conditions to create custom block processing tasks

### 3. Start the Block Scheduler
Run the scheduler to start processing blocks:
```bash
$ python manage.py block_tasks_v1
```

This command will:
- Automatically discover and register all decorated functions
- Start polling the blockchain for new blocks
- Schedule tasks based on your lambda conditions

### 4. Start Celery Workers
In separate terminals, start Celery workers to execute tasks:
```bash
$ celery -A your_project worker --loglevel=info
```

When tasks declare `backfill_queue="block-backfill"`, use dedicated worker pools so live
work always retains execution capacity:

```bash
# Default/live queue only
$ celery -A your_project worker --loglevel=info --queues=celery --hostname=live@%h

# Backfill queue only
$ celery -A your_project worker --loglevel=info --queues=block-backfill --hostname=backfill@%h
```

Celery's default queue is named `celery` unless your application changes
`CELERY_TASK_DEFAULT_QUEUE`. Include every live queue named by a task's `celery_kwargs`
in the live worker's `--queues` list and every task-specific `backfill_queue` in the
corresponding backfill workers' lists.

See examples below:

Use the `@block_task` decorator with lambda conditions to create block processing tasks:

```python
from abstract_block_dumper.v1.decorators import block_task


# Process every block
@block_task
def process_every_block(block_number: int):
    print(f"Processing every block: {block_number}")


# Process every 10 blocks
@block_task(condition=lambda bn: bn % 10 == 0)
def process_every_10_blocks(block_number: int):
    print(f"Processing every 10 blocks: {block_number}")


# Process blocks only after finalization
@block_task(finalized=True)
def process_finalized_block(block_number: int):
    print(f"Processing finalized block: {block_number}")


# Process with multiple netuids
@block_task(
    condition=lambda bn, netuid: bn % 100 == 0,
    args=[{"netuid": 1}, {"netuid": 3}, {"netuid": 22}],
    backfilling_lookback=300,
    backfill_queue="high-priority-backfill",
    celery_kwargs={"queue": "high-priority"},
)
def process_multi_netuid_task(block_number: int, netuid: int):
    print(f"Processing block {block_number} for netuid: {netuid}")
```


## Backfilling Missed Blocks

Two mechanisms process blocks the live scheduler did not handle in real time.

### 1. Per-task lookback (`backfilling_lookback`)

Pass `backfilling_lookback=N` to `@block_task` to have the **live** scheduler keep the
trailing `N`-block window filled for that task. On every new head block `H`, in addition
to processing `H`, the scheduler (re)submits any block in `[H-N, H-1]` that this task has
**not** already completed and that is **not** currently in flight, still respecting the
task's `condition`. This self-heals gaps caused by scheduler downtime or skipped polls,
bounded to at most `N` blocks of catch-up per head.

```python
# On each new head, also (re)fill the previous 300 blocks for this task
@block_task(backfilling_lookback=300, backfill_queue="index-backfill")
def index_recent_blocks(block_number: int): ...
```

Only tasks that declare `backfilling_lookback` are backfilled this way. Already-succeeded
and in-flight (`PENDING`/`RUNNING`) blocks are skipped; failed blocks are retried subject
to the normal retry backoff. The whole mechanism can be disabled globally with
`BLOCK_DUMPER_LOOKBACK_ENABLED = False` (see [Configuration Options Reference](#configuration-options-reference)).
For tasks declared with `finalized=True`, the lookback window is anchored to the finalized
head instead of the latest chain head.

Set `backfill_queue` on a task to route its lookback submissions away from its normal
queue. The same queue is also used by the historical backfill command. Live submissions at
either head continue to use the queue from the task's `celery_kwargs`, or Celery's
default queue when none is declared. If `backfill_queue` is omitted, backfill and live
submissions use the same routing. Leading and trailing whitespace is stripped from a
configured `backfill_queue`; values that are empty after stripping, or are not strings,
raise `ValueError` when the task is decorated. Automatic retries preserve the queue used
for the original submission — it is recorded on the `TaskAttempt` row, so retries
re-dispatched by a worker and retries recovered from the database after a restart both
stay on it.

### 2. Historical backfill command (`backfill_blocks_v1`)

For large, one-off historical ranges, use the management command. By default it discovers
**gaps** (blocks missing from the database) in the range and backfills only those; pass
`--no-gap-detection` to process every block in the range.

```bash
# Backfill only the missing blocks (gaps) in a range
python manage.py backfill_blocks_v1 --from-block 1000000 --to-block 1100000

# Process the entire range, not just gaps
python manage.py backfill_blocks_v1 --from-block 1000000 --to-block 1100000 --no-gap-detection

# Preview what would be processed without executing anything
python manage.py backfill_blocks_v1 --from-block 1000000 --to-block 1100000 --dry-run

# Throttle submissions (seconds between blocks) and choose the network
python manage.py backfill_blocks_v1 --from-block 1000000 --to-block 1100000 --rate-limit 0.5 --network finney
```

If `--from-block` / `--to-block` are omitted, they default to the min / max block already
present in the database. Blocks older than ~300 from the current head are automatically
fetched via the archive network.

The historical command honors each task's `backfill_queue` annotation.


## Maintenance Tasks

### Cleanup Old Task Attempts

The framework provides a maintenance task to clean up old task records and maintain database performance:

```python
from abstract_block_dumper.v1.tasks import cleanup_old_tasks

# Delete tasks older than 7 days (default)
cleanup_old_tasks.delay()

# Delete tasks older than 30 days
cleanup_old_tasks.delay(days=30)
```

This task deletes all succeeded or unrecoverable failed tasks older than the specified number of days. It never deletes tasks with PENDING or RUNNING status to ensure ongoing work is preserved.

#### Running the Cleanup Task

**Option 1: Manual Execution**
```bash
# Using Django shell
python manage.py shell -c "from abstract_block_dumper.v1.tasks import cleanup_old_tasks; cleanup_old_tasks.delay()"
```

**Option 2: Cron Job (Recommended - once per day)**
```bash
# Add to crontab (daily at 2 AM)
0 2 * * * cd /path/to/your/project && python manage.py shell -c "from abstract_block_dumper.v1.tasks import cleanup_old_tasks; cleanup_old_tasks.delay()"
```

**Option 3: Celery Beat (Automated Scheduling)**

Add this to your Django `settings.py`:

```python
from celery.schedules import crontab

CELERY_BEAT_SCHEDULE = {
    "cleanup-old-tasks": {
        "task": "abstract_block_dumper.cleanup_old_tasks",
        "schedule": crontab(hour=2, minute=0),  # Daily at 2 AM
        "kwargs": {"days": 7},  # Customize retention period
    },
}
```

Then start the Celery beat scheduler:
```bash
celery -A your_project beat --loglevel=info
```

## Configuration

### Required Django Settings

Add these settings to your Django `settings.py`:

```python
# Celery Configuration
CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"

# Abstract Block Dumper specific settings
BITTENSOR_NETWORK = "finney"  # Options: 'finney', 'local', 'test', 'devnet', 'archive'
BLOCK_DUMPER_START_FROM_BLOCK = "current"  # Options: None, 'current', or int
BLOCK_DUMPER_POLL_INTERVAL = 1  # seconds between polling for new blocks
BLOCK_DUMPER_RPC_TIMEOUT = 30.0  # seconds before a stalled chain read is abandoned
BLOCK_DUMPER_LOOKBACK_ENABLED = True  # honor per-task backfilling_lookback (default True)
BLOCK_TASK_FAST_RETRY_ATTEMPTS = 2
BLOCK_TASK_FAST_RETRY_DELAY_SECONDS = 5
BLOCK_TASK_RETRY_BACKOFF_START_SECONDS = 60
BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER = 2
BLOCK_DUMPER_MAX_ATTEMPTS = 6  # includes the initial execution
BLOCK_TASK_MAX_RETRY_DELAY_MINUTES = 1440  # maximum retry delay (24 hours)
```

## Configuration Options Reference

### `BITTENSOR_NETWORK`
- **Type:** `str`
- **Default:** `'finney'`
- **Description:** Which [Bittensor network](https://docs.learnbittensor.org/concepts/bittensor-networks) the live scheduler (`block_tasks_v1`) connects to. The historical `backfill_blocks_v1` command overrides this per run via its `--network` flag.

```python
BITTENSOR_NETWORK = "finney"
```

---

### `BLOCK_DUMPER_START_FROM_BLOCK`
- **Type:** `str | int | None`
- **Default:** `None`
- **Valid Range:** `None`, `'current'`, or any positive integer
- **Description:** Determines the starting block for processing when the scheduler first runs
  - `None` → Resume from the last block stored in the database for the tasks reading the same chain head, or that head's current block if there is none
  - `'current'` → Start from the current block of the chain head each task reads (the finalized head for `finalized=True` tasks); skips historical blocks
  - Integer → Start from a specific block number (e.g., `1000000`), for every task

  Each chain head resolves its own starting point, so a `finalized=True` task never inherits the latest head's position — which would make it skip the blocks that were already produced but not yet finalized at start-up.

```python
BLOCK_DUMPER_START_FROM_BLOCK = "current"
```

> **Performance Impact:** Starting from historical blocks may require significant processing time

---

### `BLOCK_DUMPER_POLL_INTERVAL`
- **Type:** `int`
- **Default:** `5`
- **Valid Range:** `1` to `3600` (seconds)
- **Description:** Seconds to wait between checking for new blocks

```python
BLOCK_DUMPER_POLL_INTERVAL = 5
```

> **Performance Impact:**
> - Lower values (1-2s): Near real-time processing, higher CPU/network usage
> - Higher values (10-60s): Reduced load but delayed processing
> - Very low values (<1s): May cause rate limiting

---

### `BLOCK_DUMPER_RPC_TIMEOUT`
- **Type:** `float`
- **Default:** `30.0`
- **Description:** Seconds a raw RPC chain read (used for the finalized head) may take before it is abandoned and its connection dropped, so the next poll reconnects. The scheduler reads every chain head from a single thread, so without this bound a node that accepts a request and never answers would stall the other heads too.

```python
BLOCK_DUMPER_RPC_TIMEOUT = 30.0
```

> Set it above your node's slowest healthy response time; a value below that turns normal slowness into repeated reconnects.

---

### `BLOCK_DUMPER_LOOKBACK_ENABLED`
- **Type:** `bool`
- **Default:** `True`
- **Description:** Global kill-switch for per-task lookback backfilling in the live scheduler. When `True`, tasks that declare `backfilling_lookback=N` have their trailing `N`-block window backfilled on every head advance (see [Backfilling Missed Blocks](#backfilling-missed-blocks)). Set to `False` to disable this behavior for all tasks without changing task code.

```python
BLOCK_DUMPER_LOOKBACK_ENABLED = True
```

---

### `BLOCK_DUMPER_MAX_ATTEMPTS`
- **Type:** `int`
- **Default:** `3`
- **Valid Range:** `1` to `10`
- **Description:** Maximum total executions, including the initial execution, before giving up

```python
BLOCK_DUMPER_MAX_ATTEMPTS = 5
```

> **Performance Impact:** Higher values increase resilience but may delay failure detection

---

### Staged retry settings

Defining any one of the following settings enables staged retry behavior. Unset staged
settings use the defaults shown below.

```python
BLOCK_TASK_FAST_RETRY_ATTEMPTS = 2
BLOCK_TASK_FAST_RETRY_DELAY_SECONDS = 5
BLOCK_TASK_RETRY_BACKOFF_START_SECONDS = 60
BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER = 2
```

This configuration produces delays of 5 seconds, 5 seconds, 1 minute, 2 minutes, 4
minutes, and so on. `BLOCK_DUMPER_MAX_ATTEMPTS` includes the initial execution, so it must
be at least `FAST_RETRY_ATTEMPTS + 2` for the first exponential retry to run.

For retry number `n`, fast-retry count `F`, fast delay `I`, backoff starting delay `B`,
multiplier `R`, and maximum delay `C`, the delay is:

- `min(C, I)` when `n <= F`
- `min(C, max(I, B * R ** (n - F - 1)))` when `n > F`

#### `BLOCK_TASK_FAST_RETRY_ATTEMPTS`

- **Type:** `int`
- **Default:** `2` in staged mode
- **Valid Range:** Any non-negative integer
- **Description:** Number of retries that use the constant fast-retry delay

#### `BLOCK_TASK_FAST_RETRY_DELAY_SECONDS`

- **Type:** `int | float`
- **Default:** `5` in staged mode
- **Valid Range:** Any non-negative number of seconds
- **Description:** Delay used for every fast retry

#### `BLOCK_TASK_RETRY_BACKOFF_START_SECONDS`

- **Type:** `int | float`
- **Default:** `60` in staged mode
- **Valid Range:** Any non-negative number of seconds
- **Description:** Exponential delay used for the first retry after the fast-retry phase

#### `BLOCK_TASK_RETRY_BACKOFF_MULTIPLIER`

- **Type:** `int | float`
- **Default:** `2` in staged mode
- **Valid Range:** `1` or greater
- **Description:** Dimensionless multiplier applied to each successive exponential retry

The transition uses at least the fast-retry delay, and the multiplier cannot be less than
1, guaranteeing that retry delays never decrease.

> **Performance Impact:** More fast retries recover quickly from brief connectivity issues,
> but may add load while a dependency is unavailable.

---

### `BLOCK_TASK_RETRY_BACKOFF` (legacy)

- **Type:** `int`
- **Default:** `1`
- **Valid Range:** `1` to `60` (minutes)
- **Description:** Backward-compatible exponential base used only when none of the staged
  retry settings above is defined
- **Calculation:** Delay = `BLOCK_TASK_RETRY_BACKOFF ** attempt_count` minutes

An existing project with only this setting keeps exactly the previous behavior:

```python
BLOCK_TASK_RETRY_BACKOFF = 2  # delays remain 2, 4, 8, ... minutes
```

**Performance Impact:** Lower values retry faster but may overwhelm failing services

Internally, a legacy value `B` is equivalent to zero fast retries, a backoff starting delay
of `B` minutes, and a multiplier of `B`. If any staged setting is defined, staged mode takes
precedence and this legacy setting is ignored.
---

### `BLOCK_TASK_MAX_RETRY_DELAY_MINUTES`
- **Type:** `int`
- **Default:** `1440` (24 hours)
- **Valid Range:** `1` to `10080` (1 minute to 1 week)
- **Description:** Maximum delay (in minutes) between retry attempts, caps exponential backoff

```python
BLOCK_TASK_MAX_RETRY_DELAY_MINUTES = 720  # 12 hours max
```

> **Performance Impact:** Prevents extremely long delays while maintaining backoff benefits


## Example Project

The repository includes a complete working example in the `example_project/` directory that demonstrates:

- Django application setup with abstract-block-dumper
- Multiple task types defined with `@block_task` (every-block, conditional, multi-netuid,
  `backfilling_lookback`, and task-specific `backfill_queue` routing)
- Error handling with a randomly failing task
- Docker Compose setup with all required services
- Monitoring with Flower (Celery monitoring tool)

### Running the Example

```bash
cd example_project
docker-compose up --build
```

This starts:
- **Django application** (http://localhost:8000) - Admin interface (user: `admin`, password: `admin`)
- **Celery workers** - Execute block processing tasks
- **Block scheduler** - Monitors blockchain and schedules tasks
- **Flower monitoring** (http://localhost:5555) - Monitor Celery tasks
- **Redis & PostgreSQL** - Required services


## Development


Pre-requisites:
- [uv](https://docs.astral.sh/uv/)
- [nox](https://nox.thea.codes/en/stable/)
- [docker](https://www.docker.com/) and [docker compose plugin](https://docs.docker.com/compose/)


Ideally, you should run `nox -t format lint` before every commit to ensure that the code is properly formatted and linted.
Before submitting a PR, make sure that tests pass as well, you can do so using:
```
nox -t check # equivalent to `nox -t format lint test`
```

If you wish to install dependencies into `.venv` so your IDE can pick them up, you can do so using:
```
uv sync --all-extras --dev
```

### Release process

Run `nox -s make_release -- X.Y.Z` where `X.Y.Z` is the version you're releasing and follow the printed instructions.
