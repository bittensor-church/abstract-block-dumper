# Abstract Block Dumper
&nbsp;[![Continuous Integration](https://github.com/bactensor/abstract-block-dumper/workflows/Continuous%20Integration/badge.svg)](https://github.com/bactensor/abstract-block-dumper/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/abstract_block_dumper.svg?label=License)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/abstract_block_dumper.svg?label=python%20versions)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/abstract_block_dumper.svg?label=PyPI%20version)](https://pypi.python.org/pypi/abstract_block_dumper)

This package provides a simplified framework for creating block processing tasks in Django applications.
Define tasks with lambda conditions using the @block_task decorator and run them asynchronously with Celery.

## Implementation Details

### General Workflow:
Register functions -> detect new blocks -> evaluate conditions -> send to Celery -> execute -> track results -> handle retries.


### WorkflowSteps
1. Register
- Functions are automatically discovered when the scheduler starts
- Functions must be located in installed apps in tasks.py or block_tasks.py
- Functions marked with @block_task decorators are stored in memory registry

2. Detect Blocks
- Scheduler is running by management command block_tasks
- Scheduler polls blockchain, finds new blocks, and batches them

3. Plan Tasks
- For each block, lambda conditions are evaluated against registered functions
- Tasks are created for matching conditions (with optional multiple argument sets)

4. Queue
Tasks are sent to Celery with queue and timeout settings from celery_kwargs

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
    'abstract_block_dumper',
]
```

3. Run migrations:
```bash
python manage.py migrate
```

## Usage

### 1. Define Block Processing Tasks
Create block processing tasks in `tasks.py` or `block_tasks.py` file inside any of your installed Django apps.

### 2. Use Decorators to Register Tasks
- Use `@block_task` with lambda conditions to create custom block processing tasks

### 3. Start the Block Scheduler
Run the scheduler to start processing blocks:
```bash
$ python manage.py block_tasks
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

See examples below:

Use the `@block_task` decorator with lambda conditions to create block processing tasks:

```python
from abstract_block_dumper.decorators import block_task


# Process every block
@block_task(condition=lambda bn: True)
def process_every_block(block_number: int):
    print(f"Processing every block: {block_number}")

# Process every 10 blocks
@block_task(condition=lambda bn: bn % 10 == 0)
def process_every_10_blocks(block_number: int):
    print(f"Processing every 10 blocks: {block_number}")

# Process with multiple netuids
@block_task(
    condition=lambda bn, netuid: bn % 100 == 0,
    args=[{"netuid": 1}, {"netuid": 3}, {"netuid": 22}],
    backfilling_lookback=300,
    celery_kwargs={"queue": "high-priority"}
)
def process_multi_netuid_task(block_number: int, netuid: int):
    print(f"Processing block {block_number} for netuid: {netuid}")
```


## Maintenance Tasks

### Cleanup Old Task Attempts

The framework provides a maintenance task to clean up old task records and maintain database performance:

```python
from abstract_block_dumper.tasks import cleanup_old_tasks

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
python manage.py shell -c "from abstract_block_dumper.tasks import cleanup_old_tasks; cleanup_old_tasks.delay()"
```

**Option 2: Cron Job (Recommended - once per day)**
```bash
# Add to crontab (daily at 2 AM)
0 2 * * * cd /path/to/your/project && python manage.py shell -c "from abstract_block_dumper.tasks import cleanup_old_tasks; cleanup_old_tasks.delay()"
```

**Option 3: Celery Beat (Automated Scheduling)**

Add this to your Django `settings.py`:

```python
from celery.schedules import crontab

CELERY_BEAT_SCHEDULE = {
    'cleanup-old-tasks': {
        'task': 'abstract_block_dumper.cleanup_old_tasks',
        'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
        'kwargs': {'days': 7},  # Customize retention period
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
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# Abstract Block Dumper specific settings
BITTENSOR_NETWORK = 'finney'  # Options: 'finney', 'local', 'testnet', 'mainnet'
BLOCK_DUMPER_START_FROM_BLOCK = 'current'  # Options: None, 'current', or int
BLOCK_DUMPER_POLL_INTERVAL = 1  # seconds between polling for new blocks
BLOCK_TASK_RETRY_BACKOFF = 2  # minutes for retry backoff base
BLOCK_DUMPER_MAX_ATTEMPTS = 3  # maximum retry attempts
BLOCK_TASK_MAX_RETRY_DELAY_MINUTES = 1440  # maximum retry delay (24 hours)
```

### Configuration Options Reference

#### Core Settings

**BITTENSOR_NETWORK** (str, default: `'finney'`) Specifies which [Bittensor network](https://docs.learnbittensor.org/concepts/bittensor-networks) to connect to

**BLOCK_DUMPER_START_FROM_BLOCK** (str|int|None, default: `None`)
- **Purpose**: Determines the starting block for processing when the scheduler first runs
- **Valid Values**:
  - `None`: Resume from the last processed block stored in database
  - `'current'`: Start from the current blockchain block (skips historical blocks)
  - `int`: Start from a specific block number (e.g., `1000000`)
- **Example**: `BLOCK_DUMPER_START_FROM_BLOCK = 'current'`
- **Performance Impact**: Starting from historical blocks may require significant processing time

#### Scheduler Settings

**BLOCK_DUMPER_POLL_INTERVAL** (int, default: `1`)
- **Purpose**: Seconds to wait between checking for new blocks
- **Valid Range**: `1` to `3600` (1 second to 1 hour)
- **Example**: `BLOCK_DUMPER_POLL_INTERVAL = 5`
- **Performance Impact**:
  - Lower values (1-2s): Near real-time processing, higher CPU/network usage
  - Higher values (10-60s): Reduced load but delayed processing
  - Very low values (<1s) may cause rate limiting

#### Retry and Error Handling Settings

**BLOCK_DUMPER_MAX_ATTEMPTS** (int, default: `3`)
- **Purpose**: Maximum number of attempts to retry a failed task before giving up
- **Valid Range**: `1` to `10`
- **Example**: `BLOCK_DUMPER_MAX_ATTEMPTS = 5`
- **Performance Impact**: Higher values increase resilience but may delay failure detection

**BLOCK_TASK_RETRY_BACKOFF** (int, default: `1`)
- **Purpose**: Base number of minutes for exponential backoff retry delays
- **Valid Range**: `1` to `60`
- **Example**: `BLOCK_TASK_RETRY_BACKOFF = 2`
- **Calculation**: Actual delay = `backoff ** attempt_count` minutes
  - Attempt 1: 2¹ = 2 minutes
  - Attempt 2: 2² = 4 minutes  
  - Attempt 3: 2³ = 8 minutes
- **Performance Impact**: Lower values retry faster but may overwhelm failing services

**BLOCK_TASK_MAX_RETRY_DELAY_MINUTES** (int, default: `1440`)
- **Purpose**: Maximum delay (in minutes) between retry attempts, caps exponential backoff
- **Valid Range**: `1` to `10080` (1 minute to 1 week)
- **Example**: `BLOCK_TASK_MAX_RETRY_DELAY_MINUTES = 720`  # 12 hours max
- **Performance Impact**: Prevents extremely long delays while maintaining backoff benefits


## Example Project

The repository includes a complete working example in the `example_project/` directory that demonstrates:

- Django application setup with abstract-block-dumper
- Multiple task types (`@every_block`, `@every_n_blocks` with different configurations)
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
