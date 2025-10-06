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


## Configuration

### Required Django Settings

Add these settings to your Django `settings.py`:

```python
# Celery Configuration
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# Abstract Block Dumper specific settings
BITTENSOR_NETWORK = 'finney'  # Options: 'finney', 'local', 'mainnet'
BLOCK_DUMPER_START_FROM_BLOCK = 'current'  # Options: None, 'current', or int
BLOCK_DUMPER_POLL_INTERVAL = 1  # seconds between polling for new blocks
BLOCK_TASK_RETRY_BACKOFF = 2  # minutes for retry backoff
BLOCK_DUMPER_MAX_ATTEMPTS = 3
```

### Configuration Options Explained

**BITTENSOR_NETWORK** (str) - [Bittensor network to connect](https://docs.learnbittensor.org/concepts/bittensor-networks) (default: 'finney')

**BLOCK_DUMPER_START_FROM_BLOCK** (str|int|None) - Starting block number:
- `None`: Resume from the last processed block in the database
- `'current'` (default): Start from the current block number
- `int`: Start from the specified block number

**BLOCK_DUMPER_POLL_INTERVAL** (int) - Seconds between polling for new blocks (default: 1)

**BLOCK_TASK_RETRY_BACKOFF** (int) - minutes for retry backoff

**BLOCK_DUMPER_MAX_ATTEMPTS** (int) - Max attempts to retry failed tasks


## Performance Features

### Caching
The package includes intelligent caching to improve performance:

- **Bittensor Client Caching**: The bittensor client is cached indefinitely since network configuration doesn't change during runtime
- **Active Netuids Caching**: Active netuids are cached for 5 minutes by default to reduce network calls

You can manually clear caches if needed:
```python
from abstract_block_dumper.utils import clear_caches
clear_caches()  # Clears both bittensor client and netuids caches
```

You can also customize the netuids cache duration:
```python
from abstract_block_dumper.utils import get_all_active_netuids
netuids = get_all_active_netuids(cache_duration=600)  # Cache for 10 minutes
```

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
