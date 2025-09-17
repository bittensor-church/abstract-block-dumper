# Abstract Block Dumper
&nbsp;[![Continuous Integration](https://github.com/bactensor/abstract-block-dumper/workflows/Continuous%20Integration/badge.svg)](https://github.com/bactensor/abstract-block-dumper/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/abstract_block_dumper.svg?label=License)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/abstract_block_dumper.svg?label=python%20versions)](https://pypi.python.org/pypi/abstract_block_dumper)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/abstract_block_dumper.svg?label=PyPI%20version)](https://pypi.python.org/pypi/abstract_block_dumper)

This package provides a framework for creating and managing block processing tasks in a Django application.
It allows developers to define tasks that process blockchain blocks using decorators and run them asynchronously using Celery.

## Prerequisites
- Celery


## Usage

1. Install the package:
```bash
pip install abstract_block_dumper
```

2. Define block processing tasks in tasks.py or block_tasks.py file inside any of your installed Django apps.

3. Use decorators to register tasks.
    - Use `@block_task` to create a custom block processing task.
    - Use shortcuts like `@every_block`, `@every_n_blocks`, or `@on_epoch` for common patterns.

4. Run scheduler using Django management command:
```bash
$ python manage.py block_dumper --sync-functions
```

See examples below:

Main decorator to create a block processing task is `@block_task`, example usage:

```python
from abstract_block_dumper.decorators import block_task


@block_task(
    name="example_dumper",
    description="An example block dumper",

)
def block_task(block_number: int):
    # Your block processing logic here
    print(f"Processing block {block_number}")
```

Shortcuts are available to register the task directly:

```python
from abstract_block_dumper.shortcuts import every_block, every_n_blocks, on_epoch

@every_block
def process_every_block(block_number: int):
    print(f"Processing every block: {block_number}")

@every_n_blocks(n=10)
def process_every_10_blocks(block_number: int):
    print(f"Processing every 10 blocks: {block_number}")

@on_epoch(
    position=EpochPosition.START,
)
def process_on_epoch(block_number: int):
    print(f"Processing on epoch: {block_number}")
```


## Configuration:

Required settings configs:


# Abstract Block Dumper specific settings

BITTENSOR_NETWORK (str) - network to connect to, one of:
- `finney`
- `local`
- `mainnet`
Default: `'finney'`

BLOCK_DUMPER_START_FROM_BLOCK (str|int|None) - starting block number, can be:
- `None`: resume from the last processed block in the database
- `'current'`: start from the current block number
- `int`: start from the specified block number
Default: `'current'`

BLOCK_DUMPER_POLL_INTERVAL (int) - seconds between polling for new blocks, default is 1 second for real-time processing
BLOCK_DUMPER_MAX_BLOCKS_BEHIND (int) - maximum number of blocks to process in one cycle, default is 1 for real-time processing
BLOCK_DUMPER_TASK_TIMEOUT (int) - timeout in seconds for each block processing task, default is 300 seconds

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


## Versioning

This package uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
TL;DR you are safe to use [compatible release version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release) `~=MAJOR.MINOR` in your `pyproject.toml` or `requirements.txt`.

Additionally, this package uses [ApiVer](https://www.youtube.com/watch?v=FgcoAKchPjk) to further reduce the risk of breaking changes.
This means, the public API of this package is explicitly versioned, e.g. `abstract_block_dumper.v1`, and will not change in a backwards-incompatible way even when `abstract_block_dumper.v2` is released.

Internal packages, i.e. prefixed by `abstract_block_dumper._` do not share these guarantees and may change in a backwards-incompatible way at any time even in patch releases.


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
