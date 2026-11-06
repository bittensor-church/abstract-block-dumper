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

Celery task names are derived from the decorated function's module and name, so renaming or
moving a task function is a breaking change for your own deployment: queued messages and
existing `TaskAttempt` rows no longer match it.

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
- RabbitMQ as the broker, instead of Redis, to use [`BlockDumper`](#queue-policies-with-blockdumper)

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


### Task function contract

- `@block_task` and `@block_task()` are equivalent. The decorator returns the Celery task, so
  `.delay()` / `.apply_async()` work for manual runs.
- The function is called as `func(block_number=..., **args)`; the library passes the block
  number, not block data, and the task reads what it needs from the chain.
- `condition` receives the same keyword arguments. A non-callable `condition` raises
  `TypeError` at import time; a condition that raises is logged and skips only that task and
  args set for that block.
- Each dict in `args` is a separate unit of work with its own `TaskAttempt`, status and retry
  counter. Argument dicts must be JSON-serializable.
- `celery_kwargs` apply both to the task declaration and to every submission; `args`,
  `kwargs` and `eta` in it are ignored at submission because the library sets them.
- A function that declares `**kwargs` also receives `_use_archive_network`, which is `True`
  for blocks more than 300 behind the chain head.
- The return value is stored in `TaskAttempt.execution_result`, so it must be
  JSON-serializable.

## Queue policies with `BlockDumper`

`BlockDumper` binds block tasks to queues that carry a policy, so the queue, its Celery
settings and the library's recovery rules come from one object instead of being repeated in
`celery_kwargs`, in Django settings and on the broker. The rules in this section apply to
`BlockDumper` tasks only; tasks declared with `@block_task` keep working as described in the
rest of this README.

### Requirements

- **RabbitMQ.** The queues rely on RabbitMQ's per-queue message TTL, its consumer counts and
  its `consumer_timeout`. With any other broker, `block_tasks_v1`, `backfill_blocks_v1`,
  `block_dumper_queues_v1` and workers stop with an error, and `manage.py check` reports
  `abstract_block_dumper.W004`. `memory://` is accepted for tests (see
  [Testing a project that uses `BlockDumper`](#testing-a-project-that-uses-blockdumper)).
- **Celery's prefork pool** (the default). Recovery relies on the pool killing a task at its
  hard time limit. The `threads`, `solo` and `eventlet` pools enforce no time limits, so on
  them a task that outlasts its lease is retried while it still runs.
- **Workers without `--time-limit` and `--soft-time-limit`.** Set limits on the queues, in
  `celery_kwargs` or with `CELERY_TASK_TIME_LIMIT` and `CELERY_TASK_SOFT_TIME_LIMIT` (see
  [Workers that die mid-task](#workers-that-die-mid-task)).
- **The dumper is created next to the Celery app**, in the module Django imports at startup
  (usually `your_project/celery.py`), so that `manage.py check` sees it. A dumper created in a
  task module (`tasks.py` or `block_tasks.py`) works, but the checks miss it, and
  `block_tasks_v1` and workers log a WARNING about it.

### Declaring tasks

```python
# your_project/celery.py
from celery import Celery
from celery.signals import celeryd_init

from abstract_block_dumper.v1.dumper import BlockDumper, Queue, RetryPolicy

app = Celery("your_project")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

dumper = BlockDumper(celery_app=app, block_time_seconds=12)

EVENTS_LIVE = Queue(
    "events-live",
    soft_time_limit=100,
    time_limit=120,
    message_ttl=60,
    retry=RetryPolicy(max_attempts=None, give_up_after_blocks=300),
)
EVENTS_BACKFILL = Queue(
    "events-backfill",
    soft_time_limit=100,
    time_limit=120,
    message_ttl=300,
    throttle=600,
    retry=RetryPolicy(max_attempts=None, give_up_after_blocks=7200),
)
# A rarely used task can keep live and backfill work on one queue.
MAINTENANCE = Queue("maintenance", soft_time_limit=540, time_limit=600, message_ttl=900, throttle=20)


@celeryd_init.connect
def on_worker_init(**kwargs) -> None:
    from abstract_block_dumper.v1.celery import setup_celery_tasks

    setup_celery_tasks()
```

```python
# your_app/block_tasks.py
from your_project.celery import EVENTS_BACKFILL, EVENTS_LIVE, MAINTENANCE, dumper


@dumper.block_task(live_queue=EVENTS_LIVE, backfill_queue=EVENTS_BACKFILL, backfilling_lookback=300)
def index_events(block_number: int): ...


@dumper.block_task(live_queue=MAINTENANCE, backfill_queue=MAINTENANCE, condition=lambda bn: bn % 100 == 0)
def record_chain_stats(block_number: int): ...
```

`dumper.block_task` takes the same arguments as `@block_task` except `backfill_queue`, which
is a `Queue` here, plus:

- `live_queue` (required): submissions of new head blocks go to this queue.
- `backfill_queue`: lookback and `backfill_blocks_v1` submissions go to this queue. It may be
  the same object as `live_queue`; without it backfills use `live_queue`.
  `backfilling_lookback` requires it.

A manual `.delay()` of a `BlockDumper` task starts nothing for a block without a recorded
attempt: the worker finds no `TaskAttempt` and raises `CeleryTaskLockedError`. To run the tasks
of one block by hand, use
`python manage.py backfill_blocks_v1 --from-block N --to-block N --no-gap-detection`. Without
`--no-gap-detection` the command reports "No gaps found" as soon as any task succeeded for the
block.

Every message carries the policy of the queue it is sent to: its `soft_time_limit` and
`time_limit` are passed to `apply_async` and override the same keys in `celery_kwargs`. A
limit the queue does not set comes from `celery_kwargs` or Celery's own settings, never from
another queue of the task; a hard limit found in neither is the
[default hard limit](#workers-that-die-mid-task). Declaring a task raises `ValueError` when,
on one of its queues or for messages without queue limits, its soft limit is not below its hard
limit; the message names both limits and where each is set.

`message_ttl` is how many seconds a message may wait before it is no longer worth running,
counted from its send time or, for a retry, from its ETA. The message carries a Celery
`expires`, so a worker discards it unexecuted even after prefetching it, and the queue is
declared with `x-message-ttl`, so RabbitMQ drops it while it waits. The live scheduler then
sends the attempt again, without using up an attempt (see
[Expired messages](#expired-messages)). Choose a TTL longer than messages legitimately wait
in the queue: workers count each discarded message in
`block_dumper_messages_expired_total{queue}`, and a rising count means the TTL is too short.

`throttle` caps how many attempts of the queue may be in flight (sent and not finished:
waiting in the broker, prefetched by a worker, or running) while backfills submit more. The
count comes from the database, so it needs no queue length limit on the broker.
`backfill_blocks_v1` waits for room, checking every `BLOCK_DUMPER_THROTTLE_POLL_SECONDS`;
lookback submits what fits and leaves the rest of its window for the next head. Live
submissions never wait, but they count: on a queue shared by live and backfill work,
backfills only get the room live work leaves. Lost attempts do not count, so they cannot hold
the room until the sweep recovers them. Retries and the sweep's re-sends never wait for room,
and expired or lost attempts stop counting, so once workers come back the attempts in flight
can briefly exceed the throttle. The gauge `block_dumper_inflight_tasks{queue}`,
refreshed on every reconciliation sweep and whenever a backfill or lookback reads the count,
and the counter `block_dumper_throttled_total{queue,mode}` show the throttle at work.

`retry` is the queue's `RetryPolicy`: when failed attempts are retried and when they are
given up. An attempt is given up at the first limit it reaches:

- `max_attempts` (default `3`, the first execution included);
- `give_up_after_blocks`: once the block is about that many blocks behind the chain head.
  This is **approximate**. When an attempt is first sent, the library turns the blocks left
  into a deadline in time using `BlockDumper(block_time_seconds=...)` (default `12`), and
  compares against that deadline from then on. A slower or stalled chain reaches the deadline
  after fewer blocks, a faster one after more. A block that is already too old when it is
  submitted, for example by a backfill, is given up without being sent.

With both set to `None` an attempt is retried until it succeeds. The delay before each retry
follows `fast_retry_attempts` (default `2`) retries of `fast_retry_delay` (default `5` s),
then `backoff_start` (default `60` s) growing by `backoff_multiplier` (default `2`), capped at
`max_delay` (default `1200` s). A retry waits for its ETA inside the worker without being
acknowledged, so keep `max_delay` below RabbitMQ's `consumer_timeout` (30 minutes by default)
or raise that timeout. The policy is shared by every task and role of the queue: on a queue
serving live and backfill work, `give_up_after_blocks` also gives up backfills of older
blocks.

A given-up attempt is `FAILED` with `TaskAttempt.give_up_reason` set to `attempts`,
`block_age` or `manual`. An attempt given up by a limit is logged at WARNING as "Task attempt
given up" and counted in `block_dumper_tasks_given_up_total{task_name,reason}`. To stop an
attempt that keeps failing, give it up by hand (see [Giving up attempts](#giving-up-attempts)).

> [!WARNING]
> Do not limit the queue length on the broker instead (`x-max-length` in RabbitMQ). With
> `overflow=drop-head` or `reject-publish` RabbitMQ drops messages without telling the
> sender, and the attempts wait for the stuck-task sweep.

Retries and re-dispatched attempts keep the queue and policy of their original submission,
recorded in `TaskAttempt.dispatch_queue`. Once that queue is no longer defined, for example
after a rename for a new `message_ttl`, they go to the task's `backfill_queue`, or its
`live_queue` without one, with a WARNING. Create one `BlockDumper` per Celery app; one queue
name stands for one policy of one `BlockDumper`, so defining it twice with different
settings, or in two `BlockDumper`s, raises `ValueError`.

### Database writes of a task

The function runs in one transaction with the record of its outcome, so its own database
writes commit together with `SUCCESS`, or with `FAILED` when it raises. A task can store
the part of its work that succeeded and raise to have the rest retried. The retry is sent
once that transaction has committed, and the `transaction.on_commit()` callbacks the function
registered run when it commits.

To have writes undone when the function raises, wrap them in `transaction.atomic()`. When the
exception leaves that block, Django rolls back to its savepoint and the rest of the
transaction still commits:

```python
from django.db import transaction


@dumper.block_task(live_queue=EVENTS_LIVE)
def index_events(block_number: int) -> None:
    store_events_available_now(block_number)  # kept when the task raises below
    with transaction.atomic():
        update_event_summary(block_number)  # undone if it raises
    if events_missing(block_number):
        raise EventsIncomplete(block_number)  # retried; the stored events stay
```

When the transaction cannot commit, none of the function's writes are kept: `FAILED` is
recorded in a transaction of its own and the attempt is retried. That happens when the
function leaves the transaction unusable, for example with a database error outside a
savepoint (`transaction.get_rollback()` is true, or PostgreSQL reports "current transaction
is aborted"), or when the commit itself fails, for example on a deferred foreign key. For a
function that raised, the worker logs "Discarding database writes of a failed task attempt
whose transaction could not commit" at WARNING.

A worker whose attempt was taken over while the function ran commits nothing of the run (see
[Workers that die mid-task](#workers-that-die-mid-task)).

### Celery configuration

The queues are added to the Celery app's queue table by `block_tasks_v1` and
`backfill_blocks_v1` at startup, and in workers by `setup_celery_tasks()`. Workers must call
it from `celeryd_init`, as in the example above: later signals such as `worker_ready` run
after the worker has already declared its queues. Each queue is also declared on the broker
once at startup, as the queue table holds it, and a queue that already exists there with
other arguments or flags stops the command or the worker with an error that names the queue
and explains what to do. A broker
that is unreachable at that point does not stop a worker, where Celery keeps retrying the
connection, nor `block_tasks_v1`, which logs a WARNING and starts without checking the
queues on the broker. `backfill_blocks_v1` stops.

- Any other process that sends BlockDumper tasks itself must call `setup_celery_tasks()`
  once before sending. Otherwise Celery declares
  the queue without its arguments, and RabbitMQ rejects the send once the queue has a
  `message_ttl`.
- A worker started without `-Q` consumes every queue in the table, so start live and backfill
  workers with explicit `-Q` lists to keep their capacity apart.
- The project's `task_routes` do not apply: block task messages always name their queue.
- A queue the project already lists in `task_queues` under the same name is kept, as long as
  RabbitMQ receives the same arguments for it as for the policy. Arguments Kombu derives from
  `kombu.Queue` options count: `Queue("events-live", message_ttl=60)` matches a policy with
  `message_ttl=60`, and `max_priority=10` adds `x-max-priority` the policy lacks. With
  `task_queue_max_priority`, the `x-max-priority` Celery adds is part of both. Producers,
  workers and the startup check then declare the project's queue, with its exchange and flags
  such as `durable`.
- Queue arguments cannot change on an existing RabbitMQ queue. A new `message_ttl` needs a
  new queue name, or the old queue drained and deleted first. Attempts still waiting on the
  old queue are re-sent to the task's current queue once they count as stuck.
- The broker URL must use RabbitMQ: `amqp://`, `pyamqp://` or `amqps://`.

### Checking the setup

In a project whose `BlockDumper` exists once Django has loaded, `manage.py check` (also run
by `migrate` and `runserver`) loads the block task modules and reports configurations that
usually mean a mistake. All of them are warnings, so they never stop a command. Projects
without a `BlockDumper` are not checked, and their task modules are not imported.

| Id | Warning |
| --- | --- |
| `abstract_block_dumper.W000` | The queues could not be checked, e.g. a block task module failed to import |
| `abstract_block_dumper.W001` | `CELERY_TASK_QUEUES` lists the queue with other arguments than its policy needs, counting those Kombu derives from options such as `max_priority` |
| `abstract_block_dumper.W002` | A queue named as a `backfill_queue` also serves live submissions, without a `throttle` |
| `abstract_block_dumper.W003` | A queue has a `throttle` but no backfill or lookback submissions go to it, so it has no effect |
| `abstract_block_dumper.W004` | The Celery app of a `BlockDumper` uses a broker other than RabbitMQ |
| `abstract_block_dumper.W005` | A queue has no `time_limit`, and some of its tasks run on the default hard limit |
| `abstract_block_dumper.W006` | `RetryPolicy.max_delay` exceeds how long RabbitMQ keeps a message unacknowledged (`consumer_timeout`) |

`block_dumper_queues_v1` lists every queue with its policy, the attempts in flight, the
messages waiting on the broker and its consumers. A task without a `backfill_queue` gives its
`live_queue` both roles, because its backfills go there:

```bash
$ python manage.py block_dumper_queues_v1
QUEUE            ROLE           MESSAGE_TTL  SOFT/HARD  THROTTLE  IN_FLIGHT  WAITING  CONSUMERS
events-live      live           60 s         100/120 s  -         4          4        2
events-backfill  backfill       300 s        100/120 s  600       600        540      0  no consumers
```

`SOFT/HARD` shows the queue's own limits; `-` means the limit comes from the task, Celery's
settings or the default hard limit. `IN_FLIGHT` also counts attempts whose message expired or
that count as stuck, which the throttle no longer counts, so it can be higher than
`block_dumper_inflight_tasks`.

While it runs, the live scheduler checks the queues on every reconciliation sweep, also with
`BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS = None`: it publishes
`block_dumper_queue_messages{queue}`, `block_dumper_queue_consumers{queue}` and, for throttled
queues, `block_dumper_inflight_tasks{queue}`, and logs "Block task queue has no consumers" at
WARNING when a queue has attempts in flight but no worker reads it. It reads the queues of each
Celery app over one broker connection.

### Testing a project that uses `BlockDumper`

`abstract_block_dumper.v1.testing` provides `MockBrokerContact`, a broker stand-in that
records every call as a `BrokerCall` and reports the queue mismatches, queue stats or
unreachable broker it is configured with. `use_broker_contact` makes the library use it for
its Celery app inside a `with` block, whatever the broker URL:

```python
import pytest

from abstract_block_dumper.v1.celery import setup_celery_tasks
from abstract_block_dumper.v1.testing import MockBrokerContact, use_broker_contact
from your_project.celery import app


@pytest.fixture
def broker():
    with use_broker_contact(MockBrokerContact(app)) as contact:
        yield contact


def test_worker_refuses_a_queue_declared_with_other_arguments(broker):
    broker.mismatches["events-live"] = "PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'"

    with pytest.raises(SystemExit):
        setup_celery_tasks()
```

Without `use_broker_contact`, an app on `memory://` gets a fresh `MockBrokerContact` on every
call.

### Recovering attempts of `BlockDumper` tasks

The live scheduler's reconciliation sweep (see
[Recovering stuck task attempts](#3-recovering-stuck-task-attempts)) recovers the attempts of
`BlockDumper` tasks by the rules below, apart from those of `@block_task` tasks and with a
batch of its own, so one sweep handles up to twice `BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE`
attempts. It recovers them also with `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS = None`, except
those that only their age shows to be lost (see
[Without a stuck-task timeout](#without-a-stuck-task-timeout)).

#### Workers that die mid-task

A worker **claims** an attempt before running the function: in its own committed
transaction it marks the row `RUNNING`, counts the attempt in `attempt_count`, and records a
lease in `lease_expires_at`. The function then runs in a second transaction, which also
records its outcome (see [Database writes of a task](#database-writes-of-a-task)).

If the worker dies, the row stays `RUNNING` and the lease runs out. The sweep then marks it
`FAILED` and retries it on the queue's `RetryPolicy`; once the attempt is given up it stays
`FAILED` for good. A task that always exceeds its time limit therefore stops at
`RetryPolicy.max_attempts`. Each expiry is logged at WARNING as "Task attempt lease expired"
and counted in `block_dumper_task_lease_expired_total{task_name}`.

The lease length is the task's hard time limit — the queue's `time_limit`, then `time_limit`
in `celery_kwargs`, on a base class or the default hard limit, then Celery's
`task_time_limit` — plus `BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS`. Leases are written and
checked on the database clock, so they hold even when the clocks of the worker and scheduler
hosts disagree.

A task with none of these limits gets a **default hard limit** when it is declared:
`BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS`, or, if that is more, the highest soft limit of the
task (on its queues without a `time_limit`, in `celery_kwargs`, on a base class, or Celery's
`task_soft_time_limit`) plus the margin. Celery kills the task at that limit, before the sweep
would take the attempt over.

> [!IMPORTANT]
> Give block tasks a hard time limit, ideally with a `soft_time_limit` a few seconds below it:
> `Queue(time_limit=120, soft_time_limit=100)`. The soft limit raises
> `SoftTimeLimitExceeded` inside the task, which is recorded as an ordinary failure and
> retried. A task that needs longer than the default hard limit is killed on every attempt and
> given up at its attempt limit.

> [!WARNING]
> - Do not start `BlockDumper` workers with `--time-limit` or `--soft-time-limit`; use
>   `CELERY_TASK_TIME_LIMIT` and `CELERY_TASK_SOFT_TIME_LIMIT` instead. Neither the
>   declaration nor the lease sees the command line, and the prefork pool on RabbitMQ kills a
>   task at the later of its two limits, so a `--soft-time-limit` above the hard limit makes
>   the sweep retry the task while it still runs.
> - `celery_kwargs={"time_limit": None}` removes the default hard limit; the lease is then
>   `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS` (no lease at all when that is `None`), and a task
>   that outlasts it is retried while still running. The first run then cannot record its
>   outcome and its own database writes roll back, but anything it did outside the database
>   happens twice.

A worker whose attempt was taken over this way cannot overwrite the newer attempt: every
write after the claim checks that the row still carries the worker's own Celery task id
(`celery_task_id`).

#### Expired messages

A `PENDING` attempt on a queue with a `message_ttl` is sent again once its message has
expired: `dispatched_at` (database clock) is older than the TTL plus
`BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS`, and so is the ETA of a retry. The re-send keeps the
attempt count, is logged at WARNING as "Re-dispatched task attempt whose message expired",
and is counted in `block_dumper_tasks_recovered_total` with `previous_status="pending"`.

#### Lost messages and lost retries

An attempt without a lease or message TTL counts as stuck by the rules of
[Recovering stuck task attempts](#3-recovering-stuck-task-attempts), with two differences: a
`RUNNING` attempt with a lease is never stuck, since its lease says when its worker is gone,
and an attempt stays recoverable while its `RetryPolicy` allows another try rather than below
`BLOCK_DUMPER_MAX_ATTEMPTS`. A stuck attempt is re-dispatched under its recorded policy,
without consuming an attempt; a duplicate message finds the attempt already running or
finished.

#### Without a stuck-task timeout

With `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS = None` the sweep still runs every
`BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS`: it checks the queues, recovers expired
leases and re-sends expired messages. There is no default hard limit and no recovery by age,
so:

- A task without a hard limit has no lease. If its worker dies, the attempt stays `RUNNING`
  for good, counts against the queue's `throttle`, and cannot be given up by hand.
- A lost `PENDING` attempt on a queue without `message_ttl` is not sent again until a
  `backfill_blocks_v1` run covers its block, and holds a `throttle` slot until then.
- A `FAILED` attempt whose retry was never sent waits for the next submission of its block.

Give every queue a `time_limit` and a `message_ttl` to keep these attempts recoverable.

### Giving up attempts

`block_dumper_give_up_v1` stops the retries of one `BlockDumper` task, for example after a
bug makes it fail for every block. `PENDING` and `FAILED` attempts that could still run become
`FAILED` with `give_up_reason="manual"`; `RUNNING`, finished and already given-up attempts are
left alone. The command refuses tasks declared with `@block_task`.

```bash
# Preview, then give up the attempts of blocks 100-200
$ python manage.py block_dumper_give_up_v1 --task index_events --from-block 100 --to-block 200 --dry-run
$ python manage.py block_dumper_give_up_v1 --task index_events --from-block 100 --to-block 200
```

`--task` takes the task's executable path (`module.function`) or, when it is unique, the
function name. Without `--from-block`/`--to-block` every block of the task is covered.

Each run that stops any attempts logs one WARNING, "Task attempts given up by hand", with their
number. These attempts are not counted in `block_dumper_tasks_given_up_total`: the command is a
one-off process, whose metrics nothing scrapes.

### Other differences from `@block_task`

- `attempt_count` counts executions started rather than failures.
- `celery_task_id` holds the id of the Celery message that ran the attempt.
- A message delivered again for an attempt that is still `RUNNING`, for example with
  `acks_late` after a worker died, raises `CeleryTaskLockedError`; the attempt is recovered
  once its lease passes.
- A retry keeps the archive-network hint of the failed message, and moves to the archive
  network once more than ~300 blocks were produced since the attempt was created.
- `backfill_blocks_v1` reads the chain head again at most once a minute, so the
  archive-network choice holds through a long, throttled backfill.
- `block_dumper_archive_network_requests_total` and the number of blocks lookback reports as
  submitted count only messages actually sent.
- An attempt that was given up is never reopened, not by a new submission nor by a higher
  limit.
- `post_save` and `pre_save` on `TaskAttempt` fire when an attempt is created and when the
  sweep re-dispatches a stuck one; every other status change is a conditional update that
  fires neither.

## Backfilling Missed Blocks

Two mechanisms process blocks the live scheduler did not handle in real time, and a third
re-dispatches task attempts that were recorded but never ran.

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
re-dispatched by a worker and attempts re-dispatched by the reconciliation sweep (see
[Recovering stuck task attempts](#3-recovering-stuck-task-attempts)) both stay on it.

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

### 3. Recovering stuck task attempts

Every submission is recorded as a `TaskAttempt` row **before** the Celery message is sent,
and the worker only marks the row `SUCCESS` or `FAILED` once it has run. If the database or
the broker is unreachable in between, or a worker dies mid-task, the message is gone but the
row stays `PENDING` — and nothing in the normal flow would ever pick it up again. The same
applies to a `FAILED` row whose retry the worker could not enqueue.

The live scheduler therefore runs a periodic **reconciliation sweep**. An attempt counts as
stuck when its last expected activity is older than
`BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS`:

- `PENDING` / `RUNNING`: last dispatched (`updated_at`) before the cutoff, and any retry ETA
  (`next_retry_at`) also before it;
- `FAILED`: a retry was scheduled (`next_retry_at`) and its time passed the cutoff;
- in both cases, `attempt_count` is still below `BLOCK_DUMPER_MAX_ATTEMPTS`.

Each stuck attempt is reset to `PENDING` and re-dispatched through the normal executor on the
queue recorded at its original submission, with the archive-network hint recomputed from the
current head. A re-dispatch does not consume an attempt. Attempts a worker is executing right
now hold their row lock and are skipped. Re-dispatching a message that was merely slow is
harmless: the worker takes the row lock and only runs attempts that are still `PENDING`, so a
duplicate either finds the row locked or already finished.

Each recovery is logged at WARNING and counted in the
`block_dumper_tasks_recovered_total{task_name,previous_status}` Prometheus counter. Tune the
timeout to exceed the longest time a message can legitimately wait in a queue plus execute —
deep backfills on a small worker pool may need a larger value — or set it to `None` to
stop this recovery of `@block_task` attempts. See `BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS` and
`BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE` in the
[Configuration Options Reference](#configuration-options-reference).

For protection at the Celery level as well — for example to keep a message on the broker
while a worker's database connection is down — pass the usual Celery options through
`celery_kwargs`, e.g. `celery_kwargs={"acks_late": True}`.

Attempts of `BlockDumper` tasks follow their own rules: see
[Recovering attempts of `BlockDumper` tasks](#recovering-attempts-of-blockdumper-tasks).


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

## Execution guarantees

- A database constraint keeps one `TaskAttempt` per block, task and args set. Submissions for
  units already `SUCCESS` or `RUNNING` are dropped, and a worker runs only attempts that are
  still `PENDING`, so duplicate broker deliveries are harmless.
- Workers lock the attempt row while running it; a second worker finding it locked does not
  run the function.
- Failed attempts are retried with the backoff described under
  [Staged retry settings](#staged-retry-settings) until `BLOCK_DUMPER_MAX_ATTEMPTS`, which
  includes the first run. Retries wait with a Celery `eta` and stay on the queue of their
  original submission.
- Cleaning up `SUCCESS` rows makes those blocks look missing to `backfill_blocks_v1` and to
  lookback fills, so choose the cleanup age with backfilling in mind.
- `BlockDumper` tasks claim attempts with a lease and retry by their queue's `RetryPolicy`
  instead; see [Queue policies with `BlockDumper`](#queue-policies-with-blockdumper).

## Observability

- **Metrics.** Install `abstract-block-dumper[prometheus]` to record Prometheus metrics;
  without `prometheus_client` every metric call is a no-op. Series are prefixed
  `block_dumper_`: `blocks_processed_total`, `tasks_submitted_total`, `current_block`,
  `block_lag`, `block_processing_seconds`, `task_executions_total`,
  `task_execution_seconds`, `task_retries_total`, `tasks_recovered_total`,
  `registered_tasks`, `pending_tasks`, `archive_network_requests_total`,
  `backfill_progress_percent`, `backfill_from_block` and `backfill_to_block`. Block metrics
  carry `mode` (`realtime`/`backfill`) and `source` (`latest`/`finalized`) labels.
  `BlockDumper` tasks add `task_lease_expired_total`, `messages_expired_total`,
  `tasks_given_up_total`, `inflight_tasks`, `queue_messages`, `queue_consumers` and
  `throttled_total`, described in
  [Queue policies with `BlockDumper`](#queue-policies-with-blockdumper).
  The package does not expose an HTTP endpoint; the scheduler and the backfill command are
  separate processes and need their own scrape targets.
- **Logs.** All logging goes through `structlog` with keyword context such as `block_number`,
  `executable_path`, `celery_task_id`, `queue` and `duration`; log configuration belongs to
  the host project.
- **Admin.** `TaskAttempt` is browsable read-only in Django admin, filterable by status and
  task and searchable by Celery task ID and block number. When a `BlockDumper` exists, it
  also shows the `BlockDumper` columns `dispatch_queue`, `dispatched_at`, `lease_expires_at`,
  `give_up_at` and `give_up_reason`, with filters on `give_up_reason` and `dispatch_queue`.

## Known limitations

- The live scheduler processes only the head block on each poll. Blocks produced between
  polls are covered only by `backfilling_lookback` or `backfill_blocks_v1`, and an old
  integer `BLOCK_DUMPER_START_FROM_BLOCK` does not replay history.
- Lookback windows are scanned inside the poll loop, so a large `backfilling_lookback` across
  many tasks delays the next poll; queue isolation protects worker capacity, not scheduler
  latency.
- `backfill_blocks_v1 --network` defaults to `finney` and ignores `BITTENSOR_NETWORK`; pass
  it explicitly on other networks.
- Gap detection in `backfill_blocks_v1` is per block: a block where any task succeeded is not
  reported as a gap. Per-task skipping still prevents duplicate work, but the gap summary
  can understate what is missing.
- The library's exceptions live in `abstract_block_dumper._internal` and have no stable
  import path.

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

### `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS`
- **Type:** `int | float | None`
- **Default:** `1800` (30 minutes)
- **Description:** How long a task attempt may sit without progress before the live scheduler's reconciliation sweep treats it as stuck and re-dispatches it (see [Recovering stuck task attempts](#3-recovering-stuck-task-attempts)). Must exceed the longest time a message can legitimately wait in a queue plus execute. `None` disables the sweep of `@block_task` attempts; for `BlockDumper` tasks it turns off the default hard limit and recovery by age, while expired leases and expired messages are still recovered (see [Without a stuck-task timeout](#without-a-stuck-task-timeout)). For `BlockDumper` tasks it is also the default hard time limit and the lease of a task without one (see [Workers that die mid-task](#workers-that-die-mid-task)); it is read when such a task is declared, so a change needs a restart.

```python
BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS = 3600  # one hour
```

> **Performance Impact:** Too low a value re-dispatches slow-but-healthy messages; duplicates are harmless for correctness but waste worker slots. It also kills `BlockDumper` tasks on the default hard limit that need longer, so they never finish. A `BlockDumper` task declared with `celery_kwargs={"time_limit": None}` has this timeout as its lease, so too low a value retries its long runs while they still run.

---

### `BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS`
- **Type:** `int | float`
- **Default:** `60`
- **Description:** Minimum seconds between two reconciliation sweeps in the live scheduler. It also paces the checks of `BlockDumper` queues and the recovery of expired leases and expired messages, which run also with `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS = None`.

```python
BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS = 120
```

---

### `BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE`
- **Type:** `int`
- **Default:** `200`
- **Description:** Maximum number of stuck attempts re-dispatched per sweep. After a long outage the backlog drains over several sweeps instead of flooding the broker at once. Attempts of `@block_task` and of `BlockDumper` tasks each get a batch of this size.

```python
BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE = 500
```

---

### `BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS`
- **Type:** `int | float`
- **Default:** `10`
- **Description:** `BlockDumper` tasks only. Added to a task's hard time limit to get the lease of a claimed attempt (see [Workers that die mid-task](#workers-that-die-mid-task)). It covers the delay between the worker killing the task and the sweep seeing the lease expire; the database clock is used on both sides, so host clock drift does not need to be covered. It is also the gap the default hard limit keeps above a soft limit, which is the task's cleanup time after `SoftTimeLimitExceeded`; at `0` there is none, so declaring a task whose soft limit reaches `BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS` without a hard limit of its own raises `ValueError`. Must be non-negative.

```python
BLOCK_DUMPER_RECOVERY_MARGIN_SECONDS = 30
```

---

### `BLOCK_DUMPER_THROTTLE_POLL_SECONDS`
- **Type:** `int | float`
- **Default:** `1`
- **Description:** `BlockDumper` tasks only. How often `backfill_blocks_v1` checks the database for room while a queue's `throttle` is full (see [Queue policies](#queue-policies-with-blockdumper)). Must be positive; with a throttled queue an invalid value stops the commands and workers at startup.

```python
BLOCK_DUMPER_THROTTLE_POLL_SECONDS = 5
```

> **Performance Impact:** Each check is one `COUNT` query on the `TaskAttempt` table.

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

The RabbitMQ contact tests (`tests/contacts`) are skipped unless `BLOCK_DUMPER_TEST_RABBITMQ_URL`
points at a running broker. Start the one from `docker-compose.yml` and run the tests against it:
```
docker compose up -d --wait
BLOCK_DUMPER_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672// nox -t check
```

If you wish to install dependencies into `.venv` so your IDE can pick them up, you can do so using:
```
uv sync --all-extras --dev
```

### Release process

Run `nox -s make_release -- X.Y.Z` where `X.Y.Z` is the version you're releasing and follow the printed instructions.
