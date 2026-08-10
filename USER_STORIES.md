# User Stories

This document describes what users of **abstract-block-dumper** expect from its
**external interfaces**. It is the behavioral contract that code changes are checked
against (see [AGENTS.md](AGENTS.md)): when behavior changes, this file changes in the same
commit.

Scope is deliberately limited to what a consumer of the package can touch:

| Surface | What belongs to it |
| --- | --- |
| Python API | `abstract_block_dumper.v1.*` (decorators, celery helper, maintenance tasks) |
| Django app | `INSTALLED_APPS` entry, migrations, `TaskAttempt` model, admin |
| Management commands | `block_tasks_v1`, `backfill_blocks_v1` |
| Django settings | The `BLOCK_DUMPER_*` / `BLOCK_TASK_*` / `BITTENSOR_NETWORK` settings |
| Celery contract | Task names, queues, routing, worker setup |
| Observability | Prometheus metrics (extra), structlog events, admin views |
| Packaging | PyPI distribution, dependencies, extras, ApiVer guarantees |

Everything under `abstract_block_dumper._internal` is **not** part of this contract and may
change in any release, including patch releases.

## Personas

- **Task author** — a Django developer who writes the functions that run per block.
- **Operator** — whoever runs the scheduler, the workers, and the backfills in production.
- **Integrator** — whoever wires the package into an existing Django/Celery project.
- **Analyst / support** — whoever inspects what ran, what failed, and what is missing.

---

## 1. Installation and project integration

### US-1.1 — Install as an ordinary Django app
**As an** integrator, **I want** to install the package from PyPI and add it to
`INSTALLED_APPS`, **so that** it behaves like any other Django dependency.

- `pip install abstract_block_dumper` pulls in `bittensor>=11.0.1`, `celery>=5.3`,
  `django>=3.2,<6.0`, `structlog>=25.4.0`.
- Python 3.11+ is required.
- Adding `'abstract_block_dumper'` to `INSTALLED_APPS` registers the app
  (`AbstractBlockDumperConfig`, verbose name "Abstract Block Dumper").
- `python manage.py migrate` creates the `TaskAttempt` table; migrations ship with the
  package and are forward-only.

### US-1.2 — Import only through the versioned namespace
**As an** integrator, **I want** a single explicitly versioned import path, **so that** a
future major release cannot silently break my code.

- All supported imports are `from abstract_block_dumper.v1.<module> import ...`:
  - `abstract_block_dumper.v1.decorators.block_task`
  - `abstract_block_dumper.v1.celery.setup_celery_tasks`
  - `abstract_block_dumper.v1.tasks.cleanup_old_tasks`
- `abstract_block_dumper.models.TaskAttempt` is also public — it is a Django model that
  must be importable for queries, admin, and migrations.
- `abstract_block_dumper.v1` itself is a namespace package with no re-exports; users import
  from its submodules. (See [Known gaps](#known-gaps-and-non-goals) — a convenience
  re-export would be a compatible addition.)
- `~=MAJOR.MINOR` version pinning is safe; `v1` will not break when `v2` appears.

### US-1.3 — Make Celery workers aware of block tasks
**As an** integrator, **I want** one documented call that makes every `@block_task`
function resolvable by a worker, **so that** I never see "Received unregistered task".

- `setup_celery_tasks()` imports `tasks` and `block_tasks` from every app in
  `INSTALLED_APPS`, which triggers decorator registration.
- It is expected to be called from a Celery startup signal (`celeryd_init` in the README,
  `worker_ready` in the docstring — either works).
- Missing modules are skipped silently; a module that exists but fails to import is logged
  as a warning and skipped, so one broken app does not prevent a worker from starting.
- The call is safe to make more than once.

---

## 2. Defining block tasks

### US-2.1 — Run a function on every block with zero configuration
**As a** task author, **I want** a bare decorator to be enough, **so that** the simple case
stays simple.

- `@block_task` (no parentheses) and `@block_task()` are both valid and equivalent.
- With no `condition`, the task runs for every block the scheduler processes.
- By default, the task processes the latest chain head. `finalized=True` makes it process
  blocks as they are finalized instead, so different nodes agree on the block being handled.
- The decorated function must accept `block_number: int` as a keyword argument — the
  library always invokes it as `func(block_number=..., **args)`.
- The decorator returns the Celery task object, so `.delay()` / `.apply_async()` are
  available for manual invocation; the original function is kept on `_original_func` for
  introspection.

### US-2.2 — Restrict execution with a condition
**As a** task author, **I want** a predicate that decides whether a block is interesting,
**so that** I do not have to filter inside the task body.

- `condition` is any callable receiving `(block_number, **args)` and returning a bool,
  e.g. `condition=lambda bn: bn % 10 == 0`.
- A non-callable `condition` raises `TypeError` at import time — a misconfigured task fails
  loudly at startup, not silently at runtime.
- A condition that raises is logged and that one task/args pair is skipped; other tasks for
  the same block still run. A failing condition never stops the scheduler and never creates
  a `TaskAttempt`.

### US-2.3 — Fan out one function over several argument sets
**As a** task author, **I want** to run the same function once per netuid (or any other
parameter), **so that** I do not duplicate code.

- `args=[{"netuid": 1}, {"netuid": 3}]` produces one independent submission per dict, per
  matching block.
- The `condition` receives the same extra kwargs, so it can decide per args set
  (`lambda bn, netuid: (bn + netuid) % 50 == 0`).
- Each `(block_number, executable_path, args)` triple is tracked as its own `TaskAttempt`
  with its own status and retry counter.
- Argument dicts must be JSON-serializable — they are stored with sorted keys, so key order
  never causes duplicate rows.

### US-2.4 — Control Celery submission options per task
**As a** task author, **I want** to pass Celery options through the decorator, **so that**
I can set the queue, timeouts, or retry policy for a specific task.

- `celery_kwargs={"queue": "high-priority"}` (and any other Celery option) is applied both
  when the shared task is declared and when each block submission is dispatched.
- `kwargs`, `args`, and `eta` in `celery_kwargs` are ignored at submission time — the
  library owns those.
- `backfill_queue="high-priority-backfill"` routes this task's lookback and historical
  backfill submissions to a separate queue without changing its live routing.
- If `backfill_queue` is omitted, backfill submissions use the task's `celery_kwargs`
  routing or Celery's default queue, exactly like live submissions.
- Leading and trailing whitespace in `backfill_queue` is stripped before the queue is used.
- A value that is blank after stripping, or is not a string, raises `ValueError` at import time.
- With no `queue`, submissions go to Celery's default queue
  (`CELERY_TASK_DEFAULT_QUEUE`, normally `celery`).

### US-2.5 — Get block identity, not block data
**As a** task author, **I want** clear expectations about what is handed to my function,
**so that** I know what I am responsible for fetching.

- The library passes the **block number** and the declared args. It does not fetch block
  contents; the task decides what to read from the chain (or anywhere else).
- If — and only if — the function declares `**kwargs`, it also receives
  `_use_archive_network: bool`, telling it the block is more than 300 blocks behind the
  chain head and should be read from the archive network. Functions without `**kwargs` are
  called without it and never see a `TypeError` for it.
- The return value is stored on `TaskAttempt.execution_result` (a `JSONField`), so it must
  be JSON-serializable. The Celery result is `{"result": <return value>}`.

### US-2.6 — Discover tasks by convention
**As a** task author, **I want** my tasks found automatically, **so that** there is no
registry file to maintain.

- Tasks are discovered in `tasks.py` or `block_tasks.py` inside any app in
  `INSTALLED_APPS`.
- Both `block_tasks_v1` and `backfill_blocks_v1` print how many functions were synced at
  startup; `backfill_blocks_v1` warns and exits if none were found.
- Tasks defined elsewhere are only registered if that module is imported by something in
  the discovery path.

---

## 3. Running the live scheduler

### US-3.1 — Start block processing with one command
**As an** operator, **I want** a long-running management command, **so that** the scheduler
is a normal process I can supervise.

- `python manage.py block_tasks_v1` discovers tasks, reports the count, connects to the
  chain, and consumes block streams forever.
- It connects to the network in `BITTENSOR_NETWORK` (default `finney`).
- `Ctrl+C` stops it cleanly and closes the subtensor connections.
- Any unexpected error inside the scheduler loop is logged and the loop continues after
  one processing interval — a transient RPC failure must not kill the daemon.

### US-3.2 — Predictable starting point
**As an** operator, **I want** to control where processing begins, **so that** a fresh
deployment does not surprise me.

- `BLOCK_DUMPER_START_FROM_BLOCK`:
  - `None` (default) → resume from the highest `block_number` in the database, or the
    current head if the database is empty.
  - `'current'` → start from the current chain head.
  - integer → start from that block number.
- See [Known gaps](#known-gaps-and-non-goals) for what "start from an old block" does
  **not** do today.

### US-3.3 — Tune processing cadence
**As an** operator, **I want** to trade processing latency against scheduler activity,
**so that** I can control how aggressively buffered blocks are drained.

- `BLOCK_DUMPER_POLL_INTERVAL` is the sleep between processing passes over the configured
  block streams; the code default is `5` seconds.
- Default tasks follow the latest-block stream and `finalized=True` tasks follow the
  finalized-block stream. Each stream is opened once and reused.
- Block headers remain buffered by a healthy subscription while the scheduler sleeps, so a
  long interval adds latency rather than deliberately skipping intermediate blocks.
- `backfilling_lookback` (§5) closes gaps caused by scheduler downtime or a disconnected
  subscription.

---

## 4. Execution, tracking, and failure handling

### US-4.1 — Each block/args pair runs at most once successfully
**As an** analyst, **I want** exactly-once success semantics, **so that** downstream data
is not double-counted.

- A database unique constraint on `(block_number, executable_path, args_json)` guarantees a
  single `TaskAttempt` row per logical unit of work.
- A submission for a unit already in `SUCCESS` or `RUNNING` state is dropped before it
  reaches Celery.
- If a worker picks up a message whose `TaskAttempt` is not `PENDING`, it returns without
  running the function — duplicate broker deliveries are harmless.

### US-4.2 — Concurrent workers do not collide
**As an** operator, **I want** to scale workers horizontally, **so that** throughput grows
with the pool.

- Execution takes `SELECT ... FOR UPDATE NOWAIT` on the `TaskAttempt` row; a second worker
  that finds the row locked raises `CeleryTaskLockedError` instead of running the function
  twice.
- A message whose `TaskAttempt` row no longer exists (deleted or cleaned up) also raises
  `CeleryTaskLockedError` rather than executing untracked work.

### US-4.3 — Every attempt is auditable
**As an** analyst, **I want** a durable record of each attempt, **so that** I can answer
"did block N run, and what happened".

`TaskAttempt` exposes:

| Field | Meaning |
| --- | --- |
| `block_number`, `executable_path`, `args_json` | Identity of the unit of work |
| `status` | `pending` / `running` / `success` / `failed` |
| `celery_task_id` | The Celery task id of the current/last attempt |
| `execution_result` | JSON return value on success |
| `celery_queue_override` | Queue recorded for task-specific backfill submissions (`NULL` when no backfill queue was set) |
| `attempt_count`, `last_attempted_at`, `next_retry_at` | Retry bookkeeping |
| `created_at`, `updated_at` | Timestamps |

- `args_dict` is a convenience property for reading/writing `args_json`.
- Indexes on `(status, next_retry_at)` and `(block_number, executable_path)` keep the
  common operational queries fast.

### US-4.4 — Failed tasks retry with exponential backoff
**As an** operator, **I want** transient failures handled automatically, **so that** I do
not babysit the queue.

- An exception inside a task marks the attempt `FAILED`, increments `attempt_count`, and
  computes `next_retry_at`.
- Delay is `BLOCK_TASK_RETRY_BACKOFF ** attempt_count` minutes (code default base `1`),
  capped at `BLOCK_TASK_MAX_RETRY_DELAY_MINUTES` (default `1440`).
- Retrying stops once `attempt_count` reaches `BLOCK_DUMPER_MAX_ATTEMPTS` (default `3`);
  `next_retry_at` is then cleared and the attempt stays `FAILED` permanently.
- The retry is dispatched with an `eta`, so it occupies no worker while it waits.
- The task's exception, type, message, and duration are logged with the task id and block
  number.

### US-4.5 — Retries stay on the queue they came from
**As an** operator, **I want** retries to respect queue isolation, **so that** a retried
backfill task never lands on my live workers by accident.

- The queue used for a submission is persisted on `TaskAttempt.celery_queue_override`, so
  routing survives a process restart.
- On retry, the exact recorded queue is reused. Changing a task's annotation affects new
  submissions, not attempts already persisted in the database.
- Live submissions record no override and are never pulled onto the backfill queue.

### US-4.6 — No phantom successes
**As an** analyst, **I want** impossible states cleaned up, **so that** gap detection is
trustworthy.

- On block-processor initialization, rows that are `SUCCESS` but were never started
  (`last_attempted_at` and `celery_task_id` both null) and were created in the last hour
  are deleted.
- The one-hour bound protects legitimate rows written by external processes.

---

## 5. Backfilling missed blocks

### US-5.1 — Self-healing trailing window per task
**As a** task author, **I want** a task to keep its own recent history complete, **so
that** short scheduler outages heal without operator action.

- `@block_task(backfilling_lookback=N)`: on every new selected head `H`, the live scheduler
  also submits any block in `[H-N, H-1]` for that task that has not already succeeded and
  is not in flight, still honoring `condition`. For `finalized=True` tasks, `H` is the
  finalized head.
- Blocks already `SUCCESS` are skipped; blocks `PENDING`/`RUNNING` are skipped so nothing is
  dispatched twice; `FAILED` blocks are re-submitted subject to the normal backoff.
- Skipping is evaluated **per args set**, so a multi-args task heals each netuid
  independently.
- Only tasks that declare `backfilling_lookback` are backfilled this way; catch-up per head
  is bounded to `N` blocks.
- A failure while filling one task's window is logged and does not stop the other tasks or
  the scheduler loop.
- `BLOCK_DUMPER_LOOKBACK_ENABLED = False` disables the mechanism globally without editing
  task code.

### US-5.2 — One-off historical backfill
**As an** operator, **I want** a command for large historical ranges, **so that** I can
onboard a new task over past data.

- `python manage.py backfill_blocks_v1 --from-block A --to-block B`.
- Omitting either bound defaults it to the min/max `block_number` already in the database;
  if the database is empty and bounds are omitted, the command errors and explains that
  both flags are required.
- `--from-block > --to-block` and a negative `--rate-limit` are rejected with a clear error
  instead of running.
- `--rate-limit` (default `1.0` s) throttles submissions between blocks and the command
  prints an estimated total runtime.
- `--network` (default `finney`) selects the chain for this run.
- `Ctrl+C` stops gracefully and reports how many blocks were processed.

### US-5.3 — Gap detection by default
**As an** operator, **I want** the command to fill only what is actually missing, **so
that** re-running it is cheap and safe.

- By default the command finds contiguous ranges of blocks with no successful attempt,
  prints each gap with its size, and processes only those.
- "No gaps found" is reported explicitly when the range is complete.
- `--no-gap-detection` walks every block in the range instead.
- Even in that mode, per-task submissions are skipped for units that already succeeded, so
  reprocessing does not duplicate work.

### US-5.4 — Preview before committing
**As an** operator, **I want** a dry run, **so that** I can size a backfill before starting
it.

- `--dry-run` submits nothing and reports total blocks in range, already-processed blocks,
  blocks needing tasks, and estimated task count.
- In range mode it also prints the resolved network type (regular vs archive), the current
  head, and the registered task list.
- In gap mode it summarizes gap count, total missing blocks, and estimated tasks.

### US-5.5 — Automatic archive-network selection
**As a** task author, **I want** old blocks flagged as archival, **so that** my task reads
them from a node that still has the state.

- Blocks more than **300** behind the current head are marked with
  `_use_archive_network=True` (delivered per US-2.5) and counted in the
  `block_dumper_archive_network_requests_total` metric.
- This applies to both lookback fills and the historical command.

### US-5.6 — Isolate backfill load from live work
**As an** operator, **I want** backfill submissions on a separate queue, **so that** a
large backfill cannot starve real-time processing.

- `@block_task(backfill_queue="block-backfill")` routes both that task's lookback fills
  and its `backfill_blocks_v1` submissions to the specified queue, overriding its
  `celery_kwargs` queue for backfill submissions only.
- Different tasks can declare different backfill queues. A task that omits the argument
  uses its normal Celery routing for backfills.
- Live submissions at either the latest or finalized head keep the task-level or default
  queue.
- Leading and trailing whitespace is stripped from the queue name. A value that is empty
  after stripping, or is not a string, raises `ValueError` rather than routing work to a
  nonsense queue name.
- Capacity isolation only materializes if workers are run with disjoint `--queues` lists;
  the documentation says so explicitly.
- Documented caveat: the live scheduler still scans lookback windows synchronously after
  each head, so very large windows can delay the next block-stream read even with queue
  isolation.

---

## 6. Observability

### US-6.1 — Optional Prometheus metrics
**As an** operator, **I want** metrics without a mandatory dependency, **so that**
non-instrumented deployments stay lean.

- `pip install abstract-block-dumper[prometheus]` enables metrics; without
  `prometheus_client` every metric call is a no-op and nothing fails.
- Exposed series:

  | Metric | Type | Labels |
  | --- | --- | --- |
  | `block_dumper_blocks_processed_total` | counter | `mode` (`realtime`/`backfill`) |
  | `block_dumper_tasks_submitted_total` | counter | `task_name` |
  | `block_dumper_current_block` | gauge | `mode` |
  | `block_dumper_block_lag` | gauge | `mode` |
  | `block_dumper_block_processing_seconds` | histogram | `mode` |
  | `block_dumper_task_executions_total` | counter | `task_name`, `status` |
  | `block_dumper_task_execution_seconds` | histogram | `task_name` |
  | `block_dumper_task_retries_total` | counter | `task_name` |
  | `block_dumper_registered_tasks` | gauge | — |
  | `block_dumper_pending_tasks` | gauge | — |
  | `block_dumper_archive_network_requests_total` | counter | — |
  | `block_dumper_backfill_progress_percent` | gauge | — |
  | `block_dumper_backfill_from_block` / `_to_block` | gauge | — |

- Exposing the metrics endpoint is the host project's job (see
  [Known gaps](#known-gaps-and-non-goals)).

### US-6.2 — Structured logs
**As an** operator, **I want** machine-parsable logs, **so that** I can build alerts on
them.

- All library logging goes through `structlog` with keyword context: `task_id`,
  `block_number`, `executable_path`, `celery_task_id`, `duration`, `queue`, and similar.
- Log configuration belongs to the host project; the library only emits.
- Notable events: task registration, scheduler start/stop, task scheduling, task
  start/success/failure, retry scheduling, lookback fills, backfill progress, queue
  rename/removal fallbacks.

### US-6.3 — Inspect attempts in Django admin
**As an** analyst, **I want** a UI over `TaskAttempt`, **so that** I can answer questions
without a database client.

- The admin lists `executable_path`, `block_number`, `status`.
- Filters on `status` and `executable_path`; search on `celery_task_id` and `block_number`.
- All fields are read-only and grouped into Identity / Task Execution / Retry Information /
  Timestamps — the admin is for inspection, not for editing execution state.

---

## 7. Maintenance

### US-7.1 — Bounded table growth
**As an** operator, **I want** to prune history on a schedule, **so that** the table does
not grow without limit.

- `abstract_block_dumper.v1.tasks.cleanup_old_tasks(days=7)` is a Celery task registered as
  `abstract_block_dumper.v1.cleanup_old_tasks`.
- It deletes `SUCCESS` and `FAILED` attempts whose `updated_at` is older than the cutoff.
- It **never** deletes `PENDING` or `RUNNING` attempts, so in-flight work is preserved.
- It returns `{"deleted_count": int, "cutoff_date": "<iso8601>"}` for logging and alerting.
- It can be run by hand, from cron, or from `CELERY_BEAT_SCHEDULE`; all three are
  documented.
- Retention must be chosen with §5 in mind: pruning success rows makes those blocks look
  like gaps to `backfill_blocks_v1` and to lookback fills.

---

## 8. Configuration contract

### US-8.1 — All settings are optional and documented
**As an** integrator, **I want** sane defaults and one reference table, **so that** a
minimal install just works.

Every setting is read with `getattr(settings, ..., default)` — none is mandatory.

| Setting | Type | Default (code) | Effect |
| --- | --- | --- | --- |
| `BITTENSOR_NETWORK` | `str` | `'finney'` | Network for the live scheduler |
| `BLOCK_DUMPER_START_FROM_BLOCK` | `str \| int \| None` | `None` | Starting point (US-3.2) |
| `BLOCK_DUMPER_POLL_INTERVAL` | `int` | `5` | Seconds between block-stream processing passes |
| `BLOCK_DUMPER_LOOKBACK_ENABLED` | `bool` | `True` | Global kill-switch for `backfilling_lookback` |
| `BLOCK_DUMPER_MAX_ATTEMPTS` | `int` | `3` | Maximum attempts before giving up |
| `BLOCK_TASK_RETRY_BACKOFF` | `int` | `1` | Exponential backoff base, in minutes |
| `BLOCK_TASK_MAX_RETRY_DELAY_MINUTES` | `int` | `1440` | Backoff cap |

- The README's configuration reference is expected to match these defaults and to state
  each setting's type, valid range, and performance impact.

### US-8.2 — A runnable reference deployment
**As an** integrator, **I want** a working example, **so that** I can compare my wiring
against something known-good.

- `example_project/` runs with `docker-compose up --build` and starts Django (admin
  `admin`/`admin`), Celery workers, the scheduler, Flower, Redis, and PostgreSQL.
- It demonstrates every task shape: every-block, conditional, multi-args/multi-netuid,
  `backfilling_lookback`, task-specific `backfill_queue` routing, and a deliberately
  failing task that exercises retries.

---

## 9. Compatibility promises

### US-9.1 — Stable public API within a major version
**As an** integrator, **I want** explicit stability guarantees, **so that** upgrades are
routine.

- Semantic Versioning plus ApiVer: `abstract_block_dumper.v1` will not change
  incompatibly, even after `v2` ships.
- `abstract_block_dumper._internal.*` carries no guarantees at all.
- Celery task names are derived from `module.function` of the decorated function — renaming
  or moving a task function is a **breaking change for the user's own deployment**, because
  it orphans queued messages and existing `TaskAttempt` rows.
- Behavior changes are recorded in `changelog.d/` and released into `CHANGELOG.md` via
  towncrier.

---

## Known gaps and non-goals

Recorded so the documentation stays tied to code that exists in the current tree, per
[AGENTS.md](AGENTS.md). These are **not** promises — they are places where user
expectations and current behavior diverge, or where a boundary is intentional.

1. **Orphaned retries are not recovered by the running scheduler.**
   `BlockProcessor.recover_failed_retries()` exists and works, but nothing in the live
   `TaskScheduler` loop calls it; it is exercised only by tests. In practice a `FAILED`
   attempt is retried by the in-process `apply_async(eta=...)` scheduled at failure time,
   or — for tasks with `backfilling_lookback` — when the lookback window re-submits it. A
   task with no lookback whose retry message is lost (worker crash, broker purge) is not
   picked back up automatically today.
2. **`BLOCK_DUMPER_START_FROM_BLOCK` does not replay history.** The live scheduler
   consumes live block streams only. Setting an old integer sets the initial
   `last_processed_block`, but the first stream snapshot starts at the current head; no
   intermediate history is replayed. Historical coverage comes from `backfill_blocks_v1`
   or from `backfilling_lookback`, not from this setting.
3. **Subscriptions do not recover disconnected history.** Latest and finalized block
   headers are buffered while their subscriptions are healthy. After scheduler downtime or
   a disconnection, a new subscription resumes at the node's current head. Tasks that must
   repair those gaps need `backfilling_lookback` or an explicit historical backfill.
4. **No metrics HTTP endpoint is shipped.** The package records metrics into the default
   Prometheus registry; exposing `/metrics` (via `django-prometheus`, `start_http_server`,
   or a sidecar) is the host project's responsibility. The scheduler and the backfill
   command are separate processes with separate registries and need separate scrape targets.
5. **`backfill_blocks_v1 --network` ignores `BITTENSOR_NETWORK`.** Its default is the
   literal `finney`, unlike the live scheduler which reads the setting. Operators on a
   non-default network must pass `--network` on every backfill run.
6. **Exceptions are not publicly importable.** `AbstractBlockDumperError`,
   `ConditionEvaluationError`, and `CeleryTaskLockedError` live in `_internal`, so user code
   has no stable path for catching them.
7. **Lookback filling is synchronous.** Window scanning runs inline in the scheduler loop
   after each head is scheduled, so a large `backfilling_lookback` across many tasks delays
   the next block-stream read. Queue isolation protects worker capacity, not scheduler
   latency.
8. **Gap detection is block-level, not task-level.** `backfill_blocks_v1` treats a block as
   "processed" if *any* task succeeded for it. A block where one task of several succeeded
   is not reported as a gap; the per-task skip logic still prevents duplicate work, but the
   gap summary can understate what is missing.
9. **Fetching chain data is out of scope.** The library schedules and tracks; reading block
   contents is the task's job. `BittensorConnectionClient.get_for_block()` is an unfinished
   internal stub (`NotImplementedError`) — use `get_subtensor_for_block()`, or, from user
   code, the `_use_archive_network` hint.
