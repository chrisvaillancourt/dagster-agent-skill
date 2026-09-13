# Operations and debugging

Running Dagster day to day: instance configuration in `dagster.yaml`, the daemon, how runs are queued/launched/executed, concurrency controls, retries, timeouts, monitoring, logging, and debugging live code. Deployment topologies (Docker, Kubernetes, ECS, Helm) are in `deployment-oss.md`; Dagster+ agents and deployment settings are in `dagster-plus.md`; symptom-driven fix tables are in `troubleshooting.md`. Executor selection per job is summarized here but authored in `ops-jobs-and-graphs.md`. Run config and environment variables are in `resources-and-config.md`.

## Contents

- [Instance and DAGSTER_HOME](#instance-and-dagster_home)
- [dagster.yaml key reference](#dagsteryaml-key-reference)
- [Storage and compute logs](#storage-and-compute-logs)
- [The daemon](#the-daemon)
- [Run coordinators, launchers, executors](#run-coordinators-launchers-executors)
- [Run queue and priority](#run-queue-and-priority)
- [Run retries](#run-retries)
- [Run monitoring and timeouts](#run-monitoring-and-timeouts)
- [Concurrency](#concurrency)
- [Logging](#logging)
- [Debugging](#debugging)
- [Webserver and UI](#webserver-and-ui)
- [Operational gotchas](#operational-gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Instance and DAGSTER_HOME

Use when: any process (webserver, daemon, CLI, run worker) needs to share run history, event logs, and launch policy.

Every Dagster process loads an *instance*. Resolution order:

| `DAGSTER_HOME` | `$DAGSTER_HOME/dagster.yaml` | Result |
| --- | --- | --- |
| unset | n/a | Ephemeral temp directory, wiped on exit. `dg dev` default. |
| set | missing/empty | Local filesystem storage under `$DAGSTER_HOME` (`history/runs.db`, `history/<run_id>.db`, `storage/<run_id>/compute_logs`). |
| set | present | Configured storage, launcher, coordinator, logging. |

All processes in one deployment must point at the same `dagster.yaml` and the same storage. A daemon that cannot see the webserver's storage will never dequeue its runs. `dg dev` sets `DAGSTER_IS_DEV_CLI=1`; branch on it to keep schedules/sensors stopped locally but `RUNNING` in production.

```shell
export DAGSTER_HOME=~/.dagster_home   # absolute path
dagster instance info                 # dump the resolved instance config
dagster instance migrate              # apply schema migrations after a Dagster upgrade
dagster instance reindex              # rebuild indexes over historical runs
```

## dagster.yaml key reference

Top-level keys. Every scalar accepts `{env: VAR_NAME}` in place of a literal.

| Key | Purpose | Common values |
| --- | --- | --- |
| `storage` | Run, event log, schedule/sensor tick persistence | `sqlite.base_dir` (default); `postgres.postgres_db.{username,password,hostname,db_name,port}` or `postgres.postgres_url`; `mysql.mysql_db` / `mysql.mysql_url` |
| `run_launcher` | Where a run's process/container is allocated | `DefaultRunLauncher`; `dagster_docker.DockerRunLauncher`; `dagster_k8s.launcher.K8sRunLauncher`; `dagster_aws.ecs.EcsRunLauncher` |
| `run_coordinator` | Submission policy before launch | `DefaultRunCoordinator` (immediate); `QueuedRunCoordinator` with `max_concurrent_runs`, `tag_concurrency_limits`, `dequeue_use_threads`, `dequeue_num_workers` |
| `concurrency` | Current spelling for run and pool limits | `runs.max_concurrent_runs`, `runs.tag_concurrency_limits`, `pools.default_limit`, `pools.granularity`, `pools.op_granularity_run_buffer` |
| `compute_logs` | stdout/stderr capture | `LocalComputeLogManager` (`base_dir`); `NoOpComputeLogManager`; `dagster_aws.s3.compute_log_manager.S3ComputeLogManager`; `dagster_gcp.gcs.compute_log_manager.GCSComputeLogManager`; `dagster_azure.blob.compute_log_manager.AzureBlobComputeLogManager` |
| `local_artifact_storage` | Base dir for `FilesystemIOManager` and disk-bound artifacts | `LocalArtifactStorage` with `base_dir` (only implementation) |
| `run_retries` | Whole-run retries | `enabled`, `max_retries`, `retry_on_asset_or_op_failure` |
| `run_monitoring` | Crash/hang detection | `enabled`, `start_timeout_seconds` (180), `cancel_timeout_seconds` (180), `max_resume_run_attempts` (0), `poll_interval_seconds` (120), `max_runtime_seconds`, `free_slots_after_run_end_seconds` |
| `schedules` / `sensors` / `backfills` | Daemon parallelism | `use_threads`, `num_workers`, `num_submit_workers` |
| `auto_materialize` | Legacy auto-materialize daemon | `enabled`, `minimum_interval_seconds`, `run_tags`, `max_tick_retries`, `use_sensors`, `use_threads`, `num_workers` |
| `python_logs` | Capture of stdlib `logging` | `managed_python_loggers`, `python_log_level`, `dagster_handler_config` |
| `retention` | Tick purge policy | `schedule.purge_after_days`, `sensor.purge_after_days.{skipped,failure,success}` (`-1` keeps forever) |
| `code_servers` | gRPC code server behavior | `local_startup_timeout` (default 180s before Dagster gives up loading code) |
| `secrets` | Named secrets resolved from env | `my_secret: {env: MY_SECRET_ENV_VAR}` |
| `telemetry` | Anonymized usage stats | `enabled: false` to opt out |
| `nux` | First-run new-user experience | `enabled: false` |

A production baseline:

```yaml
# $DAGSTER_HOME/dagster.yaml
storage:
  postgres:
    postgres_db:
      username: {env: DAGSTER_PG_USERNAME}
      password: {env: DAGSTER_PG_PASSWORD}
      hostname: {env: DAGSTER_PG_HOST}
      db_name: {env: DAGSTER_PG_DB}
      port: 5432
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config: {dequeue_use_threads: true, dequeue_num_workers: 8}
concurrency:
  runs: {max_concurrent_runs: 15}
  pools: {default_limit: 1}
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config: {bucket: mycorp-dagster-compute-logs, prefix: prod-}
run_retries: {enabled: true, max_retries: 3, retry_on_asset_or_op_failure: false}
run_monitoring:
  enabled: true
  max_runtime_seconds: 7200
  free_slots_after_run_end_seconds: 300
sensors: {use_threads: true, num_workers: 8}
schedules: {use_threads: true, num_workers: 8}
```

Verify what the instance actually loaded with `dagster instance info`, or in the UI under **Deployment > Configuration**.

## Storage and compute logs

Decision rule: SQLite for a single local process, Postgres (or MySQL, in UTC) for anything with more than one. `postgres` requires `dagster-postgres`, `mysql` requires `dagster-mysql`.

Compute logs are the raw `stdout`/`stderr` of each step, stored separately from the structured event log. Local disk works only when the webserver can read the same filesystem the run worker wrote to; in Docker/Kubernetes/ECS use the S3/GCS/Azure manager or accept that logs vanish with the pod. `NoOpComputeLogManager` disables capture entirely, which is the blunt answer when logs would leak sensitive data.

To redact PII instead, point `compute_logs.module`/`class` at your own compute log manager subclass. Redact-on-read keeps unredacted originals on disk for audit but needs extra handling before cloud upload; redact-on-write guarantees PII never lands in storage and is unrecoverable. Windows/Azure hosts may need `PYTHONLEGACYWINDOWSSTDIO=1` for compute logs to appear at all.

## The daemon

Use when: anything scheduled, sensed, queued, backfilled, or monitored needs to happen. The webserver alone does none of it.

```shell
dg dev                 # local: webserver + daemon together
dagster-daemon run     # daemon only; takes the same code-location args as dg dev
```

Daemons hosted by the single `dagster-daemon` process, each on its own interval and thread:

| Daemon | Does | Enabled by |
| --- | --- | --- |
| Scheduler | Creates runs from active schedules | On unless `scheduler` is overridden away from `DagsterDaemonScheduler` |
| Sensor | Creates runs from turned-on sensors, including automation condition sensors | Always |
| Run queue | Dequeues and launches queued runs under limits and priority | `run_coordinator` set to `QueuedRunCoordinator` |
| Run monitoring | Detects hung/crashed run workers, enforces timeouts, frees stale pool slots | `run_monitoring.enabled: true` |
| Backfill | Submits runs for asset and job backfills | Always |

**Deployment > Daemons** in the UI shows each daemon's last heartbeat; a missing one means the process died or cannot write to instance storage, so read the `dagster-daemon` process logs. The daemon reloads workspace files periodically, so code-location changes need no daemon restart. Instance config changes do.

Flags worth knowing: `--log-level {critical,error,warning,info,debug}` (default `info`), `--log-format {colored,json,rich}` (default `colored`), `--code-server-log-level`. `dg dev` accepts `--log-level`, `--log-format`, `-p/--port`, `-h/--host`, `--verbose`.

Sensor evaluations time out after 60 seconds by default. Raise it in OSS with `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`, but only after profiling: see `troubleshooting.md`.

## Run coordinators, launchers, executors

Three distinct layers; confusing them is the most common source of wrong concurrency config.

| Layer | Scope | Configured in | Controls |
| --- | --- | --- | --- |
| Run coordinator | Instance | `dagster.yaml` `run_coordinator` | Whether a submitted run queues, in what order, under what run-level limits |
| Run launcher | Instance | `dagster.yaml` `run_launcher` | What compute resource hosts the run worker: subprocess, Docker container, K8s job, ECS task |
| Executor | Per job / per code location | `executor_def` on the job, or `Definitions(executor=...)` | How steps inside one run are parallelized within or beyond the run worker |

Launchers: `DefaultRunLauncher` (a subprocess on the code location's node), `DockerRunLauncher`, `K8sRunLauncher`, `dagster_aws.ecs.EcsRunLauncher`, and `CeleryK8sRunLauncher` (a K8s job wired for `celery_k8s_job_executor`).

Executors: `multi_or_in_process_executor` (default), `in_process_executor`, `multiprocess_executor`, plus `k8s_job_executor`, `docker_executor`, `ecs_executor`, `celery_executor`, `celery_k8s_job_executor`, `celery_docker_executor`, `dask_executor` from integration libraries. `execute_in_process()` always overrides the job's executor with `in_process_executor`.

Celery and Dask executors distribute steps across a broker or cluster; both require persistent Postgres/MySQL storage plus a shared I/O manager (S3/GCS/ADLS/NFS), because steps exchange values across machines. `dagster-celery worker start -A dagster_celery.app`, `worker list`, `worker terminate` manage workers; pass `-y /path/to/celery_config.yaml` when using custom broker config. Dagster does not yet collect logs from Dask workers.

Launch a run from the CLI with `dagster job launch` (goes through the launcher) versus `dagster job execute` (executes in the current process, bypassing coordinator and launcher).

## Run queue and priority

Use when: many runs can be submitted at once and your infrastructure has a ceiling.

`QueuedRunCoordinator` puts submitted runs in `QUEUED`; the daemon's run queue dequeues them first-in-first-out within a priority band.

```python
# src/<project>/defs/jobs.py
@dg.job(tags={"dagster/priority": "3"})
def important_job(): ...


@dg.schedule(cron_schedule="* * * * *", job_name="important_job",
             tags={"dagster/priority": "-1"})
def less_important_schedule(_): ...
```

- `dagster/priority` is an integer **as a string**. Default `0`. Higher goes first; negatives de-prioritize (useful for backfills). Also settable per run from the launchpad.
- A run blocked by a tag limit does not block runs submitted after it: submission order `A, B, C` with a `team` limit of 1 launches `A, C, B`.
- When a pool is at capacity, dequeue order is not strictly FIFO. If order matters, encode it with `dagster/priority` rather than relying on submission time.

Terminate a run from the **Runs** page or the run details page; with `QueuedRunCoordinator`, terminating in-progress runs is how you unblock a jammed queue.

## Run retries

Use when: whole runs fail, including run-worker crashes that op retries cannot catch. Op-level `RetryPolicy` is in `ops-jobs-and-graphs.md`.

```yaml
run_retries:
  enabled: true            # omit in Dagster+, always on
  max_retries: 3
  retry_on_asset_or_op_failure: false
```

Per-job tag overrides:

```python
# src/<project>/defs/jobs.py
@dg.job(tags={"dagster/max_retries": 3, "dagster/retry_strategy": "ALL_STEPS"})
def full_rerun_job(): ...


asset_job = dg.define_asset_job(
    name="asset_job",
    selection=dg.AssetSelection.all(),
    tags={
        "dagster/max_retries": "3",
        "dagster/retry_strategy": "FROM_ASSET_FAILURE",
        "dagster/retry_on_asset_or_op_failure": "false",
    },
)
```

| Tag / setting | Values | Effect |
| --- | --- | --- |
| `dagster/max_retries` | int | Retry budget for the whole run |
| `dagster/retry_strategy` | `FROM_FAILURE` (default), `ALL_STEPS`, `FROM_ASSET_FAILURE` | Which steps the retry re-executes |
| `dagster/retry_on_asset_or_op_failure` / `run_retries.retry_on_asset_or_op_failure` | bool, default `true` | `false` retries only infrastructure failures (crash, eviction), not step failures |

- `FROM_FAILURE` skips already-successful ops but reads their outputs, so it needs an I/O manager readable across runs. `FilesystemIOManager` fails this on Kubernetes; use `s3_pickle_io_manager` or equivalent.
- `FROM_ASSET_FAILURE` reads persisted materialization records from the event log and skips already-materialized assets. Pair it with `retry_on_asset_or_op_failure: false`. Best fit for spot-instance loss and dbt pipelines, since it needs no persisted dbt artifacts.
- Enabling both op retries and run retries without `retry_on_asset_or_op_failure: false` multiplies attempts: the op retry counter resets on each retried run.
- `op_retry_policy` on `define_asset_job()` applies its limit **per asset**, not per run.

## Run monitoring and timeouts

Use when: run workers can crash, hang, or be evicted. Requires the daemon and a run launcher other than `DefaultRunLauncher` for crash detection.

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 180             # stuck in STARTING -> failed
  cancel_timeout_seconds: 180            # stuck in CANCELING -> canceled
  max_resume_run_attempts: 3             # >0 enables resuming crashed runs
  poll_interval_seconds: 120
  max_runtime_seconds: 7200              # deployment-wide run timeout
  free_slots_after_run_end_seconds: 300  # release stale pool slots
```

Per-job timeout, in seconds, via the `dagster/max_runtime` tag:

```python
# src/<project>/defs/jobs.py
@dg.job(tags={"dagster/max_runtime": 10})
def my_job(): ...


asset_job = dg.define_asset_job(
    name="some_job", selection="*", tags={"dagster/max_runtime": 10}
)
```

- `dagster/max_runtime` needs `run_monitoring.enabled: true` in OSS and a daemon restart after the config change. In Dagster+ monitoring is always on, no restart needed.
- Resuming a crashed run (rather than failing it) is supported only with `K8sRunLauncher` + `k8s_job_executor` or `DockerRunLauncher` + `docker_executor`. Crashes appear in the event log; after `max_resume_run_attempts` the run is marked failed.
- Size `max_runtime_seconds` above the observed p99, not the mean, and revisit it when data volume changes.

## Concurrency

Pick the narrowest mechanism that solves the problem:

| Mechanism | Scope | Protects against |
| --- | --- | --- |
| `concurrency.runs.max_concurrent_runs` | Whole deployment | Too many simultaneous runs |
| Concurrency pools (`pool=`) | Across all runs | Overloading a shared database or API |
| `concurrency.runs.tag_concurrency_limits` | Across runs, by run tag | One sensor/backfill/code location hogging capacity |
| Executor `max_concurrent` | Inside one run | Memory/CPU exhaustion on the run worker |
| Executor `tag_concurrency_limits` | Inside one run, by op tag | Category contention among steps of one run |

### Pools

Assign a pool in code, then set its limit out of band:

```python
# src/<project>/defs/assets.py
@dg.asset(pool="database")
def query_customers(context: dg.AssetExecutionContext): ...


@dg.op(pool="api")
def fetch_external_data(): ...
```

```yaml
concurrency:
  pools: {default_limit: 1, granularity: 'run'}   # granularity default is 'op'
```

```shell
dagster instance concurrency set database 1
```

Limits are also editable in the UI at **Deployment > Concurrency > Add pool limit**. Verify the pool stuck by opening the asset or op details pane.

`granularity: op` (default) caps concurrently *executing ops* across runs; the number of in-progress runs is unaffected. `granularity: run` caps the number of in-progress *runs* containing an op in that pool. `pools.op_granularity_run_buffer` sets how many runs beyond the available limit may be in progress under op granularity, bounding active run workers.

### Run-level limits

```yaml
concurrency:
  runs:
    max_concurrent_runs: 15
    tag_concurrency_limits:
      - {key: 'dagster/sensor_name', value: 'my_cool_sensor', limit: 5}
      - {key: 'dagster/backfill', limit: 10}
      - key: 'dagster/code_location'
        value: {applyLimitPerUniqueValue: true}
        limit: 10
```

`applyLimitPerUniqueValue: true` applies the limit separately to each distinct value of the key rather than to the key as a whole. Every run is auto-tagged with `dagster/code_location`, which makes per-team fairness a two-line config.

### Within a single run

```python
# src/<project>/defs/jobs.py
warehouse_job = dg.define_asset_job(
    name="warehouse_job",
    selection=[warehouse_sync, warehouse_aggregate],
    executor_def=dg.multiprocess_executor.configured(
        {
            "max_concurrent": 4,
            "tag_concurrency_limits": [
                {"key": "database", "value": "warehouse", "limit": 1}
            ],
        }
    ),
)
```

Op tags come from `@dg.asset(op_tags={...})` or `@dg.op(tags={...})`. To set a code-location-wide default, return `dg.Definitions(executor=dg.multiprocess_executor.configured({"max_concurrent": 4}))` from a `@dg.definitions` function in `defs/`; a job's own `executor_def` still wins. `multiprocess_executor` defaults to `multiprocessing.cpu_count()` of the run worker.

To serialize runs of one job entirely, have the schedule query the instance and skip when a run is live:

```python
@dg.schedule(job=my_job, cron_schedule="* * * * *")
def my_schedule(context):
    statuses = [dg.DagsterRunStatus.QUEUED, dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING, dg.DagsterRunStatus.STARTED]
    if context.instance.get_run_records(
        dg.RunsFilter(job_name="my_job", statuses=statuses)
    ):
        return dg.SkipReason("another run of my_job is already running")
    return dg.RunRequest()
```

## Logging

Two log streams reach the UI's **Run details** page: structured event logs (searchable, tagged with asset/step/run metadata, stored in the event log DB) and raw compute logs (the process's stdout/stderr, stored by the compute log manager). Emit structured logs with `context.log.{debug,info,warning,error,critical}`; custom log levels are unsupported and raise at runtime.

Capture stdlib `logging` calls into the event log by naming the loggers in `dagster.yaml`:

```yaml
python_logs:
  managed_python_loggers: [my_logger, my_other_logger]
  python_log_level: INFO
```

`python_log_level` sets the level for every Dagster-managed logger, including `context.log`, and is applied to `managed_python_loggers` before a run launches.

When threading `context` through nested functions is awkward, `dg.get_dagster_logger()` returns a captured logger with no `dagster.yaml` change. It is process-global, so the same arguments return the same logger object; do not set conflicting levels for it in different modules.

Handlers, formatters and filters follow Python's `logging.config` dictionary schema, minus `loggers` (Dagster owns those):

```yaml
python_logs:
  dagster_handler_config:
    handlers:
      myHandler: {class: logging.FileHandler, level: INFO,
                  filename: 'my_dagster_logs.log', mode: 'a', formatter: timeFormatter}
    formatters:
      timeFormatter: {format: '%(asctime)s :: %(message)s'}
```

Swap the console logger for JSON per code location with `dg.Definitions(..., loggers={"console": dg.json_console_logger})`, then change its level at launch time from the launchpad or a `RunRequest` with run config `loggers: {console: {config: {log_level: ERROR}}}`.

Write your own with `@dg.logger(config_schema, description=...)` returning a `logging.Logger`; Dagster metadata is on `record.__dict__["dagster_meta"]` with keys `dagster_event`, `dagster_event_batch_metadata`, `job_name`, `job_tags`, `log_message_id`, `log_timestamp`, `op_name`, `run_id`, `step_key`. There is no way to set a logger globally for every job in a code location; attach it per `Definitions` or per job.

For the service processes themselves, `--log-format json` on `dg dev`, `dagster-daemon run` and `dagster-webserver` emits machine-readable output for a log aggregator. Be selective about `managed_python_loggers` in production: flooding the event log storage makes UI pages slow to load.

## Debugging

**Breakpoints in a live run.** Add `context.pdb.set_trace()` to an asset, run `dg dev`, materialize from the UI. The asset stays in "Materializing" and a `pdb` prompt appears in the terminal running `dg dev`. `next` through to the end or `continue` to finish. Commands and output are also recorded in the run's stdout.

```python
# src/<project>/defs/assets.py
@dg.asset
def pdb_asset(context: dg.AssetExecutionContext):
    x = 10
    context.pdb.set_trace()
    x += 5
```

Set the breakpoint *before* the mutations you care about, since `pdb` cannot step backwards.

**Hangs and slow code: `py-spy`.** `py-spy dump` shows what every thread is doing right now, which usually pinpoints a hang immediately; `py-spy record -f speedscope --idle` produces a flame graph including idle time. Both need elevated permissions.

```shell
sudo py-spy record -f speedscope --idle -- dagster definitions validate
```

In Kubernetes, grant the run pod `SYS_PTRACE` temporarily, then exec in and dump PID 1. In the OSS Helm chart that is `runLauncher.config.k8sRunLauncher.runK8sConfig.containerConfig.securityContext.capabilities.add: [SYS_PTRACE]`; with a Dagster+ Kubernetes agent, the same block goes under `run_k8s_config.container_config` (and `server_k8s_config` for code servers) in `container_context.yaml`.

```shell
kubectl exec -it <pod> /bin/bash
pip install py-spy && py-spy dump --pid 1
```

**Validating definitions.** `dagster definitions validate` loads every code location, reports which are invalid, and exits 1 on error. It sets `DAGSTER_IS_DEFS_VALIDATION_CLI=1`, so code can branch to skip expensive connections during validation. `dg check defs` is the modern equivalent; see `project-setup-and-cli.md`.

**Sharing a broken run.** `dagster debug export <run_id> <file>` bundles a run's events and metadata for someone without instance access, who loads it with `dagster debug import` or views it with `dagster-webserver-debug`. Debug files also download from the UI.

**Reading logs directly.** Structured events live in the event log storage; compute logs live under `storage/<run_id>/compute_logs` locally or the configured bucket prefix remotely. If the UI says "No Log File Available" while the bucket has files, the config key is misspelled: it is `compute_logs`, never `computeLogs`.

## Webserver and UI

```shell
dg dev                                                   # local, webserver + daemon
dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml   # serve only the UI
```

`--read-only` disables mutations (launching, terminating, reloading) and `-l/--path-prefix` hosts the UI behind a reverse proxy at a subpath. Log flags are `--dagster-log-level` (Dagster events, default `info`), `--uvicorn-log-level` (the web server itself, default `warning`) and `--log-format` (`colored` default, `json`, `rich`). `--db-statement-timeout`, `--db-pool-recycle` and `--db-pool-max-overflow` tune the storage connection pool; `--live-data-poll-rate` throttles live asset-status polling on large graphs. `DAGSTER_WEBSERVER_PORT` and the other `DAGSTER_WEBSERVER_*` variables provide defaults for the corresponding flags.

Operational views worth knowing: **Deployment > Daemons** (heartbeats), **Deployment > Configuration** (resolved `dagster.yaml`), **Deployment > Concurrency** (pool limits, manual slot release), **Deployment > Code locations** (reload without restarting), **Runs > Queued / In Progress**, and per-run **Re-execute**. Per-user timezone, theme and experimental-feature toggles live behind the gear icon (OSS) or the user menu (Dagster+).

Sensor operations: **Overview > Sensors** to start/stop and inspect ticks, the sensor details page to preview a tick, and **Overview > Activity** to separate sensor-triggered runs from ad-hoc materializations. Three adjacent patterns, each with its own docs page:

- **Asset health.** Freshness policies give automatic staleness badges but no history; asset checks give pass/fail with `ERROR`/`WARN` severity; an aggregated report needs a scheduled asset that calls `instance.get_latest_materialization_event()` and scans the event log for `ASSET_CHECK_EVALUATION` events. Check authoring is in `testing-and-asset-checks.md`.
- **External streams.** Represent Kafka/Event Hub topics as `dg.AssetSpec` external assets plus a sensor emitting `AssetObservation` records for consumer lag, so stream health is a time series without Dagster owning the stream.
- **Custom launchers.** The Lambda run launcher example routes lightweight jobs off ECS by reading job tags such as `lambda/function_arn` plus an invocation type of `Event` (fire and forget) or `RequestResponse` (wait). One agent supports one run launcher, so mixed workloads need multiple agents with queue routing.

## Operational gotchas

- **SQLite is single-process.** Two processes writing one SQLite instance lock or corrupt it. Move to Postgres/MySQL once daemon, webserver and run workers are separate.
- **Nothing scheduled runs without the daemon.** Schedules, sensors, queued runs, backfills, automation conditions and run monitoring all live there. Webserver and daemon must share the same `dagster.yaml` and `DAGSTER_HOME`, or runs strand in `QUEUED` with no error.
- **`max_concurrent_runs` counts every run**, backfill and sensor runs included. `0` halts all launching: a deliberate pause switch and an easy accidental deadlock.
- **Pool limits are unlimited until set.** Adding `pool="db"` in code changes nothing until `concurrency.pools.default_limit` or an explicit `dagster instance concurrency set` exists.
- **Cancelled runs hold pool slots forever by default.** With a limit of 1 (DuckDB, single-writer warehouses) one cancelled run deadlocks the pool permanently. Always set `run_monitoring.free_slots_after_run_end_seconds` when using pools; clear an existing deadlock from **Deployment > Concurrency**.
- **Crash detection needs a real launcher.** `DefaultRunLauncher` cannot report run-worker health, so run monitoring cannot detect its crashes.
- **Compute logs need shared storage in multi-node deployments**, or the UI shows nothing while the files sit on a dead pod's disk.
- **`code_servers.local_startup_timeout` defaults to 180 seconds.** Slow-importing definitions are killed as hung; raise it rather than guessing.
- **Run queue order is not guaranteed under pool pressure.** Express ordering with `dagster/priority`.

## Legacy and superseded patterns

| Legacy | Current |
| --- | --- |
| `run_queue:` top-level key in `dagster.yaml` | `concurrency.runs.max_concurrent_runs` / `concurrency.runs.tag_concurrency_limits` (pre-1.10 instances may still need `run_queue` to enable queueing at all) |
| `concurrency.default_op_concurrency_limit` | `concurrency.pools.default_limit` |
| `dagster/concurrency_key` op tag | `pool=` argument on `@dg.asset` / `@dg.op` |
| `auto_materialize:` instance block | Automation condition sensors; see `declarative-automation.md` |
| `dagster dev`, `dagster-webserver -w workspace.yaml` | `dg dev` in a `create-dagster` project; `workspace.yaml` remains valid for multi-location OSS deployments |
| `dagster job launch/execute`, `@repository` | `dg launch`, `defs/` autoloading; see `project-setup-and-cli.md` |

## Sources

- https://docs.dagster.io/deployment/oss/dagster-yaml — full `dagster.yaml` specification
- https://docs.dagster.io/deployment/oss/oss-instance-configuration — `DAGSTER_HOME`, storage, launcher, coordinator, retention, daemon threading
- https://docs.dagster.io/deployment/oss/deployment-options/running-dagster-locally — `dg dev`, persistent local instance
- https://docs.dagster.io/deployment/execution/dagster-daemon — daemon list, heartbeats
- https://docs.dagster.io/deployment/execution/run-coordinators — default vs queued coordinator
- https://docs.dagster.io/deployment/execution/run-launchers — launcher catalog, run worker
- https://docs.dagster.io/deployment/execution/customizing-run-queue-priority — `dagster/priority`, queue ordering
- https://docs.dagster.io/deployment/execution/run-retries — retry tags and strategies
- https://docs.dagster.io/deployment/execution/run-monitoring — timeouts, crash resume, slot freeing; `job-timeouts` for `dagster/max_runtime`
- https://docs.dagster.io/deployment/execution/celery and .../dask — distributed executors
- https://docs.dagster.io/guides/operate/managing-concurrency — mechanism selection, plus the `concurrency-pools`, `run-queue-limits`, `run-tag-limits`, `run-executor-limits`, `advanced` and `troubleshooting-concurrency` subpages
- https://docs.dagster.io/guides/operate/run-executors — executor catalog and specification
- https://docs.dagster.io/guides/operate/webserver — webserver launch, UI reference; `ui-user-settings` for per-user settings
- https://docs.dagster.io/guides/log-debug/logging — structured vs compute logs, plus `python-logging` and `custom-logging` subpages
- https://docs.dagster.io/guides/log-debug/debugging/debugging-pdb — `context.pdb.set_trace()`
- https://docs.dagster.io/deployment/troubleshooting/py-spy-guide — profiling hangs; `no-log-file-available-error` and `sensor-timeouts` alongside it
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/full-deployments/full-deployment-settings-reference — `concurrency` and `run_monitoring` property tables
- https://docs.dagster.io/examples/best-practices/pii-compute-logs — redacting compute logs; `asset-health-monitoring`, `observing-streaming-processes` and `lambda-run-launcher` in the same section
- https://docs.dagster.io/guides/automate/sensors/monitoring-sensors-in-the-dagster-ui — sensor operations in the UI
- https://docs.dagster.io/api/clis/cli — `dagster instance`, `dagster debug`, `dagster-daemon`, `dagster-webserver` flags
- https://docs.dagster.io/api/dagster/loggers — `DagsterLogManager`, built-in loggers, `@dagster.logger`
