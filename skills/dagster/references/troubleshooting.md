# Troubleshooting

Symptom, likely causes, fix, verify — organized by lifecycle stage. Error text is quoted verbatim only where the docs quote it; entries whose exact wording is not in the sources describe the error class instead. This file does not teach the correct patterns: see `assets.md`, `partitions-and-backfills.md`, `resources-and-config.md`, `io-managers.md`, `components.md`, `schedules-and-sensors.md`, `declarative-automation.md`, `deployment-oss.md`, `dagster-plus.md`, `integrations-dbt-and-elt.md`, and `operations-and-debugging.md`.

## Contents
- [First 60 seconds](#first-60-seconds)
- [Definition load](#definition-load)
- [Launch and config](#launch-and-config)
- [Execution](#execution)
- [Automation not firing](#automation-not-firing)
- [Deployment](#deployment)
- [Integrations and components](#integrations-and-components)
- [Error class cheat sheet](#error-class-cheat-sheet)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## First 60 seconds

Run in order; stop at the first failure.

```bash
dg check yaml     # defs.yaml schema errors, with line/column, without importing user code
dg check defs     # loads every definition; exit code 1 on error
dg list defs      # what actually loaded
dg list envs      # env vars declared by the project's components
dg --version; uv run python -c "import dagster; print(dagster.__version__)"
uv run python -c "import my_project.definitions"   # raw import, full traceback
dagster instance info                              # storage and config actually in use
```

- `dg check defs` runs the project in its own `.venv`; a skew between `dg --version` and the in-venv `dagster` version explains many load failures. `--use-active-venv` forces the activated environment.
- `dg check defs --check-yaml` does both checks in one pass. `dg check defs` sets `DAGSTER_IS_DEFS_VALIDATION_CLI=1`, so code can skip expensive import-time work in validation mode.
- For a failed run, read the **Raw compute logs** tab; user stdout/stderr lands only there.
- For schedules, sensors, run queue, and backfills, the answer is in the daemon logs, not the run logs.

## Definition load

Use when `dg check defs` fails or the code location shows **Failed** in the UI.

### `ModuleNotFoundError` / `ImportError` in a code location

**Symptom** `ModuleNotFoundError: No module named 'my_project'`

**Causes**

- The project package is not installed in the venv `dg` uses. `uv sync` does *not* reinstall your own package; `uv pip install -e .` does.
- `[tool.dg.project] root_module` or `code_location_target_module` points at a nonexistent module. Defaults: `<root_module>.defs` and `<root_module>.definitions`.
- `dg` resolved a different interpreter. It assumes `.venv/bin/python` under the project root.
- A third-party import (`dagster_dbt`, `dagster_sling`) is missing from dependencies.

**Fix** `uv pip install -e .`, `uv add <integration>`, or `dg check defs --use-active-venv`. For a non-default venv, set the path in the project-root `.env` — `dg` reads this variable only from `.env`, never from the shell:

```shell
# my-project/.env
DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python
```

**Verify** `uv run python -c "import my_project.definitions" && dg check defs`

### `DagsterInvalidDefinitionError`

**Symptom** `dg check defs` exits 1 with `DagsterInvalidDefinitionError`. The class means the rules for a definition were violated; the message tail names the offending definition.

| Trigger | What to look for |
| --- | --- |
| Duplicate asset keys | Two `@asset` functions sharing a name, or a key produced by both a component and a hand-written asset. Autoloading imports every module under `defs/`. |
| Dependency on a key nothing produces | A typo in `deps=[...]`, or an upstream that lives in another code location and was never declared as an external asset. |
| Resource key required but not provided | An op or asset names a resource key absent from `Definitions(resources=...)`. |
| Conflicting definitions | A job, schedule, or sensor sharing a name with another, or two same-named jobs merged into one `Definitions`. |
| Schedule targets a missing job | The job was renamed, or lives in a `Definitions` that was never merged. |
| Invalid `partition_mapping` | The mapping is incompatible with the two partitions definitions it joins; see `partitions-and-backfills.md`. |
| Invalid asset name | `@asset` names must be valid Python identifiers. Use `key=` / `key_prefix=` for other characters or multi-segment keys. |

**Fix** Read the tail of the message for the key or name. `dg list defs` shows every loaded key and its source module, which settles duplicate-key questions immediately.  **Verify** `dg check defs && dg list defs`

### `DagsterInvariantViolationError` constructing `Definitions`

**Causes** `load_from_defs_folder(project_root=...)` given the `defs/` folder instead of the project root (the directory holding `pyproject.toml`); `definitions.py` exporting something other than a `Definitions`; two `Definitions` exported instead of merged.

**Fix**

```python
# src/my_project/definitions.py
from pathlib import Path

import dagster as dg
from my_project.assets import my_asset

defs = dg.Definitions.merge(
    dg.Definitions(assets=[my_asset]),
    dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
)
```

**Verify** `uv run python -c "import my_project.definitions as d; print(type(d.defs))"`

### `dg check yaml` pydantic `ValidationError`

**Symptom** File, line and column plus a schema message: unknown field, wrong type, missing required attribute, or unresolvable `type:`.  **Causes** `type:` must be a fully qualified `module.ClassName` (`dagster_dbt.DbtProjectComponent`) importable in the project venv; component settings belong under `attributes:`; an undefined Jinja variable surfaces as an `UndefinedError` rather than a schema error.  **Fix** Scaffold a known-good skeleton and edit it, with `--watch` for a fast loop.

```bash
dg scaffold defs dagster_dbt.DbtProjectComponent jdbt --project-path dbt/jdbt
dg check yaml --watch
```

**Verify** `dg check yaml` prints `All component YAML validated successfully.`

### Env var missing at load time

**Symptom** Loads locally, fails deployed; or a credential reads as `None`.  **Causes** `os.getenv` resolves when the code location loads and is visible in the UI, while `EnvVar` resolves at launch time and stays hidden — prefer `EnvVar` in resource config. Or the `.env` file is not in the folder where the webserver was launched, the only place Dagster loads it from, and the workspace must be reloaded after editing it. Or a component templates `{{ env.FOO }}` without declaring the requirement.  **Fix** Declare env requirements in the component file; `dg check yaml --validate-requirements` flags the gap:

```text
defs.yaml:1 - requirements.env Component uses environment variables that are not
specified in the component file: SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE,
SNOWFLAKE_PASSWORD, SNOWFLAKE_USER
```

**Verify** `dg list envs` shows each variable with a non-empty value.

### Circular imports in `defs/`

**Symptom** `ImportError: cannot import name 'x' from partially initialized module`  **Causes** Two `defs/` modules importing each other to share a resource or constant. Autoloading imports everything, so cycles a single entrypoint would hide become fatal.  **Fix** Move shared code out of `defs/` (into `src/<project>/components/` or a plain module) and import downward only. Express asset relationships with `deps=` / `AssetKey`, never by importing the other asset function.  **Verify** `dg check defs`

## Launch and config

### `DagsterInvalidConfigError`

**Symptom** The run fails at launch. Dagster validates run config against the corresponding pydantic model and aborts with `DagsterInvalidConfigError` or a pydantic `ValidationError`.  **Causes** A key absent from the schema; wrong nesting (`ops: {<op_name>: {config: {...}}}`, `resources: {<key>: {config: {...}}}`); a required field with no default and no env var behind it.

**Fix**

```bash
dg launch --assets my_asset -c run_config.yaml
dg launch --job my_job --config-json '{"ops": {"my_op": {"config": {"limit": 10}}}}'
```

**Verify** Re-run; a config error fails before any step starts.

### `DagsterInvalidSubsetError`

**Causes** The selection matches nothing (check spelling and key prefixes with `dg list defs`), or a `@multi_asset` was partially selected while declaring `can_subset=False`.  **Fix** Set `can_subset=True` on the multi-asset (and on `@multi_asset_check`) and branch on `context.selected_asset_keys`; otherwise select the whole multi-asset.  **Verify** `dg launch --assets <selection>` reaches execution.

### `DagsterUnknownResourceError`

**Causes** An op accessed a resource without listing it in `required_resource_keys`. With Pythonic resources the parameter name must match the key in `Definitions(resources={...})`.  **Fix** Add the key, or rename the parameter to match.  **Verify** `dg check defs && dg launch --assets <asset>`

### Partition key rejected

**Causes** `--partition` passed to an unpartitioned asset; the key format does not match the definition (daily wants `YYYY-MM-DD`, static wants a declared value); the key falls outside the definition's window.  **Fix** Use `--partition-range <start>...<end>` for a contiguous range. Key formats are in `partitions-and-backfills.md`.  **Verify** `dg launch --assets <asset> --partition 2024-01-01`

### `dg` targets the wrong project

**Causes** `dg` derives context from the nearest `dg.toml` / `pyproject.toml`. From a workspace root, `dg dev` and `dg check defs` cover every project; from a project directory, only that one.  **Fix** Pass `--target-path <dir>`, the directory holding that project's `dg.toml` or `pyproject.toml`.  **Verify** `dg list projects` from the workspace root.

### Runs stay `QUEUED`

**Causes** (OSS, most likely first) The daemon is not running — `dg dev` starts it, a bare webserver does not, so check **Deployment > Daemons** for a running **Run queue**. Or daemon and webserver read different storage, since both need the same `dagster.yaml` and therefore the same `DAGSTER_HOME`. Or `concurrency.runs.max_concurrent_runs` is `0`, or in-progress runs saturate the queue; check **Deployment > Configuration**. Or pool limits are exhausted, possibly by stale slots (next entry). In Dagster+, the Hybrid agent serving the deployment is down, which pauses runs.  **Fix** Raise the limit, or terminate blockers from **Runs > Queued / In Progress**.

```yaml
concurrency:
  runs:
    max_concurrent_runs: 15
```

**Verify** The daemon stops logging skipped runs and the queue drains.

### Slots held by cancelled runs deadlock a pool

**Symptom**

```text
Run <id> is blocked by global concurrency limits:
{"my_pool": {"pending_step_count": 1, "pending_step_run_ids": ["<cancelled_run_id>"]}}
```

with the blocking run in `CANCELED` or `FAILURE`.  **Causes** Dagster does not free op concurrency slots on cancellation or failure unless configured to. With a pool limit of 1 (common for single-writer stores like DuckDB), one stale claim blocks that pool permanently.  **Fix** Enable cleanup, then release the stuck slots.

```yaml
run_monitoring:
  enabled: true
  free_slots_after_run_end_seconds: 300
```

Release stuck slots now with `dagster instance concurrency set <pool_name> <limit>`, or from **Deployment > Concurrency** in the UI.  **Verify** Queued runs start; **Deployment > Concurrency** shows no held slots for the pool.

### Runs stuck in `STARTING` or `CANCELING`

**Causes** The run launcher could not start the worker (bad image, missing secret, no capacity, crash during container start), or the worker did not shut down cleanly.  **Fix** Enable run monitoring so these fail instead of hanging. It is always on in Dagster+ and configured in deployment settings.

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 7200   # per-run override: the dagster/max_runtime tag
```

**Verify** A deliberately broken run reaches `FAILURE` within the timeout.

## Execution

### `DagsterExecutionStepExecutionError`

**Causes** This is a framework wrapper, not the bug. The user-code error boundary catches exceptions from your code and re-raises them wrapped in a `DagsterUserCodeExecutionError` subclass with added runtime context.  **Fix** Read the innermost traceback frame. `is_user_code_error` is `True` for these.  **Verify** Reproduce in-process with `pytest` against the asset function; see `testing-and-asset-checks.md`.

### `DagsterTypeCheckDidNotPass`

**Causes** A `DagsterType` check returned `False` or a failing `TypeCheck`, with `raise_on_error=True` — the default for `job.execute_in_process()` and `graph.execute_in_process()`, which is the usual test path.  **Fix** Correct the value or loosen the declared `DagsterType`. The related `DagsterTypeCheckError` means an op received an unexpected input or produced an output not matching its output definition.  **Verify** `pytest` on the in-process execution.

### `DagsterExecutionInterruptedError` on Kubernetes

**Causes** On GKE Autopilot, pod preemption while a new node is still initializing: pods are scheduled before node init completes. It appears random and is more frequent for high-frequency jobs. Otherwise: eviction under resource pressure, or an OOM kill of the run worker.  **Fix** For preemption, set `max_retries` to 1 or more in deployment settings. `DAGSTER_DBT_CLOUD_POLL_TIMEOUT` and the `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` annotation do *not* fix preemption. For eviction, raise resource requests and pin scheduling — `tolerations` and `nodeSelector` go under `podSpecConfig`, not `jobSpecConfig`, because they are pod-spec fields:

```yaml
# Helm values
workspace:
  runK8sConfig:
    podSpecConfig:
      tolerations:
        - { key: 'example-key', operator: 'Equal', value: 'example-value', effect: 'NoSchedule' }
      nodeSelector: { workload-type: 'batch' }
```

Also enable run monitoring so crashed workers are detected and optionally resumed (`max_resume_run_attempts` > 0, supported with `K8sRunLauncher` + `k8s_job_executor` and `DockerRunLauncher` + `docker_executor`).  **Verify** Retried runs complete; run monitoring shows active in **Deployment > Daemons**.

### Retries behave unexpectedly

**Causes** Op retries (`RetryPolicy`, `op_retry_policy` on `define_asset_job`) apply per asset or op; run retries apply to the whole run and cover worker crashes. With both enabled, the op retry count resets on each retried run, multiplying attempts.

**Fix**

```yaml
run_retries:
  enabled: true                        # omit in Dagster+, where run retries are on by default
  max_retries: 3
  retry_on_asset_or_op_failure: false  # run retries only for crashes, not step failures
```

`dagster/retry_strategy` defaults to `FROM_FAILURE`, which needs an I/O manager readable across runs: on Kubernetes `s3_pickle_io_manager` works, `FilesystemIOManager` does not, since the retry is a new Job with its own filesystem. `FROM_ASSET_FAILURE` plus `retry_on_asset_or_op_failure: false` skips already-materialized assets using event-log records, which recovers dbt pipelines without persisted artifacts.  **Verify** Force a failure and count attempts in the run's retry group.

### `TypeError: cannot pickle ...` from the I/O manager

**Causes** The default `FilesystemIOManager` pickles outputs. Connections, file handles, clients, and lambdas are not picklable.  **Fix** Return plain data, or write to external storage and return a small handle. See `io-managers.md`.  **Verify** Re-run the asset; a `HANDLED_OUTPUT` event appears.

### Multiprocess executor import errors

**Causes** `multiprocess_executor` spawns a process per step, so definitions must be importable by module path, module-level side effects re-run in every child, and an unguarded script re-executes its body.  **Fix** Keep definitions in an installed package, guard script entrypoints with `if __name__ == "__main__":`, and reserve `in_process_executor` for debugging. `execute_in_process()` always overrides the job's executor with `in_process_executor`, which is why such a bug is invisible in tests.  **Verify** `dg launch --job my_job` under the default executor.

### "No Log File Available"

**Symptom** Both stdout and stderr tabs are empty and log links start with `/tmp/` instead of `s3:/`, even though logs reach the bucket.  **Causes** The config key is misspelled `computeLogs` instead of `compute_logs`.  **Fix**

```yaml
compute_logs:
  enabled: true
  custom:
    module: dagster_aws.s3.compute_log_manager
    class: S3ComputeLogManager
    config:
      bucket: your-s3-bucket-name
      region: us-east-1
      prefix: 'your-deployment-name'
```

Also confirm the bucket is in the organization's region and IAM permits reads.  **Verify** A new run shows content in both tabs and the log link starts with `s3:/`.

### Hanging or slow runs

**Fix** Profile with `py-spy`, which needs elevated permissions. `py-spy dump` gives a per-thread dump that usually pinpoints a hang; `py-spy record -f speedscope --idle` produces a flame graph for slow code, for example `sudo py-spy record -f speedscope --idle -- dagster definitions validate`. In Kubernetes, grant `SYS_PTRACE` to run pods temporarily (`runK8sConfig.containerConfig.securityContext.capabilities.add: [SYS_PTRACE]` under the `K8sRunLauncher` config), then `kubectl exec` in, `pip install py-spy`, and `py-spy dump --pid 1`.  **Verify** The dump names the blocking call.

### Measuring scheduling latency

Time the gap between `STEP_WORKER_STARTED` (worker ready) and `STEP_START` (op begins); a large gap means autoscaler delay or resource contention, not slow code. `STEP_WORKER_STARTING` marks the subprocess or pod launch. Query these through GraphQL (`runsOrError`, `CapturedLogs`).

## Automation not firing

### Schedule never runs

Work down this list; each step is cheap.

1. The schedule is stopped. Schedules start `STOPPED` unless `default_status` says otherwise. Check the **Running** toggle under **Automation**.
2. The last tick failed. A **Failed** badge in **Last tick** opens the stack trace.
3. The interval or timezone is wrong. Check **Next tick**, including its timezone. Schedules without `execution_timezone` run in UTC; schedules built from partitioned jobs use the partition definition's `timezone`.
4. The UI serves stale code. **Settings > Code locations > Reload definitions** (OSS) or **Deployment > Code locations > Redeploy** (Dagster+). A location that cannot load shows **Failed** with a **View error** link, and its schedules will not run.
5. The daemon is unhealthy (OSS). **Deployment > Daemons**, **Scheduler** row: "Not running" means the heartbeat stopped. Search the daemon logs for the schedule name or `SchedulerDaemon`.
6. Daemon logs saying the schedule cannot be found mean the daemon and webserver are reading different workspaces.

DST: for a daily schedule at a time that does not exist on the spring-forward day (2:30 AM), Dagster runs at the next existing time (3:00 AM).  **Verify** The next-tick value advances and a tick succeeds.

### Sensor times out

**Symptom** Ticks fail at the 60 second default evaluation timeout.  **Fix**, in the order the docs recommend: profile with `py-spy` (for a hanging external API call, timing out is the correct failure mode); split work across ticks with sensor cursors; if evaluations need more than about 3 minutes use a schedule instead, since sensors are for cheap frequent checks that kick off real work; only then raise the timeout, via `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS` in OSS or the `default_sensor_timeout` deployment setting in Dagster+, which applies to every sensor in the deployment including new ones.

Throughput bottlenecks look like timeouts. `DAGSTER_GRPC_MAX_WORKERS` sizes the code server's workers, `DAGSTER_CLOUD_AGENT_MAX_THREADPOOL_WORKERS` the Dagster+ agent's, and the daemon side lives in `dagster.yaml`:

```yaml
sensors:
  use_threads: true
  num_workers: 8
```

**Verify** Ticks succeed; with metrics enabled, agent logs show `Current agent threadpool utilization: N/20 threads` below saturation.

### Duplicate runs from a sensor

**Causes** No `run_key` on the `RunRequest`, so every evaluation launches again. `minimum_interval_seconds` only sets the floor between evaluations.  **Fix** Give each `RunRequest` a `run_key` identifying the work; Dagster skips requests whose run key was already used. Caveat: with both a `run_key` and a cursor, resetting the cursor relaunches nothing, because the run keys are unchanged. Omit `run_key` if cursor resets must re-fire.  **Verify** Re-running the tick yields `SKIPPED`, not a new run.

### Automation conditions never request runs

**Causes** `default_automation_condition_sensor` is off; it is created automatically but starts `STOPPED`, so toggle it on per code location under **Automation**. Or it was never created: it exists only when the location has at least one asset, asset check, or asset job carrying an `AutomationCondition` and the location loads and registers definitions successfully. Or you migrated from `AutoMaterializePolicy` without `use_sensors: true` in the `auto_materialize` config, which the UI toggle alone does not cover. Or a custom `AutomationConditionSensorDefinition` claimed the assets, since the default sensor covers only conditions no other sensor targets — asset *jobs* are always handled by the default sensor, as custom sensors cannot target jobs. Or the condition is genuinely false because upstream never updated or partitions do not line up; see `declarative-automation.md`.  **Fix** Enable the sensor, or set `default_status=RUNNING` on an explicit `AutomationConditionSensorDefinition`. The default sensor evaluates every 30 seconds. Arbitrary Python conditions require moving evaluation onto the user code server, since the daemon has no access to user code.  **Verify** The sensor's tick history shows evaluations and requested runs.

## Deployment

### Code location fails to deploy

| Symptom | Cause | Fix |
| --- | --- | --- |
| `/bin/sh: 1: ./dagster_cloud_pre_install.sh: Permission denied` | Script lacks the executable bit in git. | `git update-index --chmod=+x dagster_cloud_pre_install.sh`, commit, redeploy. |
| Deployment fails on container limits in one environment but not another | The Dagster+ agent config omits the `containerConfig` key. | Add `containerConfig` under `serverK8sConfig`, even empty, and redeploy the agent. |
| `ImagePullBackOff` with `401 Unauthorized` from `ghcr.io` | The cluster has no registry credentials; CI credentials do not apply to it. | Create a Kubernetes image pull secret in the `dagster-cloud` namespace and reference it from the deployment. |
| `DagsterImportError: Failed to initialize: Bad git executable` in a branch deployment | `dagster-dbt` needs a git executable absent from the image. | Pin to a known-good Dagster version; workaround is `GIT_PYTHON_REFRESH=quiet`. |
| `Failed to load code location` with `MemoryError`, `OutOfMemory`, or `Killed / exit 137`; pod shows `OOMKilled` or `CrashLoopBackOff` | Code server out of memory. | Increase code server container memory. |
| `UserCodeUnreachable`, gRPC server crashed, or "Can't connect to user code server", often after long import times | Code server crashed or its health check timed out. | Increase memory and CPU; cut import-time work. |

### Dagster+ Hybrid agent

`exit code 137` / `OOMKilled` in agent or code server logs is the strongest signal of insufficient memory. Correlate heartbeat timeouts with CPU spikes and restarts. Separate "the run never starts" (agent cannot schedule workers) from "the run starts, then dies" (worker or code server OOM). If lowering concurrency makes errors vanish, you were CPU/RAM saturated.

| Symptom | Fix |
| --- | --- |
| "Agent heartbeat timed out", "No healthy agents available" | Increase agent container CPU. |
| Repeated agent restarts with `OOMKilled` / `exit code 137` | Increase agent container memory. |
| `gRPC DeadlineExceeded` or `UNAVAILABLE` under load | Network egress is the bottleneck; see keepalive below. |
| Sensors or schedules over a large asset graph timing out | Increase code server CPU. |

Read timeouts and connection resets against the Dagster+ API usually mean NAT gateways or proxies dropping idle connections:

```text
dagster_cloud_cli.core.errors.GraphQLStorageError: HTTPSConnectionPool(host='{organisation}.agent.dagster.cloud', port=443): Read timed out. (read timeout=60)
```

Fix with TCP keepalive in the agent Helm values, then `helm upgrade`:

```yaml
dagsterCloud:
  socketOptions:
    - ['SOL_SOCKET', 'SO_KEEPALIVE', 1]
    - ['IPPROTO_TCP', 'TCP_KEEPIDLE', 11]
    - ['IPPROTO_TCP', 'TCP_KEEPINTVL', 7]
    - ['IPPROTO_TCP', 'TCP_KEEPCNT', 5]
```

Duplicate code locations after an agent restart come from disrupted agent-to-Dagster+ communication. Use `kubectl rollout restart` rather than deleting and recreating pods; apply config changes with `kubectl apply` first, then restart. Existing duplicates must be cleaned up in the UI.

### GitHub Actions CI

| Symptom | Fix |
| --- | --- |
| Cannot build `cryptography` wheels, "no compatible interpreter", or Python 3.10 not found | Use `ubuntu-latest` or `ubuntu-22.04` with Python 3.11 and pip caching. These are the officially supported images for the PEX-based Dagster+ action. |
| The same failures on a self-hosted Ubuntu 20.04 runner you cannot replace | Install the `dagster-cloud` CLI directly instead of the PEX file, as the first step, so branch-deployment cleanup can use it too. |

### `UnauthorizedError` from the GraphQL API

**Causes** Wrong endpoint URL, mismatched repository selector, expired token, or insufficient token permissions.  **Fix** Include the deployment name in the URL and send the token in the `Dagster-Cloud-Api-Token` header. Confirm `repositoryName` and `repositoryLocationName` match the deployment exactly.

```python
import gql
from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url="https://your-org.dagster.cloud/prod/graphql",
    headers={"Dagster-Cloud-Api-Token": YOUR_USER_TOKEN},
)
gql_client = gql.Client(transport=transport)
```

### Database growth and instance health

`event_logs` dominates a Dagster Postgres database. Delete DEBUG logs older than a week and INFO/WARNING older than two months, plus transitory event types (`ENGINE_EVENT`, `HANDLED_OUTPUT`, `LOADED_INPUT`, `STEP_INPUT`, `STEP_OUTPUT`, `STEP_WORKER_STARTING`, `RESOURCE_INIT_*`, `PIPELINE_ENQUEUED`, `ASSET_MATERIALIZATION_PLANNED`). Three warnings: deletion writes transaction logs, so disk usage rises before it falls, and a full disk turns the database read-only and stops Dagster — scale up *before* cleanup if you are near full; `VACUUM` marks space reusable but does not return it to the OS, so use `pg_repack` to actually shrink; create the two partial indexes on `event_logs` first, since they cut the cleanup job from hours to minutes. Related: `dagster instance migrate` after an upgrade, `dagster instance reindex` for historical-run query performance. SQLite storage is the default and is single-writer, so use Postgres or MySQL for any multi-process deployment.

### Fewer alerts than degraded assets

Asset health alerts fire once when the health *status changes*, not once per degraded asset, and the notification service may batch or rate-limit. Split one broad policy into several scoped by asset group or criticality.

## Integrations and components

### `DagsterDbtManifestNotFoundError`

**Symptom** `DagsterDbtManifestNotFoundError: /path/to/target/manifest.json does not exist.`  **Causes** `DbtProjectComponent` reads `manifest.json` at load time, and CI shipped the project without running `dbt parse`.  **Fix** Generate the manifest before deploying — `dbt deps && dbt parse` as a CI step before the deploy, or as a `RUN` layer in the Dockerfile so it is baked into the image.  **Verify** `dg check defs` in an environment built the same way as CI.

### `DbtCliResource` pydantic validation error from the Launchpad

**Symptom**

```text
pydantic_core._pydantic_core.ValidationError: 1 validation error for DbtCliResource
project_dir
  Value error, The absolute path of '...') does not exist
```

Often intermittent, with some retries succeeding.  **Causes** The dbt project's absolute path changes on redeploy, so saved Launchpad config points at a stale path.  **Fix** Delete the whole `resources` block from the Launchpad config (or just the `dbt` key) and launch with code defaults.  **Verify** The run launches with no resource config override.

### `dg list components` omits a component type

**Causes**, in order: the providing package is not installed in the project venv (`uv add dagster-dbt`); a local component's module is not in `[tool.dg.project] registry_modules` (default `<root_module>.components`) or the package does not expose a `dagster_dg_cli.registry_modules` entry point; the package was not reinstalled after adding that entry point (`uv sync` will not do it — use `uv pip install -e .`); `dg` is using a different venv.  **Fix** Install the package, add the module to `registry_modules`, then reinstall with `uv pip install -e .`.  **Verify** `dg list registry-modules && dg list components`

### `dg scaffold defs` writes somewhere unexpected

**Causes** Scaffolding targets `[tool.dg.project] defs_module`, default `<root_module>.defs`, in the project resolved from the current directory.  **Fix** Run from the intended project directory, pass `--target-path`, or set `defs_module` explicitly for a non-standard layout.  **Verify** `dg list defs` shows the new definition under the expected key.

## Error class cheat sheet

All framework errors inherit from `DagsterError`. `DagsterUserCodeExecutionError` subclasses wrap exceptions thrown by your code, adding runtime context; both expose `is_user_code_error`.

| Error | Means |
| --- | --- |
| `DagsterInvalidDefinitionError` | Definition rules violated (duplicate keys, missing deps, bad names). Load time. |
| `DagsterInvariantViolationError` | A runtime-enforced invariant was broken. |
| `DagsterInvalidConfigError` | Config does not type-check against the schema. |
| `DagsterInvalidConfigDefinitionError` | A config *type* was constructed from an invalid value. |
| `DagsterInvalidSubsetError` | Selected ops do not exist on the job, or the subset is invalid. |
| `DagsterUnknownResourceError` | Resource accessed without being declared in `required_resource_keys`. |
| `DagsterResourceFunctionError` | The resource's init function raised. |
| `DagsterExecutionStepExecutionError` | Wrapper around a user exception in a step body. |
| `DagsterExecutionStepNotFoundError` / `DagsterStepOutputNotFoundError` | Named step keys do not exist / upstream step outputs are unavailable. |
| `DagsterTypeCheckDidNotPass` | A `DagsterType` check failed under `raise_on_error=True`. |
| `DagsterTypeCheckError` | Op input or output did not match its declared type at runtime. |
| `DagsterSubprocessError` | One or more managed child processes failed; forwards all collected errors. |
| `DagsterUnmetExecutorRequirementsError` | The resolved executor is incompatible with the instance or storage config. |
| `DagsterConfigMappingFunctionError` | A config mapping function raised during config parsing. |
| `DagsterRunNotFoundError` / `DagsterEventLogInvalidForRun` | Run missing from run storage / malformed historical event log. |

## Legacy and superseded patterns

| Old | Current |
| --- | --- |
| `dagster definitions validate` | `dg check defs`, which adds `--check-yaml`, `--target-path`, `--use-active-venv` |
| `dagster dev` plus `workspace.yaml` | `dg dev` from a project or workspace root; the daemon starts with it |
| Workspace mismatch debugging via `workspace.yaml` | `dg.toml` workspace plus `[tool.dg.project]` per project |
| `AutoMaterializePolicy` | `AutomationCondition` and `default_automation_condition_sensor`; migration needs `use_sensors: true` |
| `computeLogs` Helm key | `compute_logs` |
| Manually freeing concurrency slots after every cancellation | `run_monitoring.free_slots_after_run_end_seconds` |

## Sources
- https://docs.dagster.io/deployment/troubleshooting — index of the troubleshooting pages
- https://docs.dagster.io/deployment/troubleshooting/code-location-deployment-failures — pre-install script permissions
- https://docs.dagster.io/deployment/troubleshooting/code-location-deployment-containerconfig — missing `containerConfig`
- https://docs.dagster.io/deployment/troubleshooting/imagepullbackoff-error — ghcr.io image pull secrets
- https://docs.dagster.io/deployment/troubleshooting/git-executable-error-branch-deployment-code-locations — `Bad git executable`
- https://docs.dagster.io/deployment/troubleshooting/dagsterexecutioninterruptederrorgke-autopilot-clusters — preemption vs eviction
- https://docs.dagster.io/deployment/troubleshooting/managing-pod-eviction — resources, annotations, `podSpecConfig` placement
- https://docs.dagster.io/deployment/troubleshooting/no-log-file-available-error — `compute_logs` key
- https://docs.dagster.io/deployment/troubleshooting/sensor-timeouts — 60s timeout, cursors, worker counts
- https://docs.dagster.io/deployment/troubleshooting/py-spy-guide — dump and record, `SYS_PTRACE`
- https://docs.dagster.io/deployment/troubleshooting/scheduling-latency-gke — step worker events
- https://docs.dagster.io/deployment/troubleshooting/hybrid-optimizing-troubleshooting — agent and code server symptom tables, TCP keepalive
- https://docs.dagster.io/deployment/troubleshooting/database-tuning — event log cleanup, VACUUM, pg_repack
- https://docs.dagster.io/deployment/troubleshooting/dbtprojectcomponent-missing-manifest-json — `dbt parse` in CI
- https://docs.dagster.io/deployment/troubleshooting/dbt-project-pydantic-validation-errors — stale Launchpad config
- https://docs.dagster.io/deployment/troubleshooting/unauthorized-error-graphql-api — endpoint, selector, token
- https://docs.dagster.io/deployment/troubleshooting/degraded-assets-pagerduty-alerts — alert batching
- https://docs.dagster.io/deployment/troubleshooting/github-ci-cd-self-hosted-ubuntu-20-04-runners — self-hosted runner workaround
- https://docs.dagster.io/deployment/troubleshooting/pex-compatibility-issues-github-actions — supported runner images
- https://docs.dagster.io/deployment/execution/run-monitoring — timeouts, slot freeing, resuming runs
- https://docs.dagster.io/deployment/execution/run-retries — retry strategies, combining op and run retries
- https://docs.dagster.io/guides/automate/schedules/troubleshooting-schedules — schedule diagnosis steps
- https://docs.dagster.io/guides/automate/schedules/customizing-execution-timezone — DST behavior
- https://docs.dagster.io/guides/operate/managing-concurrency/troubleshooting-concurrency — QUEUED runs, stale slots
- https://docs.dagster.io/guides/operate/managing-concurrency/concurrency-pools — pool limits and granularity
- https://docs.dagster.io/guides/operate/managing-concurrency/run-queue-limits — `max_concurrent_runs`
- https://docs.dagster.io/guides/operate/run-executors — executor selection, `execute_in_process` override
- https://docs.dagster.io/guides/operate/configuration/run-configuration — config validation errors
- https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets — `os.getenv` vs `EnvVar`, `.env` rules
- https://docs.dagster.io/guides/build/components/using-environment-variables-in-components — `dg check yaml --validate-requirements`
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/adding-component-definitions — `dg list components`, `dg scaffold defs`
- https://docs.dagster.io/guides/build/projects/moving-to-components/migrating-project — registry module entry points, reinstalling the package
- https://docs.dagster.io/guides/automate/declarative-automation/automation-condition-sensors — default sensor creation conditions
- https://docs.dagster.io/guides/automate/sensors — `run_key`, cursors, `minimum_interval_seconds`
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-configuration — `[tool.dg.project]` keys, `DG_PROJECT_PYTHON_EXECUTABLE`
- https://docs.dagster.io/api/dagster/errors — error class semantics
