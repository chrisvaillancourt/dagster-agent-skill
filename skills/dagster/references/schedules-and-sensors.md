# Schedules and sensors

Imperative automation: cron-driven `ScheduleDefinition` / `@dg.schedule`, event-driven `@dg.sensor`, `@dg.asset_sensor`, `@dg.multi_asset_sensor`, run-status sensors, and the jobs they target. `AutomationCondition` and the automation condition sensor are in `declarative-automation.md` — prefer that file for asset-centric automation. Partition mappings, backfills, and `end_offset` semantics are in `partitions-and-backfills.md`. Op jobs and graphs are in `ops-jobs-and-graphs.md`. Slack/email alert integrations are in `integrations-catalog.md`. Daemon deployment is in `deployment-oss.md`.

## Contents

- [Choosing an automation method](#choosing-an-automation-method)
- [Jobs as automation targets](#jobs-as-automation-targets)
- [Schedules](#schedules)
- [Cron, timezones, and DST](#cron-timezones-and-dst)
- [Schedules for partitioned jobs](#schedules-for-partitioned-jobs)
- [Sensors](#sensors)
- [Asset sensors](#asset-sensors)
- [Run status sensors](#run-status-sensors)
- [Resources and logging](#resources-and-logging)
- [Enabling and operating](#enabling-and-operating)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Choosing an automation method

| Method | Trigger | Works with | Use when |
| --- | --- | --- | --- |
| Schedule | Cron expression | Assets, ops, graphs | Fixed wall-clock cadence; regular time-based job runs |
| Declarative automation | `AutomationCondition` on the asset | Assets, asset checks, asset jobs | Asset-centric, condition-based updates. Default choice for asset graphs — see `declarative-automation.md` |
| Sensor | Your Python polling an external system | Assets, ops, graphs | Event-driven: a file lands, an API flips, another system finishes |
| Asset sensor | Materialization of a specific asset | Assets only | A side effect (notification) or an imperative job launch after a materialization |
| Run status sensor | A run reaches a `DagsterRunStatus` | Jobs, code locations | Alerting, cross-job chaining, cross-code-location monitoring |
| GraphQL trigger | External push | Assets, ops, jobs | An external system should push instead of Dagster polling |

Rule of thumb: if the goal is "keep these assets up to date relative to their dependencies", use a declarative `AutomationCondition`. If the goal is "run this thing at 2am" or "react to something outside Dagster", use a schedule or sensor.

## Jobs as automation targets

Use when a schedule or sensor must launch a fixed selection of assets, or when you need run-level config, tags, or a specific executor.

```python
# src/<project_name>/defs/jobs.py
import dagster as dg

all_assets_job = dg.define_asset_job(name="all_assets_job")

sugary_cereals_job = dg.define_asset_job(
    name="sugary_cereals_job",
    selection="sugary_cereals",  # or ["a", "b"], AssetKey list, or dg.AssetSelection
    tags={"team": "analytics"},
)
```

`define_asset_job(name, selection=..., config=, tags=, run_tags=, metadata=, description=, executor_def=, op_retry_policy=, hooks=, owners=)`. `selection` accepts a selection string (`"my_asset*"` selects the asset and everything downstream in the code location), a sequence of strings or `AssetKey`s, `AssetsDefinition` objects, or an `dg.AssetSelection`. With no `selection`, the job targets every asset in the project. It returns an `UnresolvedAssetJobDefinition`, resolved to a `JobDefinition` once loaded in a project.

`tags` annotate the job and, unless `run_tags` is set, are copied onto every run it launches. Set `run_tags` when the two should differ.

Placement: jobs live anywhere under `src/<project>/defs/` and are autoloaded by `dg` — no `Definitions` object needed. A code location also picks up any job that a loaded schedule or sensor targets, so a job referenced only from a schedule still appears in the UI. Verify with `dg list defs` and `dg check defs`.

Many schedules and sensors no longer need a job at all: `target=` accepts asset selections directly (see below). Reach for `define_asset_job` when you want the job to be a named, launchable, configurable unit.

## Schedules

Use when runs should fire on a cron cadence.

Scaffold: `dg scaffold defs dagster.schedule path/to/schedules.py` (the generated stub returns a `SkipReason` until you edit it).

Declarative form — no evaluation function, just a target and a cadence:

```python
# src/<project_name>/defs/schedules.py
import dagster as dg

daily_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    cron_schedule="0 0 * * *",
    target=[customer_data, sales_report],  # or "*", or dg.AssetSelection.groups("g")
    execution_timezone="America/Los_Angeles",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    tags={"team": "analytics"},
)
```

`target=` accepts anything coercible to an `AssetSelection` (`"*"`, a selection string, a sequence of strings, `AssetKey`, `AssetsDefinition`), plus a `JobDefinition` or the `UnresolvedAssetJobDefinition` returned by `define_asset_job`. It replaces the older `job=` and `job_name=` parameters. An asset `target=` runs an anonymous job named `__anonymous_asset_job_<schedule_name>`, which is not listed as a job by `dg list defs` or the Jobs page and cannot be launched by name; use `define_asset_job` when the job itself must be visible, launchable, or configurable. `dg list defs` shows a schedule's cron but not its `execution_timezone`; confirm the timezone in Python or the UI. `ScheduleDefinition` also takes `run_config=` / `run_config_fn=`, `tags_fn=`, `should_execute=`, `description=`, `metadata=`, and `required_resource_keys=`. Only one of `run_config`, `run_config_fn`, or `execution_fn` may be set; likewise only one of `tags`, `tags_fn`, or `execution_fn`.

Decorator form — use when the run request depends on evaluation time, resources, or conditional logic:

```python
# src/<project_name>/defs/schedules.py
import dagster as dg


class AssetConfig(dg.Config):
    scheduled_date: str


@dg.schedule(target=configurable_asset, cron_schedule="0 0 * * *")
def configurable_job_schedule(context: dg.ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=None,
        run_config={
            "ops": {"configurable_asset": {"config": {"scheduled_date": scheduled_date}}}
        },
        tags={"date": scheduled_date},
    )
```

The decorated function may return a `RunRequest`, a list of them, a `SkipReason`, a run config dict, or nothing (a silent skip); it may also yield `RunRequest` / `SkipReason`. `context.scheduled_execution_time` is the tick time the schedule was meant to fire at, which can differ slightly from actual execution time — always use it, never `datetime.now()`, so that catch-up ticks compute the right config.

`RunRequest(run_key=, run_config=, tags=, job_name=, asset_selection=, asset_check_keys=, partition_key=, stale_assets_only=)`. For schedules, `run_key` deduplicates across failure recovery within a tick; `run_key=None` means one run per evaluation. `job_name` is required when a sensor or schedule targets multiple jobs.

## Cron, timezones, and DST

`cron_schedule` takes a standard cron string, or a sequence of cron strings whose execution times are unioned (`["45 23 * * 6", "30 9 * * 0"]`). With `croniter >= 1.0.12` the aliases `@daily`, `@hourly`, and `@monthly` are also accepted; the scaffolded schedule defaults to `@daily`.

Schedules without `execution_timezone` run in UTC. The parameter accepts any IANA tz name and is available on `@dg.schedule`, `ScheduleDefinition`, and `dagster_dbt.build_schedule_from_dbt_selection`. Schedules built from a partitioned job instead inherit the `timezone` set on the partitions definition.

DST behavior:

- A daily schedule at a time that does not exist on the spring-forward day (2:30 AM) runs at the next existing time (3:00 AM).
- A daily schedule at a time that exists twice on the fall-back day (1:30 AM) runs at the second occurrence.
- Hourly schedules are unaffected: they fire once per hour, including both repeats of the 1:00–2:00 AM hour.

## Schedules for partitioned jobs

For a time-partitioned job, `dg.build_schedule_from_partitioned_job(job)` builds a schedule whose interval matches the partition spacing and whose each tick targets the latest complete partition.

```python
# src/<project_name>/defs/schedules.py
partitioned_asset_job = dg.define_asset_job("partitioned_job", selection=[daily_asset])

asset_partitioned_schedule = dg.build_schedule_from_partitioned_job(
    partitioned_asset_job, hour_of_day=1, minute_of_hour=30
)
```

`minute_of_hour`, `hour_of_day`, `day_of_week`, and `day_of_month` shift the tick within the interval. A daily schedule evaluating on 2024-05-20 targets partition `2024-05-19`, because a partition joins the set only once its time window closes; change that with `end_offset` on the partitions definition (see `partitions-and-backfills.md`).

For static partitions, or when the schedule cadence differs from the partition cadence, write the schedule by hand and set `partition_key` on the `RunRequest`:

```python
@dg.schedule(cron_schedule="0 0 * * *", job=continent_job)
def continent_schedule():
    for c in CONTINENTS:
        yield dg.RunRequest(run_key=c, partition_key=c)
```

To resolve "latest partition as of this tick", wrap the lookup in a partition loading context — `get_last_partition_key` takes no `current_time` argument:

```python
with dg.partition_loading_context(effective_dt=context.scheduled_execution_time):
    last_key = partitions_def.get_last_partition_key()
```

## Sensors

Use when a run should fire in response to something Dagster cannot see on a clock: a new file, an external API state change, another system finishing.

Scaffold: `dg scaffold defs dagster.sensor sensors.py`.

```python
# src/<project_name>/defs/sensors.py
import dagster as dg

my_job = dg.define_asset_job("my_job", selection=[my_asset])


@dg.sensor(
    target=my_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def new_file_sensor():
    new_files = check_for_new_files()
    if new_files:
        for filename in new_files:
            yield dg.RunRequest(run_key=filename)
    else:
        yield dg.SkipReason("No new files found")
```

`@dg.sensor(name=, target=, minimum_interval_seconds=, description=, default_status=, required_resource_keys=, tags=, metadata=, owners=)`. `target=` supersedes `job=`, `jobs=`, and `asset_selection=`, and accepts the same things a schedule's `target` does. The body returns or yields `RunRequest` objects, a `SkipReason`, or nothing.

`minimum_interval_seconds` is a floor, not a period: if the evaluation function takes longer than the interval, the next tick is delayed accordingly.

Idempotency: a single run is created per unique `run_key` for a given sensor, and requests reusing a previous key are skipped. Use the value that identifies the work (`filename`, `f"{filename}:{mtime}"`, an S3 key).

Cursors carry arbitrary state across ticks — use them instead of rescanning everything when event volume is high:

```python
@dg.sensor(target=my_job, minimum_interval_seconds=30)
def updated_file_sensor(context: dg.SensorEvaluationContext):
    last_mtime = float(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    for filename in os.listdir(MY_DIRECTORY):
        file_mtime = os.stat(os.path.join(MY_DIRECTORY, filename)).st_mtime
        if file_mtime <= last_mtime:
            continue
        yield dg.RunRequest(run_key=f"{filename}:{file_mtime}", run_config={})
        max_mtime = max(max_mtime, file_mtime)
    context.update_cursor(str(max_mtime))
```

For multi-stream sensors, serialize a dict into the cursor with `json.dumps` and parse it back on the next tick.

`dg.SensorResult(run_requests=, skip_reason=, cursor=, dynamic_partitions_requests=, asset_events=)` is the structured alternative to yielding. Use it when you need to do more than request runs on one tick: `dynamic_partitions_requests` takes `dg.AddDynamicPartitionsRequest` / `dg.DeleteDynamicPartitionsRequest` (run requests in the same result are evaluated against the partition set with those changes applied; keep additions and deletions under about 25k per evaluation), and `asset_events` persists `AssetMaterialization`, `AssetObservation`, or asset check evaluations not tied to any run.

The default sensor evaluation timeout is 60 seconds. Design around it rather than raising it: profile with `py-spy`, split expensive work across ticks using the cursor, and if evaluations genuinely need more than about 3 minutes, move the work into a scheduled job that emits run requests instead. Last resort: `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS` in OSS, or the `default_sensor_timeout` deployment setting in Dagster+.

## Asset sensors

Use when code should run after a specific asset materializes and declarative automation does not fit — a side effect, cross-code-location imperative launch, or a decision that depends on materialization metadata.

```python
# src/<project_name>/defs/sensors.py
@dg.asset_sensor(asset_key=dg.AssetKey("daily_sales_data"), job=my_job)
def daily_sales_data_sensor(
    context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry
):
    materialization = asset_event.dagster_event.event_specific_data.materialization
    if "specific_property" in materialization.metadata:
        yield dg.RunRequest(run_key=context.cursor)
    else:
        yield dg.SkipReason("Materialization missing the required property")
```

The evaluation function takes the context plus an `EventLogEntry` for the materialization. If the asset materialized several times since the last tick, the function is invoked once, with the latest materialization only. `@dg.asset_sensor` also accepts `name=`, `minimum_interval_seconds=`, `job=` / `jobs=`, `default_status=`, `tags=`, `metadata=`. Asset sensors work across jobs and across code locations: the watched asset need not live where the sensor does.

To pass materialization data into the launched run, build `run_config` from the metadata, for example `dg.RunConfig(ops={"weekly_report": MyConfig(param1=str(materialization.metadata.get("specific_property")))})`.

`@dg.multi_asset_sensor(monitored_assets=[...], job=...)` waits on several assets at once:

```python
@dg.multi_asset_sensor(
    monitored_assets=[dg.AssetKey("asset_a"), dg.AssetKey("asset_b")], job=asset_job
)
def asset_a_and_b_sensor(context):
    asset_events = context.latest_materialization_records_by_key()
    if all(asset_events.values()):
        context.advance_all_cursors()
        return dg.RunRequest(run_key=context.cursor, run_config={})
    return None
```

`@dg.multi_asset_sensor` is deprecated — it keeps working until Dagster 2.0, but `AutomationCondition` supersedes it. Prefer a declarative condition for "run X when A and B have both updated"; see `declarative-automation.md`. Keep a sensor when it performs side effects, inspects materialization metadata, builds per-event dynamic `run_config`, or must count multiple materializations before firing (conditions are boolean per tick, not counters).

## Run status sensors

Use when the trigger is a run reaching a status: alerting on failure, reporting success, or chaining an independent downstream job.

```python
# src/<project_name>/defs/sensors.py
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    request_job=status_reporting_job,
)
def report_status_sensor(context: dg.RunStatusSensorContext):
    # guard against the sensor retriggering on its own request_job
    if context.dagster_run.job_name != status_reporting_job.name:
        return dg.RunRequest(run_key=None, run_config={})
    return dg.SkipReason("Don't report status of status_reporting_job")


@dg.run_failure_sensor
def my_failure_sensor(context: dg.RunFailureSensorContext):
    alert(f'Job "{context.dagster_run.job_name}" failed: {context.failure_event.message}')
```

`DagsterRunStatus` values: `QUEUED`, `NOT_STARTED`, `MANAGED`, `STARTING`, `STARTED`, `SUCCESS`, `FAILURE`, `CANCELING`, `CANCELED`.

Context surface: `context.dagster_run`, `context.dagster_event`, `context.instance`, `context.log`, `context.partition_key`, `context.sensor_name`. `RunFailureSensorContext` adds `failure_event` and `get_step_failure_events()`.

Scope with `monitored_jobs=` (job definitions, or `dg.JobSelector(location_name=, repository_name=, job_name=)` / `dg.RepositorySelector` / `dg.CodeLocationSelector` for other code locations), or `monitor_all_code_locations=True` for the whole deployment. Setting both raises an error. With neither, the sensor fires for any job in its own code location only. `request_job=` / `request_jobs=` name what a returned `RunRequest` should launch.

Once `monitored_jobs` is set, runs from the sensor's own location match only the job definitions listed in it, so a list holding only other locations' selectors stops watching the home location. List the home location's selector too. One failure sensor can then cover several locations, instead of a copy per location with duplicated routing config:

```python
# lives in the "alerts" location; watches it and "analytics"
@dg.run_failure_sensor(
    monitored_jobs=[
        dg.CodeLocationSelector(location_name="alerts"),
        dg.CodeLocationSelector(location_name="analytics"),
    ],
)
def failure_alerts(context: dg.RunFailureSensorContext, notifier: NotifierResource):  # your ConfigurableResource
    notifier.send(f"{context.dagster_run.job_name} failed: {context.failure_event.message}")
```

`location_name` is compared by equality with the deployed location's name. In-process tests can't check this scoping. Calling the sensor with `build_run_status_sensor_context(...)` skips matching entirely, so it fires whatever `monitored_jobs` contains. `sensor.evaluate_tick(build_sensor_context(instance=...))` does run the matching, but an `execute_in_process` run and the test sensor both have location `None`: the run counts as own-location, and no `CodeLocationSelector` (right name or typo) ever matches it. Check cross-location scoping in a real deployment or a multi-location `dg dev` workspace.

Returning nothing is fine — Dagster reports an event back to the monitored run indicating the sensor ran. For Slack and email alerting, prefer the prebuilt `dagster_slack.make_slack_on_run_failure_sensor` and `dg.make_email_on_run_failure_sensor` (see `integrations-catalog.md`).

To fan in from several independent upstream jobs, use a plain `@dg.sensor` that queries `context.instance.get_run_records(filters=dg.RunsFilter(job_name=..., statuses=[dg.DagsterRunStatus.SUCCESS]), limit=1, order_by="update_timestamp", ascending=False)` for each upstream and stores their completion times in the cursor, so the downstream job fires exactly once per batch.

## Resources and logging

Annotate a resource as a parameter of the schedule or sensor function; it is supplied from the code location's resources.

```python
@dg.sensor(job=process_user)
def process_new_users_sensor(context: dg.SensorEvaluationContext, users_api: UsersAPI):
    last_user = int(context.cursor) if context.cursor else 0
    users = users_api.fetch_users()
    for user_id in users[last_user:]:
        yield dg.RunRequest(run_key=user_id, tags={"user_id": user_id})
    context.update_cursor(str(len(users)))
```

The same works for `@dg.schedule` (`context.scheduled_execution_time` plus a `DateFormatter` resource, for example). Resource definitions come from `Definitions(resources={...})`; see `resources-and-config.md`.

`context.log.info(...)` inside either a schedule or a sensor emits logs visible on that tick in the tick-history view. Sensor and schedule logs land in the instance's compute log storage, so that must be configured for them to be viewable.

## Enabling and operating

- The `dagster-daemon` process must be running: the scheduler daemon creates runs from active schedules, and the sensor daemon (always enabled) creates runs from active sensors. Locally, `dg dev` starts both webserver and daemon.
- Schedules and sensors are off when first deployed unless `default_status=dg.DefaultScheduleStatus.RUNNING` / `dg.DefaultSensorStatus.RUNNING`. `DefaultSensorStatus.STOPPED` is explicit off. The UI and GraphQL API override the default either way. `default_status` applies only while no state is stored: once someone toggles the sensor or schedule, the stored state wins and changing `default_status` in code changes nothing until the stored state is reset.
- Toggle and inspect under **Automation** in the UI: the **Running** toggle, the **Last tick** column (a **Failed** badge links to the stack trace), and **Next tick** for the upcoming fire time and timezone.
- Sensor pages show tick history, skip reasons, errors, and the runs each sensor launched. **Overview > Activity** separates sensor-triggered runs from ad-hoc materializations.
- `dg check defs` validates that definitions load; `dg list defs` shows what the code location exposes.

## Testing

Invoke the decorated definition as a plain Python function and assert on what it returns.

```python
# tests/test_schedules.py
import datetime
import dagster as dg


def test_configurable_job_schedule():
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime.datetime(2020, 1, 1)
    )
    run_request = configurable_job_schedule(context)
    assert dg.validate_run_config(configurable_job, run_request.run_config)
```

- `dg.build_schedule_context(instance=, scheduled_execution_time=)` — the instance, if given, must be persistent; `DagsterInstance.ephemeral()` raises. Drop the argument entirely if the schedule function takes no context.
- `dg.build_sensor_context(instance=, cursor=, repository_def=, definitions=, resources=, last_sensor_start_time=)` — same persistence rule. Pass fake resources as keyword arguments to the sensor call: `process_new_users_sensor(context, users_api=FakeUsersAPI())`.
- `dg.validate_run_config(job, run_request.run_config)` checks a generated config against the job's schema.
- Run status sensors need a real run to build their context: execute a job with `execute_in_process(instance=..., raise_on_error=False)`, then pass `result.dagster_run` and `result.get_run_success_event()` (or `get_run_failure_event()`) to `dg.build_run_status_sensor_context(sensor_name=, dagster_instance=, dagster_run=, dagster_event=, resources=)`. For a `@dg.run_failure_sensor`, chain `.for_run_failure()` onto the built context.
- `dg.instance_for_test()` gives a temporary persistent instance when a cheap ephemeral one will not do.
- Testing an asset sensor end to end: build a sensor context on an instance, assert the first call yields a `SkipReason`, `dg.materialize([my_table], instance=instance)`, then assert the next call yields a `RunRequest`.
- In the UI, **Preview tick result** on a schedule or sensor page runs the real evaluation function (side effects included), shows run config and tags per request, and can launch them. `dagster sensor preview <name>` does the same from the CLI.

## Troubleshooting

Schedule not firing — work down this list:

1. Is the schedule started? Check the **Running** toggle under **Automation**.
2. Did the last tick fail? Click the **Failed** badge for the error and stack trace.
3. Is the interval what you think? Compare **Next tick** against the expected time and timezone.
4. Is the UI running current code? **Settings > Code locations > Reload definitions** (Dagster+: **Deployment > Code locations > Redeploy**). A code location that failed to load shows **Failed** with a **View error** link. Restart `dg dev` after changing dependencies or module layout.
5. Is the daemon healthy? **Deployment > Daemons > Scheduler**; a **Not running** status or a missing heartbeat points at the daemon deployment. Search daemon logs for the schedule name or `SchedulerDaemon`. If the daemon reports the schedule as not found, confirm it loads the same workspace file as the webserver.

Sensor problems: a tick that consistently fails at ~60 seconds is the evaluation timeout — profile, then split work across ticks via the cursor. A sensor that stops requesting runs usually has a cursor that advanced past the data; reset it from the sensor page in the UI, but note that a cursor reset launches nothing if the same `run_key` values are produced again.

## Gotchas

- Sensors and schedules are evaluated outside any run: the daemon drives the tick, and the code server (for a `grpc_server` location) runs the function and builds its resources, so their env vars belong on the code server (see `deployment-oss.md`). Keep them fast, keep them side-effect free where possible, and never do heavy compute there — request a run and do the work in the run.
- A UI test evaluation executes the real function, so any side effect in it will happen.
- `minimum_interval_seconds` is a floor. A slow evaluation stretches the gap between ticks.
- Combining `run_key` with a cursor means a cursor reset relaunches nothing unless the underlying data changed, because the run keys repeat. Omit `run_key` if resettability matters more than dedup.
- Schedule and sensor names must be unique within a code location; the decorated function name is the default.
- `ScheduleDefinition` allows only one of `run_config`, `run_config_fn`, `execution_fn`, and only one of `tags`, `tags_fn`, `execution_fn`.
- A run status sensor whose `request_job` can itself match the monitored scope will loop. Guard on `context.dagster_run.job_name`.
- `RunRequest.job_name` is required when the sensor targets more than one job.
- A schedule or sensor can target an op job or an asset job; `target=` covers both, but an op job's `run_config` schema is keyed by op names, so config written for one will not validate against the other.
- Use `context.scheduled_execution_time`, not the wall clock, when computing partition keys or dated config.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `@dg.schedule(job=...)` / `job_name=`, `ScheduleDefinition(job=...)` | `target=` (accepts asset selections, `AssetsDefinition`, job definitions) |
| `@dg.sensor(job=..., jobs=..., asset_selection=...)` | `target=` |
| `@dg.multi_asset_sensor` | `AutomationCondition` (deprecated, removal no earlier than Dagster 2.0) |
| `run_status_sensor(job_selection=...)` | `monitored_jobs=` |
| `run_status_sensor(monitor_all_repositories=True)` | `monitor_all_code_locations=True` |
| `define_asset_job(partitions_def=...)` | Omit it — partitioning is inferred from the selected assets |
| Sensors that fan assets out from upstream materializations | `AutomationCondition.eager()` / `on_cron()`; see `declarative-automation.md` |
| `dagster dev` | `dg dev` (also starts the daemon) |

## Sources

- https://docs.dagster.io/guides/automate — automation method comparison
- https://docs.dagster.io/guides/automate/schedules — basic schedules, timezone, partition-derived schedules
- https://docs.dagster.io/guides/automate/schedules/defining-schedules — `ScheduleDefinition` vs `@schedule`, cron aliases, schedule logging
- https://docs.dagster.io/guides/automate/schedules/using-schedules — `defs/` placement and scaffolding
- https://docs.dagster.io/guides/automate/schedules/configuring-job-behavior — run config from scheduled execution time
- https://docs.dagster.io/guides/automate/schedules/customizing-execution-timezone — `execution_timezone` and DST behavior
- https://docs.dagster.io/guides/automate/schedules/constructing-schedules-for-partitioned-assets-and-jobs — `build_schedule_from_partitioned_job`, static partitions, partition loading context
- https://docs.dagster.io/guides/automate/schedules/using-resources-in-schedules — resources as schedule parameters
- https://docs.dagster.io/guides/automate/schedules/testing-schedules — `build_schedule_context`, `validate_run_config`, UI preview
- https://docs.dagster.io/guides/automate/schedules/troubleshooting-schedules — five-step diagnosis
- https://docs.dagster.io/guides/automate/sensors — `@sensor`, run keys, cursors, intervals
- https://docs.dagster.io/guides/automate/sensors/using-sensors — `defs/` placement and scaffolding
- https://docs.dagster.io/guides/automate/sensors/using-resources-in-sensors — resources as sensor parameters
- https://docs.dagster.io/guides/automate/sensors/logging-in-sensors — `context.log` and compute log storage
- https://docs.dagster.io/guides/automate/sensors/monitoring-sensors-in-the-dagster-ui — tick history, activity timeline
- https://docs.dagster.io/guides/automate/sensors/run-status-sensors — status reactions, cross-location selectors, job coordination
- https://docs.dagster.io/guides/automate/sensors/testing-sensors — direct invocation, `build_sensor_context`, `dagster sensor preview`
- https://docs.dagster.io/guides/automate/sensors/testing-run-status-sensors — `build_run_status_sensor_context`, `for_run_failure`
- https://docs.dagster.io/guides/automate/asset-sensors — `@asset_sensor`, `@multi_asset_sensor`, custom evaluation, run config
- https://docs.dagster.io/guides/automate/declarative-automation/migrating-from-sensors — when to keep a sensor
- https://docs.dagster.io/guides/build/jobs/asset-jobs — `define_asset_job`, autoloading
- https://docs.dagster.io/deployment/execution/dagster-daemon — scheduler and sensor daemons
- https://docs.dagster.io/deployment/troubleshooting/sensor-timeouts — 60s timeout and mitigations
- https://docs.dagster.io/api/dagster/schedules-sensors — parameter reference for all APIs above
