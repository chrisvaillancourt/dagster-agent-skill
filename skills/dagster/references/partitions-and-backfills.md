# Partitions and backfills

How to slice assets, ops and jobs into partitions, wire dependencies between differently-partitioned assets, and run backfills over them. Covers `PartitionsDefinition` types, partition mappings, `BackfillPolicy`, dynamic partitions, partitioned run config, and partition-aware scheduling. General asset definition is in `assets.md`; schedule/sensor basics are in `schedules-and-sensors.md`; automation conditions are in `declarative-automation.md`; partition-aware I/O is in `io-managers.md`.

## Contents

- [Partitions vs run config](#partitions-vs-run-config)
- [Partitions definitions](#partitions-definitions)
- [Using partitions inside an asset](#using-partitions-inside-an-asset)
- [Dependencies between partitioned assets](#dependencies-between-partitioned-assets)
- [Partition mappings](#partition-mappings)
- [Backfills](#backfills)
- [Dynamic partitions](#dynamic-partitions)
- [Schedules for partitioned assets and jobs](#schedules-for-partitioned-assets-and-jobs)
- [Partitioned ops and jobs](#partitioned-ops-and-jobs)
- [Partitions in components](#partitions-in-components)
- [Declarative automation interplay](#declarative-automation-interplay)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Partitions vs run config

Decision rule: partition when the segment identity is part of the asset's *history*; use run config when it is a runtime knob.

| Use partitions when | Use run config when |
| --- | --- |
| Data splits into discrete, enumerable segments | Parameters are dynamic or unbounded |
| You need per-segment materialization status in the UI | One materialization history is enough |
| Backfilling specific segments is routine | Reprocessing is ad-hoc and manual |
| You want automatic scheduling across all segments | Processing is infrequent |

Hybrid is legitimate and common: partition on the primary segmentation (customer, date) and take extra runtime options through a `dg.Config` parameter on the same asset.

## Partitions definitions

Attach with `@dg.asset(partitions_def=...)`, `dg.AssetSpec(..., partitions_def=...)`, or `dg.define_asset_job(..., partitions_def=...)`.

| Class | Partition key shape | Key parameters |
| --- | --- | --- |
| `dg.HourlyPartitionsDefinition` | `2024-05-20-14:00` | `start_date`, `end_date`, `minute_offset`, `fmt`, `timezone`, `end_offset`, `exclusions` |
| `dg.DailyPartitionsDefinition` | `2024-05-20` | `start_date`, `end_date`, `minute_offset`, `hour_offset`, `fmt`, `timezone`, `end_offset`, `exclusions` |
| `dg.WeeklyPartitionsDefinition` | week start date (Sunday by default) | adds `day_offset` (0 = Sunday … 6 = Saturday). The first key is the first week boundary on or after `start_date`, and earlier days belong to no partition: a Wednesday `start_date` with the default offset leaves four uncovered days unless you set `day_offset=3` |
| `dg.MonthlyPartitionsDefinition` | month start date | adds `day_offset` (day of month, default 1) |
| `dg.TimeWindowPartitionsDefinition` | per `fmt` | `cron_schedule`, `start`, `end`, `fmt`, `timezone`, `end_offset`, `exclusions` |
| `dg.StaticPartitionsDefinition` | your strings | a list of partition keys |
| `dg.DynamicPartitionsDefinition` | your strings | `name` (required for runtime add/remove) |
| `dg.MultiPartitionsDefinition` | `2024-05-20\|us` | mapping of dimension name to `PartitionsDefinition` |

```python
# src/<project>/defs/assets.py
import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])
two_dimensional = dg.MultiPartitionsDefinition(
    {"date": daily_partitions, "region": region_partitions}
)
```

`end_offset` controls the most recent partition in the set. With `0` (the default) the last partition ends before the current time, so a daily set on 2024-05-20 ends at `2024-05-19`. The formula is `current_date - 1 type_of_partition + end_offset`:

| `end_offset` | Most recent daily partition on 2024-05-20 |
| --- | --- |
| `-1` | 2024-05-18 |
| `0` | 2024-05-19 |
| `1` | 2024-05-20 |

Custom calendars use `TimeWindowPartitionsDefinition` with a cron schedule plus `exclusions` (cron strings or datetimes removed from the set):

```python
market_calendar = dg.TimeWindowPartitionsDefinition(
    start=datetime(2024, 1, 1),
    cron_schedule="0 0 * * 1-5",  # weekdays only
    fmt="%Y-%m-%d",
    exclusions=holiday_datetimes,
)
```

Verify the set with `dg list defs`, or `partitions_def.get_partition_keys()` in Python.

## Using partitions inside an asset

| Property on `dg.AssetExecutionContext` | Returns | Use when |
| --- | --- | --- |
| `partition_key` | `str` | Single-partition run (the default) |
| `partition_time_window` | `dg.TimeWindow` with `.start` / `.end` datetimes | Time-window partitions, especially single-run backfills |
| `partition_key_range` | `dg.PartitionKeyRange(start, end)` | Any partitions def, range-shaped work |
| `partition_keys` | `list[str]` | You need to iterate the exact keys in the run |

`partition_key` raises if the run covers a range, so any asset with a non-default `backfill_policy` must use one of the other three. `partition_time_window` raises if the partitions definition is not a `TimeWindowPartitionsDefinition`.

For upstream partitions inside the asset body, use `context.asset_partition_key_range_for_input(name)`, `context.asset_partition_keys_for_input(name)`, or `context.asset_partitions_time_window_for_input(name)`.

Multi-dimensional keys are `dg.MultiPartitionKey`, a `str` subclass, so string formatting works and `keys_by_dimension` splits it:

```python
@dg.asset(partitions_def=two_dimensional)
def daily_regional_sales(context: dg.AssetExecutionContext) -> None:
    # partition_key looks like "2024-01-01|us"
    dims = context.partition_key.keys_by_dimension
    date, region = dims["date"], dims["region"]
    write(f"data/sales/{date}/{region}.csv")
```

Write partitioned outputs so that the partition is part of the address: one file or object per key (`s3://bucket/sales/{partition_key}.parquet`), or a `WHERE` clause over the time window for warehouse tables. For an I/O manager to handle this for you, use `OutputContext.asset_partition_key` / `InputContext.asset_partition_keys` and `asset_partitions_time_window`; `dg.UPathIOManager` already implements the filesystem case. See `io-managers.md`.

## Dependencies between partitioned assets

Default rules, no mapping needed:

- Same `PartitionsDefinition` upstream and downstream: each downstream partition depends on the identically-keyed upstream partition.
- Both time-window partitioned but different cadences: each downstream partition depends on every upstream partition whose time window intersects it. A daily asset over an hourly upstream depends on the 24 hourly partitions of that day.

Mixing partitioned and unpartitioned assets requires you to say what you mean. An unpartitioned downstream over a partitioned upstream normally wants `dg.AllPartitionMapping()` (consume everything) or `dg.LastPartitionMapping()` (consume only the newest); set one explicitly rather than relying on inference.

Self-dependencies are an ordinary mapping on the asset's own name, used for cumulative or stateful assets:

```python
@dg.asset(
    partitions_def=daily_partitions,
    ins={
        "self_dependent_asset": dg.AssetIn(
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            )
        )
    },
)
def self_dependent_asset(context, self_dependent_asset): ...
```

Aggregating a finer upstream into a coarser downstream reads the mapped range:

```python
@dg.asset(partitions_def=weekly_partitions, deps=[daily_sales_data])
def weekly_sales_summary(context: dg.AssetExecutionContext) -> None:
    key_range = context.asset_partition_key_range_for_input("daily_sales_data")
    for day in pd.date_range(key_range.start, key_range.end):
        ...
```

## Partition mappings

| Mapping | Use when |
| --- | --- |
| `dg.TimeWindowPartitionMapping(start_offset, end_offset, allow_nonexistent_upstream_partitions)` | Shift the upstream window: `start_offset=-1, end_offset=-1` means "yesterday's partition" |
| `dg.IdentityPartitionMapping()` | Force same-key matching when the two partitions definitions differ but keys line up |
| `dg.LastPartitionMapping()` | Downstream needs only the newest upstream partition |
| `dg.AllPartitionMapping()` | Downstream (often unpartitioned) needs every upstream partition |
| `dg.StaticPartitionMapping({...})` | Explicit upstream-key to downstream-key(s) correspondence between two static sets |
| `dg.SpecificPartitionsPartitionMapping(["a"])` | Depend on a fixed, hardcoded subset of upstream partitions |
| `dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name=...)` | One side is multi-partitioned, the other is a single dimension of it. Beta |
| `dg.MultiPartitionMapping({dim: dg.DimensionPartitionMapping(...)})` | Both sides multi-partitioned, dimensions named differently. Beta |

`TimeWindowPartitionMapping` offsets are expressed in units of the *downstream* partitions definition when the two differ. `allow_nonexistent_upstream_partitions=True` returns no keys instead of raising when the mapped upstream window falls outside the upstream's start/end; use it when the downstream's history reaches further back than the upstream's.

Attach a mapping one of two ways. Use `dg.AssetDep` for dependencies declared through `deps` (no data loaded through an I/O manager), and `dg.AssetIn` for managed-loading dependencies that arrive as function arguments:

```python
@dg.asset(
    partitions_def=partitions_def,
    deps=[
        dg.AssetDep(
            events,
            partition_mapping=dg.TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        )
    ],
)
def yesterday_event_stats(): ...
```

In `MultiPartitionMapping`, dimensions you do not name explicitly fall back to all-to-all. Subclassing `PartitionMapping` outside Dagster is not supported.

## Backfills

A backfill materializes many partitions at once, either at first setup or after changing an asset's logic.

Launching:

- UI: **Materialize** on the asset details or global lineage page, then pick partitions in the backfill modal. Assets can be backfilled together as long as the most upstream ones share a partitions definition.
- CLI: `dg launch --assets my_asset --partition 2024-05-20` for one partition, `dg launch --assets my_asset --partition-range 2024-05-01...2024-05-20` for a range.
- Any multi-partition materialization needs the Dagster daemon running to queue the runs. Backfill submission is a daemon responsibility, so an idle backfill usually means an idle or crashed daemon. See `operations-and-debugging.md`.

Default behavior is one run per partition. `backfill_policy` changes that:

```python
@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"),
    backfill_policy=dg.BackfillPolicy.single_run(),
    deps=[dg.AssetKey("raw_events")],
)
def events(context: dg.AssetExecutionContext) -> None:
    start, end = context.partition_time_window
    overwrite_data_in_datetime_range(start, end, compute(read(start, end)))
```

The body writes its own output and returns `None` on purpose: a single-run backfill covers a partition range, so `context.partition_key` raises and the default I/O manager cannot persist one returned value for the range. Return `dg.MaterializeResult(metadata=...)` if you want per-run metadata. Prove the policy locally with `dg launch --assets events --partition-range 2024-05-01...2024-05-07`, which should log one step and seven materializations.

`dg.BackfillPolicy.multi_run(max_partitions_per_run=10)` batches instead: 100 partitions become 10 runs of 10. Choosing between them:

| Factor | One per partition | Batched | Single run |
| --- | --- | --- | --- |
| Run overhead | High (N runs) | Medium (N/batch) | Low (1 run) |
| Fault isolation | Best | Moderate | None |
| Retry cost | 1 partition | 1 batch | All partitions |
| Observability | Per partition | Per batch | Aggregate only |

| Scenario | Strategy |
| --- | --- |
| API with rate limits or flaky source | One per partition |
| Short processing time, reliable source | Batched, 10 to 50 per run |
| Spark, Snowflake or Databricks range queries | Single run |
| Initial backfill of 1000+ partitions | Batched, 50 to 100 per run |

Batch-size starting points: under 1 minute per partition, 20 to 50; 1 to 5 minutes, 10 to 20; 5 to 15 minutes, 5 to 10; over 15 minutes, 1 to 5 or single run. Inside a batched run, parallelize with one batch query for SQL sources, a thread pool for I/O-bound work, or a process pool for CPU-bound work.

Single-run backfills only apply when launched from the asset graph or asset page, or when every asset in the targeted asset job shares the same backfill policy.

Instance settings in `dagster.yaml` control backfill throughput. Dagster processes backfills one at a time by default:

```yaml
backfills:
  use_threads: true
  num_workers: 8
```

`num_submit_workers` additionally parallelizes run creation within a single backfill, which cuts latency when one backfill produces many runs. Tick retention (`retention.schedule` / `retention.sensor` with `purge_after_days`) is separate and does not delete partition or backfill status. See `deployment-oss.md`.

## Dynamic partitions

Use when the partition set is discovered at runtime: new customers, arriving files, regions added upstream. Requires `name=` so the keys can be stored on the instance.

```python
# src/<project>/defs/sensors.py
images_partitions_def = dg.DynamicPartitionsDefinition(name="images")


@dg.sensor(job=images_job)
def image_sensor(context: dg.SensorEvaluationContext):
    new_images = [
        f
        for f in os.listdir(os.getenv("MY_DIRECTORY"))
        if not images_partitions_def.has_partition_key(
            f, dynamic_partitions_store=context.instance
        )
    ]
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=f) for f in new_images],
        dynamic_partitions_requests=[
            images_partitions_def.build_add_request(new_images)
        ],
    )
```

`dg.SensorResult(dynamic_partitions_requests=[...])` is the preferred path because the keys are registered atomically with the run requests. The imperative escape hatch is the instance API, needed for deletion and for non-sensor code: `context.instance.add_dynamic_partitions(name, keys)`, `context.instance.delete_dynamic_partition(name, key)` (singular), `context.instance.get_dynamic_partitions(name)`, `context.instance.has_dynamic_partition(name, key)`.

Dagster never removes partition metadata when the underlying data disappears, and there is no built-in retention policy. Enforce one with two sensors over the same dynamic partitions definition: a retention sensor that deletes storage plus the key for anything past the window, and a sync sensor that adds keys for newly arrived data and deletes keys whose data vanished out of band. Alert if either sensor's last successful tick is stale or partition counts grow unexpectedly. On Dagster+, adding and removing dynamic partition keys is metadata-only and consumes no credits.

## Schedules for partitioned assets and jobs

For time-window partitions, build the schedule from the job and let the cadence follow the partitioning:

```python
# src/<project>/defs/schedules.py
daily_asset_job = dg.define_asset_job("daily_job", selection=[daily_asset])

daily_schedule = dg.build_schedule_from_partitioned_job(
    daily_asset_job, hour_of_day=1, minute_of_hour=30
)
```

Each tick targets the latest *completed* partition, so the 2024-05-20 01:30 tick runs the `2024-05-19` partition. `minute_of_hour`, `hour_of_day`, `day_of_week` and `day_of_month` shift the tick time without changing which partition is chosen; use `end_offset=1` on the partitions definition to make the schedule fill the current period instead. The job may be multi-partitioned as long as one dimension is time-partitioned.

Static and dynamic partitions have no implied cadence, so write the schedule yourself and emit one `RunRequest` per key:

```python
@dg.schedule(cron_schedule="0 0 * * *", job=continent_job)
def continent_schedule():
    for c in CONTINENTS:
        yield dg.RunRequest(run_key=c, partition_key=c)
```

When the schedule cadence deliberately differs from the partition cadence, do not use `build_schedule_from_partitioned_job`. Derive the key from `context.scheduled_execution_time` instead:

```python
@dg.schedule(job=hourly_partition_job, cron_schedule="*/1 * * * *")
def every_minute_schedule(context: dg.ScheduleEvaluationContext):
    previous_hour = context.scheduled_execution_time - timedelta(hours=1)
    return dg.RunRequest(partition_key=previous_hour.strftime("%Y-%m-%d-%H:00"))
```

Multi-partitioned schedules emit a `dg.MultiPartitionKey({"date": date, "region": region})` per combination, producing one run per cell.

To resolve "the most recent partition as of time T", wrap the lookup in `dg.partition_loading_context(effective_dt=...)`; `get_last_partition_key` takes no `current_time` argument. `get_partition_key_for_timestamp` is simpler when you just want the partition containing a timestamp, but it can return keys for partitions that do not exist yet, so validate.

## Partitioned ops and jobs

Op jobs partition through run config, not `partitions_def`. Supply a `dg.PartitionedConfig` as the job's `config`; this is what makes backfills and the Partitions tab available for op jobs.

```python
# src/<project>/defs/ops.py
@dg.daily_partitioned_config(start_date=datetime(2020, 1, 1))
def partitioned_config(start: datetime, _end: datetime):
    return {
        "ops": {
            "process_data_for_date": {"config": {"date": start.strftime("%Y-%m-%d")}}
        }
    }


@dg.job(config=partitioned_config)
def partitioned_op_job():
    process_data_for_date()
```

Available decorators: `dg.daily_partitioned_config`, `dg.hourly_partitioned_config`, `dg.weekly_partitioned_config`, `dg.monthly_partitioned_config` (all accepting `start_date`, plus the offsets their cadence allows: `minute_offset` for hourly, `minute_offset` / `hour_offset` for daily, and all three including `day_offset` for weekly and monthly), `dg.static_partitioned_config(partition_keys=[...])`, and `dg.dynamic_partitioned_config(partition_fn=...)`. Time-based decorated functions receive `(start, end)` datetimes; static and dynamic ones receive a single `partition_key` string. A `PartitionedConfig` can also be passed to `dg.define_asset_job(config=...)`.

Testing, all runnable under `pytest`:

```python
# tests/test_ops.py
def test_partitioned_config():
    run_config = my_partitioned_config(datetime(2020, 1, 3), datetime(2020, 1, 4))
    assert dg.validate_run_config(partitioned_op_job, run_config)

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2020-01-01"
    assert dg.validate_run_config(
        partitioned_op_job,
        my_partitioned_config.get_run_config_for_partition_key(keys[0]),
    )


def test_partitioned_op_job():
    assert partitioned_op_job.execute_in_process(partition_key="2020-01-01").success
```

## Partitions in components

In YAML, define the partitions definition as a template var and apply it through post-processing:

```python
# template_vars.py
@dg.template_var
def the_daily_partitions_def() -> dg.DailyPartitionsDefinition:
    return dg.DailyPartitionsDefinition(start_date="2020-01-01")
```

```yaml
# defs.yaml
type: ...
attributes: ...
template_vars_module: .template_vars
post_processing:
  assets:
    - target: "*"
      attributes:
        partitions_def: "{{ the_daily_partitions_def }}"
```

In Python, subclass the component and map over its specs: `super().build_defs(context).map_asset_specs(lambda spec: spec.replace_attributes(partitions_def=...))`.

Give every asset in a component the same partitions definition. Each execution step maps to exactly one partitions definition, so an integration that packs multiple assets into one step breaks if they disagree. Check with `dg check defs`. See `components.md`.

## Declarative automation interplay

Partitioned assets usually want automation conditions rather than hand-written schedules, because the condition handles fan-in and fan-out across mappings for you.

- `AutomationCondition.on_missing()` fills a partition as soon as all upstream partitions are available. Best default for partition fan-in. It only considers partitions added after the condition was enabled, and for time-partitioned assets it requests only the latest time partition.
- `AutomationCondition.eager()` also re-materializes when upstream partitions re-run, at the cost of many more downstream runs. Same latest-time-partition restriction.
- `AutomationCondition.on_cron()` targets only the latest time partition of a time-partitioned asset, but *all* partitions of static and dynamic-partitioned assets.
- To reach older time partitions, use `on_missing()` or compose with `in_latest_time_window(lookback_delta=...)`.

Details and customization are in `declarative-automation.md`.

## Gotchas

- **Key format is the contract.** Daily keys are `2024-05-20`, hourly keys are `2024-05-20-14:00`, multi keys are `2024-05-20|us` with dimensions ordered by name. A `RunRequest(partition_key=...)` that does not match an existing key fails at launch.
- **`context.partition_key` raises under a range-covering run.** Any asset carrying `BackfillPolicy.single_run()` or `multi_run()` must read `partition_time_window`, `partition_key_range`, or `partition_keys`. Returning a plain value also fails at handle-output time, because the default filesystem I/O manager refuses to store one object for a range of partitions: write the data yourself and return `None` or `dg.MaterializeResult(metadata=...)`.
- **Read upstream partitions through the mapping.** In a downstream body use `context.asset_partition_keys_for_input(name)`, `asset_partition_key_range_for_input`, or `asset_partitions_time_window_for_input`. Recomputing the upstream keys by hand from the partitions definition silently ignores any `start_offset` / `end_offset` on the declared `partition_mapping`.
- **`end_offset=0` excludes the current period.** If the schedule appears to be a day behind, that is the time-window semantics, not a bug. Set `end_offset=1` to include the in-progress period, accepting partial data.
- **Non-UTC timezones need a timezone offset in `fmt`.** For hourly or faster cadences, a format without an offset makes the two instances of the repeated hour at the fall DST transition ambiguous; the second one gets a UTC offset appended automatically.
- **Cap partitions at 100,000 per asset.** Beyond that the UI loads slowly. Coarsen the cadence or split the asset.
- **One partitions definition per execution step.** Mismatched definitions across assets materialized in one run, common in components and multi-assets, is an error.
- **`delete_dynamic_partition` is singular.** Adding takes a sequence (`add_dynamic_partitions(name, keys)`), deleting takes one key.
- **Deleting data does not delete partitions.** Pair deletion with a sync sensor or the partition view drifts from storage.
- **Static sets beat dynamic ones when the keys are known.** `static_partitioned_config` loads partition views faster in the UI than the dynamic equivalent (unverified).

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| "Launch as..." single/multi-run toggle in the backfill modal (removed in 1.5) | `backfill_policy=dg.BackfillPolicy.single_run()` in code |
| `PartitionedConfig.run_config_for_partition_fn` | `run_config_for_partition_key_fn` |
| `PartitionedConfig.tags_for_partition_fn` | `tags_for_partition_key_fn` |
| `DynamicPartitionsDefinition(partition_fn=...)` | `DynamicPartitionsDefinition(name=...)` plus instance add/delete or `SensorResult` |
| `dagster` CLI backfill invocations | `dg launch --assets ... --partition-range ...` |

## Sources

- https://docs.dagster.io/guides/build/partitions-and-backfills — section index
- https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets — time, static, multi, dynamic, custom-calendar partitions
- https://docs.dagster.io/guides/build/partitions-and-backfills/defining-dependencies-between-partitioned-assets — default dependency rules and mapping overrides
- https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data — launching backfills and single-run backfill policies
- https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops — `PartitionedConfig` and partitioned op jobs
- https://docs.dagster.io/guides/build/partitions-and-backfills/data-retention — retention and sync sensors for dynamic partitions
- https://docs.dagster.io/examples/best-practices/partitions-vs-config — decision matrix and hybrid approach
- https://docs.dagster.io/examples/best-practices/partition-backfill-strategies — one-per-partition vs batched vs single run, batch sizing
- https://docs.dagster.io/guides/automate/schedules/constructing-schedules-for-partitioned-assets-and-jobs — `build_schedule_from_partitioned_job`, `end_offset`, `partition_loading_context`
- https://docs.dagster.io/guides/test/testing-partitioned-config-and-jobs — `validate_run_config`, `execute_in_process(partition_key=...)`
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/using-partitions — template vars and post-processing
- https://docs.dagster.io/guides/automate/declarative-automation — `on_missing` / `eager` / `on_cron` partition behavior
- https://docs.dagster.io/guides/build/io-managers/defining-a-custom-io-manager — partition-aware `handle_output` / `load_input`
- https://docs.dagster.io/api/dagster/partitions — partitions definitions, mappings, `BackfillPolicy`, partitioned config decorators
- https://docs.dagster.io/api/dagster/execution — `partition_key`, `partition_time_window`, `partition_key_range`, `partition_keys`
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference — `dg launch --partition` / `--partition-range`
- https://docs.dagster.io/deployment/oss/dagster-yaml — `backfills` instance settings
