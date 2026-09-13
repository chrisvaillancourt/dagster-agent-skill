# Testing and asset checks

Two related topics: build-time testing of Dagster code with pytest (direct invocation, in-process execution, definition-load validation), and runtime data-quality verification with asset checks and freshness policies. Partition mappings and backfill mechanics are in `partitions-and-backfills.md`; automation-condition semantics are in `declarative-automation.md`; schedule/sensor authoring is in `schedules-and-sensors.md`; resource authoring is in `resources-and-config.md`; component authoring is in `components.md`.

## Contents
- [Choosing a test layer](#choosing-a-test-layer)
- [Direct invocation](#direct-invocation)
- [In-process execution](#in-process-execution)
- [Testing resources and config](#testing-resources-and-config)
- [Testing partitioned config and jobs](#testing-partitioned-config-and-jobs)
- [Validating that definitions load](#validating-that-definitions-load)
- [Testing schedules, sensors, and automation](#testing-schedules-sensors-and-automation)
- [Testing components](#testing-components)
- [Asset checks](#asset-checks)
- [Executing and scheduling checks](#executing-and-scheduling-checks)
- [Subsetting checks](#subsetting-checks)
- [Freshness policies](#freshness-policies)
- [Freshness checks](#freshness-checks)
- [Data contracts](#data-contracts)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Choosing a test layer

Prefer the cheapest layer that exercises the logic you changed.

| Goal | Use |
| --- | --- |
| Business logic inside one asset/op | Call the decorated function directly |
| Wiring of several assets, IO managers, resources | `dg.materialize_to_memory([...])` |
| Anything that must round-trip through an IO manager | `dg.materialize([...])` |
| Op graphs / jobs, op selection, run config | `job.execute_in_process(...)` |
| Whole repo still loads and resolves | `dg check defs` in CI, plus `Definitions.validate_loadable` |
| Data quality at runtime (not in CI) | Asset checks and freshness policies |

Unit-test individual assets rather than whole jobs. Skip unit tests when the logic lives in an external system (a Databricks job invoked over Pipes) — use an asset check instead.

## Direct invocation

Assets and ops are plain functions. Invoke them with the inputs you want.

```python
# tests/test_assets.py
import dagster as dg
from my_project.defs.assets import loaded_file, processed_file


def test_no_arguments() -> None:
    assert loaded_file() == "contents"


def test_upstream_passed_as_arg() -> None:
    # upstream asset value is supplied positionally, no IO manager involved
    assert processed_file(" contents  ") == "contents"


def test_with_config_and_resource() -> None:
    # asset signature: (primary_file: str, config: SeparatorConfig, bar: BarResource)
    # use keyword arguments once there is more than one parameter
    assert processed_file(
        primary_file="abc",
        config=SeparatorConfig(separator=","),
        bar=BarResource(my_string="bar"),
    ) == "abc,bar"
```

Mock resources either by constructing the real `ConfigurableResource` with test values, as above, or with `unittest.mock` when the resource talks to an external service.

```python
from unittest import mock


def test_file() -> None:
    mocked = mock.Mock(spec=S3FileManager)
    mocked.read_data.return_value = "contents"
    assert loaded_file(mocked) == "contents"
```

Assets that take a `context` need one built for them. `dg.build_asset_context(...)` for assets, `dg.build_op_context(...)` for ops, `dg.build_asset_check_context(...)` for checks. All accept `resources=`, `resources_config=`, `instance=`; the asset and op variants also accept `partition_key=` / `partition_key_range=` and `run_tags=`; `build_op_context` additionally accepts `op_config=` and `mapping_key=`.

```python
def test_partitioned_asset() -> None:
    context = dg.build_asset_context(partition_key="2024-08-16")
    assert loaded_file(context) == "Contents for August 16th, 2024"
```

Use them as context managers (`with dg.build_asset_context(resources={...}) as context:`) whenever a supplied resource is itself a context manager. `context.log` works unconfigured, so assets that log need no extra setup.

An asset that yields `MaterializeResult` or `AssetCheckResult` returns a generator on direct invocation — consume it and assert on the yielded objects, or run it through `dg.materialize` instead.

## In-process execution

Use these when the test must cover dependency resolution, IO managers, or check execution.

| API | IO manager behavior |
| --- | --- |
| `dg.materialize(assets, ...)` | Writes to the local filesystem by default |
| `dg.materialize_to_memory(assets, ...)` | Forces `mem_io_manager` for every IO manager key |
| `job.execute_in_process(...)` | Switches the default IO manager from filesystem to in-memory |

All three return `dg.ExecuteInProcessResult`. Both `materialize` functions accept `assets`, `resources=`, `run_config=`, `partition_key=`, `tags=`, `selection=`, and `instance=`. `execute_in_process` accepts `run_config=`, `instance=`, `partition_key=`, `raise_on_error=`, `op_selection=`, `input_values=`, and `resources=`.

```python
from unittest import mock


def test_data_assets() -> None:
    result = dg.materialize_to_memory([data_source, structured_data])
    assert result.success
    # values are addressed by the underlying op/node name
    assert result.output_for_node("structured_data") is not None


def test_assets_require_service() -> None:
    result = dg.materialize_to_memory(
        [asset_requires_service, other_asset_requires_service],
        resources={"service": mock.MagicMock()},
    )
    assert result.success
```

Result accessors: `result.success`, `result.output_for_node(node_str, output_name=...)`, `result.output_value(output_name=...)`, `result.asset_value(asset_key)`, `result.all_events`, `result.events_for_node(name)`, `result.run_id`, `result.dagster_run`, `result.job_def`. Nested graphs address nodes as `outer_graph.inner_node`.

Passing `materialize_to_memory` an explicit IO manager in `resources` raises `DagsterInvariantViolationError`. Unless the downstream asset uses `deps=`, you must include upstream assets in the list — their definitions carry the information needed to load their contents.

For a multi-asset with upstream dependencies, `input_values` does not apply. Define standalone source assets and supply a mock IO manager that branches on `context.asset_key.path` in `load_input`:

```python
defs = dg.Definitions(
    assets=[dg.SourceAsset(key="instruments"), dg.SourceAsset(key="weights"), index_multi_asset],
    resources={"io_manager": MockDataIOManager(test_instruments, test_weights)},
)
result = defs.get_implicit_global_asset_job_def().execute_in_process(
    asset_selection=[dg.AssetKey(["core", "fix", "index_metadata"])]
)
assert result.success
```

Jobs take typed run config the same way assets do: `do_math_job.execute_in_process(run_config=dg.RunConfig(ops={"add_one": AddOneConfig(num=2)}))`.

To assert on the event stream — materializations, check evaluations, expectation results — walk `result.events_for_node("op_name")` and compare `event.event_type` against `dg.DagsterEventType` members (`STEP_START`, `ASSET_MATERIALIZATION`, `STEP_OUTPUT`, `HANDLED_OUTPUT`, `STEP_SUCCESS`).

When a test needs event storage that persists across calls (sensors, automation conditions, runless events), open `dg.instance_for_test()` or `dg.DagsterInstance.ephemeral()` and pass it as `instance=`. `instance_for_test(overrides=..., temp_dir=..., set_dagster_home=True)` creates a real temporary instance and points `$DAGSTER_HOME` at it for the duration of the context manager.

## Testing resources and config

Construct a `ConfigurableResource` directly and call its methods: `MyResource(value="foo").get_value()`. Nested resources are passed as constructor arguments — `MyResourceRequiresAnother(foo=StringHolderResource(value="foo"), bar="bar")`.

If the resource reads `self.get_resource_context()`, build one with `dg.build_init_resource_context(...)` and attach it:

```python
def test_my_context_resource() -> None:
    with dg.DagsterInstance.ephemeral() as instance:
        context = dg.build_init_resource_context(instance=instance)
        resource = MyContextResource(base_path=None).with_resource_context(context)
        assert resource.effective_base_path() == instance.storage_directory()
```

## Testing partitioned config and jobs

Invoking a `PartitionedConfig` object calls the decorated function, so you can assert on the generated run config, then confirm it is valid for the job with `dg.validate_run_config(job_def, run_config)` (returns the validated config dict, raises on failure).

```python
def test_my_partitioned_config() -> None:
    run_config = my_partitioned_config(datetime(2020, 1, 3), datetime(2020, 1, 4))
    assert run_config == {"ops": {"process_data_for_date": {"config": {"date": "2020-01-03"}}}}
    assert dg.validate_run_config(partitioned_op_job, run_config)


def test_partition_keys() -> None:
    keys = my_offset_partitioned_config.get_partition_keys()
    run_config = my_offset_partitioned_config.get_run_config_for_partition_key(keys[0])
    assert dg.validate_run_config(do_more_stuff_partitioned, run_config)
```

Run a partitioned job on one partition with `partitioned_op_job.execute_in_process(partition_key="2020-01-01")`.

## Validating that definitions load

`dg check defs` is the CI gate: it loads and validates every definition using a Dagster instance, sets `DAGSTER_IS_DEFS_VALIDATION_CLI=1` while running, and exits 1 on error. Inside a project it also schema-checks `defs.yaml` files before loading; pass `--no-check-yaml` to skip that pass. Run it before `pytest` in CI.

In pytest, load the project's definitions and assert against them:

```python
# tests/test_defs.py
from pathlib import Path

import dagster as dg


def test_defs_load() -> None:
    defs = dg.load_from_defs_folder(path_within_project=Path(__file__).parent.parent)
    dg.Definitions.validate_loadable(defs)
    assert dg.AssetKey("orders") in defs.resolve_all_asset_keys()
```

`Definitions.validate_loadable(defs)` is a static method meant for unit tests. It raises if assets have conflicting keys, jobs/sensors/schedules have conflicting names, an asset job cannot be resolved, a resource requirement is unsatisfied, or a partition mapping is invalid.

Other `Definitions` accessors useful in tests: `resolve_all_asset_keys()`, `resolve_all_asset_specs()`, `get_job_def(name)`, `get_schedule_def(name)`, `get_sensor_def(name)`, `get_assets_def(key)`, `load_asset_value(key)`, and `get_asset_value_loader()` for loading several assets without re-initializing resources. Component test code in the docs also uses `defs.resolve_asset_graph().get_all_asset_keys()`.

Importing `from my_project.definitions import defs` works too, but only if module import has no side effects. Read secrets through `dg.EnvVar(...)` so nothing resolves at import time.

## Testing schedules, sensors, and automation

Details live in `schedules-and-sensors.md` and `declarative-automation.md`. The helpers are `dg.build_schedule_context(...)`, `dg.build_sensor_context(...)`, and `dg.build_run_status_sensor_context(...)`; all take an `instance`.

Automation conditions are tested with `dg.evaluate_automation_conditions(defs=, instance=, asset_selection=, evaluation_time=, cursor=)`, which returns an `EvaluateAutomationConditionsResult` carrying `total_requested` and `cursor`. Thread the cursor through successive ticks.

```python
def test_eager_condition() -> None:
    instance = dg.DagsterInstance.ephemeral()

    result = dg.evaluate_automation_conditions(defs=[upstream, downstream], instance=instance)
    assert result.total_requested == 0

    dg.materialize([upstream], instance=instance)

    result = dg.evaluate_automation_conditions(
        defs=[upstream, downstream], instance=instance, cursor=result.cursor
    )
    assert result.total_requested == 1
```

To simulate an update to an asset defined elsewhere, record a runless event: `instance.report_runless_asset_event(dg.AssetMaterialization(asset_key="external_upstream"))`. The same API accepts `dg.AssetCheckEvaluation(asset_key=..., check_name=..., passed=...)`.

## Testing components

Two entry points, both covered in `components.md`.

For components already in your `defs/` folder, `dg.ComponentTree.for_project(project_root)` gives you `tree.load_component(path)` and `tree.build_defs_at_path(path)`, where `path` is relative to the `defs` folder. Assert on the component instance for YAML-parsing behavior and on the `Definitions` for execution.

```python
def test_function_component() -> None:
    tree = dg.ComponentTree.for_project(Path(__file__).parent.parent)
    component = tree.load_component("path/to/function_component")
    defs = tree.build_defs_at_path("path/to/function_component")
    assert isinstance(component, dg.FunctionComponent)
    assert dg.materialize(defs.get_assets_def("some_asset")).success
```

For custom components you are authoring, build a throwaway `defs` folder with `dg.components.testing.create_defs_folder_sandbox()`. `sandbox.scaffold_component(component_cls=..., defs_path=..., defs_yaml_contents=...)` exercises your scaffolder exactly as `dg scaffold defs` would; `sandbox.load_component_and_build_defs(defs_path=...)` yields `(component, defs)`; `sandbox.build_all_defs()` yields the merged `Definitions` when several components must be tested together.

## Asset checks

Use when you need to verify a property of the data at runtime, not of the code at build time. Each check should test exactly one property.

`@dg.asset_check(asset=...)` defines a single check. Key parameters: `asset`, `name` (defaults to the function name; must be unique per asset), `description`, `blocking`, `additional_deps`, `additional_ins`, `required_resource_keys`, `config_schema`, `op_tags`, `compute_kind`, `retry_policy`, `metadata`, `automation_condition`, `pool`, `partitions_def`. The function returns `dg.AssetCheckResult(passed=, severity=, metadata=, description=)`; severity is `dg.AssetCheckSeverity.ERROR` (default) or `WARN`.

```python
# src/<project>/defs/asset_checks.py
@dg.asset_check(asset="orders_aggregation", description="Table is non-empty")
def orders_aggregation_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        row_count = conn.execute("select count(*) from orders_aggregation").fetchone()[0]
    return dg.AssetCheckResult(passed=row_count > 0, metadata={"row_count": row_count})
```

Declare the asset as a function parameter to receive its value through the IO manager (`def check(my_asset: DataFrame)`), or query storage directly as above. Resources are injected by type-annotated parameter, same as for assets.

`@dg.multi_asset_check(specs=[...])` runs several checks in one op. Each `dg.AssetCheckSpec(name=, asset=, description=, additional_deps=, blocking=, metadata=, automation_condition=, partitions_def=)` declares one check; yield an `AssetCheckResult` per spec, identifying each by `check_name=` and `asset_key=`.

```python
CHECKS = [("orders_id_has_no_nulls", "order_id"), ("items_id_has_no_nulls", "item_id")]


@dg.multi_asset_check(specs=[dg.AssetCheckSpec(name=n, asset="orders") for n, _ in CHECKS])
def orders_check() -> Iterable[dg.AssetCheckResult]:
    orders_df = pd.read_csv("orders.csv")
    for name, column in CHECKS:
        yield dg.AssetCheckResult(
            check_name=name, asset_key="orders", passed=bool(orders_df[column].isna().sum() == 0)
        )
```

Wrap that in a function taking the descriptor list and returning the decorated function to get a check factory; register the result under `Definitions(asset_checks=[...])`.

To run a check inside the asset's own op, pass `check_specs=` to `@dg.asset` / `@dg.multi_asset` and yield `AssetCheckResult` alongside the output, or return `dg.MaterializeResult(check_results=[...])`.

`blocking=True` makes the run wait for the check before starting downstream assets; if the check fails with `ERROR` severity, downstream assets do not execute. Gating applies only to emitted failures — if no result is emitted for the check, downstream execution proceeds and a warning is logged. Inside a multi-asset, that multi-asset is responsible for enforcing the gate on assets in the same step. Severity on its own never stops execution.

Partitioned checks are a preview feature. Pass `partitions_def=` to `@dg.asset_check` or `AssetCheckSpec`; it must match the asset's `partitions_def`. Each execution targets one partition and the UI shows per-partition status.

Two beta factories ship for common cases: `dg.build_column_schema_change_checks(assets=, severity=)` compares `dagster/column_schema` metadata between materializations, and `dg.build_metadata_bounds_checks(assets=, metadata_key=, min_value=, max_value=, exclusive_min=, exclusive_max=, severity=)` bounds a numeric metadata value. Both default to `WARN`.

Scaffold a check file with `dg scaffold defs dagster.asset_check <path> --asset-key <key>`. dbt tests can be modeled as asset checks automatically — see `integrations-dbt-and-elt.md`. Asset checks do not consume Dagster+ credits.

## Executing and scheduling checks

By default, any job or launch that targets an asset also runs the checks attached to it: `dg.AssetSelection.assets(...)` includes the asset checks that target those assets, so `dg launch --assets orders` runs `orders` and its checks. Checks can also be executed on demand from the asset's **Checks** tab in the UI.

To decouple them, build two jobs and strip or select checks explicitly:

```python
asset_job = dg.define_asset_job("asset_job", dg.AssetSelection.assets(orders).without_checks())
check_job = dg.define_asset_job("check_job", dg.AssetSelection.checks_for_assets(orders))

asset_schedule = dg.ScheduleDefinition(job=asset_job, cron_schedule="0 0 * * *")
check_schedule = dg.ScheduleDefinition(job=check_job, cron_schedule="0 6 * * *")
```

Alert on a failing check job with a run-failure sensor, for example `dg.make_email_on_run_failure_sensor(email_from=, email_password=, email_to=, monitored_jobs=[check_job])`.

Selection helpers: `AssetSelection.checks_for_assets(*assets)`, `AssetSelection.checks(*check_keys)`, `AssetSelection.all_asset_checks()`, and `.without_checks()` on any selection.

Checks also accept automation conditions directly, which is the better fit when you want checks on a different cadence than the asset. `AutomationCondition.on_cron("@hourly")` requests the check once per tick, after all upstream dependencies have updated; `AutomationCondition.eager()` requests it whenever a dependency updates. See `declarative-automation.md`.

## Subsetting checks

Set `can_subset=True` on `@dg.multi_asset_check` and branch on `context.selected_asset_check_keys`, yielding only the requested results.

```python
@dg.multi_asset_check(
    specs=[dg.AssetCheckSpec(name="check_one", asset="asset_one")], can_subset=True
)
def the_check(context: dg.AssetCheckExecutionContext) -> Iterable[dg.AssetCheckResult]:
    if dg.AssetCheckKey(dg.AssetKey("asset_one"), "check_one") in context.selected_asset_check_keys:
        yield dg.AssetCheckResult(passed=True, check_name="check_one")
```

For a `@dg.multi_asset` with `check_specs`, Dagster assumes every declared check runs after materialization; executing only some of them errors out. To allow it, set `skippable=True` on each `AssetSpec`, set `can_subset=True` on the decorator, then branch on both `context.selected_asset_keys` and `context.selected_asset_check_keys`, yielding a `MaterializeResult` per asset you actually produce.

## Freshness policies

Preview feature, under active development; APIs may change. Not enabled by default — add to `dagster.yaml`:

```yaml
freshness:
  enabled: True
```

A freshness policy on an asset yields a freshness state of `PASS`, `WARN`, `FAIL`, or `UNKNOWN` (never materialized); assets without a policy report `NOT_APPLICABLE`. Two constructors:

```python
from datetime import timedelta

# Expect a successful materialization at least every 24h; warn past 12h.
dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24), warn_window=timedelta(hours=12))

# Expect materialization between 9am and 10am America/Los_Angeles each day.
dg.FreshnessPolicy.cron(
    deadline_cron="0 10 * * *",
    lower_bound_delta=timedelta(hours=1),
    timezone="America/Los_Angeles",
)
```

Constraints: `fail_window` and `warn_window` are at least 60 seconds and `warn_window < fail_window`; `deadline_cron` has 1-minute resolution and `lower_bound_delta` must be at least a minute and fit inside the smallest interval of the cron; `timezone` defaults to UTC.

Attach per asset via `@dg.asset(freshness_policy=policy)` or `dg.AssetSpec("my_asset", freshness_policy=policy)`. Apply in bulk with `defs.map_asset_specs(func=lambda spec: dg.apply_freshness_policy(spec, policy))`, or to a selection with `defs.map_resolved_asset_specs(func=..., selection='key:"parent_asset"+')`. For a default that individual assets can override, pass `overwrite_existing=False` to `apply_freshness_policy` — otherwise bulk application overwrites a policy already set in the decorator. Not supported for observable source assets or `CacheableAssetsDefinition`.

Freshness feeds asset health status in Dagster+, where an asset's overall health is the most elevated of its latest-materialization, freshness, and asset-check statuses (Unknown / Healthy / Warning / Degraded). A materialization failure pending an automatic run retry reports Warning rather than Degraded. Health-status and freshness alerts are Dagster+ features.

## Freshness checks

Superseded by freshness policies since the 1.12 release, still supported. Reach for them only in a codebase that already uses them.

```python
checks = dg.build_last_update_freshness_checks(
    assets=[hourly_sales],
    lower_bound_delta=timedelta(minutes=45),
    deadline_cron="0 9 * * *",  # optional; defaults to check execution time
    timezone="UTC",
    blocking=False,
)
freshness_checks_sensor = dg.build_sensor_for_freshness_checks(freshness_checks=checks)
```

`dg.build_time_partition_freshness_checks(assets=, deadline_cron=, timezone=, severity=, blocking=)` is the time-window-partitioned equivalent: it passes if a record exists for the most recently completed partition once the deadline passes, and fails at runtime if handed a non-time-window-partitioned asset.

A schedule or sensor is required. If the check only runs after a materialization, it can never detect the case where materialization never happened. `build_sensor_for_freshness_checks(freshness_checks=, minimum_interval_seconds=, name=, default_status=, tags=)` skips re-evaluation while a check is already failing, since the run that fixes it will re-run the check anyway.

For external assets, emit the update time as observation metadata under the `dagster/last_updated_timestamp` key (`dg.MetadataValue.timestamp(...)`) so the check has something to compare against. Check results carry `dagster/freshness_params`, `dagster/last_updated_time`, `dagster/overdue_deadline_timestamp`, and `dagster/overdue_seconds` when overdue. `build_anomaly_detection_freshness_checks` from `dagster_cloud.anomaly_detection` is Dagster+ Pro only.

## Data contracts

Keep the expected schema in a version-controlled YAML file next to the asset, have the asset attach column schema metadata, and have a check compare the two.

```python
@dg.asset
def shipments(context: dg.AssetExecutionContext) -> pd.DataFrame:
    df = ...
    # create_table_schema_metadata_from_dataframe comes from dagster_pandas.data_frame
    context.add_output_metadata(
        {"dagster/column_schema": create_table_schema_metadata_from_dataframe(df)}
    )
    return df


@dg.asset_check(asset=shipments, description="Validate shipments schema against data contract")
def validate_shipments_data_contract(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    with open(dg.file_relative_path(__file__, "shipments_contract.yaml"), encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    event = context.instance.get_latest_materialization_event(asset_key=dg.AssetKey(["shipments"]))
    schema = event.dagster_event.event_specific_data.materialization.metadata["dagster/column_schema"]
    actual = {col.name: col.type for col in schema.value.columns}
    expected = {n: c.get("type") for n, c in contract["schema"]["columns"].items()}
    return dg.AssetCheckResult(
        passed=actual == expected,
        metadata={"expected_schema": expected, "actual_schema": actual},
        severity=dg.AssetCheckSeverity.ERROR if actual != expected else dg.AssetCheckSeverity.WARN,
    )
```

For pure "did the schema drift since last time" coverage without a contract file, use `dg.build_column_schema_change_checks` instead.

## Gotchas

- Check names must be unique per asset. `AssetCheckKey` is the (asset key, check name) pair and identifies a check across the whole deployment.
- `materialize` writes to the local filesystem. Use `materialize_to_memory` in unit tests unless you are exercising an IO manager, and never pass it an IO manager in `resources`.
- Unless the asset uses `deps=` or `non_argument_deps`, `materialize` needs the upstream asset definitions in the list, not just the one under test.
- `execute_in_process` defaults to `raise_on_error=True`. Pass `raise_on_error=False` when the test asserts on a failed run.
- `dg.build_asset_context` has no `asset_key` parameter. Identify the asset by invoking the right function; use `partition_key=` for partition-dependent logic.
- Asset checks must reach `Definitions` (autoloaded from `defs/`, or passed as `asset_checks=`) or Dagster never sees them.
- An asset check that emits no result does not block downstream assets even with `blocking=True` — it only logs a warning.
- Anything touching event history (sensors, automation conditions, freshness) needs a real instance: `dg.instance_for_test()` or `dg.DagsterInstance.ephemeral()`.
- `Definitions.get_job_def` currently falls back to resolving unresolved asset jobs and jobs attached to schedules/sensors; after 1.11 it raises instead if the job was not passed in directly.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `dg.ExpectationResult` yielded from ops | `dg.AssetCheckResult` + `@dg.asset_check` (removal in 2.0) |
| `dg.build_last_update_freshness_checks` | `FreshnessPolicy.time_window` / `FreshnessPolicy.cron` on the asset |
| `dg.build_time_partition_freshness_checks` | `FreshnessPolicy.cron` on the asset |
| `dg.build_sensor_for_freshness_checks` | Nothing — freshness policies need no sensor |
| `FreshnessPolicy` as it existed pre-1.6 | Renamed `LegacyFreshnessPolicy`; the name `FreshnessPolicy` now means the new API |
| `JobDefinition.run_request_for_partition` | `dg.RunRequest(partition_key=...)` (removal in 2.0) |

The pre-1.6 `FreshnessPolicy` was deprecated, then its name was reused for the new freshness API. Existing code that wants the old behavior must switch to `LegacyFreshnessPolicy`, per the 1.11 migration guide. Freshness checks remain supported for the foreseeable future but are no longer best practice. See `migration-and-legacy.md`.

## Sources
- https://docs.dagster.io/guides/test — testing overview index
- https://docs.dagster.io/guides/test/unit-testing-assets-and-ops — direct invocation patterns
- https://docs.dagster.io/guides/test/asset-checks — defining, blocking, partitioning, scheduling checks
- https://docs.dagster.io/guides/test/running-a-subset-of-asset-checks — `can_subset` and selected check keys
- https://docs.dagster.io/guides/test/testing-partitioned-config-and-jobs — `validate_run_config`, partitioned `execute_in_process`
- https://docs.dagster.io/guides/test/data-contracts — YAML contract + schema-validating check
- https://docs.dagster.io/guides/test/data-freshness-testing — freshness checks (superseded)
- https://docs.dagster.io/guides/observe/asset-freshness-policies — `FreshnessPolicy`, `apply_freshness_policy`
- https://docs.dagster.io/guides/observe/asset-health-status — health status rollup (Dagster+)
- https://docs.dagster.io/guides/automate/declarative-automation/automating-asset-checks — check automation conditions
- https://docs.dagster.io/guides/build/external-resources/testing-configurable-resources — resource tests
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/testing-component-definitions — `ComponentTree.for_project`
- https://docs.dagster.io/guides/build/components/creating-new-components/testing-your-component — `create_defs_folder_sandbox`
- https://docs.dagster.io/dagster-basics-tutorial/asset-checks — tutorial check example
- https://docs.dagster.io/api/dagster/asset-checks — check decorators, results, freshness check builders
- https://docs.dagster.io/api/dagster/execution — `materialize`, `execute_in_process`, context builders, `instance_for_test`
- https://docs.dagster.io/api/dagster/definitions — `Definitions.validate_loadable` and resolution accessors
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference — `dg check defs`
