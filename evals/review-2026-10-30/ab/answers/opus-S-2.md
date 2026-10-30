I checked every answer below against the installed 1.13.28 source or by running scratch scripts in `./scratch/`. `dagster-dbt` 0.29.28 isn't in this project, so for Q53 I loaded it in a throwaway `uv run --no-project --with dagster-dbt` environment. Nothing under `src/` was changed.

### Q3
Final kinds are `{b}` and final tags are `{x: '1', y: '2', 'dagster/kind/b': ''}`. Kind `a` is gone because the default `operation: merge` only merges `metadata` and `tags`; every other attribute, including `kinds`, goes through `spec.replace_attributes`. `dg check yaml` checks only the outer shape: `post_processing` must be an object whose only key is `assets`, holding a list of objects. It does not validate the keys inside those objects (`target`, `operation`, `attributes`), so mistakes there only show up when `dg check defs` loads the component.

### Q7
It works in 1.13.28. The asset gets the new `partitions_def` and can be materialized by partition (I tested `dg.materialize(..., partition_key="2024-01-02")`). It works for both `@dg.asset` definitions and plain `AssetSpec`s. Calling `Definitions.map_asset_specs` (which calls `map_resolved_asset_specs` internally) emits a `PreviewWarning`, and passing its old `selection=` argument fails an invariant check; use `map_resolved_asset_specs(func=..., selection=...)` if you need a selection.

### Q12
The default `FilesystemIOManager` is built on `UPathIOManager`. It loads each of the 24 hourly partitions mapped into the daily partition (default `TimeWindowPartitionMapping`) and raises `FileNotFoundError` on the first missing one by default, so the downstream step fails. To let it run, set input metadata: `ins={"up": dg.AssetIn(metadata={"allow_missing_partitions": True})}`. Missing partitions are then skipped with a warning, and the dict contains only the 3 partition keys that exist.

### Q21
Use `dg.AutomationCondition.on_cron("<cron>")`. It requests the asset once per cron tick, only after all of its upstream assets have updated since the previous tick, and not at all if one of them hasn't. The common wrong choice is `dg.AutomationCondition.eager()`, which fires whenever any single dependency updates. Either way, the automation condition sensor has to be running.

### Q29
`orders` materializes and both checks run. The ERROR-severity blocking check (`passed=False`, default severity ERROR) fails its step, so the downstream asset is never executed. The WARN-severity check records a failed evaluation but its step succeeds and blocks nothing. With default arguments (`raise_on_error=True`), `dg.materialize(...)` raises `DagsterAssetCheckFailedError: 1 blocking asset check failed with ERROR severity`. With `raise_on_error=False` it returns a result with `success == False`; with only the WARN check, the downstream runs and the run succeeds.

### Q34
It must go in the `.env` file in the project root, because `DgContext.project_python_executable` reads it with `dotenv_values(<project_root>/.env)`. The value is the path to the Python executable, relative to the project root, e.g. `DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python`; without it, `dg` uses `.venv/` in the project root. Exporting it in your shell does nothing, since the shell environment is never consulted.

### Q39
No. A pool with no configured limit is unlimited, so `pool="db"` does nothing until you set `concurrency.pools.default_limit` in `dagster.yaml` or a per-pool limit (`dagster instance concurrency set db 1`, or in the UI). By default, a cancelled run keeps its slot until someone frees it manually under Deployment > Concurrency. To have slots freed automatically, set `run_monitoring.free_slots_after_run_end_seconds` (e.g. 300) in `dagster.yaml`, which needs the daemon.

### Q47
- **`@asset(freshness_policy=LegacyFreshnessPolicy(...))`** fails when the module loads with `ParameterCheckError: Param "freshness_policy" is not a FreshnessPolicy`. That argument now only accepts the new `dg.FreshnessPolicy`; use `legacy_freshness_policy=` as a stopgap, or move to the new policy.
- **`AssetOut(legacy_freshness_policy=...)`** is still accepted, with a `DeprecationWarning`, and stored on the spec. Nothing in 1.13 acts on it.
- **`@observable_source_asset(auto_observe_interval_minutes=10)`** is silently ignored: the decorator swallows it in `**kwargs` (any made-up keyword is accepted too), so the asset never auto-observes. Replace it with `automation_condition=` or a schedule/sensor that runs the observation.

### Q53
Set it in the component's `translation_settings` attribute, which takes the settings as plain YAML keys:
```yaml
attributes:
  translation_settings:
    enable_source_tests_as_checks: true
```
Yes, code references are on by default. The component's default settings class, `DagsterDbtComponentTranslatorSettings`, sets `enable_code_references: bool = True`, unlike plain `DagsterDbtTranslatorSettings`, where it defaults to `False`.
