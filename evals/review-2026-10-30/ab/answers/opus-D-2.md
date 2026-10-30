### Q3
After post-processing, the asset's kinds are `{b}` (`a` is gone) and its tags are `{x: '1', y: '2', dagster/kind/b: ''}`. The default `operation: merge` only merges `metadata` and `tags` (`spec.merge_attributes`). Every other attribute, including `kinds`, is applied with `replace_attributes` (see `dagster/components/resolved/core_models.py:416-420`). I confirmed this output by running `AssetSpec` in a scratch script. `dg check yaml` only checks the outer shape: `post_processing` may contain only `assets`, which must be a list of objects (`COMPONENT_FILE_SCHEMA` in `dagster_dg_core/check.py`). Problems with `target`, `attributes` or `operation` inside each item only show up when definitions load, for example with `dg check defs`.

### Q7
It works in 1.13.28. I ran it, and the mapped asset ended up with the new `DailyPartitionsDefinition` and no error. `AssetsDefinition.map_asset_specs` only rejects key changes. Other attributes, including `partitions_def`, go through `replace_specs_on_asset`. One limit: inside a multi-asset, specs can only have different `partitions_def`s if `can_subset=True`.

### Q12
The run fails. `UPathIOManager.load_partitions` raises `FileNotFoundError` for the first of the 21 missing hourly files, so the downstream step fails while loading its input. To load just the 3 partitions that exist, set `ins={"up": dg.AssetIn(metadata={"allow_missing_partitions": True})}`. The I/O manager then skips missing partitions and logs that it did, and `up` is a dict with only 3 keys.

### Q21
Use `AutomationCondition.on_cron("<cron>")`. Once per cron tick it waits until all upstream dependencies have updated since that tick (`all_deps_updated_since_cron`), then requests the asset once. The common wrong choice is `AutomationCondition.eager()`, which fires as soon as any one dependency updates (`any_deps_updated`). With three upstreams it can run up to three times per cycle.

### Q29
`orders` materializes and both checks run. The ERROR-severity blocking check `c1` fails its step with `DagsterAssetCheckFailedError`, so the downstream asset is skipped ("Dependencies for step down failed: ['orders_c1']"). The WARN check `c2` records a failed evaluation, but its step succeeds and it would not block anything on its own. With default arguments (`raise_on_error=True`), `dg.materialize(...)` raises `DagsterAssetCheckFailedError`. With `raise_on_error=False` it returns a result with `success == False`. I confirmed both by running them.

### Q34
Put `DG_PROJECT_PYTHON_EXECUTABLE` in a `.env` file at the project root, pointing at the interpreter itself, e.g. `DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python`. A relative path is resolved against the project root. Exporting it in your shell does nothing: `DgContext.project_python_executable` only reads it from `<project_root>/.env` with `dotenv_values`. Without that entry, `dg` falls back to `<project_root>/.venv`.

### Q39
No, it doesn't throttle. Pools with no limit are unlimited unless the instance sets a default in `dagster.yaml` under `concurrency: pools: default_limit`. You can also set a per-pool limit with `dg`/`dagster instance concurrency set db 1` or in the UI. When a run is cancelled, the step worker normally releases its slot as it shuts down. If the process was killed and the slot is left claimed, the run-monitoring daemon frees slots held by finished runs, but only when `run_monitoring: free_slots_after_run_end_seconds` is set (`dagster/_daemon/monitoring/concurrency.py`). Otherwise you free it manually in the UI's concurrency/pool page or by resetting the pool limit.

### Q47
- **`@asset(freshness_policy=LegacyFreshnessPolicy(...))`:** fails at definition time with `ParameterCheckError`, because in 1.13 `freshness_policy` only accepts the new `FreshnessPolicy`, e.g. `FreshnessPolicy.time_window(fail_window=timedelta(minutes=5))`.
- **`AssetOut(legacy_freshness_policy=...)` (and `@asset(legacy_freshness_policy=...)`):** still accepted with a `DeprecationWarning`, and the policy is stored on the spec. Nothing evaluates or enforces it anymore; use the new `FreshnessPolicy` or freshness checks instead.
- **`@observable_source_asset(auto_observe_interval_minutes=10)`:** still works (it's a beta API). When the asset daemon or automation sensors are running, it launches an observation run roughly every 10 minutes (`_legacy_build_auto_observe_run_requests`).

### Q53
In the YAML, use the `translation_settings` attribute: `attributes: {project: ..., translation_settings: {enable_source_tests_as_checks: true}}`. It resolves to `DagsterDbtComponentTranslatorSettings`, a subclass of `DagsterDbtTranslatorSettings`. Yes, code references are on by default: that subclass sets `enable_code_references: bool = True`, while the plain `DagsterDbtTranslatorSettings` defaults it to `False`. I checked this in the 1.13.28 `dagster-dbt` source on GitHub (`dagster_dbt/components/dbt_component_utils.py`), since `dagster-dbt` isn't installed in this project.

Scratch scripts are in `./scratch/`; `src/` was not modified.
