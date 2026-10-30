### Q4
A `@dataclass` field can't default to a mutable instance. `DefsStateConfigArgs` is a non-frozen pydantic `Model`, so Python treats it as unhashable and raises `ValueError: mutable default <class '...DefsStateConfigArgs'> for field defs_state is not allowed: use default_factory` when the class is defined. Every `dg` command imports your component, so they all fail at import time. The docstring example has this bug; the fix is `defs_state: ResolvedDefsStateConfig = field(default_factory=DefsStateConfigArgs.local_filesystem)` (I confirmed both the error and the fix in scratch on 1.13.28). Keep the `defs_state_config` property that returns `DefsStateConfig.from_args(self.defs_state, default_key=...)`.

### Q9
Yes, the `--partition-range start...end` syntax is supported, but not with default backfill settings. `dg launch --assets` delegates to `dagster asset materialize`, which runs the range in process as a single run. For a range, it checks that every selected asset has `backfill_policy=BackfillPolicy.single_run()` and errors otherwise. So `my_asset` must be partitioned (e.g. `DailyPartitionsDefinition`) and declare `backfill_policy=dg.BackfillPolicy.single_run()`, and its code should read `context.partition_key_range` / `partition_time_window`. Otherwise, launch a backfill from the UI or run each `--partition` separately.

### Q13
- **`context.definition_metadata`**: only the metadata set on the input itself, via `AssetIn(metadata=...)` or `In(metadata=...)`.
- **`context.upstream_output.definition_metadata`**: the upstream asset's static definition metadata, i.e. `@asset(metadata={"table_name": ...})`. Read the table name from here.
- **`context.upstream_output.output_metadata`**: always `{}` in `load_input`, with a warning pointing to dagster issue #20094. Runtime `MaterializeResult(metadata=...)` values are not passed through here.

To get the reported value, query the instance: `context.instance.get_latest_materialization_event(context.asset_key).asset_materialization.metadata["key"].value`.

### Q22
By default, automation condition sensors are evaluated in the daemon, which has no access to your code, so a custom `AutomationCondition` subclass can't run there. Add a sensor that targets the asset with `use_user_code_server=True`, for example `dg.AutomationConditionSensorDefinition("automation_condition_sensor", target=dg.AssetSelection.all(), use_user_code_server=True)`. Your `evaluate(self, context: dg.AutomationContext)` must return `dg.AutomationResult(true_subset, context=context)`, using entity subsets such as `context.candidate_subset` or `context.get_empty_subset()`, not a bool. Limits: the feature is in preview (the API may change), and each sensor can target at most 500 assets or checks; evaluation also costs more because it runs on the code server.

### Q31
While the feature is in preview, it is off by default in OSS. Enable it in `dagster.yaml` with `freshness: enabled: True`, then set `freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24))` (optionally with `warn_window`) on the asset. You don't need a sensor or schedule: the `dagster-daemon` (`FreshnessDaemon`) evaluates the policies, so run `dagster dev` or the daemon. The old `FreshnessPolicy` (deprecated since 1.6) was renamed to `LegacyFreshnessPolicy`, which still exists in 1.13 for back-compat, and the new policies replace freshness checks.

### Q35
With no `run_coordinator` section, the instance defaults to `QueuedRunCoordinator`; in current Dagster, `DefaultRunCoordinator` applies only if you set it explicitly. All 25 runs go into the run queue as `QUEUED`. The `dagster-daemon` (`QueuedRunCoordinatorDaemon`) dequeues them under the default `max_concurrent_runs` of 10, so 10 start and the rest launch as slots free up. You can change the limit with `concurrency: runs: max_concurrent_runs` (`-1` removes it, `0` stops all runs), and queued runs won't launch if the daemon isn't running.

### Q43
Set `DAGSTER_CLOUD_ORGANIZATION`, `DAGSTER_CLOUD_API_TOKEN`, and `DAGSTER_BUILD_STATEDIR`, then run three stages:
1. `dg plus deploy start --deployment=<name> --project-dir=.` reads `build.yaml` and initializes the state dir.
2. `dg plus deploy build-and-push` builds the image and pushes it to the registry in `build.yaml`.
3. `dg plus deploy finish` tells Dagster+ to update the code locations to the pushed images.

A bare `dg plus deploy` is not the finish step: it runs the whole flow again (new session, rebuild and push, then finish), so you'd do a second build. The docs' generic CI page shows bare `dg plus deploy` as the last step, but the source confirms it runs the full flow. You can scaffold `build.yaml` and the Dockerfile with `dg plus deploy configure`.

### Q48
`external_asset_from_spec` and `external_assets_from_specs` no longer exist in `dagster` 1.13, so importing them fails. `SourceAsset` still works but emits a deprecation warning ("will be removed in 2.0.0. Use AssetSpec instead"). `observable_source_asset` is also a legacy pattern. The replacement is to pass `dg.AssetSpec("raw_transactions")` (with deps, metadata, and so on) directly in `Definitions(assets=[...])` or in `@asset(deps=[spec])`. Use `.with_io_manager_key(...)` if it needs to be loaded through an I/O manager. Record updates with `report_runtime_metadata`-style events: `AssetMaterialization`/`AssetObservation` from a sensor, `instance.report_runtime_event`, or the external assets REST API.

### Q54
Add `enable_semantic_model_refresh` to the `dagster_powerbi.PowerBIWorkspaceComponent` attributes in `defs.yaml`. Set it to `True` for all semantic models, or to a list of names (e.g. `- Sales Data Model`) for specific ones. The component then builds executable assets that trigger a Power BI refresh when materialized. You can check this with `dg list defs --assets 'key:semantic_model*' --columns name,kinds,is_executable`.
