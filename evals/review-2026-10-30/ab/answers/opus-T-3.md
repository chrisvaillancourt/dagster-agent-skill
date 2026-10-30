I checked each answer against the installed packages (dagster 1.13.28, dagster-dg-cli, and the dagster-powerbi 0.29.28 wheel) and reproduced Q4 in a scratch script.

### Q4
In your Python version, `dataclasses` refuses a field default that isn't hashable. A `DefsStateConfigArgs` instance is a pydantic model, so it isn't hashable. I reproduced the error: `ValueError: mutable default <class '...DefsStateConfigArgs'> for field defs_state is not allowed: use default_factory`. It happens at import time, so every `dg` command that loads your component fails. The fix is `defs_state: ResolvedDefsStateConfig = field(default_factory=DefsStateConfigArgs.local_filesystem)`, using `from dataclasses import dataclass, field`; you also still need to implement the `defs_state_config` property plus `write_state_to_path` and `build_defs_from_state`.

### Q9
No, not with the default backfill policy. `--partition-range START...END` requires every selected asset to have `backfill_policy=dg.BackfillPolicy.single_run()`, and otherwise it errors before launching. With a single-run policy, `dg launch` runs the whole range as one run, so the asset should handle `context.partition_time_window` / `context.partition_key_range` rather than a single `partition_key`. For a multi-run backfill you have to launch a backfill from the UI instead, or launch one partition at a time with `--partition`.

### Q13
- **`context.definition_metadata`** holds only the metadata on the input definition itself, meaning what you passed to `AssetIn(metadata=...)` or `In(metadata=...)`.
- **`context.upstream_output.definition_metadata`** holds the upstream asset's definition metadata (`@asset(metadata={"table_name": ...})`), so read the table name from there.
- **`context.upstream_output.output_metadata`** always returns `{}` inside `load_input`, with a warning (GitHub issue #20094), because runtime metadata isn't available there.
- **For the runtime `MaterializeResult` value**, query the instance: `context.instance.get_latest_materialization_event(context.asset_key).asset_materialization.metadata`.

### Q22
A custom subclass can't be serialized (`is_serializable` is False because it isn't `@whitelist_for_serdes`), so the daemon's default automation sensor can't evaluate it. You must also define a `dg.AutomationConditionSensorDefinition(..., target=..., use_user_code_server=True)` so evaluation runs in your code server, and your `evaluate(self, context)` must return `dg.AutomationResult(true_subset=..., context=context)`. Limits:
- `use_user_code_server` is beta.
- One such sensor can target at most 500 assets or checks; split it into several sensors if you have more.
- It can't be combined with `default_condition` or asset-job keys.
- Evaluation is slower and adds load to the code server.

### Q31
No extra configuration is needed: in 1.13 the instance setting `freshness: enabled:` defaults to `True`. Pass `freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24))` on `@asset` or `AssetSpec`. You don't need a sensor or schedule, because the built-in `FreshnessDaemon` evaluates the policy; just run `dagster-daemon` (`dg dev` / `dagster dev` start it). The old class is now `dg.LegacyFreshnessPolicy` (deprecated, passed as `legacy_freshness_policy=`); `FreshnessPolicy` now refers to the new class, and the old one's auto-materialize role has moved to `AutomationCondition`.

### Q35
`QueuedRunCoordinator` is in effect, because that's the instance default when there's no `run_coordinator` section. All 25 runs start out `QUEUED`. The daemon's `QueuedRunCoordinatorDaemon` then dequeues them, keeping at most 10 running at once (`max_concurrent_runs` defaults to 10), and starts the rest as slots free up. If `dagster-daemon` isn't running, they stay queued.

### Q43
Run `dg plus deploy start` (with `--deployment` / `--organization` as needed), then `dg plus deploy build-and-push`, then `dg plus deploy finish`. `finish` is the step that tells Dagster+ to update the code locations to the newly pushed code. A bare `dg plus deploy` doesn't finish your session: with no subcommand it runs the full pipeline again, starting a new session, building and pushing again, and then finishing. So you'd get a redundant second build rather than a deploy of what you already pushed.

### Q48
`external_asset_from_spec` and `external_assets_from_specs` no longer exist in `dagster`, so imports of them fail. `SourceAsset` still exists but is deprecated (removal planned for 2.0), and its `freshness_policy` argument is now `legacy_freshness_policy`. The replacement is to put a plain `dg.AssetSpec(...)` directly in `dg.Definitions(assets=[...])`, using `AssetSpec(...).with_io_manager_key(...)` where you used `SourceAsset.io_manager_key`. For observations, use `@dg.observable_source_asset` or `@dg.multi_observable_source_asset`, or report events with `report_runless_asset_event`.

### Q54
Set `enable_semantic_model_refresh` in the `dagster_powerbi.PowerBIWorkspaceComponent` attributes in `defs.yaml`. Use `true` for all semantic models, or a list of model names for specific ones. The component then builds a materializable asset for each enabled model that calls `trigger_and_poll_refresh` on the workspace resource. Without this setting (it defaults to `false`), the models are just non-materializable specs.
