All nine answers below were checked against the installed Dagster 1.13.28 source and scratch scripts under `./scratch/`. `dagster-powerbi` isn't in this project, so for Q54 I installed `dagster-powerbi==0.29.28` into a throwaway environment in `/tmp/pbi` to read its code. Nothing in `src/` was changed.

### Q4
The docstring example is broken: `DefsStateConfigArgs` is a pydantic model and isn't hashable, so `@dataclass` refuses it as a default. You get `ValueError: mutable default <class 'DefsStateConfigArgs'> for field defs_state is not allowed: use default_factory` at import time, which is why every `dg` command fails. The fix is `defs_state: ResolvedDefsStateConfig = field(default_factory=DefsStateConfigArgs.local_filesystem)`. Also subclass `dg.Resolvable` (`class X(dg.StateBackedComponent, dg.Resolvable)`) so the fields can be loaded from `defs.yaml`; `dagster_powerbi.PowerBIWorkspaceComponent` does both. You still have to implement the `defs_state_config` property, `write_state_to_path` and `build_defs_from_state`.

### Q9
It only works if every selected asset has `backfill_policy=dg.BackfillPolicy.single_run()`. Otherwise `dg launch` fails with "Partition ranges with the CLI require all selected assets to have a BackfillPolicy.single_run() policy", because a default backfill (one run per partition) needs the daemon. In that case, run the backfill from the UI with `dg dev` running. With `single_run()`, the asset gets the whole range in one run, so it should read `context.partition_keys` / `context.partition_time_window` and usually return `dg.MaterializeResult`, not a single value for the I/O manager. Both endpoints must be valid partition keys.

### Q13
- **`context.definition_metadata`**: metadata on the input definition (`dg.AssetIn(metadata=...)` / `dg.In(metadata=...)`).
- **`context.upstream_output.definition_metadata`**: the upstream asset's definition metadata, such as `@dg.asset(metadata={"table": ...})`. This is where to read the table name.
- **`context.upstream_output.output_metadata`**: from an `InputContext` this always returns `{}` and logs a warning (dagster-io/dagster#20094). It does not contain the runtime `MaterializeResult(metadata=...)` values.

To get a runtime value, read it from the event log: `context.instance.get_latest_materialization_event(context.asset_key).asset_materialization.metadata`.

### Q22
A custom subclass isn't serializable, so the daemon's `AssetDaemon` can't evaluate it. Run it in your code server with `dg.AutomationConditionSensorDefinition("my_sensor", target=..., use_user_code_server=True, default_status=dg.DefaultSensorStatus.RUNNING)` targeting the asset. `evaluate(self, context: dg.AutomationContext)` must return a `dg.AutomationResult(context, true_subset=...)`, built from `context.candidate_subset`, not a bool. Limits:
- `use_user_code_server` is beta/preview.
- One such sensor can target at most 500 assets or checks (`MAX_ENTITIES = 500`).
- It can't target asset jobs.

### Q31
`dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24), warn_window=...)` is evaluated by the `FreshnessDaemon`. It is on by default (`freshness: enabled:` in `dagster.yaml` defaults to true), so the only setup is a running daemon (`dg dev` locally, or `dagster-daemon run`). You don't need a sensor, schedule or `build_freshness_checks`. Attach it with `@dg.asset(freshness_policy=...)` (`FreshnessPolicy.cron(...)` also exists). The old `FreshnessPolicy(maximum_lag_minutes=...)` class was renamed `dg.LegacyFreshnessPolicy` and is deprecated.

### Q35
With no `run_coordinator` or `run_queue` set, the instance defaults to `QueuedRunCoordinator` (I confirmed this on a storage-only `dagster.yaml`). Its default `max_concurrent_runs` is 10. All 25 runs go into the queue, the daemon's run queue launches up to 10 at once, and the other 15 start as slots free up. If the daemon isn't running, they stay queued.

### Q43
1. `dg plus deploy start` (with `--deployment` / `--organization`) creates the deploy session.
2. `dg plus deploy build-and-push` builds and pushes the image. If you build and push yourself, use `dg plus deploy set-build-output --image-tag ...` instead.
3. `dg plus deploy finish` tells Dagster+ to update the code locations.

If the last stage is a bare `dg plus deploy`, it ignores the session you built and runs a whole new deploy: a new session, another build and push, then finish.

### Q48
- **Fails outright:** `external_asset_from_spec` and `external_assets_from_specs` were removed from `dagster`, so importing or calling them raises an error.
- **Still loads but deprecated:** `SourceAsset(key=...)` warns "will be removed in 2.0.0. Use AssetSpec instead". `@observable_source_asset` / `multi_observable_source_asset` still work but are beta.
- **Replacement:** pass `dg.AssetSpec("raw_table", ...)` directly to `Definitions` / `defs/`, and use `.with_io_manager_key(...)` if it's loaded through an I/O manager. Report its updates or observations with `dg.AssetMaterialization` / `AssetObservation`, for example from a sensor or the REST API.

### Q54
Set `enable_semantic_model_refresh` on `dagster_powerbi.PowerBIWorkspaceComponent` in `defs.yaml`. Use `true` for all semantic models, or a list of semantic model names for only those:
```yaml
type: dagster_powerbi.PowerBIWorkspaceComponent
attributes:
  workspace:
    credentials: {client_id: "{{ env.POWERBI_CLIENT_ID }}", client_secret: "{{ env.POWERBI_CLIENT_SECRET }}", tenant_id: "{{ env.POWERBI_TENANT_ID }}"}
    workspace_id: my-workspace-id
  enable_semantic_model_refresh: true   # or [ "Sales Model" ]
```
Those semantic models then become materializable assets that trigger a Power BI refresh.
