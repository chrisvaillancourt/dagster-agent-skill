### Q2
Only the assets in the `dg.Definitions` returned by the `@dg.definitions` function show up, and there is no warning. The module-level `@dg.asset` is silently dropped. In `PythonFileComponent.build_defs` (`dagster/components/core/defs_module.py`), a module that contains a `@dg.definitions` function (or a `Definitions` object) just returns that object. The module's other decorated objects are scanned (`load_definitions_from_module`) only when neither exists. The only errors are for having both a `@dg.definitions` function and a `Definitions` object, or more than one `Definitions` object; to keep the asset, move it to its own module or add it to the returned `Definitions`.

### Q6
Every `dg` command sets `os.environ["DAGSTER_IS_DEV_CLI"] = "1"` in the root `dg` click group (`dagster_dg_cli/cli/__init__.py`), before any subcommand runs. That includes `dg check defs`, `dg list defs`, `dg launch` and `dg dev`. The legacy `dagster dev` sets it too (`dagster/_cli/dev.py`). Production processes such as `dagster code-server`, `dagster api grpc`, `dagster-webserver` and `dagster-daemon` don't set it, so your `os.getenv` branch takes the "dev" path in CI and the other path in production. Dagster reads the variable through `dagster._utils.env.using_dagster_dev()`, which also changes behaviour such as state-backed components refreshing when `refresh_if_dev` is set.

### Q11
It runs partition **`2024-05-19`**, the most recent *completed* daily window, because by default the schedule targets the last full partition rather than the one in progress. I checked this with `build_schedule_context` + `evaluate_tick`. Three things change the answer:
- `end_offset=1` on `DailyPartitionsDefinition` makes it run `2024-05-20`.
- A partition `hour_offset`/`minute_offset` later than the tick time leaves the 05-19 window unfinished: `hour_offset=2` gives `2024-05-18`.
- The partitions definition's `timezone` becomes the schedule's `execution_timezone`, so 01:30 is read in that zone.

### Q20
No. Once `monitored_jobs` is non-empty, a run from the sensor's own location matches only if its job is listed in `monitored_jobs`. A `CodeLocationSelector` is turned into a `RepositorySelector` and matches only runs from that location. To cover both, use `monitored_jobs=[dg.CodeLocationSelector(location_name="analytics"), dg.CodeLocationSelector(location_name="alerts")]` (or list the local jobs), or use `monitor_all_code_locations=True`.

### Q28
It fails in two ways:
- `materialize_to_memory` raises `DagsterInvariantViolationError` if you pass any resource under an IO-manager key such as `"io_manager"`, because it forces in-memory IO.
- With `[down]` alone, the `up` input can't be resolved: `DagsterInvalidDefinitionError: Input asset "["up"]" is not produced by any of the provided asset ops`.

Either pass `[up, down]` with no IO-manager resource, or use `dg.materialize([down], resources={"io_manager": ...})` where the IO manager can load `up`. With `deps=[up]`, `up` is only a non-data dependency, so `dg.materialize_to_memory([down])` succeeds on its own, but passing `io_manager` still raises the same error.

### Q33
The enum value is written as the member **name**: `color: BLUE` works, and `color: blue` fails with "not in enum type Color". The discriminated union becomes a selector keyed by the discriminator value, holding that variant's fields, so the discriminator itself isn't written: `pet: {dog: {barks: 2.0}}`. Writing `{pet_type: dog, barks: 2.0}` is rejected with "You can only specify a single field … available fields are ['cat', 'dog']". Unknown keys at any level raise `DagsterInvalidConfigError` ("Received unexpected config entry …").

### Q37
No. Both `concurrency.runs.max_concurrent_runs` and `run_retries.max_retries` are plain `Int` in the instance schema, so loading fails with `Invalid scalar … Value "{'env': 'MAXRUNS'}" … not valid for expected type "Int"`. The legacy `run_coordinator:` block (`class: QueuedRunCoordinator`) does accept `max_concurrent_runs: {env: MAXRUNS}`, which I checked (it resolved to 7). There's no env-sourced form of `run_retries.max_retries`, so template the dagster.yaml when you deploy it instead.

### Q45
No. Branch-deployment concurrency is an **organization** setting, not a deployment setting. Set `max_concurrent_runs_per_branch_deployment: 5` and `max_concurrent_branch_deployment_runs: 100` with `dg api organization settings get > org-settings.yaml`, edit the file, then `dg api organization settings set org-settings.yaml`. You can also edit it in the UI under Organization Settings > Advanced. It needs the **Organization Admin** role, and the global limit of 100 is allowed on Hybrid (max 500) but not on Serverless, where the cap is 50 unless Support raises it.

### Q52
The asset key is `AssetKey(["target", "public", "accounts"])` and its upstream dep is `AssetKey(["public", "accounts"])`. I got this by calling `DagsterSlingTranslator().get_asset_spec(...)` on the installed `dagster-sling`. From a downstream asset, write `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])`, or equivalently `deps=["target/public/accounts"]`.

### Q57
By default, the first Dagster-triggered sync sets the connector's Fivetran schedule to manual, so Fivetran stops scheduling it automatically. The two `FivetranWorkspace` settings are:
- `disable_schedule_on_trigger` (default `True`): set it to `False` to keep Fivetran's own schedule running; pair that with `build_fivetran_polling_sensor` to record syncs Fivetran runs itself.
- `retry_on_reschedule` (default `True`): if Fivetran postpones the sync for quota reasons, the step raises `RetryRequested`. With `False`, it keeps polling until the rescheduled time passes.
