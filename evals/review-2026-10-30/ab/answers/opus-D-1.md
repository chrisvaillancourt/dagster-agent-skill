I checked all ten questions. Eight are confirmed by running scratch code or reading the installed 1.13.28 source. Q45 comes from the official Dagster+ docs page. Q52 and Q57 come from the dagster-sling and dagster-fivetran source on GitHub `master`, because those packages aren't installed here. Scratch files are under `./scratch/`, and `src/` was not touched.

### Q2
Only the assets in the `Definitions` returned by the `@dg.definitions` function show up. The module-level `@dg.asset` is silently dropped, with no warning. I tested this: `dg list defs` listed only `inner_asset` and printed no warning. In `DagsterDefsComponent.build_defs`, a `@definitions` function (or a module-level `Definitions` object) takes over the whole module, so the scan for loose asset objects never runs. To keep the module-level asset, include it in the returned `dg.Definitions(assets=[...])`. Having both a `Definitions` object and a `@definitions` function in one module is an error (`DagsterInvalidDefinitionError`).

### Q6
The top-level `dg` click group sets `os.environ["DAGSTER_IS_DEV_CLI"] = "1"` before any subcommand runs. So every `dg` command sets it, including `dg check defs`, `dg list defs` and `dg dev`. The legacy `dagster dev` sets it too; Dagster reads it through `dagster._utils.env.using_dagster_dev()`. In production the code is loaded by code servers or webserver processes that are not started through those CLIs, so the variable is unset and your code takes the non-dev branch. CI running `dg check defs` therefore validates the dev branch, not the production one.

### Q11
It runs partition `2024-05-19`. By default the schedule targets the last *complete* partition, and the May 20 day isn't finished at 01:30. Verified with `evaluate_tick`, the answer changes as follows:
- `DailyPartitionsDefinition(end_offset=1)` makes it run `2024-05-20`, the in-progress day.
- A partition `hour_offset`/`minute_offset` later than 01:30 changes it; for example `hour_offset=2` gives `2024-05-18`, because the May 19 partition hasn't ended yet.
- The partitions definition's `timezone` also matters, since the cron fires in that timezone.

### Q20
No. Once `monitored_jobs` is set, the sensor's own location only matches runs whose jobs are listed in it, and a `CodeLocationSelector('analytics')` covers only `analytics` runs. To fix it, also add `dg.CodeLocationSelector(location_name='alerts')` (or the specific `alerts` jobs), or use `monitor_all_code_locations=True`.

### Q28
There are two problems. First, `materialize_to_memory` always uses an in-memory IO manager and raises `DagsterInvariantViolationError` ("Do not provide resources for io manager keys…") if you pass an `io_manager` resource. Second, `down` alone fails with `DagsterInvalidDefinitionError`: input asset `up` "is not produced by any of the provided asset ops". Pass `[up, down]` (which I confirmed succeeds), or use `dg.materialize` with your IO manager and a `selection`. If `down` declares `deps=[up]` instead, `up` is a non-data dependency with nothing to load, so `dg.materialize_to_memory([down])` succeeds on its own.

### Q33
The enum is written as the member **name**: `color: RED` works, while `color: red` fails with "not in enum type Color". The discriminated union becomes a selector keyed by the discriminator value, with the remaining fields nested under it: `pet: {cat: {meows: 3}}`. The flat form `{pet_type: cat, meows: 3}` is rejected with "You can only specify a single field". Unknown keys are an error ("Received unexpected config entry 'extra'"), because `dg.Config` schemas are strict.

### Q37
No. Both `concurrency.runs.max_concurrent_runs` and `run_retries.max_retries` are typed as plain `Int`, so loading the instance fails with `DagsterInvalidConfigError` ("Invalid scalar … expected type Int"). I tested this in 1.13.28. The legacy `run_coordinator:` block with `QueuedRunCoordinator` does accept it: `config: max_concurrent_runs: {env: MAX_RUNS}` loaded as 7. Otherwise, template the YAML at deploy time.

### Q45
No. Branch deployments share organization-scoped settings, not deployment settings, and they require the **Organization Admin** role. Set `max_concurrent_runs_per_branch_deployment: 5` and `max_concurrent_branch_deployment_runs: 100` (default 50; maximum 500 on Hybrid, 50 on Serverless). Apply them with `dg api organization settings get > org.yaml`, edit the file, then `dg api organization settings set org.yaml`, or use the UI under Organization Settings → Advanced. Both limits apply at the same time.

### Q52
The asset key is `AssetKey(["target", "public", "accounts"])`, written `target/public/accounts`. The code splits the sanitized stream name on dots, even though an older docstring says "target_public_accounts". The default upstream dependency is `AssetKey(["public", "accounts"])`. Downstream, use `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])`, or `deps=["target/public/accounts"]`, or pass the `@sling_assets` definition and select by key.

### Q57
By default, when Dagster triggers a sync it sets the connector's `schedule_type` to `"manual"`, which turns off Fivetran's own schedule so the connector only syncs when Dagster runs it. The two `FivetranWorkspace` settings are:
- `disable_schedule_on_trigger` (default `True`); set it to `False` to keep Fivetran's schedule.
- `retry_on_reschedule` (default `True`): when Fivetran reschedules the sync because of quota limits, the step raises `RetryRequested`. Set it to `False` to keep polling until the rescheduled time instead.
