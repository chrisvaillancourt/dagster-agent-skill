I checked every answer against the installed Dagster 1.13.28: Q2 and Q11 by running them, Q28, Q33 and Q37 with scratch scripts, and Q6 and Q20 by reading the source. For Q52 and Q57 I ran the latest `dagster-sling` / `dagster-fivetran` in a temporary `uv run --no-project --with` env, since neither is in this project, and Q45 comes from the Dagster+ docs. All scratch files are under `./scratch/` and `src/` is unchanged.

### Q2
Only the assets in the `Definitions` returned by the `@dg.definitions` function show up in `dg list defs`. The module-level `@dg.asset` is silently dropped, with no warning or error. `PythonFileComponent.build_defs` only scans the module's attributes when it finds no `Definitions`/`@definitions` object, so either list the asset in the returned `Definitions` or remove the decorator. (The only conflict that raises an error is having both a module-level `Definitions` object and a `@definitions` function, or more than one `Definitions` object.)

### Q6
`using_dagster_dev()` in `dagster._utils.env` returns true when `DAGSTER_IS_DEV_CLI` is set. The `dg` root command sets `os.environ["DAGSTER_IS_DEV_CLI"]="1"` before any subcommand runs, so it is set for every `dg` command (`dg check defs`, `dg list defs`, `dg dev`, `dg launch`, …), and child processes inherit it. `dagster dev` also sets it. Production processes (`dagster code-server start`, `dagster-webserver`, `dagster-daemon`, run workers) don't set it, so code that branches on it takes different paths in CI and in prod.

### Q11
It runs partition `2024-05-19`. With the default `end_offset=0`, a time-window partitioned schedule targets the most recent complete partition at tick time, which is the previous day; `hour_of_day`/`minute_of_hour` only shift the cron to `30 1 * * *`. Setting `DailyPartitionsDefinition(..., end_offset=1)` makes it run `2024-05-20` (I confirmed both with `evaluate_tick`). The partitions definition's `timezone` also changes which day "01:30" falls on.

### Q20
No. Once `monitored_jobs` is set, a run from the sensor's own location only matches if its job is listed directly (as a `JobDefinition`/`UnresolvedAssetJobDefinition`/`GraphDefinition`). The `CodeLocationSelector` is turned into a `RepositorySelector` for `analytics` only. Add the `alerts` jobs (or `dg.CodeLocationSelector(location_name="alerts")`) to `monitored_jobs`, or use `monitor_all_code_locations=True`.

### Q28
That call fails in two ways. `materialize_to_memory` raises `DagsterInvariantViolationError` if you pass any I/O manager resource, because it forces in-memory I/O for every key. And `[down]` alone raises `DagsterInvalidDefinitionError: Input asset "["up"]" is not produced by any of the provided asset ops and is not one of the provided sources`, so you must include `up` (`materialize_to_memory([up, down])`), or just call `down(1)` directly. With `deps=[up]` there is no data input, so `materialize_to_memory([down])` works without `up`; passing an `io_manager` resource is still an error.

### Q33
Write the enum by member name (`color: RED`), not by its value; `"r"` fails with "not in enum type Color". Dagster turns the discriminated union into a selector keyed by the discriminator value, so you write `pet: {cat: {meows: 3}}`; the flat pydantic shape `{pet_type: cat, meows: 3}` is rejected ("You can only specify a single field…"). Unknown keys at any level fail validation with `DagsterInvalidConfigError: Received unexpected config entry "x"`.

### Q37
No, the instance fails to load with `DagsterInvalidConfigError: Invalid scalar at path root:concurrency:runs:max_concurrent_runs … expected type "Int"`, and the same error for `run_retries:max_retries`. Both fields take plain `Int`, not `IntSource`. The legacy `run_coordinator: {module: dagster.core.run_coordinator, class: QueuedRunCoordinator, config: {max_concurrent_runs: {env: MAXC}}}` does accept `{env: …}`. Otherwise, write `dagster.yaml` from a template at deploy time.

### Q45
No. These are organization settings, not deployment settings: `max_concurrent_runs_per_branch_deployment: 5` and `max_concurrent_branch_deployment_runs: 100` (default 50), plus optional `branch_deployment_tag_concurrency_limits`. Edit them in the UI or with `dg api organization settings get > org-settings.yaml` and then `dg api organization settings set org-settings.yaml`, after `dg plus login`. Either method needs the Organization Admin role.

### Q52
The asset key is `AssetKey(["target", "public", "accounts"])` and its single upstream dep is `AssetKey(["public", "accounts"])`; I confirmed this with `DagsterSlingTranslator().get_asset_spec(...)`. Reference it downstream with `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])`. If the stream sets `config.object`, the key is built from that object name instead.

### Q57
By default (`disable_schedule_on_trigger=True`), when Dagster triggers a sync it first sets the connector's `schedule_type` to `"manual"`, which turns off the connector's Fivetran schedule so only Dagster triggers syncs. The two `FivetranWorkspace` settings are:
- `disable_schedule_on_trigger=False`, which leaves the Fivetran schedule on.
- `retry_on_reschedule`, which defaults to `True` and raises `RetryRequested` when Fivetran reschedules a sync for quota limits. With `False`, Dagster keeps polling until the rescheduled time passes.

Sources:
- [Configuring concurrency for branch deployments](https://dagster.io/docs/deployment/dagster-plus/deploying-code/branch-deployments/branch-deployment-concurrency)
- [Advanced concurrency](https://docs.dagster.io/guides/operate/managing-concurrency/advanced)
