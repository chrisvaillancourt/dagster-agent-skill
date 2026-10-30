### Q2
Only the assets included in the `dg.Definitions(...)` returned by the `@dg.definitions` function show up; module-level `@dg.asset` objects are not automatically merged into it. A module-level asset still appears if that function explicitly includes it. There is no warning about the ignored assets; an isolated `uv run dg list defs --json` probe confirmed this.

### Q6
`dg` sets `DAGSTER_IS_DEV_CLI="1"` in its root CLI callback, not just in `dg dev`, and subprocesses inherit it. Consequently, `dg check defs`, `dg list defs`, `dg launch`, and other `dg` commands run with the flag set, whereas production loading outside `dg` may not. Treat it as a CLI-execution flag, not a reliable development-versus-production switch.

### Q11
With default midnight-aligned daily partitions, `end_offset=0`, and the tick interpreted in the partitions’ timezone, the tick selects **`2024-05-19`**, the latest completed partition. `hour_of_day=1, minute_of_hour=30` controls when the schedule fires, not which partition date it names. Partition timezone, boundary offsets, `end_offset`, and availability bounds or exclusions can change the result: probes selected `2024-05-20` with `end_offset=1`, but `2024-05-18` with a 02:00 daily boundary and `end_offset=0`.

### Q20
No: supplying `monitored_jobs` replaces the default monitoring of jobs in the sensor’s own code location; it does not add to it. With only `dg.CodeLocationSelector(location_name="analytics")`, failures in `analytics` match, but failures in `alerts` do not. Include a selector for `alerts` too, or use `monitor_all_code_locations=True` without `monitored_jobs` to monitor the whole deployment.

### Q28
`dg.materialize_to_memory` overrides asset I/O managers with in-memory managers and rejects an explicitly supplied resource under an I/O-manager key such as `"io_manager"`; use `dg.materialize` if you want `dg.FilesystemIOManager`. Removing that resource still leaves `dg.materialize_to_memory([down])` invalid for `def down(up)`, because its value-bearing upstream input is missing; include `up` to compute and supply the value. If `down` instead uses `deps=[up]` and has no `up` parameter, the dependency requires no input loading, so materializing `[down]` alone can succeed—but supplying `"io_manager"` to `materialize_to_memory` remains invalid.

### Q33
In run-config YAML under `ops: {<asset_op_name>: {config: ...}}`, an enum field uses the **member name**, such as `color: RED`, not its underlying value such as `"red"`. The discriminated union uses a selector keyed by the discriminator value, such as `pet: {cat: {meows: 3}}`, not `pet: {pet_type: cat, meows: 3}`; omit `pet_type` inside the selected branch. Ordinary `dg.Config` rejects unknown keys, including inside union branches; `dg.PermissiveConfig` explicitly allows extras at the model level where it is used.

### Q37
No: in Dagster 1.13.28, both `concurrency.runs.max_concurrent_runs` and `run_retries.max_retries` are plain integer config fields, not `IntSource` fields. `{env: NAME}` therefore causes `DagsterInvalidConfigError`, even when the environment variable contains a valid integer. Supply literal integers or render the YAML before loading it; the older `run_queue.max_concurrent_runs` field does support `{env: NAME}`.

### Q45
These are **organization-level** settings: apply YAML containing `max_concurrent_runs_per_branch_deployment: 5` and `max_concurrent_branch_deployment_runs: 100` with `dg api organization settings set org-settings.yaml`, not `dg api deployment settings set`. Both limits apply simultaneously, and changing them requires the **Organization Admin** role. The [branch-deployment concurrency documentation](https://github.com/dagster-io/dagster/blob/1.13.28/docs/docs/deployment/dagster-plus/deploying-code/branch-deployments/branch-deployment-concurrency.md) allows 100 on Hybrid; Serverless has a documented maximum of 50 unless Support increases it.

### Q52
The default translator generates `dg.AssetKey(["target", "public", "accounts"])`, with upstream dependency `dg.AssetKey(["public", "accounts"])`. Reference the generated asset with `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])` for an ordering-only dependency, or use `dg.AssetIn(key=...)` for a value-bearing input with an appropriate I/O manager. The [translator implementation](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-sling/dagster_sling/dagster_sling_translator.py) uses multipart keys, not a single `"target_public_accounts"` string.

### Q57
By default, Dagster sets the connector’s Fivetran `schedule_type` to `"manual"` before triggering a sync, disabling Fivetran’s independent schedule. Set `FivetranWorkspace(disable_schedule_on_trigger=False, ...)` to preserve that schedule. The other setting is `retry_on_reschedule`: its default `True` raises Dagster `RetryRequested` when Fivetran reschedules because of quota limits, whereas `False` keeps polling instead, as described in the [Fivetran integration documentation](https://github.com/dagster-io/dagster/blob/1.13.28/docs/docs/integrations/libraries/fivetran/index.md).
