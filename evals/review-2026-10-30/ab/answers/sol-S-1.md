### Q2
Only assets explicitly included in the `dg.Definitions(...)` returned by the `@dg.definitions` function contribute from that module; other module-level assets are silently skipped. There is no warning about those skipped assets—include them in the returned `assets=[...]` or move the function to a separate module.

### Q6
In Dagster 1.13.28, the root `dg` CLI callback sets `DAGSTER_IS_DEV_CLI="1"` for all subcommands, not just `dg dev`, so `dg check defs`, `dg list defs`, and `dg launch` also inherit it. Consequently, checking that variable detects execution through `dg`, not whether the environment is actually development; production processes started outside `dg` do not automatically receive it.

### Q11
With default midnight-aligned daily partitions, UTC, and `end_offset=0`, the tick requests partition `2024-05-19`, the latest completed daily window. The schedule inherits the partitions definition’s timezone, and partition boundaries (`hour_offset`/`minute_offset`), `end_offset`, and start/end bounds can change the result or leave no available partition. For example, `end_offset=1` selects `2024-05-20`, while daily windows ending at 03:00 select `2024-05-18` at that 01:30 tick.

### Q20
No: supplying `monitored_jobs` replaces the default monitoring of jobs in the sensor’s own repository rather than extending it. Include both `dg.CodeLocationSelector(location_name="analytics")` and `dg.CodeLocationSelector(location_name="alerts")` to monitor both standard Definitions-based locations. Alternatively, use `monitor_all_code_locations=True` without `monitored_jobs` to monitor the whole deployment.

### Q28
`dg.materialize_to_memory` rejects explicit IO-manager resources with `DagsterInvariantViolationError`; it supplies in-memory IO managers itself. After removing that resource, `[down]` still fails because the value input `up` requires an upstream definition/source: use `dg.materialize_to_memory([up, down])`, or use `dg.materialize` with a suitable source and filesystem IO manager to load previously stored data. If `down` instead uses `@dg.asset(deps=[up])` and removes the `up` parameter, the dependency is ordering/lineage only, so `dg.materialize_to_memory([down])` works without including or loading `up`; the explicit IO-manager restriction remains.

### Q33
Under `ops: {asset_name: {config: ...}}`, write an enum’s **member name**, not its value—for `Color.RED = "red-value"`, use `color: RED`. Write the discriminated union as a selector, such as `pet: {cat: {meows: 10}}`, not `pet: {pet_type: cat, meows: 10}`. Unknown keys in ordinary `dg.Config` schemas, including nested variants, raise `DagsterInvalidConfigError`; use `dg.PermissiveConfig` where extra fields are intentionally allowed.

### Q37
No: in Dagster 1.13.28, both `concurrency.runs.max_concurrent_runs` and `run_retries.max_retries` use plain integer schemas, not `IntSource`. An `{env: NAME}` mapping therefore fails instance-config validation even if the variable exists; provide literal integers or render them into `dagster.yaml` before Dagster loads it.

### Q45
These are **organization settings**, not deployment settings: set `max_concurrent_runs_per_branch_deployment: 5` and `max_concurrent_branch_deployment_runs: 100` using `uv run dg api organization settings set org-settings.yaml`, preserving the existing settings obtained with `organization settings get`. Both limits apply simultaneously, and changing them requires the **Organization Admin** role. The documented aggregate maximum is 500 for Hybrid but 50 for Serverless, so 100 requires Hybrid or a Support-approved increase ([reference](https://docs.dagster.io/deployment/dagster-plus/deploying-code/branch-deployments/branch-deployment-concurrency)).

### Q52
The generated key is `dg.AssetKey(["target", "public", "accounts"])`, and its upstream dependency is `dg.AssetKey(["public", "accounts"])`. Reference it downstream with `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])`; `"target_public_accounts"` and the single string `"target/public/accounts"` are different keys. The [1.13.28 translator implementation](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-sling/dagster_sling/dagster_sling_translator.py) splits dots into path segments, despite outdated underscore-based examples in its docstrings.

### Q57
By default, triggering a sync through `FivetranWorkspace` changes the connector’s Fivetran `schedule_type` to `"manual"`, disabling its automatic schedule rather than merely pausing it for that run. Set `disable_schedule_on_trigger=False` to preserve Fivetran scheduling. The other setting, `retry_on_reschedule=True`, makes quota-related rescheduling raise Dagster `RetryRequested`; set it to `False` to continue polling in the same step instead ([1.13.28 source](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-fivetran/dagster_fivetran/resources.py)).
