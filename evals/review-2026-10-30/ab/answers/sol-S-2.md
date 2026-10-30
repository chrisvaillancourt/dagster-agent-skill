### Q3
Final kinds are `{"b"}`, not `{"a", "b"}`; user tags are `{"x": "1", "y": "2"}`, with the internal kind tag `dagster/kind/b: ""` also present. Default post-processing merges `tags` and `metadata` but replaces `kinds`. In 1.13.28, `uv run dg check yaml` does **not** validate the nested `post_processing` keys because that field has a permissive mapping schema; use `uv run dg check defs` to catch resolution-time errors.

### Q7
As written, it raises `TypeError`: `Definitions.map_asset_specs` requires the keyword-only argument `func`. Use `defs.map_asset_specs(func=lambda spec: spec.replace_attributes(partitions_def=pdef))`; in 1.13.28 this successfully changes the partitions definition, including inside `Component.build_defs`, provided the resulting asset definitions remain valid.
[later addendum] The late type-checker diagnostics confirm the positional-call error described above; the corrected call uses `map_asset_specs(func=...)`.

### Q12
The I/O manager tries to load all 24 mapped hourly partitions and fails on the first missing file, so the downstream function never executes. Set `ins={"up": dg.AssetIn(metadata={"allow_missing_partitions": True})}` on the downstream asset to skip missing files and receive a dictionary containing the three available partitions. `TimeWindowPartitionMapping(allow_nonexistent_upstream_partitions=True)` is not the fix: it concerns partition keys outside the upstream’s defined range, not missing stored outputs.

### Q21
Use `dg.AutomationCondition.on_cron("<cycle cron>")` to establish a cycle boundary and wait for all three upstream assets to update since that boundary. The common wrong choice is `dg.AutomationCondition.eager()`, which reacts when **any** upstream updates rather than requiring a fresh update from every upstream each cycle. Choose a cron window that contains the intended upstream cycle.

### Q29
With default severity `ERROR`, `orders` materializes and its blocking check runs, but the downstream does not run; `dg.materialize(...)` defaults to `raise_on_error=True` and raises `DagsterAssetCheckFailedError`. A failed blocking check with `severity=dg.AssetCheckSeverity.WARN` records the failed check but allows downstream execution, and an otherwise successful materialization returns a result with `success=True`. If both checks are selected in one run, the `ERROR` failure still blocks downstream; `raise_on_error=False` returns an unsuccessful result instead of raising.

### Q34
Set `DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python` in the **project root’s `.env` file**, pointing to the executable rather than the virtualenv directory. Relative paths resolve against the project root. Exporting that variable only in your shell does **not** work for this selection mechanism: `dg` reads the value directly from `.env`.

### Q39
`pool="db"` alone does not throttle execution: it needs an explicit pool limit or `concurrency.pools.default_limit`. Normal cooperative cancellation releases claimed slots through terminal-step handling, but an abruptly killed worker can leave slots behind. For leaked slots, enable run monitoring and configure `run_monitoring.free_slots_after_run_end_seconds`; the monitoring daemon frees them after the run reaches a finished state and the timeout elapses, or you can free them manually through the concurrency UI.

### Q47
`@dg.asset(freshness_policy=LegacyFreshnessPolicy(...))` fails definition construction with a parameter-type error: `freshness_policy` now expects the new `dg.FreshnessPolicy`. Surprisingly, `dg.AssetOut(legacy_freshness_policy=...)` is still accepted and stores the legacy policy in 1.13.28 despite its removal/deprecation annotations. `@dg.observable_source_asset(auto_observe_interval_minutes=10)` accepts the obsolete keyword through `**kwargs` but silently ignores it, leaving the interval unset; use supported `automation_condition` configuration instead.
[later addendum] The obsolete observation keyword is rejected by the public typing overloads but accepted and ignored at runtime in 1.13.28. The separate `AssetOut` runtime probe confirmed that `legacy_freshness_policy` remains accepted; the temporary probe file has been removed.

### Q53
Set `attributes.translation_settings.enable_source_tests_as_checks: true` in the `DbtProjectComponent`’s `defs.yaml`, leaving `enable_asset_checks` enabled, as it is by default. The component **does enable code references by default**, unlike the base Python `DagsterDbtTranslatorSettings`; disable them with `attributes.translation_settings.enable_code_references: false`. These settings are defined in the [1.13.28 component source](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-dbt/dagster_dbt/components/dbt_project/component.py#L205-L218) and its [component-specific settings class](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-dbt/dagster_dbt/components/dbt_component_utils.py#L34-L38).