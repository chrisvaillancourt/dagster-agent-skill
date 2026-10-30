### Q3
The final kinds are `{"b"}`, not `{"a", "b"}`; user tags are `{"x": "1", "y": "2"}`, with Dagster also storing `dagster/kind/b: ""` in the tags. The default post-processing operation merges `tags` and `metadata` but replaces `kinds`. `dg check yaml` validates the outer structure, including rejecting unknown keys directly under `post_processing`, but does **not** validate the keys inside individual `assets` entries or their `attributes`; those are validated when definitions load.

### Q7
As written, it raises `TypeError`: `Definitions.map_asset_specs` requires the keyword argument `func=`, not a positional callback. With `defs.map_asset_specs(func=lambda spec: spec.replace_attributes(partitions_def=pdef))`, changing the partitions definition works in 1.13.28; being inside `Component.build_defs` does not prohibit it. The method returns new definitions, so return or otherwise use that result.

### Q12
By default, input loading raises `FileNotFoundError` when the filesystem I/O manager reaches a missing hourly partition, so the downstream asset body does not execute. Configure the downstream input with `ins={"up": dg.AssetIn(metadata={"allow_missing_partitions": True})}`. The I/O manager then warns about and skips missing partitions, passing a dictionary containing only the three available partitions.

### Q21
Use `dg.AutomationCondition.on_cron(cron_schedule)`: it waits until all dependencies have updated since the latest cron tick, with `all_deps_updated_since_cron(...)` providing the underlying dependency condition. The common wrong choice is `dg.AutomationCondition.eager()`, which reacts when **any** dependency updates rather than forming an all-upstreams barrier.

### Q29
A blocking check returning `passed=False` defaults to `AssetCheckSeverity.ERROR`: `orders` materializes, the check step fails, and its downstream asset is skipped. With default arguments, `dg.materialize(...)` raises `DagsterAssetCheckFailedError` because `raise_on_error=True`; the recorded `orders` materialization is not undone. A failed blocking check with `severity=dg.AssetCheckSeverity.WARN` does not block downstream execution, and materialization returns a successful result, assuming nothing else fails.

### Q34
Set `DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python` in the **project root’s `.env` file**, beside its `pyproject.toml`; relative paths resolve from that root. Exporting it in the shell alone does **not** work: this setting is read from `.env`, not `os.environ`. Also, single-project `dg dev` and `dg check defs` use the interpreter running `dg`, whereas workspace loading uses the per-project executable override unless `--use-active-venv` is supplied.

### Q39
`pool="db"` alone does not throttle execution when neither an explicit pool limit nor `concurrency.pools.default_limit` is configured. During normal cancellation, the interrupted step emits `STEP_FAILURE`, and executor event handling releases its claimed slot. Merely marking the run canceled does not release an orphaned slot: abrupt termination may require explicit freeing or the monitoring daemon’s configured `run_monitoring.free_slots_after_run_end_seconds` cleanup.

### Q47
`@asset(freshness_policy=LegacyFreshnessPolicy(...))` raises `ParameterCheckError` because `freshness_policy` now expects the new `FreshnessPolicy`; the hidden `legacy_freshness_policy=` argument still accepts the legacy object. `AssetOut(legacy_freshness_policy=...)` likewise still accepts and retains the policy, with deprecation warnings, despite the advertised removal version having passed. `@observable_source_asset(auto_observe_interval_minutes=10)` silently ignores that argument in 1.13.28, leaving the resulting asset’s interval as `None`.

### Q53
Set `attributes.translation_settings.enable_source_tests_as_checks: true` in the `DbtProjectComponent` YAML; `enable_asset_checks` must also remain enabled, which it is by default. The component enables code references by default through `DagsterDbtComponentTranslatorSettings(enable_code_references=True)`, unlike the base Python `DagsterDbtTranslatorSettings`, whose default is `False`.
