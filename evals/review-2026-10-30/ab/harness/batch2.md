You are working in a Dagster project (Dagster 1.13.28, managed with uv; run Python and dg as `uv run ...`). Answer the questions below accurately. You may use any tools you have (run commands, write scratch code, read docs or source) to make sure your answers are right; put scratch files under ./scratch/ and do not modify src/. Each answer: 1-4 sentences with concrete API/CLI names. Format exactly as `### Q<id>` followed by the answer.

### Q3
A defs.yaml has `post_processing: assets: [{attributes: {kinds: [b], tags: {y: '2'}}}]` and the component already sets `kinds: [a]` and `tags: {x: '1'}` on its asset. What are the final kinds and tags, and does `dg check yaml` validate the post_processing keys?

### Q7
I call `defs.map_asset_specs(lambda spec: spec.replace_attributes(partitions_def=pdef))` inside a Component.build_defs override. What happens in 1.13.28?

### Q12
A downstream daily asset takes `up: dict[str, pd.DataFrame]` from an hourly upstream using a UPathIOManager-based I/O manager (the default filesystem one). Only 3 of the 24 hourly partitions for the day exist. What happens, and how do you let the downstream run?

### Q21
I want a downstream asset to materialize only after all three of its upstream assets have updated each cycle. Which built-in AutomationCondition do I use, and which one is the common wrong choice?

### Q29
An `@dg.asset_check(asset=orders, blocking=True)` returns `AssetCheckResult(passed=False)`; another returns `passed=False, severity=WARN`. In a job that also materializes a downstream asset of `orders`, what runs and what does `dg.materialize(...)` do with default arguments?

### Q34
I changed `[tool.dg.project]` settings and need `dg` to use a non-default virtualenv, e.g. `../.venvs/my-project`. Where must I set `DG_PROJECT_PYTHON_EXECUTABLE` for it to take effect, and does exporting it in my shell work?

### Q39
I add `pool="db"` to an asset but configure no pool limit anywhere. Does it throttle? And if a run holding the only slot of a limit-1 pool is cancelled, what frees the slot?

### Q47
In Dagster 1.13, I port old code: `@asset(freshness_policy=LegacyFreshnessPolicy(maximum_lag_minutes=5))`, `AssetOut(legacy_freshness_policy=...)` and `@observable_source_asset(auto_observe_interval_minutes=10)`. What does each do?

### Q53
In a `DbtProjectComponent` defs.yaml, how do you make dbt source tests load as Dagster asset checks, since the Python `DagsterDbtTranslatorSettings` object is not available in YAML? Does the component enable code references by default?
