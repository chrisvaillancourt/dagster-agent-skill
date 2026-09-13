---
type: llm
focus: last_message
weight: 2
---
Grade as a Dagster operations expert. PASS only if ALL hold:
1. Assets are assigned to a pool via the `pool="snowflake"` (any name) argument on `@dg.asset` / `@dg.op` (or equivalent `AssetSpec` field), NOT via `op_tags`-based `dagster/concurrency_key` alone.
2. The pool limit of 3 is set either in `dagster.yaml` under `concurrency: pools: ...` (e.g. `default_limit: 3`, or a per-pool limit if the answer shows a documented way) or via the UI/`dagster instance concurrency set <pool> 3` CLI — any of these is acceptable, but at least one must be shown.
3. The total run cap of 10 is set with `concurrency: runs: max_concurrent_runs: 10` or `run_queue: max_concurrent_runs: 10` (legacy-but-valid) or `run_coordinator: class: QueuedRunCoordinator` + `config: max_concurrent_runs: 10`. The answer must mention the run queue / QueuedRunCoordinator requirement or use the `concurrency.runs` form.
4. No fabricated YAML keys (e.g. `max_concurrent_ops`, `pool_limits`, `snowflake_limit`, `concurrency_limit`) and no fabricated decorator args.
5. YAML is syntactically plausible and placed in `dagster.yaml` under `$DAGSTER_HOME` (mentioned or implied).
Otherwise FAIL; name the failing criterion.
