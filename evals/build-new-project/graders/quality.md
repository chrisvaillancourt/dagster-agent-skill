---
type: llm
focus: trace
weight: 2
---
You are grading a coding agent's transcript for a Dagster project-creation task. PASS only if ALL hold:
1. The project was scaffolded with `create-dagster project` (via `uvx create-dagster@latest project ...` or `create-dagster project ...`), not by hand-writing pyproject.toml/definitions.py from scratch and not with the legacy `dagster project scaffold`.
2. Asset code lives under `src/sales_pipeline/defs/` (any filename), uses `import dagster as dg` (or explicit dagster imports), defines `raw_orders` and `orders_by_region` with a dependency expressed either as a function parameter named `raw_orders` or `deps=[raw_orders]`.
3. The schedule is a `dg.ScheduleDefinition` or `@dg.schedule` targeting a job built with `dg.define_asset_job` (or `target=` asset selection) with cron `0 6 * * *` and timezone UTC (explicit `execution_timezone="UTC"` or implicitly UTC and stated).
4. No legacy constructs: no `@repository`, no `workspace.yaml`, no `Definitions(assets=[...])` hand-listing in definitions.py that bypasses `load_from_defs_folder` (it is acceptable to leave the generated definitions.py untouched).
5. The agent actually ran `dg check defs` (any invocation form: `uv run dg check defs`, `.venv/bin/dg check defs`, `dg check defs`) and it exited successfully (look for absence of errors / success output), and the final message names that command. A claim of success without a successful run FAILS.
Otherwise FAIL and cite the criterion and evidence from the trace.
