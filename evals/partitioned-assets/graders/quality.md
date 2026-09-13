---
type: llm
focus: trace
weight: 2
---
Grade a transcript where an agent added partitioned assets to a dg project. PASS only if ALL hold:
1. `daily_events` uses `dg.DailyPartitionsDefinition(start_date="2025-01-01")` (any equivalent kwargs form).
2. `weekly_summary` uses `dg.WeeklyPartitionsDefinition(start_date=...)` and declares its dependency on `daily_events` with a `dg.TimeWindowPartitionMapping()` (default offsets are correct here) via `deps=[dg.AssetDep("daily_events", partition_mapping=...)]` or `ins={"daily_events": dg.AssetIn(partition_mapping=...)}`. Using the default (no explicit mapping) is ALSO acceptable ONLY if the agent states that Dagster's default mapping between time-window partitions maps by overlapping time windows; otherwise an explicit mapping is required.
3. `daily_events` has `backfill_policy=dg.BackfillPolicy.single_run()` and, if `context.partition_key` is used, the agent either handles ranges (`context.partition_key_range` / `partition_time_window`) or notes that single-run backfills provide a range.
4. The agent ran `dg check defs` successfully AND launched the 2025-01-01 partition with `dg launch --assets daily_events --partition 2025-01-01` (any equivalent form) and the run succeeded.
5. Files were added under `analytics/src/analytics/defs/` (scaffolded with `dg scaffold defs dagster.asset ...` or written directly), using `import dagster as dg`.
6. No fabricated APIs or flags.
Otherwise FAIL; cite the criterion and evidence.
