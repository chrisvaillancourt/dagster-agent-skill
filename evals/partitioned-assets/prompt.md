In the existing Dagster project at `./analytics` (already set up with uv; run commands from inside it), add:

1. A daily-partitioned asset `daily_events` with partitions starting 2025-01-01. It can just return the partition key it ran for.
2. A weekly-partitioned asset `weekly_summary` downstream of `daily_events` that, for each weekly partition, depends on the 7 daily partitions that fall in that week (do not load data through an IO manager; just declare the dependency correctly and log the partition range it covers).
3. Backfills of `daily_events` should execute as a single run instead of one run per partition.

Verify that the definitions load and materialize `daily_events` for the 2025-01-01 partition locally. Report the commands you ran.
