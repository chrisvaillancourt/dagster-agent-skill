# Judge Report — `partitioned-assets` eval

Judged against Dagster **1.13.22** (both venvs identical). Every `dg.*` symbol and CLI flag
below was cross-checked against the real installed package; every command was re-run by the
judge, not taken from the agents' reports.

- Arm **WITH** (had the skill): `with/analytics/src/analytics/defs/assets.py`
- Arm **WITHOUT** (no skill):   `without/analytics/src/analytics/defs/events.py`

---

## 1. Rubric table

| # | Criterion | WITH | Evidence (WITH) | WITHOUT | Evidence (WITHOUT) |
|---|---|---|---|---|---|
| 1 | `daily_events` uses `DailyPartitionsDefinition(start_date="2025-01-01")` | **PASS** | `assets.py:11` `daily_partitions = dg.DailyPartitionsDefinition(start_date="2025-01-01")`, applied `assets.py:16` | **PASS** | `events.py:15,17` `START_DATE = "2025-01-01"` → `dg.DailyPartitionsDefinition(start_date=START_DATE)`, applied `events.py:25` |
| 2 | `weekly_summary` weekly-partitioned + explicit `TimeWindowPartitionMapping` dep | **PASS** | `assets.py:12` weekly def; `assets.py:51-56` `deps=[dg.AssetDep(daily_events, partition_mapping=dg.TimeWindowPartitionMapping())]`. Judge-verified mapping resolves week `2025-01-05` → exactly 7 daily keys `2025-01-05..2025-01-11` | **PASS** | `events.py:21` weekly def; `events.py:45-51` same `AssetDep` + `TimeWindowPartitionMapping()`. Same 7-key resolution verified |
| 3 | `backfill_policy=single_run()` + range-safe body | **PASS** | `assets.py:21` `backfill_policy=dg.BackfillPolicy.single_run()`; body reads `context.partition_keys` + `context.partition_time_window` (`assets.py:26-27`), never `partition_key`. Comment `assets.py:18-20` states `partition_key` raises under a range run | **PASS** | `events.py:27` same policy; body reads `context.partition_keys` (`events.py:33`) with comment `events.py:31-32` giving the same reason |
| 4 | `dg check defs` OK **and** 2025-01-01 partition launched successfully | **PASS** | `dg check defs` → *"All definitions loaded successfully."* Launch used the **modern** form `dg launch --assets daily_events --partition 2025-01-01` → `RUN_SUCCESS` (run `03651f23…`), log line `daily_events covering 1 partition(s): 2025-01-01` | **PASS** | `dg check defs` → same success. Launch used the **legacy** form `dagster asset materialize --select daily_events --partition 2025-01-01 -m analytics.definitions` → `RUN_SUCCESS` (run `bc35d6de…`). Rubric permits this equivalent form. Judge also confirmed `dg launch` works here (run `143014ea…`) |
| 5 | Files under `analytics/src/analytics/defs/`, `import dagster as dg` | **PASS** | `with/analytics/src/analytics/defs/assets.py:9` `import dagster as dg`; autoloaded via existing `load_from_defs_folder`, `definitions.py` untouched | **PASS** | `without/analytics/src/analytics/defs/events.py:13` `import dagster as dg`; same autoload, `definitions.py` untouched |
| 6 | No fabricated APIs or flags | **PASS** | All of `DailyPartitionsDefinition`, `WeeklyPartitionsDefinition`, `BackfillPolicy.single_run`, `AssetDep`, `TimeWindowPartitionMapping`, `AssetExecutionContext`, `MaterializeResult`, and ctx methods `partition_keys` / `partition_time_window` / `asset_partition_keys_for_input` / `asset_partition_key_range_for_input` resolve in the venv. Flags `--assets`, `--partition`, `--partition-range` confirmed real via click introspection of `dagster_dg_cli.cli.launch.launch_command` | **PASS** | Same symbol set (minus the two `asset_partition_*_for_input` helpers) plus `get_partition_keys_in_time_window` — all real. Flags `--select`, `--partition`, `-m` real on `dagster asset materialize`; `--partition-range` also real there |

### Overall verdict

| Arm | Verdict |
|---|---|
| **WITH** | **PASS** (6/6) |
| **WITHOUT** | **PASS** (6/6) |

Both arms produced correct, working, idiomatic code with no fabricated APIs. The rubric does
not separate them; the differences below do.

---

## 2. Judge re-verification log

```
# both arms
uv run dg check defs        -> "All component YAML validated successfully. / All definitions loaded successfully."
uv run dg list defs         -> daily_events, weekly_summary (weekly's Deps column shows daily_events)
                               WITH group=events, WITHOUT group=default

# required launch, re-run by judge in both arms
DAGSTER_HOME=$PWD/.dagster_home uv run dg launch --assets daily_events --partition 2025-01-01
  WITH    -> RUN_SUCCESS, "daily_events covering 1 partition(s): 2025-01-01"
  WITHOUT -> RUN_SUCCESS, "daily_events materializing partitions: ['2025-01-01']"

# single-run backfill proof, re-run by judge in both arms
uv run dg launch --assets daily_events --partition-range 2025-01-01...2025-01-07
  both -> 1x RUN_START, 1x STEP_START, 7x ASSET_MATERIALIZATION, 1x STEP_SUCCESS, 1x RUN_SUCCESS
```

One run and one step emitting seven materializations is the actual confirmation that
`single_run()` is in effect — both arms' implementations satisfy it.

---

## 3. Quality differences

**Where WITH is better**

1. **Modern `dg` CLI for the launch.** WITH used `dg launch --assets daily_events --partition
   2025-01-01`. WITHOUT fell back to the legacy `dagster asset materialize --select … -m
   analytics.definitions`, which requires hand-supplying the module path that the `dg` project
   scaffolding already knows. In a `dg`-scaffolded project the module-path form is a regression;
   the judge confirmed `dg launch` works fine in the WITHOUT arm, so it was available and unused.
2. **Real end-to-end proof of the single-run policy vs. a simulation.** WITH ran an actual
   `dg launch --partition-range 2025-01-01...2025-01-07` and showed one run with seven
   materializations. WITHOUT could not reach that through its legacy path and instead
   *hand-simulated* the backfill in a throwaway `/tmp` script by setting the internal run tags
   `ASSET_PARTITION_RANGE_START_TAG` / `…_END_TAG` on `dg.materialize(...)`. That does exercise the
   same execution path, but it asserts the tags rather than exercising the operator-facing command,
   and it depends on a private `dagster._core.storage.tags` import. `--partition-range` exists on
   `dagster asset materialize` too, so this was an avoidable detour.
3. **`weekly_summary` reads the mapping Dagster actually resolved.** WITH uses
   `context.asset_partition_keys_for_input("daily_events")` and
   `asset_partition_key_range_for_input("daily_events")` (`assets.py:61-62`) — these go through the
   declared `AssetDep` mapping. WITHOUT instead recomputes the window itself
   (`events.py:56`: `daily_partitions.get_partition_keys_in_time_window(week)`), **bypassing the
   declared partition mapping**. Today both agree because the mapping is the zero-offset default,
   but if anyone later sets `TimeWindowPartitionMapping(start_offset=-1)`, WITH's logging/metadata
   follows the change and WITHOUT's silently keeps reporting the wrong seven days. This is the most
   substantive correctness-of-idiom gap between the two.
4. **Repo hygiene.** WITH appended `.dagster_home/` to `analytics/.gitignore` (verified in the
   file). WITHOUT created the same instance directory and relied on deleting it afterward, leaving
   no ignore rule for the next person who runs a `dg` command.
5. **Organization and metadata.** WITH sets `group_name="events"` on both assets (confirmed in
   `dg list defs`); WITHOUT leaves both in `default`. WITH also emits `window_start`/`window_end`
   and a `daily_partition_range` string in `MaterializeResult` metadata.
6. **Actionable remedy for the partial-week gap.** WITH not only identified that 2025-01-01..04
   map to no weekly partition but gave the concrete fix
   (`WeeklyPartitionsDefinition(start_date="2025-01-01", day_offset=3)`). WITHOUT identified the
   same gap but stopped at "no `day_offset` override was applied since none was requested."

**Where WITHOUT is better**

1. **No duplicated literal.** WITHOUT defines `START_DATE = "2025-01-01"` once (`events.py:15`) and
   feeds both partition defs from it, with assumption #3 explaining why the two must stay aligned.
   WITH repeats the `"2025-01-01"` string on `assets.py:11` and `:12` with no note binding them.
2. **Broader spot-check of the weekly asset.** WITHOUT materialized `weekly_summary` at two
   partitions (`2025-01-05` and `2025-01-12`), showing the window advances correctly. WITH checked
   only `2025-01-05`.
3. **Explicit scratch cleanup** is documented (assumption #6), including reproducing the deleted
   scripts' contents inline so the verification stays auditable.

---

## 4. Skill-induced errors

**None found.**

Nothing incorrect in the WITH arm traces back to skill guidance. The WITH `EVAL_REPORT.md` never
cites the skill by name, so there is no quotable "the skill said X" to attribute a defect to. Two
candidates were checked and both cleared:

- WITH assumption #3 asserts Dagster "infers the same intersecting-window mapping by default for
  two time-window partitioned assets." Verified true — the default between two time-window
  partitions defs is `TimeWindowPartitionMapping(start_offset=0, end_offset=0,
  allow_nonexistent_upstream_partitions=False)`, exactly what was written explicitly. Accurate, and
  stating it explicitly is the better practice anyway.
- WITH notes a `BetaWarning` on the `backfill_policy` parameter and calls it expected rather than an
  error. Confirmed: the warning is emitted by Dagster 1.13.22 and is not a failure.

---

## 5. Skill gaps — candidate additions to the skill

These are things the WITH arm had to derive unaided. Each was independently verified by the judge.

1. **`single_run()` backfills break the naive "just return the value" asset — the highest-value
   addition.** The task text says `daily_events` "can just return the partition key," and taken
   literally that is *wrong* the moment `BackfillPolicy.single_run()` is set. Judge-verified, two
   distinct failures:
   - `return context.partition_key` succeeds for a single partition but under a range raises
     `DagsterInvariantViolationError: Cannot access partition_key for a partitioned run with a
     range of partitions. Call partition_key_range instead.`
   - Even a correctly range-aware asset that returns a **plain value** fails under a range with
     `DagsterExecutionHandleOutputError`, because the default `fs_io_manager` cannot persist one
     object spanning multiple partitions.

   Both arms rediscovered this from scratch and both wrote it up as "Assumption 1" — strong
   evidence the skill is silent on it. The skill should state the rule directly: *with
   `single_run()`, read `context.partition_keys` / `partition_time_window`, and return
   `MaterializeResult` (or use a partition-range-aware IO manager) instead of a plain value.*

2. **Weekly partition boundary alignment.** `WeeklyPartitionsDefinition` defaults to Sunday-start
   weeks, so `start_date="2025-01-01"` (a Wednesday) yields a first partition of `2025-01-05`, and
   `2025-01-01..2025-01-04` belong to *no* weekly partition. Both arms had to discover this
   empirically. The skill should document the default `day_offset`, the resulting partial-period
   gap, and the `day_offset=N` escape hatch for aligning weeks to an arbitrary start date.

3. **How to read upstream partitions when the dep is `deps=` rather than `ins=`.** WITH found
   `context.asset_partition_keys_for_input(...)` / `asset_partition_key_range_for_input(...)`;
   WITHOUT never found them and hand-recomputed the window, bypassing the declared mapping. The
   skill should name these context methods and stress that they honor the declared
   `partition_mapping` while manual recomputation does not.

4. **The one-command way to prove a single-run backfill.** `dg launch --assets <a>
   --partition-range <start>...<end>` (also available as `dagster asset materialize
   --partition-range`) is the direct verification, and the observable signal is *one* `STEP_START`
   emitting *N* `ASSET_MATERIALIZATION` events. Absent that guidance the WITHOUT arm resorted to
   importing private `dagster._core.storage.tags` constants to fake a backfill. The skill should
   name the flag and the signal to look for.

5. **`DAGSTER_HOME` for CLI launches.** Both arms independently invented the same
   `DAGSTER_HOME=$PWD/.dagster_home` workaround to get persisted run history, and Dagster still
   warns about the missing `dagster.yaml`. A one-liner in the skill (set `DAGSTER_HOME`, drop an
   empty `dagster.yaml` to silence the warning, gitignore the directory) would save the rediscovery.

6. **`deps=` vs `ins=` as the lever for "dependency only, no IO manager loading."** Both arms got
   this right and both felt the need to explain it at length in prose, suggesting it is not stated
   crisply anywhere they could point to. A single sentence in the skill — *`AssetDep` declares
   lineage without loading; `AssetIn` loads through the IO manager* — would make this a lookup
   instead of a derivation.
