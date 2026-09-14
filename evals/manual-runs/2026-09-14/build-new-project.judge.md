# Judge report — eval `build-new-project`

Judged 2026-09-14. Both arms re-verified from scratch by the judge in each arm's own venv
(`uv run --offline`, Dagster **1.13.22**, Python 3.14). Nothing was installed; no project file was modified.

Paths:
- WITH (skill): `with/sales_pipeline/`, report `with/EVAL_REPORT.md`
- WITHOUT (no skill): `without/sales_pipeline/`, report `without/EVAL_REPORT.md`

## 1. Rubric table

| # | Criterion | WITH | Evidence (WITH) | WITHOUT | Evidence (WITHOUT) |
|---|---|---|---|---|---|
| 1 | Scaffolded with `create-dagster project` | **PASS** | `with/EVAL_REPORT.md:8` — `uvx create-dagster@latest project sales_pipeline --uv-sync`; generated `pyproject.toml` carries `[tool.dg] directory_type="project"` + `[tool.hatch.build.targets.wheel] force-include` (`with/sales_pipeline/pyproject.toml:20-30`), the exact scaffold shape | **PASS** | `without/EVAL_REPORT.md:46` — `sfw uvx create-dagster@latest project --uv-sync sales_pipeline`; `diff with/…/pyproject.toml without/…/pyproject.toml` → identical, and both `definitions.py` identical to the generated file |
| 2 | Assets under `src/<pkg>/defs/`, `import dagster as dg`, dep by parameter, `orders_by_region` logs | **PASS** | `with/sales_pipeline/src/sales_pipeline/defs/assets.py:5` `import dagster as dg`; `:38-39` `def orders_by_region(context, raw_orders: list[dict])`; `:47-48` two `context.log.info` calls. Judge launch log: `Region east total amount: 310.00 … Totals by region: {'east': 310.0, 'north': 165.75, 'south': 99.99}` | **PASS** | `without/…/defs/assets.py:6` `import dagster as dg`; `:55-57` `def orders_by_region(context, raw_orders: list[Order])`; `:69-71` `context.log.info`. Judge launch log: `Totals by region: {'emea': 375.75, 'us-east': 165.75, 'us-west': 99.99}` |
| 3 | Schedule: `ScheduleDefinition`, job/target, cron `0 6 * * *`, UTC | **PASS** | `defs/schedules.py:7-13` — `dg.ScheduleDefinition(cron_schedule="0 6 * * *", execution_timezone="UTC", target=[raw_orders, orders_by_region])`. Judge probe: `cron 0 6 * * * tz UTC job_name __anonymous_asset_job_daily_sales_schedule`; that job's asset keys = `['orders_by_region', 'raw_orders']`; `evaluate_tick` at 2026-09-14T06:00Z → 1 run request | **PASS** | `defs/schedules.py:10-22` — `dg.define_asset_job(selection=dg.AssetSelection.groups("sales"))` + `dg.ScheduleDefinition(job=daily_sales_job, cron_schedule="0 6 * * *", execution_timezone="UTC")`; asserted in `tests/test_defs.py:36-57` (passes) |
| 4 | No legacy constructs | **PASS** | `grep -rnE '@repository\|workspace\.yaml\|Definitions\(assets='` over `src/` + `tests/` → no matches; no `workspace.yaml` anywhere; generated `definitions.py` untouched (`@definitions` + `load_from_defs_folder`) | **PASS** | Same grep → no matches; `definitions.py` byte-identical to the WITH/generated one |
| 5 | Actually ran `dg check defs`, named in report | **PASS** | Report §2 names it as *the* proof (`with/EVAL_REPORT.md:46,109`). Judge re-run: `uv run dg check defs` → `All component YAML validated successfully. / All definitions loaded successfully.` exit=0 | **PASS** | Report §2 "Primary load check" (`without/EVAL_REPORT.md:56`). Judge re-run → same two lines, exit=0 |
| 6 | No fabricated APIs/flags | **PASS** | Every symbol used (`dg.asset`, `MaterializeResult(value=…)`, `MetadataValue.json`, `ScheduleDefinition(target=…, execution_timezone=…, default_status=…)`, `DefaultScheduleStatus`, `materialize`, `definitions`, `load_from_defs_folder`) resolved in-venv; `inspect.signature(dg.ScheduleDefinition.__init__)` lists `target`, `execution_timezone`, `default_status`; `dg launch --assets 'key:…,key:…'` ran clean | **PASS** | Same probe: `AssetSelection`, `define_asset_job`, `build_schedule_context`, `evaluate_tick`, `MetadataValue.md`, `add_output_metadata`, `resolve_schedule_def`, `resolve_job_def` all present and exercised by passing tests |

### Judge re-run summary

| Command | WITH | WITHOUT |
|---|---|---|
| `uv run dg check defs` | exit 0, "All definitions loaded successfully." | exit 0, same |
| `uv run dg list defs` | 2 assets (group `sales`), 1 schedule `daily_sales_schedule` cron `0 6 * * *`, **no Jobs section** (target= makes an anonymous job) | 2 assets (group `sales`, kinds `python`), job `daily_sales_job`, schedule `daily_sales_schedule` cron `0 6 * * *` |
| `uv run dg launch --assets 'key:raw_orders,key:orders_by_region'` | RUN_SUCCESS; per-region lines + `Totals by region: {...}` logged | RUN_SUCCESS; `Totals by region: {...}` plus per-region `  emea: 375.75` lines logged |
| `uv run pytest -q` | `3 passed in 0.54s` | `4 passed in 0.57s` |

## 2. Verdicts

- **WITH the skill: PASS** (6/6).
- **WITHOUT the skill: PASS** (6/6).

Both arms produced a correct, loadable, idiomatic 1.13 project. The differentiation is in idiom, tooling
choice, test depth, and wasted effort — below.

## 3. Quality differences

**WITH did better**

1. **Schedule idiom.** WITH uses `ScheduleDefinition(target=[raw_orders, orders_by_region])`
   (`with/…/schedules.py:11`). WITHOUT uses `ScheduleDefinition(job=daily_sales_job)`
   (`without/…/schedules.py:17`) — `job=`/`job_name=` are the superseded parameters that `target=`
   replaced; the skill's own migration table lists that swap. WITHOUT is not wrong, but it is one
   generation behind, and it needed an extra `define_asset_job` object to get there.
2. **Execution tooling.** WITH proved end-to-end execution with `dg launch --assets 'group:sales'`
   (the modern `dg` CLI path). WITHOUT used
   `DAGSTER_HOME= uv run dagster job execute -m sales_pipeline.definitions -j daily_sales_job`
   (`without/EVAL_REPORT.md:78`) — the legacy `dagster` CLI, plus a manual `DAGSTER_HOME=` blanking
   workaround that `dg launch` does not need. WITHOUT's project does work under `dg launch` (judge
   verified), the agent just did not know to reach for it.
3. **No scaffolding misfire.** WITH created stubs with `dg scaffold defs` and filled them in, with no
   cleanup step. WITHOUT lost a cycle to `dg scaffold defs dagster.asset src/sales_pipeline/defs/assets.py`
   creating `src/sales_pipeline/defs/src/sales_pipeline/defs/assets.py`, then had to delete the nested
   tree (`without/EVAL_REPORT.md:35`). The skill states the rule up front (`SKILL.md:157`: "path is
   relative to `src/<pkg>/defs/` from any CWD"), which is the clearest single instance of skill value here.
4. **Verification breadth.** WITH additionally ran the skill's `scripts/check_project.sh . --tests`
   (yaml + defs + list + pytest in one pass, `ALL CHECKS PASSED`), i.e. one repeatable gate rather than
   four ad-hoc commands.

**WITHOUT did better**

1. **Test depth.** 4 tests vs 3, and the extra two are the valuable ones: `test_schedule_is_daily_at_6am_utc`
   asserts the schedule's *target* (`job.asset_layer.asset_graph.get_all_asset_keys() == {raw_orders,
   orders_by_region}`), and `test_schedule_tick_requests_a_run` actually calls
   `schedule.evaluate_tick(dg.build_schedule_context(scheduled_execution_time=…))` and asserts one run
   request. WITH's schedule test only checks `cron_schedule` and `execution_timezone` strings — it would
   still pass if the schedule targeted the wrong assets.
2. **Test loading style.** WITHOUT reloads the code location the way the runtime does
   (`dg.load_from_defs_folder(path_within_project=Path(sales_pipeline.__file__).parent)`), which exercises
   autoloading. WITH imports the `LazyDefinitions` object and calls `defs()` — also valid (and what the
   skill template shows) but it tests the entry point rather than the defs-folder walk.
3. **Named, launchable job.** `define_asset_job` gives a job visible in `dg list defs` / the UI and
   launchable as `dg launch --job daily_sales_job`; `target=` produces `__anonymous_asset_job_daily_sales_schedule`
   and no Jobs row. For a two-asset demo this is a wash, but it is a genuine trade-off the WITH report
   does not mention.
4. **Group-based selection.** `AssetSelection.groups("sales")` auto-picks up future `sales` assets; WITH
   hardcodes the two asset objects in `target=[...]`. (WITH did use `group:sales` for its `dg launch`.)
5. **README updated** with the definitions table and the verification commands (`without/sales_pipeline/README.md:1-23`);
   WITH left the generated README untouched.
6. **Supply-chain rule followed**: WITHOUT wrapped `uvx create-dagster` and `uv add` in `sfw` and said so
   (`without/EVAL_REPORT.md:119`); WITH's report shows bare `uvx`/`uv add`.

**Neither arm** reproduced real `dg list defs` output faithfully — both paraphrased the rich table into
plain text in their reports. Harmless here (judge re-run confirms the content), but it is a fidelity habit
worth discouraging: paste the tool's actual output or say it is a summary.

## 4. Skill-induced errors

**None found.** Everything traceable to skill guidance in the WITH arm is correct against Dagster 1.13.22:

- `ScheduleDefinition(target=[assets…])` — valid, verified resolving to a job over exactly both assets.
- `dg.MaterializeResult(value=…, metadata=…)` as an asset return — `value` is a real parameter, and the
  downstream typed parameter received it (`STEP_INPUT … Got input "raw_orders" of type "[dict]"`).
- `from sales_pipeline.definitions import defs` then `defs()` in tests — matches
  `assets/templates/test_defs.py` and works (`LazyDefinitions.__call__` → `Definitions`).
- `scripts/check_project.sh` ran clean.

The only skill-shaped *weakness* (not an error) is that the skill's test template stops at "definitions load
+ materialize", and the WITH arm's schedule test inherited that shallowness — it never asserts what the
schedule targets. See gap 2 below.

## 5. Skill gaps (candidate additions)

1. **Assert schedule *targets*, not just cron strings.** Add to `assets/templates/test_defs.py` /
   `references/testing-and-asset-checks.md` a schedule test that resolves the schedule
   (`defs.resolve_schedule_def(name)`), checks `cron_schedule` + `execution_timezone`, **and** evaluates a
   tick: `schedule.evaluate_tick(dg.build_schedule_context(scheduled_execution_time=datetime(…, tzinfo=timezone.utc)))`
   → assert `len(.run_requests) == 1`. The WITHOUT arm derived this unaided; the WITH arm shipped a weaker test.
   (`build_schedule_context` / `evaluate_tick` are not currently shown together anywhere in the skill.)
2. **Document the `target=` side effect on job naming.** `ScheduleDefinition(target=[...])` creates
   `__anonymous_asset_job_<schedule_name>`, so `dg list defs` shows no Jobs section and any test asserting
   `schedule.job_name` must expect the mangled name. `references/schedules-and-sensors.md` recommends
   `target=` but never says what job you get. Add the naming rule plus the one-line trade-off: use
   `define_asset_job` when you want a named, launchable, UI-visible job; use `target=` otherwise.
3. **Say how to test a `@dg.definitions` entry point both ways.** `defs()` on the imported
   `LazyDefinitions` (template's way) vs re-walking the folder with
   `dg.load_from_defs_folder(path_within_project=Path(<pkg>.__file__).parent)` (WITHOUT's way). Note that
   the second is the one that catches autoload regressions, and that `Path(pkg.__file__).parent` is the
   correct argument (an easy place to be off by one directory).
4. **A one-shot execution recipe with the `DAGSTER_HOME` caveat.** `dg launch` provisions a throwaway
   instance; `dagster job execute` inherits an ambient `DAGSTER_HOME` and may write outside the project
   (WITHOUT discovered the `DAGSTER_HOME=` blanking trick on its own). Pair the modern command with the
   legacy one it replaces in `references/operations-and-debugging.md`.
5. **Name `create-dagster project .`'s naming rule explicitly.** Both arms independently reasoned that
   `create-dagster project .` takes the package name from the last path component (here `with` / `without`)
   and chose a subdirectory instead. `references/project-setup-and-cli.md` should state this in one line
   so it is not re-derived, along with the note that a directory name with a `-` becomes an underscored
   module name.
6. **Typed rows: avoid `TypedDict` for asset outputs.** WITHOUT recorded that Dagster's runtime type check
   on `list[Order]` does an `isinstance` against the annotation, which raises for a `TypedDict`
   (`without/EVAL_REPORT.md:117`). Worth a sentence in `references/assets.md` next to the return-type
   discussion.
7. **Metadata key hygiene.** WITH used the reserved-namespace key `dagster/row_count` for a Python list and
   for a dict of totals; WITHOUT used plain `num_orders` / `num_regions`. The skill should say which
   `dagster/*` metadata keys are standardized (`dagster/row_count`, `dagster/column_schema`, …) and that
   they should only be used with their intended semantics, since the UI renders them specially.
