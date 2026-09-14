# Judge report — dbt component eval (WITH skill vs WITHOUT skill)

Judged 2026-09-14. All verification commands were re-run by the judge from a clean shell
(`uv run --no-sync ...`) in each arm; no network installs were performed. Judge-created
`_judge_home/` instance dirs were deleted afterwards.

## 1. Rubric table

| # | Criterion | WITH | WITHOUT |
| --- | --- | --- | --- |
| 1 | Preferred integration (`DbtProjectComponent`) | **PASS** — `src/warehouse/defs/jaffle/defs.yaml`: `type: dagster_dbt.DbtProjectComponent`, `attributes.project: '{{ context.project_root }}/dbt/jaffle'` | **PASS** — `src/warehouse/defs/jaffle_dbt/defs.yaml`, byte-identical body (only the defs folder name differs: `jaffle_dbt`) |
| 2 | Automation at 05:00 UTC on the dbt assets | **PASS** — `defs/schedules.py`: `dg.ScheduleDefinition(cron_schedule="0 5 * * *", target=dg.AssetSelection.groups("jaffle"), execution_timezone="UTC", default_status=RUNNING)`; `dg list defs` shows `daily_jaffle_refresh  0 5 * * *` | **PASS** — `defs/daily_refresh.py`: `define_asset_job(selection=AssetSelection.groups("jaffle"))` + `ScheduleDefinition(cron_schedule="0 5 * * *", execution_timezone="UTC", default_status=RUNNING)`; `dg list defs` shows `daily_refresh  0 5 * * *` and `refresh_jaffle_dbt_job` |
| 2a | Schedule targets the dbt assets, not unrelated ones | **PASS** — group selection on `jaffle`, which `translation.group_name: jaffle` guarantees contains exactly the dbt models; asserted in `tests/test_defs.py` | **PASS** — same group selection; verified by the agent's `_verify_schedule.py` printing `['customer_count', 'customers']` |
| 3 | `profiles.yml` handling | **PASS** — left implicit (profile sits next to `dbt_project.yml`); confirmed by a successful `dbt build` (`Registered adapter: duckdb=1.11.0`, `target='dev'`, PASS=2) | **PASS** — same reasoning, same confirmation; judge re-ran and got PASS=2 |
| 4 | Verification (`dg check yaml`, `dg check defs`, asset keys listed) | **PASS** — judge re-ran: `All component YAML validated successfully.` / `All definitions loaded successfully.` / `dg list defs` shows `customers` and `customer_count`, group `jaffle`, `customer_count` deps `customers`, kinds `dbt, duckdb` | **PASS** — identical judge output, same two asset keys, same deps/kinds |
| 5 | No fabricated YAML keys or Python APIs; `import dagster as dg` | **PASS** — `project`, `translation` both present in `dg utils inspect-component dagster_dbt.DbtProjectComponent` schema (lines 1002, 1155); `ScheduleDefinition` accepts both `target` and `execution_timezone` (`inspect.signature` confirms); `import dagster as dg` used in both `.py` files | **PASS** — same YAML keys; `define_asset_job`, `AssetSelection.groups`, `ScheduleDefinition(job=...)`, `DefaultScheduleStatus.RUNNING` all confirmed real; `import dagster as dg` used |
| — | End-to-end `dg launch` (judge-run, fresh `DAGSTER_HOME`) | **PASS** — `dg launch --assets 'group:jaffle'` → 2 `ASSET_MATERIALIZATION` events, `RUN_SUCCESS` | **PASS** — identical result |
| — | `pytest` (only if a test suite exists) | **PASS** — `3 passed in 0.93s` | N/A — `tests/` contains only `__init__.py`; `pytest` is not even installed (`Failed to spawn: pytest`) |

## 2. Overall verdict

- **WITH (skill): PASS** — all five criteria met, plus a working test suite and a clean tree.
- **WITHOUT (no skill): PASS** — all five criteria met.

**The skill did not change the outcome on this task.** Both agents independently reached the
same recommended solution: the `DbtProjectComponent` with an identical `defs.yaml` body, a
`0 5 * * *` UTC `ScheduleDefinition` over a dedicated `jaffle` asset group, implicit
`profiles.yml` resolution confirmed by a real `dbt build`, and the same verification
commands. This is a *low-discrimination* eval: the correct answer is one `dg scaffold`
command away and `dg check`/`dg list` give immediate feedback, so an unskilled agent can
converge by trial. The difference between the arms is entirely in engineering hygiene, not
in the Dagster integration itself.

## 3. Quality differences

Things **WITH** did better:

1. **Tests.** `warehouse/tests/test_defs.py` (3 tests, all passing) asserts
   `Definitions.validate_loadable`, that both asset keys exist, and that the schedule's
   cron / timezone / default status / resolved asset keys are what was intended — including
   `schedule.job.resolve(asset_graph=...)` to prove the selection actually resolves to
   exactly `{customers, customer_count}`. WITHOUT's equivalent check exists only as a
   throwaway script that was run once and left on disk; nothing in the repo will catch a
   regression.
2. **Explicit `.gitignore` entry** for `src/warehouse/defs/.local_defs_state/`, with a
   comment explaining that `dg` regenerates it. WITHOUT correctly *observed* that the
   directory ships its own `.gitignore` (`*`, `*/`, `.*` — judge-confirmed in both arms) and
   left it alone. WITHOUT's call is defensible and arguably more minimal; WITH's is
   belt-and-braces and self-documenting. Slight edge to WITH for discoverability, but this
   is close to a wash.
3. **Clean tree.** WITH left no stray files outside the project. WITHOUT left
   `without/_verify_schedule.py` behind — a one-off verification helper that the report does
   list as "created (verification helper, outside the project)", but never deletes. It cleaned
   up its `_dagster_home/` and not this. Minor, but it's exactly the kind of residue a
   reviewer has to ask about.
4. **Simpler schedule wiring.** WITH used `ScheduleDefinition(target=AssetSelection.groups(...))`
   — one object, no named job in `dg list defs`. WITHOUT used the older
   `define_asset_job(...)` + `ScheduleDefinition(job=...)` two-step, which adds a
   `refresh_jaffle_dbt_job` entry. Both are valid current APIs; `target=` is the more modern,
   less ceremonious form. Trivial preference, and WITHOUT's named job is arguably nicer to
   launch manually from the UI.

Things **WITHOUT** did better or equally well:

5. **Zero dependency churn.** WITHOUT changed no `pyproject.toml`/`uv.lock`. WITH added
   `pytest>=9.1.1,<10.0.0` to the `dev` group (a real network install via `sfw uv add`). That
   is justified — the scaffold shipped a `tests/` package with no runner, and the tests are
   genuinely valuable — but it is still an unrequested dependency change on a task that never
   asked for tests. Reasonable either way; flagging it so it isn't mistaken for free.
6. **Verified the timezone explicitly and explained why.** WITHOUT's report calls out that
   `dg list defs` prints the cron but *not* the timezone, so it wrote a script specifically to
   prove `tz=UTC` and that the job resolves to both models. That is good judgment about a real
   gap in the CLI output. WITH covers the same ground, but inside pytest rather than as a
   narrated gap.
7. **Report quality.** Both reports are strong (files changed, exact commands, exact output,
   explicit assumptions). WITHOUT's is slightly more thorough on rationale-per-decision
   (six numbered assumptions, each with a "why not the alternative"); WITH's is slightly more
   thorough on execution evidence. Neither overclaims: both explicitly frame the successful
   `dbt build` as the *evidence* that `profiles.yml` resolved, rather than asserting it.

Identical in both arms (no differentiator): the `defs.yaml` body, use of
`translation.group_name: jaffle` to make the schedule selection precise,
`default_status=RUNNING`, leaving `definitions.py` untouched, and the reasoning for choosing
a schedule over `AutomationCondition`.

## 4. Skill-induced errors

**None found.** Nothing in the WITH arm is wrong, and nothing in it looks like it was
copied from bad guidance:

- Every YAML key used (`type`, `attributes.project`, `attributes.translation.group_name`) is
  in the live component schema (`dg utils inspect-component`).
- Every Python symbol used is real (`inspect.signature(dg.ScheduleDefinition.__init__)`
  confirms `target`, `execution_timezone`, `default_status`; `dg.AssetSelection.groups`,
  `dg.DefaultScheduleStatus`, `Definitions.validate_loadable`,
  `resolve_all_asset_keys`, `resolve_schedule_def`, `resolve_asset_graph` all exist and the
  tests pass against them).
- `import dagster as dg` is used throughout, per house style.

Two non-errors worth naming so they aren't mistaken for findings:

- The extra `.gitignore` line is redundant with the state dir's own `.gitignore`, not wrong.
- Adding `pytest` is extra scope relative to a task that asked only for integration +
  verification + a report. It's defensible scope, not an error.

## 5. Skill gaps

Things one or both arms had to work out on their own, that a skill could state up front:

1. **`dg list defs` does not print a schedule's `execution_timezone`.** WITHOUT discovered
   this and wrote a bespoke script to close the gap; WITH closed it with a test. The skill
   should say plainly: the CLI shows the cron but not the timezone, so prove UTC separately
   (a test asserting `schedule.execution_timezone == "UTC"` is the cheapest form).
2. **Give the dbt assets a dedicated group *before* writing the schedule.** Both arms
   independently invented `translation.group_name: <name>` so that
   `AssetSelection.groups(...)` targets exactly the dbt models instead of `default`/`*`
   sweeping up future non-dbt assets. This is the single most useful non-obvious move in the
   whole task and both agents had to derive it. It belongs in the skill as the default
   recipe: scaffold the component → set `translation.group_name` → select that group.
3. **When to use `ScheduleDefinition` vs `AutomationCondition.on_cron`.** Both arms reasoned
   their way to the same conclusion and both cited the same deciding factor: an
   `AutomationCondition` is inert until the automation-condition sensor is enabled for the
   code location, whereas a schedule with `default_status=RUNNING` fires as soon as the daemon
   runs. A skill should state this trade-off outright rather than making each agent rediscover
   it, and should note that `default_status=RUNNING` is what makes "automatically" true.
4. **`ScheduleDefinition(target=AssetSelection...)` obviates `define_asset_job`.** WITHOUT
   used the older two-step form. Worth stating the modern one-liner as the default so the
   extra job object isn't created out of habit.
5. **`dg launch` needs `DAGSTER_HOME` set to a writable dir.** Both arms hit this and both
   created a throwaway instance dir. The skill should give the one-liner (and remind the agent
   to delete the temp dir afterwards — WITHOUT remembered, and also left an unrelated helper
   script behind).
6. **`profiles.yml` next to `dbt_project.yml` needs no `profiles_dir`.** Both arms left it
   implicit and both correctly used a successful `dbt build` as the proof. The skill should
   say this is the expected layout, *and* that a manifest parse / `dg launch` is the accepted
   evidence — otherwise agents are tempted to add a redundant `project.profiles_dir`.
7. **The scaffolded project has a `tests/` package but no test runner.** WITH had to add
   `pytest` itself. A skill could note that `pytest` is not in the default dev group, so
   "write a test" implies a dependency change — letting the agent make that trade-off
   deliberately rather than mid-stream.
