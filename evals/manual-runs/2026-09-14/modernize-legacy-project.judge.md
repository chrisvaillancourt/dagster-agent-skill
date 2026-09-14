# Judge report — `modernize-legacy-project`

Judged 2026-09-14. Both arms re-verified from scratch using each arm's own
`legacy_pipeline/.venv` (dagster **1.13.22**, Python 3.14). Nothing was installed from the
network; no arm file was modified (the two `DAGSTER_HOME` scratch dirs created for the job runs
were removed afterwards).

Final layouts:

| | WITH | WITHOUT |
| --- | --- | --- |
| Layout | `src/legacy_pipeline/` + `defs/` autoloading (`create-dagster` shape) | flat `legacy_pipeline/` package, hand-listed `Definitions` |
| Assets | `src/legacy_pipeline/defs/assets.py` | `legacy_pipeline/assets.py` |
| Job + schedule | `src/legacy_pipeline/defs/jobs.py` | `legacy_pipeline/definitions.py` |
| Entry point | `definitions.py` → `@dg.definitions` + `load_from_defs_folder` | `definitions.py` → module-level `dg.Definitions(...)` |
| Config | `[tool.dg]`, `[tool.dg.project]`, registry-module entry point | `[tool.dagster]`, `[tool.dg]`, `[tool.dg.project]` |

---

## 1. Criterion × arm

| # | Criterion | WITH | WITHOUT |
| --- | --- | --- | --- |
| 1 | `raw_events` is `dg.AssetSpec` and present in defs | **PASS** — `raw_events = dg.AssetSpec(key="raw_events", description="Landed by an external loader")` in `defs/assets.py`; `dg list defs` shows `raw_events` with the description; resolved graph reports `executable: False` | **PASS** — identical spec in `legacy_pipeline/assets.py`; `dg list defs` shows `raw_events`; `executable: False` |
| 2 | `cleaned_events` uses `deps=[...]` | **PASS** — `@dg.asset(deps=[raw_events], ...)`; graph parent keys `['raw_events']`; zero `non_argument_deps` matches repo-wide | **PASS** — same; graph parent keys `['raw_events']` |
| 3 | Both computed assets use `automation_condition=dg.AutomationCondition.eager()`; sensor caveat noted | **PASS** — 2 occurrences of `AutomationCondition.eager()`; resolved graph shows `automation: eager` on `cleaned_events` and `event_counts`. Report states no `default_status=RUNNING` sensor was added *because* the legacy policy's sensor also started stopped | **PASS** — same 2 occurrences and same resolved labels. Report is more explicit: "still needs the `AssetDaemon` running and still has to be toggled on in the UI under **Automation**" |
| 4 | `@repository` → `Definitions`/`@dg.definitions`; `workspace.yaml` consistent | **PASS** — `repository.py` deleted, `workspace.yaml` deleted, entry point is `@dg.definitions` + `load_from_defs_folder`; `[tool.dg.project] root_module` resolves it | **PASS** — `repository.py` deleted, `workspace.yaml` deleted, `defs = dg.Definitions(...)`; `[tool.dagster] module_name = "legacy_pipeline.definitions"` replaces it. No `@repository` anywhere except one explanatory comment |
| 5 | Job + schedule preserved: `0 2 * * *`, all-asset selection, name `all_job_schedule` | **PASS** — `dg list defs`: `all_job_schedule  0 2 * * *`; `define_asset_job("all_job", selection="*")`; name left derived (deliberate, stated) | **PASS** — same `dg list defs` output; name pinned explicitly as `name="all_job_schedule"` with a comment on why |
| 6 | Load verified with evidence | **PASS** — re-ran `.venv/bin/dg check defs` → "All definitions loaded successfully" (rc 0); `.venv/bin/dagster definitions validate` → "All code locations passed validation"; `.venv/bin/dg launch --job all_job` → both materializations + `RUN_SUCCESS` | **PASS** — same `dg check defs` result; `dagster definitions validate` passes; `dg launch --job all_job` and `dagster job execute -m legacy_pipeline.definitions -j all_job` both → `RUN_SUCCESS` |
| 7 | No fabricated APIs | **PASS** (with a caveat, see §4) — every `dg.*` symbol used (`AssetSpec`, `asset`, `AutomationCondition.eager`, `definitions`, `load_from_defs_folder`, `define_asset_job`, `ScheduleDefinition`) exists in the arm's venv. Caveat: the `project_root=` **parameter** it passes is deprecated in 1.13.22 | **PASS** — every `dg.*` symbol used exists in the arm's venv; loads clean under `-W error::DeprecationWarning` |
| R | Regex: has `AutomationCondition.eager()`; no `AutoMaterializePolicy` / `non_argument_deps` / `SourceAsset(` | **PASS** — 2 / 0 / 0 / 0 in `src/legacy_pipeline/defs/assets.py`; repo-wide scan clean | **PASS** — 2 / 0 / 0 / 0 in `legacy_pipeline/assets.py`; repo-wide scan clean |

### 2. Overall verdict

| Arm | Verdict |
| --- | --- |
| **WITH (skill)** | **PASS** (all 7 criteria + regex) — with one real defect that does not trip any criterion: the entry point passes a parameter that Dagster 1.13.22 itself deprecates (§4). |
| **WITHOUT (no skill)** | **PASS** (all 7 criteria + regex) — no defects found. |

Both arms resolve to semantically identical definitions. Independent probe of each arm's
resolved `Definitions`:

```
SCHEDULE: all_job_schedule | cron: 0 2 * * * | job: all_job | default_status: STOPPED
ASSET: cleaned_events | executable: True  | deps: ['raw_events']     | automation: eager
ASSET: event_counts   | executable: True  | deps: ['cleaned_events'] | automation: eager
ASSET: raw_events     | executable: False | deps: []                 | automation: None
JOBS: ['__ASSET_JOB', 'all_job']
SENSOR: default_automation_condition_sensor | default_status: STOPPED | AutomationConditionSensorDefinition
```

Byte-identical between the two arms. Both preserved the schedule's derived name, its `STOPPED`
default status, the auto-created automation-condition sensor's name and `STOPPED` default, and
`raw_events`'s unexecutable-external character.

---

## 3. Quality differences

### 3a. Did the `src/` + `defs/` restructure exceed the brief?

**Judgment: legitimate, but the WITH arm is the weaker reading of *this* brief.**

The brief said "modernize it to current Dagster (1.13) idioms without changing what it does:
same assets, same dependency structure, same automatic-update behavior, same daily schedule."
The `src/` + `defs/` layout genuinely *is* the current idiom — it is what `create-dagster`
emits and what `dg` and components expect — so calling it "modernize to current idioms" is
defensible, and the restructure demonstrably preserved every externally-observable property
listed above (verified byte-identical against the WITHOUT arm). The rubric explicitly allows it.

Two things keep it from being the clearly better answer:

- **It changes more than the APIs the brief was about.** The WITHOUT arm's closing section
  argues this explicitly and well: the components migration "restructures the project rather
  than modernizing its APIs," so it left `[tool.dg]` in place as the prerequisite and named the
  follow-up as mechanical. That is a deliberate, articulated scope decision, not an omission.
- **It silently changed the code location name.** The WITH arm dropped `[tool.dagster]`
  entirely. `dagster definitions validate` then resolves the location as
  `legacy_pipeline.definitions` and emits, six times:

  > The code location name 'legacy_pipeline' configured on this project does not match the name
  > 'legacy_pipeline.definitions' this code location is registered under. … Update
  > `code_location_name` under [tool.dg.project] (or remove it) to silence this warning.

  The WITHOUT arm pinned `code_location_name = "legacy_pipeline"` and validates with no
  warnings at all. Code location name participates in instigator-state identity, so this is the
  same class of concern the rubric raises about the schedule name — and the WITH arm applied
  that reasoning to the schedule but not to the code location.

The WITH arm also added scaffolding the brief did not ask for and nothing uses: an empty
`src/legacy_pipeline/components/` package plus a `dagster_dg_cli.registry_modules` entry point.
Harmless and conventional for a `create-dagster` project, but it is net-new surface area in a
"don't change what it does" task.

### 3b. Behavior-preservation checks

**WITHOUT is materially stronger here, and it is the biggest quality gap between the arms.**

The WITHOUT arm built a before/after snapshot: it loaded the *old* `@repository` object and the
*new* `Definitions`, dumped a normalized JSON view (asset keys, deps, automation conditions,
executability, jobs, schedules, sensors) and diffed them. It reports jobs, schedules and sensors
diffing byte-identical, including the sensor's `STOPPED` default status and its two targets, and
correctly characterizes the only two deltas (the automation-condition string representation, and
`raw_events` moving from the `source_assets_by_key` bucket to an `AssetSpec` with
`"executable": false`). It also ran the import under `-W error::DeprecationWarning` and
correctly identified the one surviving warning as an internal Dagster `BetaWarning` from the
selection parser on `selection="*"`, not from project code. I reproduced both: the strict-import
check passes, and the only warning is that `BetaWarning`.

The WITH arm verified the *after* state well (`dg check defs`, `dg check toml`, `dg check yaml`,
`dg list defs`, `dg list defs --json` for the automation conditions, a real `dg launch` run, and
a re-run of the skill's `find_legacy_patterns.py` with a before/after hit count). But it never
compared against the *before* state — it asserts the sensor "is the same sensor the legacy
policies produced" without having loaded the old object to check. That assertion happens to be
correct, but it is reasoning, not evidence, in a task whose whole premise is behavior
preservation.

### 3c. Other differences

- **Schedule name.** WITH left the name derived and explained why (relying on the derivation
  staying stable); WITHOUT pinned `name="all_job_schedule"` explicitly and explained why
  (run state is keyed by name). Both land on the right name; pinning is the more robust choice
  because it survives a later job rename, and it documents the constraint in the code rather
  than only in the report.
- **CLI compatibility.** WITHOUT kept `[tool.dagster]`, so the classic `dagster dev` /
  `dagster definitions validate` path keeps working with no flags — a genuine equivalent of what
  `workspace.yaml` did. WITH removed it and relies on `[tool.dg]` alone; `dagster definitions
  validate` still works but warns.
- **Both** correctly kept `event_counts(cleaned_events)` as a function parameter (preserving the
  I/O-manager load) rather than flattening it to `deps=`, and both left `define_asset_job` and
  `ScheduleDefinition` alone as still-current APIs.
- **Both reports** are well-organized, name the version they verified against, and give a
  per-change rationale. WITH's is tighter; WITHOUT's carries more evidence per claim.
- **Speed/economy:** WITH needed a network install (`dagster-dg-cli` via `sfw uv pip install`)
  plus an editable reinstall for the new `src` layout. WITHOUT also declared `dagster-dg-cli`
  and reinstalled editable. Neither is a correctness issue, but WITH's restructure required more
  moving parts to end up at the same observable behavior.

---

## 4. Skill-induced errors

**One real defect, and it is squarely traceable to the skill.**

`with/legacy_pipeline/src/legacy_pipeline/definitions.py`:

```python
return dg.load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
```

`project_root` is **deprecated in Dagster 1.13.22**. Verified against the arm's own venv:

```
$ .venv/bin/python -W error::DeprecationWarning -c "import legacy_pipeline.definitions as m; m.defs()"
DeprecationWarning: Parameter `project_root` of function `load_from_defs_folder` is
deprecated and will be removed in 2.0. Use `path_within_project` instead.
```

So in a task whose entire point was removing deprecated APIs, the WITH arm's only new
entry-point line introduces one. The WITHOUT arm's project loads clean under
`-W error::DeprecationWarning`; the WITH arm's does not. This is invisible to every rubric
criterion (the symbol exists, defs load, `dg check defs` passes) which is exactly why it is
worth flagging.

Provenance is clear — the skill is internally inconsistent on this parameter:

- `skills/dagster/SKILL.md:87` — `path_within_project=` (correct)
- `skills/dagster/references/project-setup-and-cli.md:77` —
  `load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)` (deprecated;
  **exactly** the line the WITH arm emitted, three-`.parent` chain included)
- `skills/dagster/references/migration-and-legacy.md:101, 208, 215` — `project_root=`
- `skills/dagster/references/troubleshooting.md:91` — `project_root=`
- `skills/dagster/references/api-quick-reference.md:40` — "`(path_within_project=)` — snippets
  also pass `project_root=`" (notes the divergence, does not say one is deprecated)

And `project-setup-and-cli.md:80` actively steers the agent wrong:

> "docs examples use both spellings, so **match whatever the scaffold emitted rather than
> renaming it**."

That sentence treats a deprecated parameter and its replacement as interchangeable spellings.
The migration reference — the page an agent reads for *this exact task* — is the worst offender,
since it is the one place that should say "and the modern parameter name is
`path_within_project`."

No other skill-induced error found. Everything else the WITH arm did (AssetSpec for SourceAsset,
`deps=`, `AutomationCondition.eager()`, `@dg.definitions`, the `[tool.dg]` block, leaving the
schedule name derived, declining to add a `default_status=RUNNING` sensor) is correct and
matches the skill.

---

## 5. Skill gaps

### 5a. `@dg.definitions` in a `defs/` module hides that module's module-scope definitions — **CONFIRMED**

The WITH arm's report claims this; it is **true** for Dagster 1.13.22. Reproduced in a throwaway
project (`scratchpad/shadow_repro/`, never touching either arm) using the WITH arm's own
interpreter:

| Case | `defs/mod.py` contents | Loaded asset keys |
| --- | --- | --- |
| A | module-scope `@dg.asset` **+** `@dg.definitions` returning `Definitions(assets=[spec])` | `['ext_spec']` — **the decorated asset is silently dropped** |
| B | module-scope `@dg.asset` only | `['ext_spec', 'module_scope_asset']` |
| C | module-scope `@dg.asset` + `@dg.definitions` returning `Definitions(assets=[spec, module_scope_asset])` | `['ext_spec', 'module_scope_asset']` |
| D | `@dg.definitions` in `mod.py`, decorated asset in a **separate** `other.py` | `['ext_spec', 'module_scope_asset']` |
| E | module-scope `@dg.asset` + a module-level `defs = dg.Definitions(...)` object (no decorator) | `['ext_spec']` — **same shadowing** |

Precise rule, and it is broader than the WITH arm stated: **if a module under `defs/` exposes an
explicit `Definitions` — whether via `@dg.definitions` or as a plain module-level
`Definitions` object — that object becomes the module's entire contribution, and every other
module-scope definition in that same module is ignored.** Scoping is per-module (case D), and
**no warning of any kind is emitted** (verified under `-W always`) — the assets simply vanish
from `dg list defs`, which is how the WITH arm had to discover it.

This directly contradicts the skill, in two places:

- `SKILL.md:30` — "Everything under `src/<pkg>/defs/` is **autoloaded**: decorated assets,
  checks, schedules, sensors, jobs; **and** any `dg.Definitions` returned by a `@dg.definitions`
  function" — the "and" is wrong; within one module it is "or".
- `project-setup-and-cli.md:93` — "A function decorated with `@dg.definitions` returning
  `dg.Definitions` | **The returned `Definitions` is merged in**" — merged with *other modules*,
  yes; with *the same module's* module-scope definitions, no.

This matters most in the pattern the skill itself recommends most often: `@dg.definitions` is
presented throughout (`resources-and-config.md`, `io-managers.md`, `integrations-catalog.md`,
`resources_defs.py` template) as the way to bind resources. An agent that adds a resource
binding to an existing `defs/assets.py` rather than a separate `resources.py` will silently lose
every asset in that file.

**Recommended wording:** *"One module under `defs/` contributes **either** its module-scope
definitions **or** an explicit `Definitions` (from `@dg.definitions` or a module-level
`Definitions` object) — never both, and no warning is emitted. Keep `@dg.definitions` functions
in their own module (`defs/resources.py`), or list the module's other definitions inside the
returned `Definitions` explicitly. Run `dg list defs` after adding one."*

### 5b. `load_from_defs_folder(project_root=)` is deprecated — say so once, consistently

Per §4. The skill should replace `project_root=` with `path_within_project=` in
`project-setup-and-cli.md`, `migration-and-legacy.md` and `troubleshooting.md`, delete the
"match whatever the scaffold emitted rather than renaming it" advice, and state plainly that
`project_root` is deprecated with removal in 2.0. Note the two parameters are **not** positional
equivalents in the examples as written (`project_root` examples point at the project root;
`path_within_project` examples pass `Path(__file__).parent`), so a mechanical rename of the
keyword alone would be wrong — the guidance needs to give the correct argument for each.

### 5c. Preserve `code_location_name`, not just the schedule name

The skill teaches (correctly, and the WITH arm applied it) that a schedule's derived name must
not drift because run state is keyed by it. The same reasoning applies to the code location name
when replacing `workspace.yaml` with TOML config, and neither arm's source of guidance says so.
The WITH arm dropped `[tool.dagster]` and now trips a repeated "code location name … does not
match" warning from Dagster; the WITHOUT arm pinned `code_location_name` and validates silently.
A migration checklist line — *"when removing `workspace.yaml`, pin `code_location_name` to what
the old workspace entry resolved to, and keep `[tool.dagster] module_name` if anything still
invokes the classic `dagster` CLI"* — would have caught it.

### 5d. Make the before/after definitions diff a first-class migration step

The WITHOUT arm invented, unprompted, the single most convincing verification in either arm:
load the old repository object and the new `Definitions` side by side and diff a normalized
dump (asset keys, deps, automation conditions, executability, job names, schedule
name/cron/status, sensor name/status/targets). For any task phrased as "modernize without
changing what it does," that check is the difference between evidence and assertion — and the
skill's migration reference does not prescribe it. It would pair naturally with the existing
`find_legacy_patterns.py` script (which the WITH arm did use well, with a before/after hit
count) as a companion `snapshot_defs.py`.

### 5e. Minor: `dg` CLI emits a venv-mismatch warning on every invocation

Both arms' `dg check defs` / `dg list defs` runs print a multi-line "The active virtual
environment does not match the virtual environment found in the project root directory" banner
when `dg` is invoked as `.venv/bin/dg` without the venv activated. It is noise, not an error
(both still exited 0), but a one-line note — activate the venv, or add
`project_and_activated_venv_mismatch` to `cli.suppress_warnings` — would save an agent from
mistaking it for a failure.
