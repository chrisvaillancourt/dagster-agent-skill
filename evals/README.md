# Evals

Eval suite for the `dagster` skill, run with Claude Code's built-in harness. Each case runs twice — with the plugin loaded and without it — so the report shows the skill's delta, not just an absolute score.

## Cases

| Case | Kind | What it tests |
| --- | --- | --- |
| `qa-automation-condition` | Q&A | Chooses `AutomationCondition` composition over a custom sensor; no invented operators; mentions enabling the sensor |
| `qa-concurrency-pools` | Q&A | Real `dagster.yaml` concurrency keys (`concurrency.pools`, `concurrency.runs`) plus `pool=` on assets |
| `qa-schedule-not-firing` | Q&A | Diagnoses stopped-by-default schedules, timezone, daemon; verification via tick history |
| `build-new-project` | Build (tools) | Uses `create-dagster`/`dg scaffold defs`, `defs/` layout, `ScheduleDefinition`, runs `dg check defs` |
| `modernize-legacy-project` | Migration (fixture) | Replaces `@repository`, `SourceAsset`, `non_argument_deps`, `AutoMaterializePolicy`; verifies load |
| `partitioned-assets` | Build (fixture) | Daily→weekly partitions, `TimeWindowPartitionMapping`, `BackfillPolicy.single_run`, partition launch |
| `dbt-component` | Integration (fixture) | `DbtProjectComponent` via `dg scaffold defs`, 05:00 automation, `dg check yaml`/`defs`, asset keys listed |
| `negative-pandas-pivot` | Negative | Unrelated pandas task must not activate the skill |

Graders: a `tool_used: Skill` indicator per case (reported, not scored, in two-arm mode), cheap `regex`/`file_exists`/`tool_used` checks, and one weighted `llm` rubric that demands evidence of verification in the transcript. Rubrics list the allowed API names where hallucination is the main risk.

## Running

Fixture cases create real `uv` environments (network required; a few minutes each). Build/fixture cases need write and shell access granted on the command line.

```bash
# everything, Opus agent + Opus judge, with/without comparison, 4 in parallel
claude plugin eval . --model opus --judge-model opus --allow-tools Bash Write Edit --scaffold -j 4 --no-publish

# Q&A cases only (fast, no tools needed)
claude plugin eval . --tag qa --model opus --judge-model opus --no-publish

# one case, skill arm only, keep the sandbox for inspection
claude plugin eval . --case dbt-component --ablation none --allow-tools Bash Write Edit --scaffold --keep-temp
```

Results land in `evals/results/<timestamp>/` (git-ignored): `aggregate-result.json` and `report.html`. Add `--json out.json` for CI.

## Known limitation: Bash-granting runs on machines with Docker Desktop

`claude plugin eval` refuses to grant `Bash` when `~/.docker` contains symlinks (Docker Desktop installs its CLI plugins as symlinks under `~/.docker/cli-plugins/`), reporting that the sandbox "cannot reliably exclude" the credential store. `DOCKER_CONFIG` does not bypass the check. On such machines only the Q&A and negative cases run through the harness; run the four build cases with the manual two-arm procedure below (that is how the results in this repo were produced), or run the full suite in CI / on a machine without Docker Desktop.

Manual two-arm procedure for a build case: create two copies of the fixture (`bash evals/<case>/scaffold.sh` in `with/` and `without/`), give an Opus agent the exact `prompt.md` text in each — prefixing the `with` arm with "A skill named `dagster` is installed; read `<repo>/skills/dagster/SKILL.md` first and follow it" — then have a separate Opus judge grade both directories against `graders/quality.md` and the regex graders, re-running `dg check defs` itself to confirm claims.

## Results

### Q&A cases through `claude plugin eval` (2026-09-13)

`claude plugin eval . --tag qa --model opus --judge-model opus` on Claude Code 2.1.270, two runs per case, default `with-without` ablation: 4/4 cases passed, overall score 1.00, mean delta +0.06, $5.73, 291 s.

| Case | With skill | Without | Skill trigger |
| --- | --- | --- | --- |
| qa-automation-condition | 1.00 | 0.75 | fired in both runs |
| qa-concurrency-pools | 1.00 | 1.00 | fired in both runs |
| qa-schedule-not-firing | 1.00 | 1.00 | fired in both runs |
| negative-pandas-pivot | 1.00 | 1.00 | not fired (correct) |

### Build cases, manual two-arm procedure (2026-09-14)

Four fixture cases × two arms, each arm a fresh Opus agent in its own copy of the fixture, graded by a separate Opus judge that re-ran `dg check defs`, `dg list defs`, a launch, and the tests in every arm. Judge reports: `evals/manual-runs/2026-09-14/`.

| Case | With skill | Without | What the skill changed |
| --- | --- | --- | --- |
| build-new-project | PASS 6/6 | PASS 6/6 | `target=` on the schedule instead of a job, `dg launch` instead of `dagster job execute`, no scaffold-path misfire (the without arm created `defs/src/…/defs/assets.py` before recovering) |
| modernize-legacy-project | PASS 7/7 | PASS 7/7 | Neutral to slightly negative: the with arm wrote a deprecated `load_from_defs_folder(project_root=…)` copied from the references (now fixed) and dropped the code-location name; the without arm diffed before/after instigators unprompted |
| partitioned-assets | PASS 6/6 | PASS 6/6 | Upstream keys read through the declared mapping (`asset_partition_keys_for_input`) rather than recomputed by hand; `dg launch --partition-range` as proof; both arms had to discover the single-run/IO-manager interaction themselves |
| dbt-component | PASS 5/5 | PASS 5/5 | No difference in the component YAML (byte-identical); with arm added regression tests, without arm left a stray helper script |

Fixes made from this round: replaced every `project_root=` with `path_within_project=Path(__file__).parent` (five places) and documented the deprecation; corrected the autoload rule that a `defs/` module contributes either its module-scope objects or an explicit `Definitions`, never both (reproduced by the judge, no warning emitted); added the single-run backfill / default I/O manager gotcha, weekly `day_offset` alignment, `for_input` helpers, `code_location_name` pinning and a before/after snapshot step for migrations, `translation.group_name` in the dbt pattern, the anonymous-job behavior of `target=`, `dg scaffold defs` path semantics, and a schedule test that evaluates a tick in the test template.

## Interpreting

- `WITH` is the score with the skill, `W/OUT` without, `Δ` the difference. The skill should never lower a score; a negative delta on a case means the skill is steering the agent wrong — read that case's transcript first.
- A failing `skill-fired` indicator on a Dagster case means the description did not trigger; adjust `SKILL.md` frontmatter, not the rubric.
- Q&A cases run twice by default; build cases once (they are slow). Raise `--runs` when a result looks noisy.

## Authoring history

Reference files were distilled from `https://docs.dagster.io/llms.txt` (guide pages via the docs source in `dagster-io/dagster`, API pages via `llms-full.txt`) at Dagster 1.13.x, one author agent per file under a shared contract (verify every name against the sources, `import dagster as dg`, 200–500 lines, table of contents, sources list). SKILL.md and the references were then evaluated against these cases (see [Results](#results)) and revised wherever the skill arm underperformed or a judge traced an error back to skill text.
