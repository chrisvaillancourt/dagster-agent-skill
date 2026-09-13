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

## Interpreting

- `WITH` is the score with the skill, `W/OUT` without, `Δ` the difference. The skill should never lower a score; a negative delta on a case means the skill is steering the agent wrong — read that case's transcript first.
- A failing `skill-fired` indicator on a Dagster case means the description did not trigger; adjust `SKILL.md` frontmatter, not the rubric.
- Q&A cases run twice by default; build cases once (they are slow). Raise `--runs` when a result looks noisy.

## Authoring history

Reference files were distilled from `https://docs.dagster.io/llms.txt` (guide pages via the docs source in `dagster-io/dagster`, API pages via `llms-full.txt`) at Dagster 1.13.x, one author agent per file under a shared contract (verify every name against the sources, `import dagster as dg`, 200–500 lines, table of contents, sources list). SKILL.md was then evaluated against these cases and revised where the skill arm underperformed.
