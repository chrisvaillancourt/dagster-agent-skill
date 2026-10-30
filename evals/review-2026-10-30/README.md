# Skill review, 2026-10-30

A review of the `dagster` skill at `24b8436` against Dagster 1.13.28, released today (no 1.14 exists yet). Its primary consumers are Claude Opus 5.5 and GPT-6.1 Sol.

## Verdict

**The skill isn't worth maintaining** (the A/B result below decides it). On questions picked because the models got them wrong closed-book, an agent that can run code in an installed Dagster project did as well without the skill as with it:

- Opus 5.5 scored about the same in all three arms.
- Sol did slightly worse with the skill, because one of its stale claims misled it.
- The skill added 15% to cost on Opus and 39% on Sol.

A one-line pointer to the docs didn't beat tools alone either. What makes agents right is the installed project, not a curated cache.

The rest of this review was written before the A/B test. It still records what's wrong in the current skill, and the "Recommended plan" section is superseded.

## A/B test: tools only, a docs pointer, or the skill

- **Setup:** 38 questions: the 28 that either model got wrong closed-book, plus 10 controls they both got right. Each arm ran in a fresh copy of a Dagster 1.13.28 project, with full tools (shell, web, file edits) and the instruction to check its answers. The arms differed in one thing only:
  - **T:** nothing extra.
  - **D:** an `AGENTS.md`/`CLAUDE.md` line pointing to `https://docs.dagster.io/llms.txt`.
  - **S:** the current skill.
- **Models:** Opus 5.5 ran through `claude -p` with medium effort and no user config. Sol ran through omp with medium thinking, `--no-skills` in T and D, and the skill discoverable in S.
- **Grading:** blind. Opus 5.5 graders, at high effort, saw the arms as X/Y/Z in a different order per question, and checked disputed keys against the installed Dagster.
- **Evidence:** `ab/` (questions, answers, grades, the unblinding map, metrics, and the harness `run-one.sh`).

| Arm | Opus 5.5 correct / partial / wrong | Opus cost, time | Sol correct / partial / wrong | Sol cost, time |
| --- | --- | --- | --- | --- |
| T: tools only | 32 / 5 / 1 | $2.69, 544 s | 36 / 2 / 0 | $1.78, 924 s |
| D: + docs pointer | 30 / 7 / 1 | $3.02, 505 s | 36 / 2 / 0 | $2.32, 810 s |
| S: + skill | 33 / 4 / 1 | $3.09, 606 s | 34 / 3 / 1 | $2.47, 860 s |

Costs are list-price estimates summed over four batches, one run per arm.

- **Arms disagreed on very few questions:** 9 of 38 for Opus and 4 of 38 for Sol. There was no consistent winner.
- **The skill's only "wrong" on Sol was the skill's own stale claim.** It said `uv sync` doesn't reinstall after an entry-point change (items 5 and 50).
- **With tools, both models confirmed by running code** what they had gotten wrong closed-book.
- **With the pointer, Sol fetched 13–32 docs URLs per batch.** In the tools-only arm it fetched almost none and ran 28–39 commands instead.
- **The graders found the answer keys wrong on items 1, 23, 31, 39 and 47, and partly on 50.** The models' answers were right there. The keys came from the audit lanes, which suggests that hand-curated "non-obvious facts" go wrong at a rate comparable to the skill's own content.

**What this does not measure:**

- Behavior the skill steers without being asked: the Dagster+ write gate, a safe verification scope, choosing modern patterns in open-ended build tasks. The September build evals on Opus 5 also showed near-zero delta there.
- Agents with no installed project to probe, as in conceptual questions or a project not yet created.

The sample is small: one run per arm, and 38 questions picked from the skill's own content.

## Method

| Lane | Who | Evidence |
| --- | --- | --- |
| Independent review, run in an isolated clone with real commands | GPT-6.1 Sol, high effort, via omp | its findings below; four reproduced in `repro_sol_findings.py` |
| Reference audit: 7 checkers, each covering about 3 reference files and testing claims against a scaffolded 1.13.28 project and the docs | Sonnet 5.5 workflow | `reference-audit.json` |
| Closed-book probes: 58 questions on the references' least obvious facts, answered with no tools and no skill | Opus 5.5 (`claude -p`, isolated) and Sol (omp, `--no-tools --no-skills`) | `probes/` |
| Grading of the probe answers against the key | Opus 5.5 | `probes/grades.json` |
| What changed in Dagster since 1.13.27, plus `tools/check_symbols.py` | Sonnet scout | summarized below |
| Current skill-authoring guidance | Sonnet scout | summarized below |
| Redesign critique | Sol, high effort | folded into the plan |

## Findings

### 1. The references contain many wrong claims

The audit reported 98 problems across the 20 reference files: wrong, stale, misleading, or contradicting another part of the skill. Nearly all came with command output or a docs quote (`reference-audit.json`, `findings[]`). `check_symbols.py` still passes (0 unknown symbols), because the errors are wrong *behavior*, not misspelled names. Representative cases:

- `io-managers.md:41`: "`deps=` skips I/O managers entirely". In fact the downstream asset's output is still stored. Reproduced.
- `SKILL.md:196`: says mixed partition definitions need a `partition_mapping`. In fact daily→weekly is inferred as a `TimeWindowPartitionMapping`. Reproduced.
- `api-quick-reference.md:34,415`: "there is no `resolve_job_def`" and "no `terminate_run`". Both exist. Reproduced.
- `SKILL.md:4`: "Python 3.9+". The `create-dagster` scaffold requires `>=3.10,<3.15`. Reproduced.
- `project-setup-and-cli.md:171`, `components.md:311`, `troubleshooting.md:46`, `migration-and-legacy.md:189`: "`uv sync` won't reinstall after an entry-point change". In fact uv 0.13.6 reinstalls the package and registers the entry point. Reproduced.
- `components.md:366`: the state-backed component example raises `ValueError: mutable default` on Python 3.11+. It also needs `dg.Resolvable` in its bases.
- `operations-and-debugging.md:46,52`: says every `dagster.yaml` scalar accepts `{env: VAR}`, and that the default run coordinator is `DefaultRunCoordinator`. Both are wrong. The default is `QueuedRunCoordinator`.
- `assets.md:351`: `tag:"stage"` is described as "tag presence". It matches only an empty tag value.
- `api-quick-reference.md:274`: `materialize_to_memory` is marked deprecated. It isn't. Separately, `AssetSpec` is shown with a `backfill_policy` parameter that doesn't exist.
- `integrations-dbt-and-elt.md:112`: `DbtCliResource` is said to default to `dbt`. It defaults to `dbtf` when that is on `PATH`.
- `SKILL.md:191`: "Resources are not components" is too broad. A custom component returning `Definitions(resources=…)` works. Sol verified this.

### 2. The models already know most of it

These were closed-book answers to the references' least obvious facts, picked on purpose to catch stale knowledge (`probes/grades.json`):

| | Correct | Partial | Wrong |
| --- | --- | --- | --- |
| Opus 5.5 | 40 | 12 | 6 |
| GPT-6.1 Sol | 37 | 11 | 10 |

The 28 items either model missed (`probes/misses.txt`) fall into three groups:

- **Real traps, missed by both or by Sol.** These are worth keeping:
  - post-processing `operation: merge` replaces `kinds` but merges tags (3);
  - `map_asset_specs` takes keyword arguments only (7);
  - `ConfigurableResource[T]` with `create_resource` and `ResourceParam`; `ConfigurableResourceFactory` isn't exported (32);
  - how legacy freshness policies fail (47);
  - removed external-asset helpers, and `deps=AssetKey` not being iterable (48);
  - an unset `{{ env.X }}` renders as `None` and doesn't fail the load (1);
  - multi-segment Sling/dlt keys (52);
  - `partition_loading_context` in schedules (19);
  - BI refresh flags on components (54);
  - freshness needs `freshness: enabled` in `dagster.yaml` (31).
- **The skill was wrong and the models were right.** Here reading the reference would have made the agent worse: the run coordinator default (35), `{env:}` scalars (37), Pipes `table_schema` (36), and `uv sync` reinstall (5).
- **Bad answer keys.** Item 50 is stale (`uv sync` does reinstall), and items 1 and 56 are disputed.

The earlier evals point the same way. Build cases showed a near-zero delta on Opus 5 (`evals/README.md`).

### 3. Instructions that misfire for strong models

- **Rule 1 and "open exactly the reference(s) the router names"** force retrieval even when the model's answer is right, and that retrieval reaches the wrong claims above. Open the topic's risk checklist first. Then use the installed `--help`, signatures, or version-matched docs for executable details.
- **The verification loop runs `dg launch` by default.** In an unfamiliar project, an ephemeral instance doesn't isolate database writes or external services. Scale the checks to the change. Validating definitions is always safe; run assets only against fixtures or targets the user has authorized.
- **No precedence statement.** OpenAI's GPT-6 guide says skills should state that user instructions win. Rule 4 (`import dagster as dg` everywhere) can churn a repo's own conventions.
- **Bare triggers in the description** ("assets", "resources", "components", "dg") can fire outside Dagster work. Require Dagster context.
- **"Live docs win"** ignores the installed version. `dagster_docs.py` fetches `master`, and `--api` reads the unversioned `llms-full.txt` even when `--ref` is set (`scripts/dagster_docs.py:209-215,278-303`; Sol verified this).

### 4. Scripts

- **`find_legacy_patterns.py` misfires on a production OSS repo.** All 31 of its hits were false positives:
  - 15 `workspace.yaml` files with `grpc_server` entries, which is current OSS wiring and contradicts `SKILL.md` rule 2;
  - 12 `OpExecutionContext` uses inside `@dg.op` functions;
  - 4 mentions of `dagster dev` in Markdown and docstrings.

  Sol also saw it flag `dagster instance info` and `dagster instance concurrency set`, both supported. Rule 7 tells agents to run it before editing any project. Make it parse Python and check decorators, skip strings and comments, and stop judging `workspace.yaml` by topology alone.
- **`check_project.sh` has three faults:**
  - It prints `FAILED (exit 0)` for every failure, because `$?` is read after `FAILED=1` (line 66).
  - `--tests` fails on every fresh scaffold: pytest exits 5 when it collects no tests.
  - `ALL CHECKS PASSED` doesn't say which checks were skipped.
- **`validate_skill.py`** passes. Its line and size caps don't catch any of the problems above.

### 5. Changes since 1.13.27 (small)

- 1.13.26: `dagster-dbt` no longer installs `dbt-core`; use the `dagster-dbt[dbt-core]` extra. `--deployment` was added to `dg list envs` and `dg plus create|pull env`.
- 1.13.28:
  - `install_deps=False` on the dbt project, preparer and component;
  - the `dagster-dbt[dbt]` Fusion extra;
  - the Insights "run concurrency" metric deprecated in favor of "runs in progress";
  - `PipesDatabricksClient.run_job()` (preview).

## Authoring guidance that applies

Sources were fetched 2026-10-30. Several pages show no publication date.

- **Add only what the model gets wrong**, and put the traps in `SKILL.md` itself. Sources: agentskills.io best practices; Anthropic's skill-authoring best practices.
- **Small, focused bundles beat exhaustive ones.** SkillsBench (arXiv 2602.12670, Jun 2026). Dagster Labs' own skill is 236 KB across 174 files, with no file over 11.4 KB and an 8.2 KB `SKILL.md`. This one is 603 KB across 20 files of 23–49 KB, with an 18 KB `SKILL.md`.
- **Codex cuts shell output at about 10K tokens** and drops the middle, so 30–49 KB references can lose content silently when Sol reads them through `exec_command`.
- **Opus 5.x** over-verifies when told to, and Anthropic advises against emphatic MUST/NEVER wording.
- **GPT-6:** OpenAI says the models are "more sensitive to instructions contained in skills", that skills should say user instructions win, and that skills shouldn't add approval flows nobody asked for.
- **Evaluate with paired with/without runs** on tasks that need knowledge newer than the models' training. Score process and cost (tokens, time, files read) as well as the outcome.

## Recommended plan

Order of priority: correct semantics, then selective routing, then safe verification, then behavioral evidence. Byte limits support these goals; they aren't goals themselves.

1. **`SKILL.md`, 4–6 KB.**
   - Scope, the tested version and the version policy (below).
   - Precedence: user requirements and repo conventions win.
   - The side-effect boundary: Dagster+ writes and any execution against real resources.
   - Change-scaled verification ("smallest check that exercises the changed contract; report what ran and what remains unverified").
   - The broad traps: silent autoload exclusion, validation versus execution, local versus deployed, preserving identity during migrations.
   - A task → topic → specific-risk router, including symptom routes ("schedule didn't fire" → automation).
2. **Version policy: a versioned 1.13 companion, with the tested patch named.**
   - An existing project uses its own installed versions; report any mismatch and never upgrade just to fit the skill.
   - A new project uses the version the user asks for, or the skill's tested pin.
   - Outside the tested range, verify the affected behavior instead of refusing.
3. **Topic files of about 8 KB or less, holding only risk checklists and decision guidance.** Keep automation's DA, schedule and sensor sections separately addressable. Carry over every high-value item from `reference-audit.json` (`files[].cache_worthy`) and the trap probes above; fix or drop every finding. Delete `api-quick-reference.md`, `integrations-catalog.md` and `troubleshooting.md` as files, but keep three things from them:
   - consequential removals and renames, in the migration topic;
   - discovery guidance for integrations that aren't installed (`dg list components` sees only the current environment), plus side effects such as Fivetran's scheduling switch;
   - a Pipes boundary checklist.

   `reviewing-dagster-code.md` stays as it is (the audit rated it high value and found no errors).
4. **Retrieval wording:** open the matching topic's risk checklist before choosing an approach. Use the installed `--help` and signatures for executable details. Use version-matched docs or the installed source when the behavior is still unclear.
5. **Behavioral checks for the costly contracts only:** silent autoload loss, range backfills, resource lifecycle, automation timing, blocking checks, deploy sequencing. Group them by subsystem under `tools/`, run them in CI on the pinned version, and run them on demand against a candidate version before a bump. Keep a small claim → evidence → tested-version index. Hosted-only facts get a dated source instead.
6. **Fix the scripts** as described in finding 4, and make `dagster_docs.py` version-aware, or label its output as unversioned.
7. **Evals:**
   - Use the 58 probes diagnostically, after fixing keys 50, 1 and 56.
   - Add held-out with/without build tasks for both Opus 5.5 and Sol, including negative and legacy cases.
   - Score correctness, false migrations, side effects, tokens, time and files read.
   - Keep a change only if it lifts a score or cuts cost without losing accuracy.

## Open decisions

- Is a correction-layer rebuild acceptable, even though it deletes most of the 600 KB?
- Should the skill enforce house style (`import dagster as dg`), or defer to the repo's own conventions? This review recommends deferring.
- What counts as enough evidence to keep a piece of content: a minimum probe-miss rate or eval delta?
