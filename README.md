# Dagster Agent Skill (retired)

This was a cross-harness [Agent Skill](https://agentskills.io) for building, testing, automating, operating and deploying [Dagster](https://dagster.io) pipelines. It is retired and no longer maintained. Don't install it.

## Why

An A/B test on 2026-10-30 compared three setups for Claude Opus 5.5 and GPT-6.1 Sol, each running in a real Dagster 1.13.28 project with full tools:

- tools only;
- tools plus a one-line pointer to the Dagster docs;
- tools plus this skill.

The 38 questions were 28 that at least one model got wrong or only partly right without tools, plus 10 controls both got right. All three setups scored about the same. The skill raised cost by 15% on Opus and 39% on Sol, and one of its stale claims made Sol answer a question wrong.

An audit in the same review reported 98 issues (wrong, stale, misleading, self-contradicting or unverifiable claims) across the skill's 20 reference files. Agents that can check against the installed Dagster (`--help`, signatures, a quick script) don't need a curated cache of Dagster facts. The cache goes stale and then misleads them.

The full review, evidence and harness are in [`evals/review-2026-10-30/`](evals/review-2026-10-30/README.md).

## What to use instead

Put a few lines in the `AGENTS.md` (or `CLAUDE.md`) of each repository that uses Dagster. They cover what the test didn't measure: checking the installed version, safe verification, and not touching real resources. Adapt this:

```markdown
## Dagster

- The Dagster version is pinned in the lockfile and can lag the docs and what models remember. Check APIs and CLI flags against the installed version first: `uv run dg <command> --help`, `inspect.signature` on the symbol, or the installed source. For behavior that doesn't settle, the docs index is https://docs.dagster.io/llms.txt (it tracks the latest release).
- After changing definitions, run `uv run dg check defs` in each code location you changed. It loads every definition without running anything.
- Don't execute assets or jobs outside tests (`dg launch`, `materialize` against real resources), or run `dg plus` commands that change a deployment, unless the user asks.
```

## Contents (historical)

- `skills/dagster/`: the skill as last released (`SKILL.md`, 20 reference files, scripts, templates). The review lists its known errors.
- `evals/`: the original `claude plugin eval` suite and the 2026-10-30 review.
- `tools/`: maintainer checks (`validate_skill.py`, `check_symbols.py`).
