# Dagster Agent Skill

An independent, cross-harness [Agent Skill](https://agentskills.io) for building, testing, automating, operating and deploying [Dagster](https://dagster.io) data pipelines. Works in Claude Code, OpenAI Codex, Cursor, GitHub Copilot and any other tool that reads `SKILL.md`.

The skill is grounded in the official documentation (`https://docs.dagster.io/llms.txt` and the docs source in `dagster-io/dagster`), targets the modern toolchain (`dg` CLI, `create-dagster`, `defs/` autoloading, components, declarative automation, Pipes), and ships a docs fetcher so answers never go stale.

## What's inside

```
skills/dagster/
├── SKILL.md                 # entry point: rules, mental model, task router, canonical patterns
├── references/              # 20 focused files, loaded on demand (progressive disclosure)
│   ├── project-setup-and-cli.md      assets.md                partitions-and-backfills.md
│   ├── schedules-and-sensors.md      declarative-automation.md resources-and-config.md
│   ├── io-managers.md                components.md            testing-and-asset-checks.md
│   ├── ops-jobs-and-graphs.md        operations-and-debugging.md deployment-oss.md
│   ├── dagster-plus.md               pipes-external-execution.md integrations-catalog.md
│   ├── integrations-dbt-and-elt.md   api-quick-reference.md   migration-and-legacy.md
│   └── troubleshooting.md            reviewing-dagster-code.md
├── scripts/
│   ├── dagster_docs.py          # fetch any docs page (snippets inlined) or API page; search the index
│   ├── check_project.sh         # dg check yaml → dg check defs → dg list defs → optional launch/tests
│   └── find_legacy_patterns.py  # flag superseded APIs with their modern replacement
└── assets/templates/            # pyproject [tool.dg], dagster.yaml, Dockerfile, CI, resources, tests
evals/                           # `claude plugin eval` suite (with/without-skill comparison)
tools/                           # maintainer checks: validate_skill.py (spec + structure), check_symbols.py (API names vs docs corpus)
```

## Install

**Claude Code** (plugin marketplace):

```
/plugin marketplace add chrisvaillancourt/dagster-agent-skill
/plugin install dagster-agent-skill@dagster-agent-skill
```

**Codex, Cursor, Copilot and 70+ other agents** (via the [`skills`](https://skills.sh) CLI; `-g` installs for all projects):

```bash
npx skills add chrisvaillancourt/dagster-agent-skill -g
```

**Manual**: copy `skills/dagster/` into your tool's skills directory (`~/.claude/skills/`, `.claude/skills/`, `.agents/skills/`, `~/.codex/skills/`, …).

Invoke explicitly with `/dagster <request>` where your tool supports slash-skills; otherwise it activates automatically on Dagster-related tasks.

## Requirements

- Python 3.9+ and [uv](https://docs.astral.sh/uv/) (Dagster's default toolchain; `dg` is run as `uv run dg`).
- Network access for `scripts/dagster_docs.py` (results cached 24h).

## Relationship to the official `dagster-io/skills`

Dagster Labs publishes an official skill (`dagster-expert` inside the `dagster` plugin). This project is independent: broader topical coverage (deployment, operations, Pipes, testing, migration, troubleshooting), a live docs fetcher, and verification scripts. Its repository, marketplace, and plugin are named `dagster-agent-skill`; the skill itself is named `dagster`. These names distinguish it from both the official Dagster project and its agent skill.

## Evaluating

```bash
claude plugin eval . --model claude-opus-5 --allow-tools Bash Write Edit --scaffold -j 4
```

See `evals/README.md` for the case list, grading rubric, and the recorded results of the last with/without-skill run.

## Updating

The reference files were distilled from the docs at Dagster 1.13.x. To refresh: re-run the author process described in `evals/README.md`, or fetch individual pages with `scripts/dagster_docs.py <path>` and update the affected reference. Before committing, run:

```bash
uv run python tools/validate_skill.py skills/dagster            # frontmatter, line caps, links, TOCs, residue
curl -sL https://docs.dagster.io/llms-full.txt -o /tmp/llms-full.txt
uv run python tools/check_symbols.py /tmp/llms-full.txt <dagster-repo>/docs/docs skills/dagster/references
```
