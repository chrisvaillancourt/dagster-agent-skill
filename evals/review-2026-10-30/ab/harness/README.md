# A/B harness inputs

- `batch0.md`–`batch3.md`: the prompts each arm received.
- `run-one.sh` (one level up): runs one model × arm × batch. Set `SCRATCH`, `REPO` and `USE_OMP` (the use-omp skill directory) first. `AB` is `$SCRATCH/ab`, and it holds these files plus a Dagster project at `$SCRATCH/probe-proj`.
- `project/`: that project's `pyproject.toml` and `uv.lock`, from `uvx create-dagster@1.13.28 project probe-proj`.
- `claude-settings.json`: the Opus arms' permission allow list. `omp-base.yml`: the Sol arms' overlay (subagents and bash run in the foreground, memory off).
- `prep-grade.py`: builds the blinded grading files (`grade-*.json`, `map-*.json`) from the answers.
