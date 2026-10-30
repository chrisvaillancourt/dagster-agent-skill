#!/bin/bash
# usage: run-one.sh <model opus|sol> <arm T|D|S> <batch>
set -u
AB=$SCRATCH/ab
SRC=$SCRATCH/probe-proj
REPO=$REPO
m=$1; arm=$2; b=$3; id="$m-$arm-$b"; d="$AB/runs/$id"
rm -rf "$d"; mkdir -p "$d"; cp -R "$SRC" "$d/proj"; rm -rf "$d/proj/.venv"; (cd "$d/proj" && uv sync -q >/dev/null 2>&1)
if [ "$arm" = D ]; then
  printf '# Agent notes\n\nFor Dagster questions and work, use the official Dagster docs. The index is https://docs.dagster.io/llms.txt (each entry is a page at https://docs.dagster.io/<path>; the full text of every page is https://docs.dagster.io/llms-full.txt).\n' > "$d/proj/AGENTS.md"
  cp "$d/proj/AGENTS.md" "$d/proj/CLAUDE.md"
fi
start=$(date +%s)
if [ "$m" = opus ]; then
  extra=(); [ "$arm" = S ] && extra=(--plugin-dir "$REPO")
  (cd "$d/proj" && timeout 1500 claude -p --setting-sources project --strict-mcp-config --settings "$AB/claude-settings.json" \
     --model claude-opus-5-5 --effort medium --no-session-persistence --output-format json ${extra[@]+"${extra[@]}"} < "$AB/batch$b.md" > "$d/out.json" 2> "$d/err.txt")
  jq -r .result "$d/out.json" > "$d/answer.md" 2>/dev/null
  jq -c '{cost:.total_cost_usd, turns:.num_turns, ms:.duration_ms, in:(.usage.input_tokens+.usage.cache_read_input_tokens+.usage.cache_creation_input_tokens), out:.usage.output_tokens}' "$d/out.json" > "$d/metrics.json" 2>/dev/null
else
  noskill=(); [ "$arm" != S ] && noskill=(--no-skills)
  (cd "$d/proj" && omp -p --cwd "$d/proj" --mode json --max-time 25m --model openai-codex/gpt-6.1-sol --thinking medium \
     --config "$AB/omp-base.yml" --no-session ${noskill[@]+"${noskill[@]}"} "@$AB/batch$b.md" -- "The attached file is my task for you. Carry it out." \
     < /dev/null > "$d/events.jsonl" 2> "$d/err.txt")
  jq -s -f $USE_OMP/scripts/summarize-events.jq "$d/events.jsonl" > "$d/summary.json" 2>/dev/null
  jq -rs '[.[] | objects | select(.type == "message_end" and .message.role? == "assistant") | .message] | last | [.content[]? | select(.type == "text") | .text] | join("\n")' "$d/events.jsonl" > "$d/answer.md" 2>/dev/null
  jq -c '{cost:.primary_cost_usd, tokens:.tokens, complete:.looks_complete, tool_errors:(.tool_errors|length)}' "$d/summary.json" > "$d/metrics.json" 2>/dev/null
fi
echo "{\"secs\": $(( $(date +%s) - start ))}" > "$d/time.json"
echo "done $id"
