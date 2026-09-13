#!/usr/bin/env bash
# Verification loop for a Dagster (dg) project: validate YAML, load definitions, list them,
# optionally launch a selection and run tests. Run after every change to definitions.
#
# Usage:
#   scripts/check_project.sh [PROJECT_DIR] [--launch "<asset selection>"] [--tests] [--quiet]
#
# Exit codes: 0 all steps passed · 1 a step failed · 2 no dg project found · 3 dg not runnable
#
# How dg is resolved (first match wins):
#   1. `uv run dg` if uv is on PATH      -> uv activates the project environment (preferred)
#   2. PROJECT_DIR/.venv/bin/dg          -> the project's own environment without uv
#   3. `dg` on PATH
#   4. `uvx --from dagster-dg-cli dg`    -> works for dg-level checks but the project's
#                                           dependencies may be missing; a warning is printed.
set -u

QUIET=0; LAUNCH=""; TESTS=0; TARGET="."
while [[ $# -gt 0 ]]; do
  case "$1" in
    --launch) LAUNCH="${2:-}"; shift 2 ;;
    --tests) TESTS=1; shift ;;
    --quiet) QUIET=1; shift ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) TARGET="$1"; shift ;;
  esac
done

log() { [[ $QUIET -eq 1 ]] || printf '%s\n' "$*"; }
hr()  { log "──────────────────────────────────────────────"; }

# Locate the project root: walk up from TARGET until pyproject.toml with [tool.dg] or dg.toml.
find_root() {
  local d; d="$(cd "$1" 2>/dev/null && pwd)" || return 1
  while [[ "$d" != "/" ]]; do
    if [[ -f "$d/dg.toml" ]] || { [[ -f "$d/pyproject.toml" ]] && grep -q '^\[tool\.dg' "$d/pyproject.toml"; }; then
      printf '%s\n' "$d"; return 0
    fi
    d="$(dirname "$d")"
  done
  return 1
}

ROOT="$(find_root "$TARGET")" || {
  echo "No dg project found at or above '$TARGET' (need pyproject.toml with [tool.dg] or dg.toml)." >&2
  echo "Legacy project? Look for workspace.yaml / @repository and see references/migration-and-legacy.md." >&2
  exit 2
}
cd "$ROOT" || exit 2

if command -v uv >/dev/null 2>&1 && [[ -f pyproject.toml ]]; then DG=(uv run dg)   # activates the project env; no venv-mismatch warning
elif [[ -x .venv/bin/dg ]]; then DG=(.venv/bin/dg)
elif command -v dg >/dev/null 2>&1; then DG=(dg)
elif command -v uvx >/dev/null 2>&1; then
  DG=(uvx --from dagster-dg-cli dg)
  log "warning: using uvx dagster-dg-cli; project dependencies may be missing (run 'uv sync' first)."
else
  echo "dg is not available. Install uv (https://docs.astral.sh/uv/) and run 'uv sync' in the project." >&2
  exit 3
fi

FAILED=0
run_step() {  # run_step "<label>" cmd...
  local label="$1"; shift
  hr; log "▶ $label"; log "  $*"
  if "$@"; then log "  ✔ ok"; else FAILED=1; log "  ✘ FAILED (exit $?)"; fi
}

hr; log "project: $ROOT"; log "dg:      ${DG[*]}"
"${DG[@]}" --version 2>/dev/null | sed 's/^/  /' || true

# Legacy markers worth flagging before anything else.
for marker in workspace.yaml; do
  [[ -f "$marker" ]] && log "note: $marker present — superseded by dg.toml/pyproject [tool.dg]; see references/migration-and-legacy.md"
done
if grep -rqs --include='*.py' -E '@(dg\.)?repository\b' src 2>/dev/null; then
  log "note: @repository found — superseded by Definitions/@definitions; run scripts/find_legacy_patterns.py"
fi

# 1. Schema-check defs.yaml files (cheap, precise errors). Only when components exist.
if find . -path ./.venv -prune -o -name defs.yaml -print -quit 2>/dev/null | grep -q .; then
  run_step "dg check yaml" "${DG[@]}" check yaml
fi
# 2. Load every definition with a real Dagster instance (the authoritative check).
run_step "dg check defs" "${DG[@]}" check defs
# 3. Show what loaded so the agent can confirm keys/groups/deps.
if [[ $FAILED -eq 0 ]]; then
  run_step "dg list defs" "${DG[@]}" list defs
fi
# 4. Optional: execute a selection locally.
if [[ -n "$LAUNCH" && $FAILED -eq 0 ]]; then
  run_step "dg launch --assets $LAUNCH" "${DG[@]}" launch --assets "$LAUNCH"
fi
# 5. Optional: unit tests.
if [[ $TESTS -eq 1 ]]; then
  if [[ -d tests ]]; then
    if [[ -x .venv/bin/pytest ]]; then run_step "pytest" uv run pytest tests -q
    elif [[ "${DG[0]}" == "uv" ]]; then
      log "note: pytest is not a project dependency (add it with 'uv add --dev pytest'); running it ephemerally."
      run_step "pytest (ephemeral)" uv run --with pytest pytest tests -q
    else run_step "pytest" pytest tests -q; fi
  else
    log "note: no tests/ directory."
  fi
fi

hr
if [[ $FAILED -eq 0 ]]; then log "ALL CHECKS PASSED"; exit 0
else log "SOME CHECKS FAILED — read the first error above; common causes are in references/troubleshooting.md"; exit 1; fi
