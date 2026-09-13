#!/usr/bin/env bash
# Fixture: a fresh create-dagster project with its environment synced. Runs in the case working dir.
set -euo pipefail
uvx create-dagster@latest project analytics --uv-sync >/dev/null 2>&1 || uvx create-dagster@latest project analytics --no-uv-sync
cd analytics && uv sync -q
echo "fixture ready: $(pwd)"
