#!/usr/bin/env bash
# Fixture: a small pre-dg Dagster project using superseded APIs. Runs in the case working dir.
set -euo pipefail
mkdir -p legacy_pipeline/legacy_pipeline
cat > legacy_pipeline/pyproject.toml <<'PY'
[project]
name = "legacy_pipeline"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = ["dagster>=1.13,<1.14", "dagster-webserver>=1.13,<1.14"]

[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["legacy_pipeline*"]
PY
cat > legacy_pipeline/workspace.yaml <<'PY'
load_from:
  - python_module: legacy_pipeline.repository
PY
cat > legacy_pipeline/legacy_pipeline/__init__.py <<'PY'
PY
cat > legacy_pipeline/legacy_pipeline/assets.py <<'PY'
from dagster import AutoMaterializePolicy, SourceAsset, asset

raw_events = SourceAsset(key="raw_events", description="Landed by an external loader")


@asset(non_argument_deps={"raw_events"}, auto_materialize_policy=AutoMaterializePolicy.eager())
def cleaned_events():
    return [{"id": 1}, {"id": 2}]


@asset(auto_materialize_policy=AutoMaterializePolicy.eager())
def event_counts(cleaned_events):
    return {"count": len(cleaned_events)}
PY
cat > legacy_pipeline/legacy_pipeline/repository.py <<'PY'
from dagster import ScheduleDefinition, define_asset_job, repository

from legacy_pipeline.assets import cleaned_events, event_counts, raw_events

all_job = define_asset_job("all_job", selection="*")
daily = ScheduleDefinition(job=all_job, cron_schedule="0 2 * * *")


@repository
def legacy_repo():
    return [raw_events, cleaned_events, event_counts, all_job, daily]
PY
cd legacy_pipeline && uv venv -q && uv pip install -q -e . >/dev/null 2>&1 || true
echo "fixture ready: $(pwd)"
