#!/usr/bin/env python3
"""Scan a Dagster project for superseded/deprecated APIs and legacy project layout.

Prints each hit with file:line, the modern replacement, and the docs page to read, then a
summary. Informational by default (exit 0); `--strict` exits 1 when anything is found so it
can gate CI. Stdlib only.

Usage:
  find_legacy_patterns.py [PATH ...] [--strict] [--json]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".tox", ".mypy_cache", ".ruff_cache",
             "dist", "build", ".dagster", "target", "dbt_packages"}
CODE_SUFFIXES = {".py", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".sh", ".md", "Makefile", "Dockerfile"}

# (regex, legacy label, replacement, docs path). Order roughly by how often each appears.
PATTERNS: list[tuple[str, str, str, str]] = [
    (r"@(dg\.|dagster\.)?repository\b", "@repository",
     "dg.Definitions / @dg.definitions with load_from_defs_folder", "guides/build/projects/moving-to-components/migrating-definitions"),
    (r"\bSourceAsset\(", "SourceAsset",
     "dg.AssetSpec (external asset) or a plain upstream @asset", "guides/build/assets/external-assets"),
    (r"\bnon_argument_deps\s*=", "non_argument_deps=",
     "deps=[...]", "guides/build/assets/defining-assets-with-asset-dependencies"),
    (r"\bAutoMaterializePolicy\b|\bAutoMaterializeRule\b", "AutoMaterializePolicy / AutoMaterializeRule",
     "dg.AutomationCondition (eager / on_cron / on_missing)", "guides/automate/declarative-automation"),
    (r"\bbuild_asset_reconciliation_sensor\b", "build_asset_reconciliation_sensor",
     "dg.AutomationConditionSensorDefinition (or the default automation condition sensor)", "guides/automate/declarative-automation/automation-condition-sensors"),
    (r"@(dg\.|dagster\.)?freshness_policy_sensor\b", "@freshness_policy_sensor",
     "freshness checks: dg.build_last_update_freshness_checks + build_sensor_for_freshness_checks", "guides/test/data-freshness-testing"),
    (r"\b\w+_step_launcher\b", "*_step_launcher",
     "Dagster Pipes clients (PipesDatabricksClient, PipesEMRClient, ...)", "integrations/external-pipelines/migrating-from-step-launchers-to-pipes"),
    (r"@(dg\.|dagster\.)?io_manager\b", "@io_manager (function style)",
     "class MyIOManager(dg.ConfigurableIOManager)", "guides/build/io-managers/defining-a-custom-io-manager"),
    (r"@(dg\.|dagster\.)?resource\b", "@resource (function style)",
     "class MyResource(dg.ConfigurableResource)", "guides/build/external-resources/defining-resources"),
    (r"\bconfig_schema\s*=", "config_schema=",
     "typed dg.Config / dg.ConfigurableResource classes", "guides/operate/configuration/run-configuration"),
    (r"context\.op_config\b", "context.op_config",
     "a typed `config: MyConfig` parameter", "guides/operate/configuration/run-configuration"),
    (r"\bload_assets_from_dbt_(project|manifest)\b", "load_assets_from_dbt_project/manifest",
     "@dbt_assets(manifest=...) or DbtProjectComponent", "integrations/libraries/dbt"),
    (r"\bdagster_embedded_elt\b|dagster-embedded-elt", "dagster-embedded-elt",
     "dagster-sling / dagster-dlt", "integrations/libraries/sling"),
    (r"\b(load_assets_from_airbyte_instance|build_airbyte_assets)\b", "OSS Airbyte asset loaders",
     "AirbyteCloudWorkspace + load_airbyte_cloud_asset_specs / Airbyte component", "integrations/libraries/airbyte"),
    (r"@(dg\.|dagster\.)?(solid|pipeline|composite_solid|lambda_solid)\b|\b(ModeDefinition|PresetDefinition)\b",
     "@solid / @pipeline / ModeDefinition / PresetDefinition",
     "@dg.op / @dg.graph / @dg.job with dg.Config", "guides/build/ops"),
    (r"\bAssetGroup\b", "AssetGroup", "group_name= on assets", "guides/build/assets/metadata-and-tags"),
    (r"\bdagit\b", "dagit", "dagster-webserver (or `dg dev` locally)", "guides/operate/webserver"),
    (r"(^|[\s`\"'(])dagster\s+(dev|asset|job|schedule|sensor|instance)\b", "`dagster` CLI",
     "`dg` CLI (dg dev / dg launch / dg list defs / dg check defs)", "api/clis/dg-cli/dg-cli-reference"),
    (r"(^|[\s`\"'(])dagster-cloud\s+\w+", "dagster-cloud CLI",
     "dg plus ... / dg api ...", "api/clis/dg-cli/dg-plus"),
    (r"\bDagsterDbtTranslator\b[\s\S]{0,400}?def get_(asset_key|group_name|metadata|description|tags|owners|automation_condition)\b",
     "DagsterDbtTranslator per-attribute overrides",
     "override get_asset_spec() (or use DbtProjectComponent translation)", "integrations/libraries/dbt/reference"),
    (r"\bOpExecutionContext\b", "OpExecutionContext in asset functions (check context)",
     "dg.AssetExecutionContext for assets (OpExecutionContext is still correct for ops)", "guides/build/assets/defining-assets"),
    (r"\bLegacyFreshnessPolicy\b|from dagster\.deprecated import", "LegacyFreshnessPolicy / dagster.deprecated imports",
     "the current top-level dg.FreshnessPolicy (asset freshness policies) or freshness checks", "guides/observe/asset-freshness-policies"),
    (r"\bmulti_asset_sensor\b", "@multi_asset_sensor (superseded)",
     "dg.AutomationCondition on the downstream asset (any_deps_updated / eager) or an asset job with an automation condition",
     "guides/automate/declarative-automation"),
]
FILE_MARKERS: list[tuple[str, str, str, str]] = [
    ("workspace.yaml", "workspace.yaml", "dg.toml / pyproject [tool.dg]; `dg dev` discovers the project",
     "guides/build/projects/workspaces/migrating-workspace-yaml"),
    ("repository.py", "repository.py module", "src/<pkg>/definitions.py + defs/ folder", "guides/build/projects/project-structure"),
]


def iter_files(paths: list[str]):
    for p in paths:
        root = Path(p)
        if root.is_file():
            yield root
            continue
        for f in root.rglob("*"):
            if any(part in SKIP_DIRS for part in f.parts):
                continue
            if f.is_file() and (f.suffix in CODE_SUFFIXES or f.name in CODE_SUFFIXES):
                yield f


def scan(paths: list[str]) -> list[dict]:
    hits: list[dict] = []
    compiled = [(re.compile(rx, re.M), *rest) for rx, *rest in PATTERNS]
    for f in iter_files(paths):
        for marker, label, repl, doc in FILE_MARKERS:
            if f.name == marker:
                hits.append({"file": str(f), "line": 0, "legacy": label, "replacement": repl, "docs": doc, "text": ""})
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for rx, label, repl, doc in compiled:
            for m in rx.finditer(text):
                line_no = text.count("\n", 0, m.start()) + 1
                line = text.splitlines()[line_no - 1].strip() if text else ""
                # Skip comments/docstrings that merely mention the legacy name in .py files.
                if f.suffix == ".py" and line.lstrip().startswith("#"):
                    continue
                hits.append({"file": str(f), "line": line_no, "legacy": label, "replacement": repl, "docs": doc,
                             "text": line[:160]})
    return hits


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="*", default=["."])
    ap.add_argument("--strict", action="store_true", help="exit 1 if any legacy pattern is found")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args(argv)

    hits = scan(args.paths)
    if args.json:
        print(json.dumps(hits, indent=2))
    elif not hits:
        print("No legacy Dagster patterns found.")
    else:
        by_label: dict[str, list[dict]] = {}
        for h in hits:
            by_label.setdefault(h["legacy"], []).append(h)
        for label, group in by_label.items():
            print(f"\n== {label}  ({len(group)} hit{'s' if len(group) != 1 else ''})")
            print(f"   replace with: {group[0]['replacement']}")
            print(f"   docs: https://docs.dagster.io/{group[0]['docs']}")
            for h in group[:20]:
                loc = f"{h['file']}:{h['line']}" if h["line"] else h["file"]
                print(f"   - {loc}  {h['text']}")
            if len(group) > 20:
                print(f"   ... {len(group) - 20} more")
        print(f"\n{len(hits)} hit(s) across {len({h['file'] for h in hits})} file(s). "
              "See references/migration-and-legacy.md for the modernization order.")
    return 1 if (args.strict and hits) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
