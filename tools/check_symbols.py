#!/usr/bin/env python3
"""Cross-check every `dg.<Symbol>` and `from dagster_<lib> import <Symbol>` mentioned in the reference
files against the official API corpus (llms-full.txt) and the docs source, to catch invented names.

Usage: uv run python tools/check_symbols.py <llms-full.txt> <docs-dir> [skills/dagster/references]
Prints unknown symbols per file. Exit 1 if any unknown symbol is found.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

llms = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
docs_dir = Path(sys.argv[2])
refs = Path(sys.argv[3] if len(sys.argv) > 3 else "skills/dagster/references")
docs_text = "\n".join(p.read_text(encoding="utf-8", errors="replace") for p in docs_dir.rglob("*.md"))
snippets_dir = docs_dir.parent.parent / "examples" / "docs_snippets" / "docs_snippets"
snip_text = "\n".join(p.read_text(encoding="utf-8", errors="replace") for p in snippets_dir.rglob("*.py")) if snippets_dir.exists() else ""
corpus = llms + "\n" + docs_text + "\n" + snip_text

# API anchors look like id='dagster.AssetKey' / id='dagster_dbt.DbtCliResource'
anchors = set(re.findall(r"id='([\w.]+)'", llms))
core_names = {a.split(".", 1)[1] for a in anchors if a.startswith("dagster.") and a.count(".") == 1}
lib_names: dict[str, set[str]] = {}
for a in anchors:
    mod, _, name = a.partition(".")
    if mod.startswith("dagster_"):
        lib_names.setdefault(mod, set()).add(name.split(".")[0])

# Methods/attributes we should not flag (used as dg.X.method in text).
unknown_total = 0
for f in sorted(refs.glob("*.md")):
    text = f.read_text(encoding="utf-8")
    bad: dict[str, int] = {}
    for m in re.finditer(r"\bdg\.([A-Za-z_]\w*)", text):
        name = m.group(1)
        if name in core_names:
            continue
        # accept if the token appears in the API corpus or the docs' own code snippets in any form
        # (call, attribute access like Enum.MEMBER, annotation, import)
        if re.search(rf"\bdagster\.{re.escape(name)}\b", corpus) or re.search(rf"\b{re.escape(name)}\b", llms + snip_text):
            continue
        bad[f"dg.{name}"] = bad.get(f"dg.{name}", 0) + 1
    for m in re.finditer(r"from (dagster_\w+)(?:\.[\w.]+)? import ([\w, \n()]+)", text):
        mod, names = m.group(1), m.group(2)
        for n in re.findall(r"\w+", names):
            if n in lib_names.get(mod, set()) or re.search(rf"\b{re.escape(n)}\b", corpus):
                continue
            bad[f"{mod}.{n}"] = bad.get(f"{mod}.{n}", 0) + 1
    for m in re.finditer(r"\b(dagster_\w+)\.([A-Z]\w+)", text):
        mod, n = m.group(1), m.group(2)
        if n in lib_names.get(mod, set()) or re.search(rf"\b{re.escape(n)}\b", corpus):
            continue
        bad[f"{mod}.{n}"] = bad.get(f"{mod}.{n}", 0) + 1
    if bad:
        unknown_total += len(bad)
        print(f"\n{f.name}:")
        for k, v in sorted(bad.items()):
            print(f"  ? {k}  (x{v})")
print(f"\n{unknown_total} unknown symbol(s) across {len(list(refs.glob('*.md')))} files")
sys.exit(1 if unknown_total else 0)
