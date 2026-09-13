#!/usr/bin/env python3
"""Validate the skill against the Agent Skills spec and this repo's conventions.

Checks: SKILL.md frontmatter (name/description/compatibility constraints, name == dir name),
SKILL.md body length, every references/*.md is linked from SKILL.md, every relative link in
SKILL.md and references/*.md resolves, reference files have an H1 + '## Contents' TOC + '## Sources',
no MDX residue or absolute local paths, line caps. Exit 1 on any error; warnings do not fail.

Usage: uv run python tools/validate_skill.py [skills/dagster]
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SKILL_MAX_LINES = 500
REF_MAX_LINES = 500
REF_MIN_LINES = 120
RESIDUE = re.compile(r"<PyObject|<CodeExample|<CliInvocationExample|<Tabs|<TabItem|^:::|@site/|/private/tmp|/Users/", re.M)


def parse_frontmatter(text: str) -> dict[str, str]:
    m = re.match(r"^---\n(.*?)\n---\n", text, flags=re.S)
    if not m:
        raise SystemExit("SKILL.md: missing YAML frontmatter")
    fm: dict[str, str] = {}
    key = None
    for line in m.group(1).splitlines():
        if re.match(r"^[A-Za-z_-]+:", line):
            key, _, val = line.partition(":")
            fm[key.strip()] = val.strip()
        elif key and line.startswith((" ", "\t")):
            fm[key] = (fm[key] + " " + line.strip()).strip()
    return fm


def main(root: Path) -> int:
    errors: list[str] = []
    warns: list[str] = []
    skill_md = root / "SKILL.md"
    text = skill_md.read_text(encoding="utf-8")
    fm = parse_frontmatter(text)

    name = fm.get("name", "")
    if not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name) or len(name) > 64:
        errors.append(f"name {name!r} violates spec (lowercase a-z0-9 and single hyphens, <=64 chars)")
    if name != root.name:
        errors.append(f"name {name!r} must equal directory name {root.name!r}")
    if any(w in name for w in ("anthropic", "claude")):
        errors.append("name contains a reserved word")
    desc = fm.get("description", "")
    if not (1 <= len(desc) <= 1024):
        errors.append(f"description length {len(desc)} not in 1..1024")
    if "<" in desc or ">" in desc:
        errors.append("description must not contain XML tags")
    if re.search(r"\b(I|you|your)\b", desc):
        warns.append("description should be third person (found I/you)")
    compat = fm.get("compatibility", "")
    if compat and len(compat) > 500:
        errors.append(f"compatibility length {len(compat)} > 500")

    body_lines = len(text.splitlines())
    if body_lines > SKILL_MAX_LINES:
        errors.append(f"SKILL.md has {body_lines} lines (> {SKILL_MAX_LINES})")

    # Links from SKILL.md and references must resolve; every reference must be linked from SKILL.md.
    refs_dir = root / "references"
    ref_files = sorted(refs_dir.glob("*.md"))
    linked = set(re.findall(r"`?references/([\w-]+\.md)`?", text))
    for rf in ref_files:
        if rf.name not in linked:
            errors.append(f"{rf.name} is not referenced from SKILL.md")
    for m in re.finditer(r"`((?:references|scripts|assets)/[\w./-]+)`|\]\(((?:references|scripts|assets)/[\w./-]+)\)", text):
        rel = m.group(1) or m.group(2)
        if not (root / rel).exists():
            errors.append(f"SKILL.md points at missing path {rel}")

    for rf in ref_files:
        t = rf.read_text(encoding="utf-8")
        n = len(t.splitlines())
        if n > REF_MAX_LINES:
            errors.append(f"{rf.name}: {n} lines (> {REF_MAX_LINES})")
        if n < REF_MIN_LINES:
            warns.append(f"{rf.name}: only {n} lines")
        if not t.lstrip().startswith("# "):
            errors.append(f"{rf.name}: must start with an H1")
        if "## Contents" not in t:
            errors.append(f"{rf.name}: missing '## Contents' table of contents")
        if "## Sources" not in t:
            warns.append(f"{rf.name}: missing '## Sources'")
        if t.lstrip().startswith("---"):
            errors.append(f"{rf.name}: reference files must not have YAML frontmatter")
        for rm in RESIDUE.finditer(t):
            line_no = t.count("\n", 0, rm.start()) + 1
            errors.append(f"{rf.name}:{line_no}: residue {rm.group(0)!r}")
        # sibling links
        for sib in set(re.findall(r"`([\w-]+\.md)`", t)):
            if sib != "SKILL.md" and not (refs_dir / sib).exists() and sib not in {"defs.yaml", "dagster.yaml"}:
                if sib.endswith(".md") and sib not in ("README.md", "CHANGELOG.md", "SKILL.md"):
                    warns.append(f"{rf.name}: mentions `{sib}` which is not a sibling reference")
        # TOC anchors point at real H2s
        h2s = {re.sub(r"[^\w\s-]", "", h).strip().lower().replace(" ", "-") for h in re.findall(r"^## (.+)$", t, flags=re.M)}
        for anchor in re.findall(r"\]\(#([\w-]+)\)", t):
            if anchor not in h2s and not any(a.startswith(anchor) or anchor.startswith(a) for a in h2s):
                warns.append(f"{rf.name}: TOC anchor #{anchor} has no matching H2")

    for s in (root / "scripts").glob("*"):
        if not s.stat().st_mode & 0o111:
            warns.append(f"{s.name} is not executable")

    for w in warns:
        print(f"warn:  {w}")
    for e in errors:
        print(f"ERROR: {e}")
    print(f"\n{len(ref_files)} reference files, SKILL.md {body_lines} lines, description {len(desc)} chars, "
          f"{len(errors)} error(s), {len(warns)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "skills/dagster").resolve()))
