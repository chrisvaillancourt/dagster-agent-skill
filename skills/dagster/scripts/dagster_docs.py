#!/usr/bin/env python3
"""Fetch a Dagster documentation page as clean Markdown, with embedded code snippets inlined.

The skill's reference files summarize the docs; this script gives on-demand access to any
page so the skill never goes stale. Sources: the dagster-io/dagster GitHub repo (guide pages
and their code snippets) and https://docs.dagster.io/llms-full.txt (API reference pages, which
are generated at build time and do not exist as Markdown in the repo).

Usage:
  dagster_docs.py guides/build/assets/defining-assets      # guide page by docs path
  dagster_docs.py https://docs.dagster.io/guides/automate/schedules
  dagster_docs.py --search partition mapping                # search the docs index (AND match)
  dagster_docs.py --api "asset checks"                      # API reference page by title
  dagster_docs.py --api dagster-dbt                         # library API reference
  dagster_docs.py --list-api                                # list API page titles

Stdlib only (Python 3.9+). Network access required. Responses are cached for 24h under
$XDG_CACHE_HOME/dagster-docs-skill (default ~/.cache/dagster-docs-skill); pass --no-cache to bypass.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import textwrap
import time
import urllib.error
import urllib.request
from pathlib import Path

RAW = "https://raw.githubusercontent.com/dagster-io/dagster/{ref}/"
DOCS_ROOT = "docs/docs/"
SNIPPETS_ROOT = "examples/docs_snippets/docs_snippets/"
LLMS_TXT = "https://docs.dagster.io/llms.txt"
LLMS_FULL = "https://docs.dagster.io/llms-full.txt"
# Docs pages rarely change within a day; 24h keeps repeated lookups in one session instant.
CACHE_TTL_SECONDS = 24 * 3600
# Generous timeout: llms-full.txt is ~2.5 MB and GitHub raw can be slow on cold fetches.
HTTP_TIMEOUT_SECONDS = 60

# Self-closing MDX tag whose attribute values may contain '>' (e.g. title="src/<project_name>/defs/x.py").
TAG_RE = r"<{name}\b((?:\s+[\w-]+(?:=(?:\"[^\"]*\"|'[^']*'|\{{[^}}]*\}}|[^\s>]+))?)*)\s*/>"

LANG_BY_EXT = {".py": "python", ".yaml": "yaml", ".yml": "yaml", ".toml": "toml", ".sh": "shell",
               ".txt": "shell", ".json": "json", ".sql": "sql", ".md": "markdown", ".js": "javascript",
               ".ts": "typescript", ".scala": "scala", ".dockerfile": "dockerfile"}


def cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    d = Path(base) / "dagster-docs-skill"
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch(url: str, use_cache: bool = True) -> str | None:
    """Return the body of `url`, or None on 404. Raises on other network errors."""
    key = cache_dir() / (hashlib.sha1(url.encode()).hexdigest() + ".txt")
    if use_cache and key.exists() and time.time() - key.stat().st_mtime < CACHE_TTL_SECONDS:
        return key.read_text(encoding="utf-8")
    req = urllib.request.Request(url, headers={"User-Agent": "dagster-docs-skill/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    if use_cache:
        key.write_text(body, encoding="utf-8")
    return body


def normalize_path(arg: str) -> str:
    p = arg.strip()
    p = re.sub(r"^https?://docs\.dagster\.io/", "", p)
    p = p.split("#")[0].split("?")[0].strip("/")
    p = re.sub(r"\.mdx?$", "", p)
    return p


def slice_markers(text: str, start: str | None, end: str | None) -> str:
    lines = text.splitlines()
    if start:
        idx = next((i for i, l in enumerate(lines) if start in l), None)
        lines = lines[idx + 1:] if idx is not None else lines
    if end:
        idx = next((i for i, l in enumerate(lines) if end in l), None)
        lines = lines[:idx] if idx is not None else lines
    # Drop other region markers so nested examples read cleanly.
    lines = [l for l in lines if not re.match(r"^\s*#\s*(start|end)_\w+\s*$", l)]
    return textwrap.dedent("\n".join(lines)).strip("\n")


def attr(tag: str, name: str) -> str | None:
    m = re.search(name + r'\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|\{`([^`]*)`\})', tag)
    if not m:
        return None
    return next(g for g in m.groups() if g is not None)


def render_mdx(md: str, ref: str, use_cache: bool, inline_snippets: bool) -> str:
    # Drop frontmatter but keep the title as an H1 for orientation.
    title = None
    fm = re.match(r"^---\n(.*?)\n---\n", md, flags=re.S)
    if fm:
        tm = re.search(r"^title:\s*['\"]?(.*?)['\"]?\s*$", fm.group(1), flags=re.M)
        title = tm.group(1) if tm else None
        md = md[fm.end():]

    # Partials: import Foo from '@site/docs/partials/\_Foo.md'; ... <Foo />
    partials: dict[str, str] = {}
    for m in re.finditer(r"^import\s+(\w+)\s+from\s+'@site/docs/partials/\\?_?([\w-]+)\.md';?\s*$", md, flags=re.M):
        partials[m.group(1)] = m.group(2)
    md = re.sub(r"^import\s+.*?from\s+'@(site|theme)/.*?';?\s*$", "", md, flags=re.M)

    def partial_repl(m: re.Match) -> str:
        name = m.group(1)
        if name not in partials:
            return ""
        body = fetch(RAW.format(ref=ref) + DOCS_ROOT + "partials/_" + partials[name] + ".md", use_cache)
        return render_mdx(body, ref, use_cache, inline_snippets) if body else ""

    md = re.sub(r"<(\w+)\s*/>", lambda m: partial_repl(m) if m.group(1) in partials else m.group(0), md)

    def snippet_repl(m: re.Match) -> str:
        tag = m.group(0)
        path = attr(tag, "path")
        contents = attr(tag, "contents")
        lang = attr(tag, "language")
        ttl = attr(tag, "title")
        if contents is not None:
            return f"```shell\n{contents}\n```"
        if not path or not inline_snippets:
            return f"*(code example: {path})*"
        rel = path.replace("docs_snippets/docs_snippets/", "", 1)
        body = fetch(RAW.format(ref=ref) + SNIPPETS_ROOT + rel, use_cache)
        if body is None:
            return f"*(code example not found: {rel})*"
        body = slice_markers(body, attr(tag, "startAfter"), attr(tag, "endBefore"))
        lang = lang or LANG_BY_EXT.get(Path(rel).suffix, "")
        header = f"# {ttl}\n" if ttl and lang == "python" else ""
        return f"```{lang}\n{header}{body}\n```"

    md = re.sub(TAG_RE.format(name="(?:CodeExample|CliInvocationExample)"), snippet_repl, md, flags=re.S)

    # <PyObject object="asset" module="dagster" decorator /> -> `asset`
    def pyobj(m: re.Match) -> str:
        obj = attr(m.group(0), "object") or "?"
        mod = attr(m.group(0), "module")
        disp = attr(m.group(0), "displayText")
        if disp:
            return f"`{disp}`"
        return f"`{mod}.{obj}`" if mod and mod != "dagster" else f"`{obj}`"

    md = re.sub(TAG_RE.format(name="PyObject"), pyobj, md, flags=re.S)

    # Tabs -> labelled sections; admonitions -> blockquotes; strip leftover JSX.
    md = re.sub(r"<TabItem\b[^>]*?label=\"([^\"]*)\"[^>]*>", r"\n**\1**\n", md)
    md = re.sub(r"</?(Tabs|TabItem|div|details|summary|CardGroup|Card|DocCardList|Preview|Beta)\b[^>]*>", "", md)
    md = re.sub(r"^:::(\w+)\s*(.*)$", lambda m: f"> **{m.group(1).capitalize()}**{(' — ' + m.group(2)) if m.group(2) else ''}", md, flags=re.M)
    md = re.sub(r"^:::\s*$", "", md, flags=re.M)
    md = re.sub(r"\s*\\\{#[\w-]+\}", "", md)  # heading anchors like \{#single-asset}
    md = re.sub(r"\n{3,}", "\n\n", md).strip() + "\n"
    return (f"# {title}\n\n" if title else "") + md


GITHUB_CONTENTS = "https://api.github.com/repos/dagster-io/dagster/contents/"


def resolve_prefixed_file(path: str, ref: str, use_cache: bool) -> str | None:
    """Some source files carry an ordering prefix the site strips (docs/dagster-basics-tutorial/1-assets.md
    is served at /dagster-basics-tutorial/assets). Look the directory up and match on the stripped name."""
    import json
    parent, _, slug = path.rpartition("/")
    listing = fetch(GITHUB_CONTENTS + DOCS_ROOT + parent + f"?ref={ref}", use_cache)
    if not listing:
        return None
    try:
        entries = json.loads(listing)
    except ValueError:
        return None
    for e in entries if isinstance(entries, list) else []:
        name = e.get("name", "")
        stem = re.sub(r"\.mdx?$", "", name)
        if re.sub(r"^\d+-", "", stem) == slug:
            return f"{parent}/{name}" if parent else name
    return None


def get_page(path: str, ref: str, use_cache: bool, raw: bool, inline_snippets: bool) -> str | None:
    base = RAW.format(ref=ref) + DOCS_ROOT
    candidates = [path + ".md", path + "/index.md", path + ".mdx"]
    for candidate in candidates:
        body = fetch(base + candidate, use_cache)
        if body is not None:
            break
    else:
        real = resolve_prefixed_file(path, ref, use_cache)
        body = fetch(base + real, use_cache) if real else None
        if body is None:
            return None
    src = f"<!-- source: https://docs.dagster.io/{path} -->\n"
    return src + (body if raw else render_mdx(body, ref, use_cache, inline_snippets))


def api_pages(use_cache: bool) -> list[tuple[str, str]]:
    full = fetch(LLMS_FULL, use_cache) or ""
    pages: list[tuple[str, str]] = []
    for block in re.split(r"(?m)^---\n(?=title:)", full):
        m = re.match(r"title:\s*['\"]?(.*?)['\"]?\s*\n", block)
        if m:
            pages.append((m.group(1).strip(), block))
    return pages


def clean_api(block: str) -> str:
    """Strip the HTML/MDX scaffolding of an llms-full.txt API page into readable Markdown."""
    import html as _html
    # Drop the frontmatter; the page body already carries its own H1.
    block = re.sub(r"^title:.*?\n---\n", "", block, count=1, flags=re.S)
    block = re.sub(r"<a\b[^>]*>\[source\]</a>", "", block)
    block = re.sub(r"<Link\b[^>]*>(.*?)</Link>", r"### \1", block, flags=re.S)
    block = re.sub(r"<dt>\s*### ", "### ", block)
    block = re.sub(r"</?(dl|dt|dd|div|p|span|Link)\b[^>]*>", "", block)
    block = re.sub(r"<(strong|b)>(.*?)</\1>", r"**\2**", block, flags=re.S)
    block = re.sub(r"<(em|i)>(.*?)</\1>", r"\2", block, flags=re.S)
    block = re.sub(r"<[^>\n]{1,120}>", "", block)  # any remaining inline tags
    block = _html.unescape(block)
    block = re.sub(r"\]\((\w[\w-]*)\.mdx#", r"](https://docs.dagster.io/api/dagster/\1#", block)
    block = re.sub(r"[ \t]+\n", "\n", block)
    block = re.sub(r"\n{3,}", "\n\n", block)
    return block.strip() + "\n"


def search_index(terms: list[str], use_cache: bool) -> list[str]:
    idx = fetch(LLMS_TXT, use_cache) or ""
    rows = []
    for line in idx.splitlines():
        m = re.match(r"- \[(.*?)\]\((.*?)\)(?::\s*(.*))?", line)
        if m:
            rows.append((m.group(2), m.group(1), (m.group(3) or "").strip()))
    lowered = [t.lower() for t in terms]
    def fmt(r): return f"{r[0]}  —  {r[1]}: {r[2]}"
    hits = [fmt(r) for r in rows if all(t in " ".join(r).lower() for t in lowered)]
    if hits or len(lowered) == 1:
        return hits
    partial = [fmt(r) for r in rows if any(t in " ".join(r).lower() for t in lowered)]
    return ([f"(no page matches all of {terms}; showing pages matching any term)"] + partial) if partial else []


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", nargs="?", help="docs path (e.g. guides/build/assets/defining-assets) or full URL")
    ap.add_argument("--search", nargs="+", metavar="TERM", help="search the docs index; all terms must match")
    ap.add_argument("--api", metavar="TITLE", help="print an API reference page by (partial) title")
    ap.add_argument("--list-api", action="store_true", help="list API reference page titles")
    ap.add_argument("--raw", action="store_true", help="print the page source without MDX rendering")
    ap.add_argument("--no-snippets", action="store_true", help="do not inline referenced code snippets")
    ap.add_argument("--no-cache", action="store_true", help="bypass the 24h on-disk cache")
    ap.add_argument("--ref", default="master", help="git ref of dagster-io/dagster to read from (default: master)")
    args = ap.parse_args(argv)
    use_cache = not args.no_cache

    try:
        if args.search:
            hits = search_index(args.search, use_cache)
            if not hits:
                print(f"No index entries match {args.search!r}. Try fewer or broader terms.", file=sys.stderr)
                return 1
            print("\n".join(hits))
            return 0
        if args.list_api:
            print("\n".join(t for t, _ in api_pages(use_cache)))
            return 0
        if args.api:
            pages = api_pages(use_cache)
            q = args.api.lower()
            exact = [b for t, b in pages if t.lower() == q]
            partial = [b for t, b in pages if q in t.lower()]
            hit = exact or partial
            if not hit:
                print(f"No API page titled like {args.api!r}. Run --list-api to see titles.", file=sys.stderr)
                return 1
            if len(hit) > 1 and not exact:
                print(f"Multiple API pages match {args.api!r}; printing the first. Matches: "
                      + ", ".join(t for t, _ in pages if q in t.lower()), file=sys.stderr)
            print(clean_api(hit[0]))
            return 0
        if not args.path:
            ap.print_help()
            return 2
        path = normalize_path(args.path)
        page = get_page(path, args.ref, use_cache, args.raw, not args.no_snippets)
        if page is None:
            # API pages are not in the repo; try llms-full by the last path segment.
            tail = path.rsplit("/", 1)[-1].replace("-", " ")
            pages = api_pages(use_cache)
            m = [b for t, b in pages if t.lower() == tail.lower()]
            if m:
                print(clean_api(m[0]))
                return 0
            print(f"Page not found: {path}\nTry: {sys.argv[0]} --search {tail}", file=sys.stderr)
            return 1
        print(page)
        return 0
    except urllib.error.URLError as e:
        print(f"Network error fetching docs: {e}. Check connectivity or retry with --no-cache.", file=sys.stderr)
        return 3


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except BrokenPipeError:  # e.g. piped into `head`
        sys.exit(0)
