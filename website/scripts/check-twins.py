#!/usr/bin/env python3
"""Every Learn and Reference page has a docs/ twin at the same path, and the other way round.

    python3 website/scripts/check-twins.py

website/src/pages/learn/<slug>.mdx       <->  docs/learn/<slug>.md
website/src/pages/reference/<path>.mdx   <->  docs/reference/<path>.md
(an index.mdx is <dir>.md's twin: reference/tables/index.mdx <-> docs/reference/tables.md)
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
PAGES = ROOT / "website" / "src" / "pages"
DOCS = ROOT / "docs"


def site_pages() -> set[str]:
    out = set()
    for section in ("learn", "reference"):
        for page in (PAGES / section).rglob("*.mdx"):
            rel = page.relative_to(PAGES).with_suffix("")
            if rel.name == "index":
                rel = rel.parent
            out.add(rel.as_posix())
    return out


def doc_pages() -> set[str]:
    out = set()
    for section in ("learn", "reference"):
        for page in (DOCS / section).rglob("*.md"):
            out.add(page.relative_to(DOCS).with_suffix("").as_posix())
    return out


def main() -> int:
    site, docs = site_pages(), doc_pages()
    problems = [f"website only (no docs/{p}.md): {p}" for p in sorted(site - docs)]
    problems += [f"docs only (no website page): docs/{p}.md" for p in sorted(docs - site)]
    for line in problems:
        print(line)
    print(f"{len(site & docs)} page(s) in both" + (f", {len(problems)} problem(s)" if problems else ""))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
