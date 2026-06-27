#!/usr/bin/env python3
"""Validate intra-handbook markdown anchors and relative file links.

slugify() reproduces GitHub's heading-anchor algorithm closely enough for
handbook use: lowercase; keep alphanumerics (incl. CJK), spaces, '-', '_';
drop all other characters; spaces -> '-'; NO hyphen collapsing. Validated
against real handbook anchors in test_anchor_check.py.

Usage:
    python3 anchor_check.py <dir>        # scan a directory tree of .md files
    python3 anchor_check.py --self-test  # run built-in assertions
Exit: 0 = all links resolve, 1 = broken links, 2 = usage error.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def slugify(text: str) -> str:
    text = text.strip().lower()
    kept = [c for c in text if c.isalnum() or c in " -_"]
    return "".join(kept).replace(" ", "-")


def heading_slugs(md_text: str) -> list[str]:
    slugs: list[str] = []
    seen: dict[str, int] = {}
    in_fence = False
    for line in md_text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = _HEADING_RE.match(line)
        if not m:
            continue
        base = slugify(m.group(2))
        if base in seen:
            seen[base] += 1
            slugs.append(f"{base}-{seen[base]}")
        else:
            seen[base] = 0
            slugs.append(base)
    return slugs


def check_dir(root: Path) -> list[str]:
    root = Path(root)
    md_files = sorted(
        f for f in root.rglob("*.md")
        if not any(part.startswith(".") for part in f.relative_to(root).parts[:-1])
    )
    slug_cache: dict[Path, set[str]] = {
        f.resolve(): set(heading_slugs(f.read_text(encoding="utf-8"))) for f in md_files
    }
    errors: list[str] = []
    for f in md_files:
        in_fence = False
        for line_no, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            # strip inline code spans (`...`): links inside them are not rendered
            for target in _LINK_RE.findall(re.sub(r"`[^`]*`", "", line)):
                if target.startswith(("http://", "https://", "mailto:", "#!")):
                    continue
                file_part, _, anchor = target.partition("#")
                if file_part:
                    tgt = (f.parent / file_part).resolve()
                    if not tgt.exists():
                        errors.append(f"{f}:{line_no}: missing file -> {target}")
                        continue
                else:
                    tgt = f.resolve()
                if anchor:
                    slugs = slug_cache.get(tgt)
                    if slugs is None and tgt.exists():
                        slugs = set(heading_slugs(tgt.read_text(encoding="utf-8")))
                        slug_cache[tgt] = slugs
                    if not slugs or anchor not in slugs:
                        errors.append(f"{f}:{line_no}: missing anchor -> {target}")
    return errors


def _self_test() -> None:
    for text, expected in {
        "全書地圖": "全書地圖",
        "場景速查：依你的工作型態": "場景速查依你的工作型態",
        "5.5 管好檔案大小：小碎檔問題": "55-管好檔案大小小碎檔問題",
        "Spark 怎麼跑你的 SQL": "spark-怎麼跑你的-sql",
        "用 `EXPLAIN` 找瓶頸": "用-explain-找瓶頸",
    }.items():
        got = slugify(text)
        assert got == expected, f"slugify({text!r})={got!r} != {expected!r}"
    assert heading_slugs("# A\n# A\n") == ["a", "a-1"]
    print("anchor_check self-test: OK")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_test()
        return 0
    if len(argv) != 2:
        print(__doc__)
        return 2
    errors = check_dir(Path(argv[1]))
    if errors:
        print(f"FAIL: {len(errors)} broken link(s):")
        for e in errors:
            print("  " + e)
        return 1
    print(f"OK: all relative links and anchors resolve under {argv[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
