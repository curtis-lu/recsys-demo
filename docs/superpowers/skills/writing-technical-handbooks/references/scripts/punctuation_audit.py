#!/usr/bin/env python3
"""Prove a unified diff changed ONLY punctuation, not words.

Strips the language's punctuation set from removed (-) and added (+) content
lines; if the resulting ordered lists are identical, the edit is punctuation-
only. Also reports insertion/deletion symmetry. The punctuation set is read
from punctuation_set.txt (single shared source, also quoted in
language-conventions.md).

Usage:
    git diff | python3 punctuation_audit.py
    python3 punctuation_audit.py --file changes.diff
    python3 punctuation_audit.py --self-test
Exit: 0 = punctuation-only, 1 = words changed, 2 = usage error.
"""
from __future__ import annotations
import sys
from pathlib import Path

_DEFAULT_PUNCT_FILE = Path(__file__).parent / "punctuation_set.txt"


def load_punctuation(path: Path = _DEFAULT_PUNCT_FILE) -> set[str]:
    return set(path.read_text(encoding="utf-8").rstrip("\n"))


def _strip(line: str, punct: set[str]) -> str:
    return "".join(c for c in line if c not in punct)


def audit(diff_text: str, punct: set[str]) -> dict:
    removed: list[str] = []
    added: list[str] = []
    for line in diff_text.splitlines():
        if line.startswith(("+++", "---", "@@", "diff ", "index ",
                            "new file", "deleted file", "rename ", "similarity ")):
            continue
        if line.startswith("-"):
            removed.append(line[1:])
        elif line.startswith("+"):
            added.append(line[1:])
    rs = [_strip(l, punct) for l in removed]
    as_ = [_strip(l, punct) for l in added]
    return {"punctuation_only": rs == as_, "symmetric": len(removed) == len(added),
            "removed": removed, "added": added}


def _self_test() -> None:
    punct = load_punctuation()
    r = audit("-甲怎麼跑——再調快。\n+甲怎麼跑，再調快。\n", punct)
    assert r["punctuation_only"] and r["symmetric"], r
    r2 = audit("-甲怎麼跑——再調快。\n+甲如何跑，再調快。\n", punct)
    assert not r2["punctuation_only"], r2
    print("punctuation_audit self-test: OK")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_test()
        return 0
    punct = load_punctuation()
    if "--file" in argv:
        diff_text = Path(argv[argv.index("--file") + 1]).read_text(encoding="utf-8")
    else:
        diff_text = sys.stdin.read()
    r = audit(diff_text, punct)
    if r["punctuation_only"]:
        print(f"OK: punctuation-only edit (-{len(r['removed'])}/+{len(r['added'])} "
              f"lines, symmetric={r['symmetric']}).")
        return 0
    print("FAIL: non-punctuation change detected. First differing pair:")
    for a, b in zip([_strip(l, punct) for l in r["removed"]],
                    [_strip(l, punct) for l in r["added"]]):
        if a != b:
            print(f"  - {a!r}\n  + {b!r}")
            break
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
