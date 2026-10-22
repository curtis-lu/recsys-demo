"""Compare two evaluation output trees for a "nothing changed" claim.

Two runs of the same code differ only in run-time noise; anything else is a
real difference. The noise, and only it, is masked:

* the report's ``Generated At`` cell;
* plotly's per-figure div ids (a fresh uuid4 each run), pinned to their order
  of appearance, so a figure that moved still shows as a difference.

Everything else is compared as text, byte for byte — JSON key order included,
since the landed files are what a reader diffs. ``manifest.json`` records run
times and is skipped.

Run it on two runs of the unchanged code first (the noise floor, ADR-0031's
tickets and ``docs/agents/pipeline-refactor-process.md`` rule 9); only when
that comes back all ``SAME`` does the same comparison against the changed code
mean anything. Each run directory holds a ``SOURCE`` file
(``eval_parity.sh``): check the changed run's digest is the code you are
claiming about.

Usage: python scripts/local/compare_eval_runs.py <run_a> <run_b>
Exit code 0 when every file is the same, 1 otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

#: Files that differ between any two runs by construction.
SKIPPED = frozenset({"manifest.json", "SOURCE"})

_GENERATED_AT = re.compile(r"(<tr><th>Generated At</th><td>)[^<]*(</td></tr>)")
_UUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def normalise_html(text: str) -> str:
    """``text`` with the run-time noise masked (see the module docstring)."""
    text = _GENERATED_AT.sub(r"\1<masked>\2", text)
    pinned: dict[str, str] = {}
    return _UUID.sub(
        lambda m: pinned.setdefault(m.group(0), f"uuid-{len(pinned)}"), text)


def compare_dirs(a: Path, b: Path) -> list[tuple[str, str]]:
    """``(status, relative path)`` for every file under either tree:
    ``SAME``, ``DIFF`` or ``MISSING`` (on one side only)."""
    files = sorted(
        {p.relative_to(a).as_posix() for p in a.rglob("*") if p.is_file()}
        | {p.relative_to(b).as_posix() for p in b.rglob("*") if p.is_file()})
    out = []
    for rel in files:
        if Path(rel).name in SKIPPED:
            continue
        pa, pb = a / rel, b / rel
        if not (pa.is_file() and pb.is_file()):
            out.append(("MISSING", rel))
            continue
        ta, tb = pa.read_text(), pb.read_text()
        if rel.endswith(".html"):
            ta, tb = normalise_html(ta), normalise_html(tb)
        out.append(("SAME" if ta == tb else "DIFF", rel))
    return out


def _first_difference(a: Path, b: Path) -> str:
    la = normalise_html(a.read_text()).splitlines()
    lb = normalise_html(b.read_text()).splitlines()
    i = next((i for i, (x, y) in enumerate(zip(la, lb)) if x != y),
             min(len(la), len(lb)))
    left = la[i][:200] if i < len(la) else "<end of file>"
    right = lb[i][:200] if i < len(lb) else "<end of file>"
    return f"    line {i + 1}\n    a: {left}\n    b: {right}"


def main(argv: list[str]) -> int:
    a, b = Path(argv[0]), Path(argv[1])
    for run in (a, b):
        source = run / "SOURCE"
        if source.is_file():
            print(f"{run.name}: {source.read_text().splitlines()[0]}")
    results = compare_dirs(a, b)
    for status, rel in results:
        print(f"{status:8}{rel}")
        if status == "DIFF":
            print(_first_difference(a / rel, b / rel))
    return 0 if all(status == "SAME" for status, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
