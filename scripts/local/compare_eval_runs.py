"""Compare two evaluation output trees for a "nothing changed" claim.

Two runs of the same code differ only in run-time noise; anything else is a
real difference. The noise, and only it, is masked:

* the report's ``Generated At`` cell;
* the div id of each plotly figure (a fresh uuid4 each run): only the ids
  declared by ``<div id="..." class="plotly-graph-div"``, pinned to their order
  of appearance, so a figure that moved still shows as a difference and a
  uuid-shaped value in a table is still compared;
* ``created_at``, ``run_id`` and ``git_commit`` at the top of
  ``manifest.json`` — the commit differs between the runs being compared by
  design; ``SOURCE`` records which code each one ran.

Everything else is compared as text, byte for byte — JSON key order included,
since the landed files are what a reader diffs.

Run it on two runs of the unchanged code first (the noise floor, ADR-0031's
tickets and ``docs/agents/pipeline-refactor-process.md`` rule 9); only when
that comes back all ``SAME`` does the same comparison against the changed code
mean anything. Each run directory holds a ``SOURCE`` file
(``eval_parity.sh``): check the changed run's digest is the code you are
claiming about.

Usage: python scripts/local/compare_eval_runs.py <run_a> <run_b>
Exit code 0 when every file is the same, 1 otherwise — including when either
directory is missing or empty, so a mistyped path cannot pass.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

#: Written by ``eval_parity.py`` beside the outputs, not by the run.
SKIPPED = frozenset({"SOURCE"})

#: The top-level ``manifest.json`` fields that differ between the runs.
MANIFEST_RUN_FIELDS = ("created_at", "run_id", "git_commit")

_GENERATED_AT = re.compile(r"(<tr><th>Generated At</th><td>)[^<]*(</td></tr>)")
_PLOT_DIV = re.compile(
    r'<div id="([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"'
    r' class="plotly-graph-div"')


def normalise_html(text: str) -> str:
    """``text`` with the run-time noise masked (see the module docstring)."""
    text = _GENERATED_AT.sub(r"\1<masked>\2", text)
    for i, plot_id in enumerate(dict.fromkeys(_PLOT_DIV.findall(text))):
        text = text.replace(plot_id, f"plot-{i}")
    return text


def normalise_manifest(text: str) -> str:
    """``manifest.json`` without the fields every run changes."""
    manifest = json.loads(text)
    for field in MANIFEST_RUN_FIELDS:
        manifest.pop(field, None)
    return json.dumps(manifest, indent=2, ensure_ascii=False)


def _normalised(path: Path) -> str:
    text = path.read_text()
    if path.suffix == ".html":
        return normalise_html(text)
    if path.name == "manifest.json":
        return normalise_manifest(text)
    return text


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
        ta, tb = _normalised(pa), _normalised(pb)
        out.append(("SAME" if ta == tb else "DIFF", rel))
    return out


def _first_difference(a: Path, b: Path) -> str:
    la, lb = _normalised(a).splitlines(), _normalised(b).splitlines()
    i = next((i for i, (x, y) in enumerate(zip(la, lb)) if x != y),
             min(len(la), len(lb)))
    left = la[i][:200] if i < len(la) else "<end of file>"
    right = lb[i][:200] if i < len(lb) else "<end of file>"
    return f"    line {i + 1}\n    a: {left}\n    b: {right}"


def main(argv: list[str]) -> int:
    a, b = Path(argv[0]), Path(argv[1])
    for run in (a, b):
        if not run.is_dir() or not any(run.rglob("*.json")):
            print(f"{run}: not a run directory (missing, or no JSON in it)")
            return 1
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
