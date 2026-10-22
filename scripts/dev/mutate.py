"""Mutation check: break one line, run the tests, put the original text back.

A new test proves something only if breaking the line it guards turns it red
(``test-false-green``; ``docs/agents/pipeline-refactor-process.md`` rule 8).
This does the three steps with the two traps of doing them by hand closed:

* The original is restored from the text read before the mutation, not with
  ``git checkout``, which would also wipe uncommitted work in the same file.
* The child runs with ``PYTHONDONTWRITEBYTECODE=1``: a same-length edit written
  and restored within one second can leave Python on the wrong ``.pyc``.

Read where it went red, not only that it did: the target assertion should be
the first failure (``failure_lines``), not a fixture or an import.

Usage:
  PYTHONPATH=<wt>/src .venv/bin/python scripts/dev/mutate.py \\
      <file> <old text> <new text> <pytest node> [<pytest node> ...]
``<old text>`` must occur exactly once. Exit code 0 when the tests went red
(the mutation was caught), 1 when they stayed green.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple


class MutationResult(NamedTuple):
    caught: bool
    #: The first ``E`` lines pytest printed: where it went red.
    failure_lines: list[str]


def mutate(path: Path, old: str, new: str, nodes: list[str], *,
           cwd: Path | None = None) -> MutationResult:
    """Replace the one occurrence of ``old`` in ``path`` with ``new``, run
    pytest on ``nodes``, and restore ``path`` exactly as it was."""
    path = Path(path)
    original = path.read_text()
    count = original.count(old)
    if count != 1:
        raise ValueError(f"{old!r} occurs {count} times in {path}; "
                         "make it unique so the mutation is unambiguous")
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    try:
        path.write_text(original.replace(old, new))
        run = subprocess.run(
            [sys.executable, "-m", "pytest", *nodes, "-q", "-x",
             "-p", "no:cacheprovider", "--no-header"],
            capture_output=True, text=True, cwd=cwd, env=env)
    finally:
        path.write_text(original)
    lines = [line for line in (run.stdout + run.stderr).splitlines()
             if line.startswith("E ")]
    return MutationResult(caught=run.returncode != 0, failure_lines=lines[:4])


def main(argv: list[str]) -> int:
    path, old, new, *nodes = argv
    result = mutate(Path(path), old, new, nodes)
    print("RED (caught)" if result.caught else "GREEN (missed)")
    for line in result.failure_lines:
        print("   ", line[:220])
    return 0 if result.caught else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
