"""Mutation check: break one line, run the tests, put the original text back.

A new test proves something only if breaking the line it guards turns it red
(``test-false-green``; ``docs/agents/pipeline-refactor-process.md`` rule 8).
This does the three steps with the two traps of doing them by hand closed:

* The original is restored from the text read before the mutation, not with
  ``git checkout``, which would also wipe uncommitted work in the same file.
* The child runs with ``PYTHONDONTWRITEBYTECODE=1``: a same-length edit written
  and restored within one second can leave Python on the wrong ``.pyc``.

Two more ways a red can lie are refused. The tests run once unmutated first
and must pass, or a mistyped node (pytest exits non-zero) would read as a
caught mutation. And only failing tests count as caught (pytest exit 1); a
mutation that breaks collection or the run itself is ``error``, red for a
reason that says nothing about the test. SIGTERM and SIGHUP restore the file
too. Read where it went red, not only that it did: the target assertion
should be the first failure (``failure_lines``), not a fixture.

Usage:
  PYTHONPATH=<wt>/src .venv/bin/python scripts/dev/mutate.py \\
      <file> <old text> <new text> <pytest node> [<pytest node> ...]
``<old text>`` must occur exactly once. Exit code 0 when a test failed (the
mutation was caught), 1 otherwise.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple


#: pytest's exit codes: tests ran and all passed / tests ran and some failed.
_PASSED, _FAILED = 0, 1


class MutationResult(NamedTuple):
    #: ``caught`` (a test failed), ``missed`` (all passed) or ``error``
    #: (pytest could not run the tests: collection, usage, internal error).
    status: str
    #: The first ``E`` lines pytest printed: where it went red.
    failure_lines: list[str]

    @property
    def caught(self) -> bool:
        return self.status == "caught"


def _pytest(nodes: list[str], cwd: Path | None) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [sys.executable, "-m", "pytest", *nodes, "-q", "-x",
         "-p", "no:cacheprovider", "--no-header"],
        capture_output=True, text=True, cwd=cwd, env=env)


def _raise_on_termination(signum, frame):
    raise SystemExit(f"interrupted by signal {signum}; file restored")


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
    baseline = _pytest(nodes, cwd)
    if baseline.returncode != _PASSED:
        raise RuntimeError(
            f"the tests do not pass before the mutation (pytest exit "
            f"{baseline.returncode}); a red after it would mean nothing:\n"
            + (baseline.stdout + baseline.stderr)[-800:])
    handlers = {sig: signal.signal(sig, _raise_on_termination)
                for sig in (signal.SIGTERM, signal.SIGHUP)}
    try:
        path.write_text(original.replace(old, new))
        run = _pytest(nodes, cwd)
    finally:
        path.write_text(original)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
    lines = [line for line in (run.stdout + run.stderr).splitlines()
             if line.startswith("E ")]
    status = {_PASSED: "missed", _FAILED: "caught"}.get(run.returncode, "error")
    return MutationResult(status=status, failure_lines=lines[:4])


def main(argv: list[str]) -> int:
    path, old, new, *nodes = argv
    result = mutate(Path(path), old, new, nodes)
    print({"caught": "RED (caught)", "missed": "GREEN (missed)",
           "error": "ERROR (pytest could not run the tests)"}[result.status])
    for line in result.failure_lines:
        print("   ", line[:220])
    return 0 if result.caught else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
