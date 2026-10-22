"""Prove a move moved code without changing it: compare every top-level
definition of the old files with the same name in the new files, by AST.

``docs/agents/pipeline-refactor-process.md`` rule 8 asks a pure move for this
(``AST 逐函式比對``): it turns "only moved" from a claim into a check. Each
name gets one verdict: ``SAME``, ``CHANGED``, ``OLD-ONLY``, ``NEW-ONLY`` or
``DUPLICATE`` (defined in two new files). Imports are not compared — moving
code changes them by construction.

The AST has no comments, so a comment edited in a moved function still reads
``SAME``; read the comments in ``git diff``. ``--rename`` maps a name the
move renamed (old=new), applied to the old side's identifiers before
comparing. ``--ignore-docstrings`` drops docstrings, for the move's
"docstrings that had to change".

Usage:
  .venv/bin/python scripts/dev/ast_compare.py --rev <base> \\
      --old src/a.py [...] --new src/b.py src/c.py [...] \\
      [--rename old=new ...] [--ignore-docstrings]
Without ``--rev`` the old files are read from disk. Exit code 0 when every
name is ``SAME``.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
from pathlib import Path

_DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


class _Rename(ast.NodeTransformer):
    def __init__(self, renames: dict[str, str]):
        self.renames = renames

    def visit_Name(self, node: ast.Name) -> ast.Name:
        node.id = self.renames.get(node.id, node.id)
        return node

    def generic_visit(self, node):
        if isinstance(node, _DEFS):
            node.name = self.renames.get(node.name, node.name)
        return super().generic_visit(node)


def _drop_docstrings(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if (isinstance(node, _DEFS) and node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
            node.body = node.body[1:]


def _definitions(text: str, renames: dict[str, str],
                 ignore_docstrings: bool) -> dict[str, str]:
    """``{name: ast dump}`` of the top-level definitions in ``text``."""
    tree = _Rename(renames).visit(ast.parse(text)) if renames \
        else ast.parse(text)
    if ignore_docstrings:
        _drop_docstrings(tree)
    out = {}
    for node in tree.body:
        if isinstance(node, _DEFS):
            names = [node.name]
        elif isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names = [node.target.id]
        else:
            continue
        for name in names:
            out[name] = ast.dump(node, include_attributes=False)
    return out


def _read(path, rev: str | None) -> str:
    if rev is None:
        return Path(path).read_text()
    return subprocess.run(["git", "show", f"{rev}:{path}"], check=True,
                          capture_output=True, text=True).stdout


def compare(old: list, new: list, *, rev: str | None = None,
            renames: dict[str, str] | None = None,
            ignore_docstrings: bool = False) -> list[tuple[str, str]]:
    """``(name, verdict)`` for every top-level definition on either side,
    sorted by name. ``rev`` reads the old files from git at that revision."""
    old_defs: dict[str, str] = {}
    for path in old:
        old_defs.update(_definitions(_read(path, rev), renames or {},
                                     ignore_docstrings))
    new_defs: dict[str, str] = {}
    duplicates = set()
    for path in new:
        for name, dump in _definitions(Path(path).read_text(), {},
                                       ignore_docstrings).items():
            if name in new_defs:
                duplicates.add(name)
            new_defs[name] = dump
    verdicts = []
    for name in sorted(set(old_defs) | set(new_defs)):
        if name in duplicates:
            verdict = "DUPLICATE"
        elif name not in new_defs:
            verdict = "OLD-ONLY"
        elif name not in old_defs:
            verdict = "NEW-ONLY"
        else:
            verdict = "SAME" if old_defs[name] == new_defs[name] else "CHANGED"
        verdicts.append((name, verdict))
    return verdicts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rev")
    parser.add_argument("--old", nargs="+", required=True)
    parser.add_argument("--new", nargs="+", required=True, type=Path)
    parser.add_argument("--rename", action="append", default=[])
    parser.add_argument("--ignore-docstrings", action="store_true")
    args = parser.parse_args(argv)
    renames = dict(pair.split("=", 1) for pair in args.rename)
    verdicts = compare(args.old, args.new, rev=args.rev, renames=renames,
                       ignore_docstrings=args.ignore_docstrings)
    for name, verdict in verdicts:
        print(f"{verdict:10}{name}")
    counts = {v: sum(1 for _, x in verdicts if x == v)
              for v in sorted({x for _, x in verdicts})}
    print(" ".join(f"{v}={n}" for v, n in counts.items()))
    return 0 if all(v == "SAME" for _, v in verdicts) else 1


if __name__ == "__main__":
    raise SystemExit(main())
