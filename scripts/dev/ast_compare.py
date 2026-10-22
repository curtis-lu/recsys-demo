"""Prove a move moved code without changing it: compare every top-level
definition of the old files with the same name in the new files, by AST.

``docs/agents/pipeline-refactor-process.md`` rule 8 asks a pure move for this
(``AST 逐函式比對``): it turns "only moved" from a claim into a check. Each
name gets one verdict: ``SAME``, ``CHANGED``, ``OLD-ONLY``, ``NEW-ONLY`` or
``DUPLICATE`` (defined twice on one side). Every other top-level statement
(a registry call, an ``if`` or ``try`` block, a tuple assignment) is compared
as one group, ``<module statements>``, wherever in the new files it landed.
Imports and module docstrings are not compared — moving code changes them by
construction.

The AST has no comments, so a comment edited in a moved function still reads
``SAME``; read the comments in ``git diff``. ``--rename`` maps a name the
move renamed (old=new), applied to every identifier of that name on the old
side; a name also bound locally there (a parameter, a local variable) is
refused, since renaming it would hide a real difference.
``--ignore-docstrings`` drops docstrings, for the move's "docstrings that had
to change".

Usage:
  .venv/bin/python scripts/dev/ast_compare.py --rev <base> \\
      --old src/a.py [...] --new src/b.py src/c.py [...] \\
      [--rename old=new ...] [--ignore-docstrings]
Without ``--rev`` the old files are read from disk. Exit code 0 when every
name is ``SAME``; 1 otherwise, and when there was nothing to compare.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
from collections import Counter
from pathlib import Path

_DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
#: The verdict key of the top-level statements that define no name.
MODULE_STATEMENTS = "<module statements>"


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


def _locally_bound(tree: ast.AST) -> set[str]:
    """Names bound inside a function: parameters and assignment targets."""
    bound = set()
    for func in ast.walk(tree):
        if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            for node in ast.walk(func):
                if isinstance(node, ast.arg):
                    bound.add(node.arg)
                elif (isinstance(node, ast.Name)
                        and isinstance(node.ctx, ast.Store)):
                    bound.add(node.id)
    return bound


def _parse(text: str, renames: dict[str, str], ignore_docstrings: bool,
           where: str) -> ast.Module:
    tree = ast.parse(text)
    shadowed = sorted(set(renames) & _locally_bound(tree))
    if shadowed:
        raise ValueError(
            f"--rename {shadowed}: also bound locally in {where}; renaming "
            "every identifier of that name would hide a real difference")
    if renames:
        tree = _Rename(renames).visit(tree)
    if ignore_docstrings:
        _drop_docstrings(tree)
    return tree


def _split(tree: ast.Module) -> tuple[list[tuple[str, str]], list[str]]:
    """``([(name, dump)], [dump])``: the top-level definitions, and every
    other top-level statement but imports and the module docstring."""
    defs, others = [], []
    for i, node in enumerate(tree.body):
        dump = ast.dump(node, include_attributes=False)
        if isinstance(node, _DEFS):
            names = [node.name]
        elif (isinstance(node, ast.Assign)
              and all(isinstance(t, ast.Name) for t in node.targets)):
            names = [t.id for t in node.targets]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names = [node.target.id]
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        elif (i == 0 and isinstance(node, ast.Expr)
              and isinstance(node.value, ast.Constant)
              and isinstance(node.value.value, str)):
            continue
        else:
            others.append(dump)
            continue
        defs.extend((name, dump) for name in names)
    return defs, others


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
    duplicates = set()

    def collect(sources, side_renames):
        defs: dict[str, str] = {}
        others: Counter = Counter()
        for path, text in sources:
            names, rest = _split(_parse(text, side_renames, ignore_docstrings,
                                        str(path)))
            for name, dump in names:
                if name in defs:
                    duplicates.add(name)
                defs[name] = dump
            others.update(rest)
        return defs, others

    old_defs, old_others = collect(
        [(p, _read(p, rev)) for p in old], renames or {})
    new_defs, new_others = collect(
        [(p, Path(p).read_text()) for p in new], {})
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
    if old_others or new_others:
        verdicts.append((MODULE_STATEMENTS,
                         "SAME" if old_others == new_others else "CHANGED"))
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
    if not verdicts:
        print("nothing to compare: no top-level definition on either side")
        return 1
    for name, verdict in verdicts:
        print(f"{verdict:10}{name}")
    counts = {v: sum(1 for _, x in verdicts if x == v)
              for v in sorted({x for _, x in verdicts})}
    print(" ".join(f"{v}={n}" for v, n in counts.items()))
    return 0 if all(v == "SAME" for _, v in verdicts) else 1


if __name__ == "__main__":
    raise SystemExit(main())
