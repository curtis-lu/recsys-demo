"""One module's shape without reading it: each top-level definition, its line
span, what it uses in the module, and who uses it inside and outside.

Splitting or moving a module comes down to "which helper does each part
use, and who imports what"; reading a two-thousand-line file end to end to
answer that costs tens of thousands of tokens, this prints it in a screen.
Static only: a name reached through ``getattr`` or a string is not seen.

Usage:
  .venv/bin/python scripts/dev/module_map.py <module.py> [--root <repo>]
``--root`` (default: the current directory) is where importers are searched;
``src/`` under it is stripped when naming the module.
"""

from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import NamedTuple


class Entry(NamedTuple):
    name: str
    first_line: int
    last_line: int
    #: Other top-level names of the module this one reads.
    uses: list[str]
    #: Top-level names of the module that read this one.
    used_by: list[str]
    #: Files (relative to the root) that import it or read it off the module.
    imported_by: list[str]


def _defined_names(node: ast.stmt) -> list[str]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return [node.name]
    if isinstance(node, ast.Assign):
        return [t.id for t in node.targets if isinstance(t, ast.Name)]
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return [node.target.id]
    return []


def _module_name(path: Path, root: Path) -> str:
    rel = path.resolve().relative_to(root.resolve()).with_suffix("")
    parts = rel.parts[1:] if rel.parts[0] == "src" else rel.parts
    return ".".join(parts)


def _importers(dotted: str, root: Path, skip: Path) -> dict[str, set[str]]:
    """``{name: {file}}`` for every file under ``root`` that imports ``name``
    from the module, or reads it off an alias of the module."""
    package, _, leaf = dotted.rpartition(".")
    found: dict[str, set[str]] = {}
    for path in sorted(root.rglob("*.py")):
        if path.resolve() == skip.resolve() or ".venv" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = path.relative_to(root).as_posix()
        aliases = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level == 0:
                for alias in node.names:
                    if node.module == dotted:
                        found.setdefault(alias.name, set()).add(rel)
                    elif node.module == package and alias.name == leaf:
                        aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == dotted and alias.asname:
                        aliases.add(alias.asname)
        for node in ast.walk(tree):
            if (isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id in aliases):
                found.setdefault(node.attr, set()).add(rel)
    return found


def module_map(path: Path, root: Path) -> list[Entry]:
    """Every top-level definition of the module at ``path``, in file order."""
    path, root = Path(path), Path(root)
    tree = ast.parse(path.read_text())
    defs = [(name, node) for node in tree.body for name in _defined_names(node)]
    top = {name for name, _ in defs}
    uses = {
        name: sorted({n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
                     & top - {name})
        for name, node in defs
    }
    importers = _importers(_module_name(path, root), root, path)
    return [
        Entry(
            name=name,
            first_line=node.lineno,
            last_line=node.end_lineno,
            uses=uses[name],
            used_by=sorted(other for other, used in uses.items()
                           if name in used),
            imported_by=sorted(importers.get(name, ())),
        )
        for name, node in defs
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("module", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    for e in module_map(args.module, args.root):
        print(f"{e.name}  L{e.first_line}-{e.last_line}")
        for label, names in (("uses", e.uses), ("used by", e.used_by),
                             ("imported by", e.imported_by)):
            if names:
                print(f"    {label}: {', '.join(names)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
