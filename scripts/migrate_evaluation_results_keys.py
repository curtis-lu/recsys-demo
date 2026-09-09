"""One-time migration: rename the pre-#327 ``dataset_overview`` keys in place.

#327 stopped spelling the framework's own landed keys in the example
deployment's business vocabulary. Inside ``evaluation_results.json`` ::

    dataset_overview.totals.n_products                -> n_items
    dataset_overview.totals.n_customers               -> n_entities
    dataset_overview.totals.avg_positives_per_customer-> avg_positives_per_entity
    dataset_overview.by_snap_date[*].n_customers      -> n_entities
    dataset_overview.by_item[*].n_customers           -> n_entities
    dataset_overview.by_segment[*].n_customers        -> n_entities

and the same again inside the nested ``category`` bundle, which carries its own
``dataset_overview``.

**The time vocabulary is left alone on purpose** — ``by_snap_date`` and
``n_snap_dates`` keep that spelling (ADR-0017). Reading a rename into them is
the single most likely way to break a file with this script.

Why a script rather than a read-side fallback: the reader
(``evaluation/report_builder._dataset_overview``) refuses an old file outright,
so nothing has to carry two spellings forever, and nobody has to re-run a whole
evaluation just to change key names.

Defaults to a dry run that prints what it would rewrite; pass ``--apply`` to
write. Re-running after a successful migration is a no-op (idempotent), and a
file that already carries *both* spellings is reported and skipped rather than
guessed at.

Run from the repo root::

    PYTHONPATH=src .venv/bin/python scripts/migrate_evaluation_results_keys.py data/models
    PYTHONPATH=src .venv/bin/python scripts/migrate_evaluation_results_keys.py data/models --apply
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple

import typer

from recsys_tfb.evaluation.report_builder import (
    OVERVIEW_CELL_GROUPS as CELL_GROUPS,
    RENAMED_OVERVIEW_KEYS as RENAMES,
)

app = typer.Typer(add_completion=False)

#: ``RENAMES`` (``old key -> new key``) and ``CELL_GROUPS`` are **imported, not
#: restated**. The reader that refuses an un-migrated file
#: (``report_builder._dataset_overview``) reads the same two, so the migrator
#: cannot come to disagree with the detector about which keys are old — the one
#: drift that would leave a file refused by the reader and reported "already
#: migrated" by this script.
#:
#: Deliberately absent from ``RENAMES``: anything spelled ``snap_date``. The
#: time vocabulary is kept (ADR-0017), so ``by_snap_date`` / ``n_snap_dates``
#: must survive untouched — a migration that "tidies" them corrupts the file.

#: Default filename this script looks for when handed a directory.
RESULTS_FILENAME = "evaluation_results.json"


class Plan(NamedTuple):
    """What one file needs. ``renames`` is ``[(json path, old, new)]``."""

    path: Path
    renames: list[tuple[str, str, str]]
    conflicts: list[str]


def _rename_in_cell(cell: dict, where: str) -> tuple[dict, list, list]:
    """Rewrite one cell's keys, preserving insertion order.

    Returns ``(new_cell, renames, conflicts)``. A cell holding both spellings is
    left untouched and reported: the two values could disagree, and picking one
    silently is exactly the kind of guess this migration exists to avoid.
    """
    renames: list[tuple[str, str, str]] = []
    conflicts: list[str] = []
    for old, new in RENAMES.items():
        if old in cell and new in cell:
            conflicts.append(f"{where}: both {old!r} and {new!r} present")
    if conflicts:
        return cell, [], conflicts
    out = {}
    for key, value in cell.items():
        new_key = RENAMES.get(key, key)
        if new_key != key:
            renames.append((where, key, new_key))
        out[new_key] = value
    return out, renames, conflicts


def _migrate_overview(overview: dict, where: str) -> tuple[dict, list, list]:
    """Rewrite one ``dataset_overview`` dict. Pure; returns a new dict."""
    renames: list[tuple[str, str, str]] = []
    conflicts: list[str] = []
    out = dict(overview)

    totals = out.get("totals")
    if isinstance(totals, dict):
        out["totals"], r, c = _rename_in_cell(totals, f"{where}.totals")
        renames += r
        conflicts += c

    for group in CELL_GROUPS:
        cells = out.get(group)
        if not isinstance(cells, dict):
            continue
        new_cells = {}
        for key, cell in cells.items():
            if not isinstance(cell, dict):
                new_cells[key] = cell
                continue
            new_cells[key], r, c = _rename_in_cell(cell, f"{where}.{group}[{key}]")
            renames += r
            conflicts += c
        out[group] = new_cells
    return out, renames, conflicts


def migrate_payload(payload: dict) -> tuple[dict, list, list]:
    """Rewrite a whole ``evaluation_results.json`` payload.

    Covers the top-level ``dataset_overview`` and the one nested inside
    ``category`` (the category bundle is the same shape and never re-nests, so
    two levels is the whole of it).
    """
    renames: list[tuple[str, str, str]] = []
    conflicts: list[str] = []
    out = dict(payload)

    overview = out.get("dataset_overview")
    if isinstance(overview, dict):
        out["dataset_overview"], r, c = _migrate_overview(
            overview, "dataset_overview"
        )
        renames += r
        conflicts += c

    category = out.get("category")
    if isinstance(category, dict) and isinstance(
        category.get("dataset_overview"), dict
    ):
        category = dict(category)
        category["dataset_overview"], r, c = _migrate_overview(
            category["dataset_overview"], "category.dataset_overview"
        )
        renames += r
        conflicts += c
        out["category"] = category
    return out, renames, conflicts


def find_files(target: Path) -> list[Path]:
    """Files to consider: ``target`` itself if a file, else every
    ``evaluation_results.json`` under it."""
    if target.is_file():
        return [target]
    return sorted(target.rglob(RESULTS_FILENAME))


def plan_file(path: Path) -> Plan:
    """What ``path`` needs, without writing anything."""
    payload = json.loads(path.read_text())
    _, renames, conflicts = migrate_payload(payload)
    return Plan(path=path, renames=renames, conflicts=conflicts)


def write_payload(path: Path, payload: dict) -> None:
    """Write ``payload`` back over ``path``."""
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def apply_file(path: Path) -> None:
    """Read, migrate and rewrite ``path`` in place."""
    migrated, _, _ = migrate_payload(json.loads(path.read_text()))
    write_payload(path, migrated)


@app.command()
def main(
    target: Path = typer.Argument(
        ...,
        help=(
            "An evaluation_results.json, or a directory to search recursively "
            "(e.g. data/models)."
        ),
    ),
    apply: bool = typer.Option(
        False, "--apply", help="Actually rewrite files (default: dry run)."
    ),
) -> None:
    """Rename the pre-#327 dataset_overview keys in evaluation_results.json."""
    if not target.exists():
        typer.echo(f"not found: {target}")
        raise typer.Exit(code=1)

    files = find_files(target)
    if not files:
        typer.echo(f"no {RESULTS_FILENAME} under {target}")
        raise typer.Exit(code=1)

    verb = "RENAME" if apply else "PLAN"
    n_changed = 0
    n_conflicted = 0
    for path in files:
        # One read and one migration per file: the payload that gets written is
        # the same object the plan was printed from, so what the operator saw in
        # the dry run is what --apply writes.
        payload = json.loads(path.read_text())
        migrated, renames, conflicts = migrate_payload(payload)
        if conflicts:
            n_conflicted += 1
            for conflict in conflicts:
                typer.echo(f"CONFLICT {path}: {conflict}")
            continue
        if not renames:
            typer.echo(f"OK       {path} (already migrated)")
            continue
        n_changed += 1
        for where, old, new in renames:
            typer.echo(f"{verb}   {path}: {where}.{old} -> {new}")
        if apply:
            write_payload(path, migrated)

    typer.echo(
        f"--- {len(files)} file(s): {n_changed} to migrate, "
        f"{n_conflicted} conflicted ---"
    )
    if n_conflicted:
        typer.echo(
            "A conflicted file carries both spellings of a key; the two values "
            "may disagree, so it is left alone. Resolve it by hand."
        )
    if apply:
        typer.echo("Applied.")
    else:
        typer.echo("Dry run. Re-run with --apply to rewrite.")


if __name__ == "__main__":
    app()
