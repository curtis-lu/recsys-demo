"""One-time migration: reorganize flat logs/ into logs/<pipeline>/<YYYY-MM>/.

Old layout: ``logs/{pipeline}_{run_id}.jsonl``  (run_id = ``YYYYMMDD_HHMMSS_{6hex}``)
New layout: ``logs/<pipeline>/<YYYY-MM>/{pipeline}_{run_id}.jsonl``

Defaults to a dry run that prints the planned moves; pass ``--apply`` to perform
them. Only top-level ``*.jsonl`` files are considered, so re-running after a
successful migration is a no-op (idempotent).

Run from the repo root::

    PYTHONPATH=src .venv/bin/python scripts/migrate_logs_layout.py            # dry run
    PYTHONPATH=src .venv/bin/python scripts/migrate_logs_layout.py --apply    # move
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

import typer

app = typer.Typer(add_completion=False)

# Old flat name: {pipeline}_{YYYYMMDD}_{HHMMSS}_{6hex}.jsonl
# pipeline is greedy (.+) so names containing '_' (e.g. source_etl) split correctly.
_LOG_NAME_RE = re.compile(
    r"^(?P<pipeline>.+)_(?P<date>\d{8})_\d{6}_[0-9a-f]{6}\.jsonl$"
)


class Move(NamedTuple):
    src: Path
    dst: Path


def plan_moves(
    logs_dir: Path,
) -> tuple[list[Move], list[tuple[Path, str]]]:
    """Compute moves for top-level ``*.jsonl`` files in ``logs_dir``.

    Returns ``(moves, skipped)`` where ``moves`` are ``Move(src, dst)`` for files
    matching the old flat naming, and ``skipped`` are ``(path, reason)`` for files
    left in place (``"unmatched-name"`` or ``"destination-exists"``).
    """
    moves: list[Move] = []
    skipped: list[tuple[Path, str]] = []
    for path in sorted(logs_dir.glob("*.jsonl")):
        if not path.is_file():
            continue
        match = _LOG_NAME_RE.match(path.name)
        if match is None:
            skipped.append((path, "unmatched-name"))
            continue
        pipeline = match.group("pipeline")
        date = match.group("date")
        month = f"{date[:4]}-{date[4:6]}"
        dst = logs_dir / pipeline / month / path.name
        if dst.exists():
            skipped.append((path, "destination-exists"))
            continue
        moves.append(Move(src=path, dst=dst))
    return moves, skipped


def apply_moves(moves: list[Move]) -> None:
    """Execute planned moves, creating parent directories as needed."""
    for src, dst in moves:
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)


@app.command()
def main(
    logs_dir: Path = typer.Option(
        Path("logs"), "--logs-dir", help="Root logs directory to migrate."
    ),
    apply: bool = typer.Option(
        False, "--apply", help="Actually move files (default: dry run)."
    ),
) -> None:
    """Migrate flat logs/ into logs/<pipeline>/<YYYY-MM>/ layout."""
    if not logs_dir.exists():
        typer.echo(f"logs dir not found: {logs_dir}")
        raise typer.Exit(code=1)

    moves, skipped = plan_moves(logs_dir)
    verb = "MOVE" if apply else "PLAN"
    for src, dst in moves:
        typer.echo(f"{verb} {src.name} -> {dst.relative_to(logs_dir)}")
    for path, reason in skipped:
        typer.echo(f"SKIP {path.name} ({reason})")
    typer.echo(f"--- {len(moves)} to move, {len(skipped)} skipped ---")

    if apply:
        apply_moves(moves)
        typer.echo("Applied.")
    else:
        typer.echo("Dry run. Re-run with --apply to move.")


if __name__ == "__main__":
    app()
