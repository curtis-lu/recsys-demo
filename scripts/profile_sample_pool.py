"""Profile sample_pool — land per-cell positive / negative counts as profile.json.

Counts, once, how many positive and negative rows each sampling cell has in the
train months, and writes them to ``data/profiling/sample_pool_profile/
profile.json``. It only counts: keep-rates and weights are decided by whatever
reads the file (``scripts/sampling_budget_planner.py`` today), each by its own
logic, so changing a budget or a knob never rescans the table.

**The population is the one the dataset pipeline draws from**
(``pipelines/dataset/nodes.py``):

- months: ``months_filter_as_date``, the restriction ``select_sample_keys``
  uses. Not ``F.col(time).isin([Timestamp...])``: against a time column stored
  as a string that matches no row and raises nothing.
- item: a multi-column item is combined with ``combine_item_columns`` first,
  as ``select_sample_keys`` does.
- rows whose entity is NULL are left out: ``split_train_keys`` drops them, so
  they never reach train or train_dev and take no part of the budget.
- label: sample_pool's own column, the one the draw's keys are built from.

**Granularity** is ``(sample_group_keys ∪ sample_weight_keys) \\ {label}``,
sampling keys first — the editor's granularity, so that the editor, once
rewritten to read this file, needs no profile of its own.
A weight key sample_pool does not have (a categorical only the feature table
carries) is skipped, said out loud, and recorded. Every value is recorded the
way the draw's key sees it: cast to string in Spark.

A row with a NULL in a key column matches no override key: the draw keeps it at
``dataset.sample_ratio`` and training weights it 1.0. Such rows are counted
aside (``null_rows``), never folded into a cell, and warned about.

Self-contained dev tool (logic + CLI in one file), mirroring
scripts/sampling_overrides_editor.py — unit-tested via
``from scripts.profile_sample_pool import ...`` in tests/scripts/. Not part of
the production DAG.

Usage (from the directory holding conf/, the repo root as a rule; config read
as ``--env`` reads it for the pipelines):
  python scripts/profile_sample_pool.py run ml_recsys.sample_pool --env <env>
  python scripts/profile_sample_pool.py run data/sample_pool.parquet --env <env>
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import typer

from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.consistency import resolved_env_dir
from recsys_tfb.core.schema import get_schema

# Bumped when the layout below changes; scripts/sampling_budget_planner.py
# refuses any other number.
PROFILE_FORMAT_VERSION = 1
DEFAULT_OUT = Path("data/profiling/sample_pool_profile/profile.json")

_LABEL = "_label"
_ENTITY_NULL = "_entity_null"
_ROWS = "_rows"


def granularity(parameters: dict, columns: list[str]) -> tuple[list[str], list[str]]:
    """``(columns to count by, weight keys skipped because sample_pool lacks them)``.

    A sampling key sample_pool lacks is refused rather than skipped: the
    pipeline's own draw would fail on it.
    """
    schema = get_schema(parameters)
    label = schema["label"]
    group_keys = list((parameters.get("dataset") or {}).get("sample_group_keys") or [])
    weight_keys = list((parameters.get("training") or {}).get("sample_weight_keys") or [])
    dims = [k for k in group_keys if k != label]
    missing = [k for k in dims if k not in columns]
    if missing:
        raise ValueError(
            f"dataset.sample_group_keys column(s) {missing} are not in sample_pool "
            f"(columns: {columns}). The dataset pipeline draws on these columns, "
            f"so sample_pool must carry them."
        )
    skipped = []
    for k in weight_keys:
        if k == label or k in dims:
            continue
        if k in columns:
            dims.append(k)
        else:
            skipped.append(k)
    return dims, skipped


def _train_months(dataset_cfg: dict) -> list[str]:
    """``dataset.train_snap_dates`` as ``YYYY-MM-DD`` strings (ConfigLoader
    has already expanded a ``{start, end, step}`` range into its list)."""
    months = dataset_cfg.get("train_snap_dates")
    if not months:
        raise ValueError(
            "dataset.train_snap_dates is not set in conf/base or the --env "
            "overlay; the profile counts the train months, so it needs them."
        )
    return [str(m)[:10] for m in months]


def build_profile(df, parameters: dict, source: str) -> dict:
    """The profile.json payload for ``df`` (sample_pool as read).

    One Spark action: a group-by over the train months that keeps the label and
    a NULL-entity flag as group columns, collected (it is cells x 2 x 2 rows),
    then sorted into cells, NULL-key rows and dropped NULL-entity rows on the
    driver. No UDF.
    """
    from pyspark.sql import functions as F

    from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date
    from recsys_tfb.utils.item_columns import combine_item_columns

    schema = get_schema(parameters)
    label, time_col = schema["label"], schema["time"]
    months = _train_months(parameters.get("dataset") or {})
    df = combine_item_columns(df, schema, "sample_pool")
    dims, skipped = granularity(parameters, df.columns)

    entity_null = F.lit(False)
    for col in schema["entity"]:
        entity_null = entity_null | F.col(col).isNull()
    rows = (
        df.filter(months_filter_as_date(time_col, months))
        .groupBy(*[F.col(d).cast("string").alias(d) for d in dims],
                 F.col(label).cast("string").alias(_LABEL),
                 entity_null.alias(_ENTITY_NULL))
        .agg(F.count(F.lit(1)).alias(_ROWS))
        .collect()
    )

    cells: dict[tuple, list[int]] = {}
    null_by_column: dict[str, int] = {}
    null_rows = dropped_entity = 0
    bad_labels: set = set()
    for r in rows:
        n = r[_ROWS]
        if r[_ENTITY_NULL]:
            dropped_entity += n
            continue
        values = tuple(r[d] for d in dims)
        null_cols = [d for d, v in zip(dims, values) if v is None]
        if r[_LABEL] is None:
            null_cols.append(label)
        if null_cols:
            null_rows += n
            for col in null_cols:
                null_by_column[col] = null_by_column.get(col, 0) + n
            continue
        if r[_LABEL] not in ("0", "1"):
            bad_labels.add(r[_LABEL])
            continue
        counts = cells.setdefault(values, [0, 0])
        counts[0 if r[_LABEL] == "1" else 1] += n
    if bad_labels:
        raise ValueError(
            f"label column {label!r} reads as {sorted(bad_labels)} once cast to "
            f"string; the sampling keys' label part is '0' or '1', so these rows "
            f"would match no override. Store the label as an integer 0/1."
        )

    return {
        "format_version": PROFILE_FORMAT_VERSION,
        "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "granularity": dims,
        "label_column": label,
        "item_column": schema["item"],
        "time_column": time_col,
        "train_snap_dates": months,
        "skipped_weight_columns": skipped,
        "dropped_null_entity_rows": dropped_entity,
        "null_rows": {"rows": null_rows,
                      "by_column": {d: null_by_column[d] for d in [*dims, label]
                                    if d in null_by_column}},
        "cells": [{"values": dict(zip(dims, values)), "n_pos": p, "n_neg": n}
                  for values, (p, n) in sorted(cells.items())],
    }


def profile_warnings(profile: dict) -> list[str]:
    warnings = []
    if profile["skipped_weight_columns"]:
        warnings.append(
            f"training.sample_weight_keys column(s) {profile['skipped_weight_columns']} "
            f"are not in sample_pool, so the profile does not split by them "
            f"(recorded as skipped_weight_columns). The budget planner does not "
            f"need them."
        )
    nulls = profile["null_rows"]
    if nulls["rows"]:
        warnings.append(
            f"{nulls['rows']:,} row(s) have a NULL in a key column (by column: "
            f"{nulls['by_column']}). Their key matches no override, so the draw "
            f"keeps them at dataset.sample_ratio and training weights them 1.0 — "
            f"a whole group kept, possibly hundreds of times lighter than its "
            f"neighbours. They are recorded as null_rows, not in any cell."
        )
    return warnings


# ---------------------------------------------------------------------------
# Typer CLI (thin orchestration around the logic above).
# ---------------------------------------------------------------------------
app = typer.Typer(help="Profile sample_pool into profile.json", add_completion=False)


@app.callback()
def _main() -> None:
    """Keeps ``run`` a named subcommand (Typer collapses a lone command)."""


def _load_parameters(env: str) -> dict:
    """The parameters the pipelines read for ``--env``: conf/base with
    conf/<env> deep-merged over it, from the working directory's ``conf/``.

    A conf/<env> that does not exist is refused (A30), as the pipelines refuse
    it: the loader would read it as an empty overlay and count with conf/base
    alone. Same text in sampling_budget_planner.py and
    sampling_overrides_editor.py (each script stays self-contained). Raises
    ``ValueError``.
    """
    conf_dir = Path.cwd() / "conf"
    resolved_env_dir(conf_dir, env)
    return ConfigLoader(str(conf_dir), env=env).get_parameters()


def _load_spark_df(source: str):
    """A Hive table (db.table) or a parquet path, as the editor reads it."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session({"app_name": "profile-sample-pool"})
    if "/" in source or source.endswith(".parquet"):
        return spark.read.parquet(source)
    return spark.table(source)


@app.command()
def run(
    source: str = typer.Argument(..., help="Hive table db.table or parquet path"),
    env: str = typer.Option(
        "local", "--env", "-e",
        help="config environment, as for the pipelines: conf/base with conf/<env> over it"),
    out: Path = typer.Option(DEFAULT_OUT, help="where profile.json goes"),
) -> None:
    try:
        parameters = _load_parameters(env)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"[1/3] reading {source} (SparkSession start-up can take minutes)…")
    df = _load_spark_df(source)
    typer.echo("[2/3] counting: one group-by over the train months…")
    try:
        profile = build_profile(df, parameters, source)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    for w in profile_warnings(profile):
        typer.echo(f"WARNING: {w}", err=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(profile, indent=1, ensure_ascii=False) + "\n")
    typer.echo(
        f"[3/3] wrote {out}: {len(profile['cells'])} cell(s) by {profile['granularity']}, "
        f"{len(profile['train_snap_dates'])} month(s); "
        f"{profile['dropped_null_entity_rows']:,} NULL-entity row(s) left out."
    )


if __name__ == "__main__":
    app()
