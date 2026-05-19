"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool, write a self-contained HTML
                     editor to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5/A9-validated) for manual paste into config.

Mirrors scripts/suggest_categorical_cols.py conventions. Not part of the
production DAG.

Usage:
  python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
  python scripts/sampling_overrides_editor.py profile data/sample_pool.parquet
  python scripts/sampling_overrides_editor.py to-yaml \\
      data/profiling/sampling_overrides_export.json
"""

from __future__ import annotations

import json
from pathlib import Path

import typer
import yaml

from recsys_tfb.tooling.sampling_suggest import (
    build_grid,
    grid_to_yaml,
    profile_stats,
    render_html,
)

PROFILING_DIR = Path("data/profiling")

app = typer.Typer(
    help="Sampling overrides editor for parameters_dataset/training.yaml",
    add_completion=False,
)


def _load_spark_df(source: str):
    """Load a Hive table (db.table) or a parquet path into a Spark DataFrame.

    Same dual-input convention as scripts/suggest_categorical_cols.py.
    """
    from recsys_tfb.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session({"app_name": "sampling-overrides-editor"})
    if "/" in source or source.endswith(".parquet"):
        return spark.read.parquet(source)
    return spark.table(source)


@app.command()
def profile(
    source: str = typer.Argument(..., help="Hive table db.table or parquet path"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="dataset params yaml"),
    target_neg_pos: float = typer.Option(5.0, help="downsample target neg:pos R"),
    alpha: float = typer.Option(0.5, help="cold-weight damping exponent"),
    w_max: float = typer.Option(5.0, help="cold-weight cap"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds = cfg.get("dataset", cfg)
    snap_dates = ds["train_snap_dates"]
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    df = _load_spark_df(source)
    stats = profile_stats(
        df, snaps,
        segment_col="cust_segment_typ", item_col="prod_name",
        label_col="label", time_col="snap_date",
    )
    grid = build_grid(stats, target_neg_pos, alpha, w_max)
    html = render_html(grid, default_ratio=float(ds.get("sample_ratio", 1.0)))
    PROFILING_DIR.mkdir(parents=True, exist_ok=True)
    out = PROFILING_DIR / "sampling_overrides_editor.html"
    out.write_text(html)
    typer.echo(f"Wrote {out} ({len(grid)} cells). Open it in a browser.")


@app.command("to-yaml")
def to_yaml(
    export_json: Path = typer.Argument(..., help="browser JSON export"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="params yaml for A5/A9"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    export = json.loads(export_json.read_text())
    default_ratio = float(cfg.get("dataset", cfg).get("sample_ratio", 1.0))
    try:
        out = grid_to_yaml(export, cfg, default_ratio)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo("# --> conf/base/parameters_dataset.yaml (under dataset:)")
    typer.echo(out["sample_ratio_overrides_yaml"])
    typer.echo("# --> conf/base/parameters_training.yaml (under training:)")
    typer.echo(out["sample_weights_yaml"])


if __name__ == "__main__":
    app()
