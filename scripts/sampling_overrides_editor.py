"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool, write a self-contained HTML
                     editor to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5/A9-validated) for manual paste into config.

Self-contained dev tool (logic + CLI in one file), mirroring the
scripts/promote_model.py / scripts/suggest_categorical_cols.py convention —
unit-tested via ``from scripts.sampling_overrides_editor import ...`` in
tests/scripts/. Not part of the production DAG. Implements the D8 formulas
from docs/superpowers/specs/2026-05-18-sampling-overrides-editor-design.md.

Usage:
  python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
  python scripts/sampling_overrides_editor.py profile data/sample_pool.parquet
  python scripts/sampling_overrides_editor.py to-yaml \\
      data/profiling/sampling_overrides_export.json
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path

import typer
import yaml

from recsys_tfb.core.consistency import (
    override_unknown_items,
    weight_unknown_items,
)

PROFILING_DIR = Path("data/profiling")

# Override key is '|'-joined sample_group_keys; cold-product downsample
# targets negatives, so the label component is fixed to "0".
_NEG_LABEL = "0"


# ---------------------------------------------------------------------------
# Pure suggestion logic (D8 formulas) — no Spark, no Typer, no I/O.
# ---------------------------------------------------------------------------
def suggest_ratio(n_pos: int, n_neg: int, target_neg_pos: float) -> float:
    """Downsample ratio for negatives: keep all positives, target neg:pos = R.

    neg_ratio = clamp(R * n_pos / n_neg, 0, 1). n_neg == 0 -> 1.0 (nothing to
    downsample).
    """
    if n_neg <= 0:
        return 1.0
    return min(1.0, max(0.0, target_neg_pos * n_pos / n_neg))


def suggest_weight(
    n_pos: int, median_pos: float, alpha: float, w_max: float
) -> float:
    """Cold-product boost weight: clamp((median_pos/n_pos)**alpha, 1.0, w_max).

    n_pos <= 0 -> treated as maximally cold (returns w_max).
    """
    if n_pos <= 0:
        return w_max
    raw = (median_pos / n_pos) ** alpha
    return min(w_max, max(1.0, raw))


def build_grid(
    stats: list[tuple[str, str, int, int]],
    target_neg_pos: float,
    alpha: float,
    w_max: float,
) -> list[dict]:
    """Turn per-(segment,product) (n_pos, n_neg) stats into editor grid rows.

    ``median_pos`` is the per-cell median of n_pos across the whole grid
    (D8). Each row carries the raw stats plus suggested_ratio /
    suggested_weight starting values.
    """
    pos_counts = [np for (_, _, np, _) in stats]
    median_pos = float(statistics.median(pos_counts)) if pos_counts else 1.0
    grid: list[dict] = []
    for seg, prod, n_pos, n_neg in stats:
        total = n_pos + n_neg
        grid.append({
            "segment": seg,
            "product": prod,
            "n_pos": n_pos,
            "n_neg": n_neg,
            "pos_rate": (n_pos / total) if total else 0.0,
            "suggested_ratio": suggest_ratio(n_pos, n_neg, target_neg_pos),
            "suggested_weight": suggest_weight(n_pos, median_pos, alpha, w_max),
        })
    return grid


def grid_to_yaml(
    export: list[dict],
    parameters: dict,
    default_ratio: float,
    default_weight: float = 1.0,
) -> dict:
    """Convert the browser JSON export into two sparse YAML blocks.

    Emits only cells deviating from defaults. Validates the resulting keys
    via the A5 / A9 consistency predicates (collect-all) BEFORE returning, so
    a bad product value is caught before the user pastes into config.
    """
    overrides: dict[str, float] = {}
    weights: dict[str, float] = {}
    for row in export:
        seg, prod = row["segment"], row["product"]
        ratio = float(row.get("ratio", default_ratio))
        weight = float(row.get("weight", default_weight))
        if ratio != default_ratio:
            overrides[f"{seg}|{prod}|{_NEG_LABEL}"] = ratio
        if weight != default_weight:
            weights[f"{seg}|{prod}"] = weight

    # Reuse the single-source consistency predicates (A5 + A9).
    probe = {**parameters}
    probe.setdefault("dataset", {})
    probe["dataset"] = {**probe["dataset"], "sample_ratio_overrides": overrides}
    probe["training"] = {**probe.get("training", {}), "sample_weights": weights}
    bad = sorted(set(override_unknown_items(probe)) | set(weight_unknown_items(probe)))
    if bad:
        raise ValueError(
            f"editor export references unknown product value(s) {bad} "
            f"absent from schema.categorical_values[item]; fix before paste."
        )

    return {
        "sample_ratio_overrides_yaml": yaml.safe_dump(
            {"sample_ratio_overrides": overrides}, sort_keys=True,
            allow_unicode=True, default_flow_style=False),
        "sample_weights_yaml": yaml.safe_dump(
            {"sample_weights": weights}, sort_keys=True,
            allow_unicode=True, default_flow_style=False),
    }


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Sampling Overrides Editor</title>
<style>
 body{{font-family:system-ui,monospace;margin:1.5rem}}
 table{{border-collapse:collapse}} td,th{{border:1px solid #ccc;padding:4px 8px}}
 th{{background:#f2f2f2}} td.edit{{background:#fffbe6}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<p>default ratio = <b>{default_ratio}</b>. 編輯 ratio / weight 欄；
只匯出 ≠ default 的 cell。</p>
<table id="g"><thead><tr>
<th>segment</th><th>product</th><th class="stat">n_pos</th>
<th class="stat">n_neg</th><th class="stat">pos_rate</th>
<th>ratio</th><th>weight</th></tr></thead><tbody></tbody></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const GRID={grid_json};
const DR={default_ratio};
const tb=document.querySelector('#g tbody');
GRID.forEach((r,i)=>{{
 const tr=document.createElement('tr');
 tr.innerHTML=`<td>${{r.segment}}</td><td>${{r.product}}</td>`+
  `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
  `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
  `<td class=edit contenteditable data-k=ratio data-i=${{i}}>`+
  `${{r.suggested_ratio}}</td>`+
  `<td class=edit contenteditable data-k=weight data-i=${{i}}>`+
  `${{r.suggested_weight}}</td>`;
 tb.appendChild(tr);
}});
function collect(){{
 const o=GRID.map(r=>({{segment:r.segment,product:r.product,
  ratio:r.suggested_ratio,weight:r.suggested_weight}}));
 document.querySelectorAll('td.edit').forEach(td=>{{
  o[+td.dataset.i][td.dataset.k]=parseFloat(td.textContent);}});
 return o;
}}
function exp(kind){{
 const o=collect();
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json';a.click();
 }}else{{
  const ov={{}},sw={{}};
  o.forEach(r=>{{ if(r.ratio!==DR) ov[r.segment+'|'+r.product+'|0']=r.ratio;
   if(r.weight!==1.0) sw[r.segment+'|'+r.product]=r.weight; }});
  document.getElementById('out').textContent=
   '# -> parameters_dataset.yaml (dataset.sample_ratio_overrides)\\n'+
   'sample_ratio_overrides:\\n'+
   Object.entries(ov).map(([k,v])=>'  "'+k+'": '+v).join('\\n')+
   '\\n\\n# -> parameters_training.yaml (training.sample_weights)\\n'+
   'sample_weights:\\n'+
   Object.entries(sw).map(([k,v])=>'  "'+k+'": '+v).join('\\n');
 }}
}}
</script></body></html>"""


def render_html(grid: list[dict], default_ratio: float) -> str:
    """Render a self-contained HTML editor (pure stdlib, no external assets)."""
    return _HTML_TEMPLATE.format(
        default_ratio=default_ratio,
        grid_json=json.dumps(grid),
    )


def profile_stats(
    df,
    snap_dates: list,
    *,
    segment_col: str,
    item_col: str,
    label_col: str,
    time_col: str,
) -> list[tuple[str, str, int, int]]:
    """Spark groupBy -> per-(segment,product) (n_pos, n_neg) over snap_dates.

    Single Spark action (one .collect of a tiny grouped frame). No UDF.
    """
    from pyspark.sql import functions as F

    rows = (
        df.filter(F.col(time_col).isin(list(snap_dates)))
        .groupBy(segment_col, item_col)
        .agg(
            F.sum(F.col(label_col)).alias("n_pos"),
            F.sum(F.lit(1) - F.col(label_col)).alias("n_neg"),
        )
        .collect()
    )
    return [
        (r[segment_col], r[item_col], int(r["n_pos"]), int(r["n_neg"]))
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Typer CLI (thin orchestration around the pure logic above).
# ---------------------------------------------------------------------------
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
