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
 th{{background:#f2f2f2;cursor:pointer;user-select:none}}
 td.edit{{background:#fffbe6}}
 td.calc{{background:#eaffea;color:#060}} th.calc{{cursor:default}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
 details{{background:#eef6ff;border:1px solid #cde;padding:.5rem 1rem;
  margin:.6rem 0;max-width:60rem}}
 details summary{{cursor:pointer;font-weight:bold}}
 details code{{background:#fff;padding:0 .25rem;border:1px solid #ddd}}
 #flt{{margin:.5rem 0;padding:.35rem;width:22rem}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<details open><summary>ratio / weight 是什麼？用途是什麼？（點此展開/收合）</summary>
<p><b>ratio — 負樣本下採樣保留率</b>（逐 segment×product 格子）。保留<b>全部</b>
正樣本(label=1)，把負樣本(label=0)隨機抽樣到此比例，使 neg:pos 逼近目標
R=<code>{target_neg_pos}</code>。建議值 = clamp(R × n_pos / n_neg, 0, 1)。
<code>ratio = {default_ratio}</code>（= default）代表不下採樣、全留。
<b>用途</b>：壓低類別極不平衡格子的負樣本量，縮短訓練、避免模型被海量負樣本
淹沒。匯出後貼到 <code>parameters_dataset.yaml</code> 的
<code>dataset.sample_ratio_overrides</code>（key 格式 <code>segment|product|0</code>，
label 固定 0 因為只下採樣負樣本）。</p>
<p><b>weight — 冷門產品加權</b>（訓練時該格子樣本的 loss 權重）。建議值 =
clamp((median_pos / n_pos) ^ <code>{alpha}</code>, 1.0, <code>{w_max}</code>)，
median_pos = 全表各格 n_pos 的中位數。<code>weight = 1.0</code> 代表不加權。
<b>用途</b>：正樣本稀少的冷門 segment×product 容易被熱門產品壓過，提高其權重
讓模型別忽略長尾。匯出後貼到 <code>parameters_training.yaml</code> 的
<code>training.sample_weights</code>（key 格式 <code>segment|product</code>）。</p>
<p><b>kept_neg / new_pos_rate — 下採樣後試算</b>（綠底，唯讀）。編輯某格
<code>ratio</code> 時即時更新：<code>kept_neg</code> = round(n_neg × ratio)
為下採樣後保留的負樣本筆數，<code>new_pos_rate</code> =
n_pos / (n_pos + kept_neg) 為下採樣後該格的正樣本比例。<b>用途</b>：填數字前
先看到平衡效果，不必匯出跑訓練才知道。</p>
<p class=stat>只匯出與 default 不同的 cell。點欄位標題排序（再點一次反向）；
上方輸入框即時篩選 segment / product；編輯值在排序/篩選後會保留。</p>
</details>
<input id="flt" placeholder="篩選 segment / product…" oninput="flt()">
<table id="g"><thead><tr>
<th onclick="sort('segment')">segment ⇅</th>
<th onclick="sort('product')">product ⇅</th>
<th class="stat" onclick="sort('n_pos')">n_pos ⇅</th>
<th class="stat" onclick="sort('n_neg')">n_neg ⇅</th>
<th class="stat" onclick="sort('pos_rate')">pos_rate ⇅</th>
<th>ratio</th>
<th class="calc">kept_neg</th><th class="calc">new_pos_rate</th>
<th>weight</th></tr></thead><tbody></tbody></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const GRID={grid_json};
const DR={default_ratio};
const tb=document.querySelector('#g tbody');
let sortKey=null,sortAsc=true;
function syncEdits(){{
 document.querySelectorAll('td.edit').forEach(td=>{{
  const v=parseFloat(td.textContent);
  if(!isNaN(v)) GRID[+td.dataset.i][
   td.dataset.k==='ratio'?'suggested_ratio':'suggested_weight']=v;
 }});
}}
function preview(r,ratio){{
 // post-downsample preview: keep all positives, keep n_neg*ratio negatives.
 if(isNaN(ratio)) return {{kn:'—',pr:'—'}};
 const keptNeg=Math.round(r.n_neg*ratio),total=r.n_pos+keptNeg;
 return {{kn:String(keptNeg),pr:(total>0?r.n_pos/total:0).toFixed(4)}};
}}
function recalc(td){{
 const r=GRID[+td.dataset.i],tr=td.closest('tr');
 const pv=preview(r,parseFloat(td.textContent));
 tr.querySelector('td.kn').textContent=pv.kn;
 tr.querySelector('td.pr').textContent=pv.pr;
}}
function render(){{
 const q=(document.getElementById('flt').value||'').toLowerCase();
 let idx=GRID.map((_,i)=>i);
 if(q) idx=idx.filter(i=>
  (GRID[i].segment+' '+GRID[i].product).toLowerCase().indexOf(q)>=0);
 if(sortKey) idx.sort((a,b)=>{{
  let x=GRID[a][sortKey],y=GRID[b][sortKey];
  if(typeof x==='string'){{x=x.toLowerCase();y=y.toLowerCase();}}
  return (x<y?-1:x>y?1:0)*(sortAsc?1:-1);
 }});
 tb.innerHTML='';
 idx.forEach(i=>{{
  const r=GRID[i],tr=document.createElement('tr');
  const pv=preview(r,r.suggested_ratio);
  tr.innerHTML=`<td>${{r.segment}}</td><td>${{r.product}}</td>`+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=ratio data-i=${{i}} `+
   `oninput="recalc(this)">${{r.suggested_ratio}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`+
   `<td class=edit contenteditable data-k=weight data-i=${{i}}>`+
   `${{r.suggested_weight}}</td>`;
  tb.appendChild(tr);
 }});
}}
function sort(k){{
 syncEdits();
 if(sortKey===k){{sortAsc=!sortAsc;}}else{{sortKey=k;sortAsc=true;}}
 render();
}}
function flt(){{ syncEdits(); render(); }}
function collect(){{
 syncEdits();
 return GRID.map(r=>({{segment:r.segment,product:r.product,
  ratio:r.suggested_ratio,weight:r.suggested_weight}}));
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
render();
</script></body></html>"""


def render_html(
    grid: list[dict],
    default_ratio: float,
    *,
    target_neg_pos: float = 5.0,
    alpha: float = 0.5,
    w_max: float = 5.0,
) -> str:
    """Render a self-contained HTML editor (pure stdlib, no external assets).

    The tuning knobs (``target_neg_pos`` / ``alpha`` / ``w_max``) are surfaced
    in the in-page explanation so the rendered help reflects the *configured*
    values, not hardcoded prose. They default to the ``profile`` command
    defaults so existing two-arg callers keep working.
    """
    return _HTML_TEMPLATE.format(
        default_ratio=default_ratio,
        grid_json=json.dumps(grid),
        target_neg_pos=target_neg_pos,
        alpha=alpha,
        w_max=w_max,
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
    typer.echo(f"[1/4] config: {len(snap_dates)} snap date(s) from {params}")
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    typer.echo(
        "[2/4] starting SparkSession + reading source… "
        "(standalone client-template/spark init can take a few minutes; "
        "client-template-local local[*] is far faster for this script)"
    )
    df = _load_spark_df(source)
    typer.echo("[3/4] profiling: Spark groupBy + single collect over snap dates…")
    stats = profile_stats(
        df, snaps,
        segment_col="cust_segment_typ", item_col="prod_name",
        label_col="label", time_col="snap_date",
    )
    typer.echo(f"[3/4] {len(stats)} (segment,product) cell(s) profiled")
    grid = build_grid(stats, target_neg_pos, alpha, w_max)
    typer.echo("[4/4] rendering self-contained HTML…")
    html = render_html(
        grid, default_ratio=float(ds.get("sample_ratio", 1.0)),
        target_neg_pos=target_neg_pos, alpha=alpha, w_max=w_max,
    )
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
