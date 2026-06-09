"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool at union(sample_group_keys ∪
                     sample_weight_keys)\\{label} granularity, write a
                     self-contained two-tab HTML editor (ratio surface keyed by
                     sample_group_keys, weight surface keyed by
                     sample_weight_keys) to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5 for ratio, A9b/A9c for weights against the real
                     sample_weight_keys) for manual paste into config.

Self-contained dev tool (logic + CLI in one file), mirroring the
scripts/promote_model.py / scripts/suggest_categorical_cols.py convention —
unit-tested via ``from scripts.sampling_overrides_editor import ...`` in
tests/scripts/. Not part of the production DAG.

Usage:
  python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
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
    weight_key_arity_mismatch,
    weight_unknown_items,
)
from recsys_tfb.core.schema import get_schema
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


def resolve_keys(dataset_cfg: dict, training_cfg: dict, schema_cfg: dict) -> dict:
    """Resolve ratio dims, weight keys, and the finest profiling granularity.

    The ratio surface is keyed by ``ratio_dims`` = ``sample_group_keys`` minus
    the label column (order preserved); it may be any length (0, 1, or many) —
    matching what the framework's ``select_keys`` supports. ``label`` MUST be a
    ``sample_group_key``: the editor splits each cell into n_pos/n_neg via
    sum(label) and fixes the label component to "0" on export, so a group-key
    set without label is incompatible (hand-write those overrides instead).

    The weight surface is keyed by ``training.sample_weight_keys`` (arbitrary
    available columns). ``union_dims`` is the finest granularity to profile at:
    ``(sample_group_keys ∪ sample_weight_keys) \\ {label}``, ratio dims first.
    ``label`` must be absent from ``sample_weight_keys`` (the per-group
    n_pos/n_neg model splits on label).
    """
    cols = schema_cfg.get("columns", {})
    try:
        label_col, time_col = cols["label"], cols["time"]
    except KeyError as exc:
        raise ValueError(
            f"schema.columns is missing {exc}; cannot resolve profiling "
            "columns. Check the base parameters yaml."
        ) from exc
    group_keys = list(dataset_cfg.get("sample_group_keys", []))
    if label_col not in group_keys:
        raise ValueError(
            f"sampling editor requires the label column {label_col!r} in "
            f"sample_group_keys (it is the pos/neg split axis); got "
            f"{group_keys}. For label-free group keys, hand-write "
            "sample_ratio_overrides."
        )
    weight_keys = list(training_cfg.get("sample_weight_keys") or [])
    if label_col in weight_keys:
        raise ValueError(
            f"sampling editor cannot edit weights keyed by the label column "
            f"{label_col!r} (per-group n_pos/n_neg is derived by splitting on "
            f"label). Remove it from sample_weight_keys, or hand-write those "
            f"sample_weights."
        )
    # ratio dims = group keys minus label, order preserved (may be empty).
    ratio_dims = [k for k in group_keys if k != label_col]
    # union dims: ratio dims first, then any extra weight-key columns.
    union_dims: list[str] = list(ratio_dims)
    for k in weight_keys:
        if k not in union_dims:
            union_dims.append(k)
    return {
        "ratio_dims": ratio_dims,
        "label_col": label_col,
        "time_col": time_col,
        "weight_keys": weight_keys,
        "union_dims": union_dims,
    }


def aggregate_surfaces(
    stats: list[dict],
    neg_mults: dict,
    *,
    ratio_dims: list,
    weight_keys: list,
    alpha: float,
    w_max: float,
    default_neg_mult: float,
) -> dict:
    """Roll finest-granularity stats up into the ratio and weight surfaces.

    Pure: no Spark, no I/O. ``stats`` are union-granularity dict rows from
    profile_stats. ``ratio_dims`` is the (possibly empty) list of ratio-surface
    dimensions (sample_group_keys minus label). ``neg_mults`` maps a
    ``tuple(row[d] for d in ratio_dims)`` -> target neg:pos multiplier
    (missing -> ``default_neg_mult``).

    Ratio surface: aggregate fine cells to the ratio_dims tuple; each row's
    keep-rate ``ratio = clamp(neg_mult * n_pos / n_neg, 0, 1)`` (n_pos==0 ->
    1.0, keep all negatives). Weight surface: aggregate to the weight_keys
    tuple; n_pos unchanged by downsampling, n_neg_post = sum of
    ``n_neg * ratio[ fine cell's ratio_dims projection ]`` (negatives summed as
    fractions, rounded only for display). Both ratio_rows and weight_rows carry
    a variable-length ``keys`` list.
    """
    # --- ratio surface: aggregate to ratio_dims tuple ---
    racc: dict = {}
    for s in stats:
        k = tuple(s[d] for d in ratio_dims)
        a = racc.setdefault(k, [0, 0])
        a[0] += s["n_pos"]
        a[1] += s["n_neg"]
    ratio_by_key: dict = {}
    ratio_rows: list[dict] = []
    for key, (npos, nneg) in racc.items():
        mult = float(neg_mults.get(key, default_neg_mult))
        # n_pos == 0 -> keep all negatives (ratio 1.0): suggest_ratio would give
        # 0 and silently drop a cold cell's entire negative set. Any JS mirror
        # MUST apply the same n_pos==0 guard.
        ratio = 1.0 if npos == 0 else suggest_ratio(npos, nneg, mult)
        ratio_by_key[key] = ratio
        kept = round(nneg * ratio)
        total = npos + kept
        ratio_rows.append({
            "keys": list(key), "n_pos": npos, "n_neg": nneg,
            "pos_rate": (npos / (npos + nneg) if npos + nneg else 0.0),
            "neg_mult": mult, "ratio": ratio, "kept_neg": kept,
            "new_pos_rate": (npos / total if total else 0.0),
        })
    ratio_rows.sort(key=lambda r: tuple(str(x) for x in r["keys"]))

    # --- weight surface: aggregate to weight_keys tuple (post-downsample) ---
    weight_rows: list[dict] = []
    if weight_keys:
        wacc: dict = {}
        for s in stats:
            wk = tuple(s[k] for k in weight_keys)
            rk = tuple(s[d] for d in ratio_dims)
            a = wacc.setdefault(wk, [0, 0.0])
            a[0] += s["n_pos"]
            a[1] += s["n_neg"] * ratio_by_key[rk]
        pos_list = [v[0] for v in wacc.values()]
        median_pos = float(statistics.median(pos_list)) if pos_list else 1.0
        for wk, (npos, nneg_post) in wacc.items():
            nneg_round = round(nneg_post)
            total = npos + nneg_round
            weight_rows.append({
                "keys": list(wk), "n_pos": npos, "n_neg_post": nneg_round,
                "pos_rate_post": (npos / total if total else 0.0),
                "suggested_weight": round(
                    suggest_weight(npos, median_pos, alpha, w_max), 4),
            })
        weight_rows.sort(key=lambda r: tuple(str(x) for x in r["keys"]))

    return {"ratio_rows": ratio_rows, "weight_rows": weight_rows}


def grid_to_yaml(
    export: dict,
    parameters: dict,
    default_ratio: float,
    default_weight: float = 1.0,
) -> dict:
    """Convert the browser JSON export into two sparse YAML blocks.

    ``export`` is the self-describing object emitted by the editor:
    ``{sample_group_keys, sample_weight_keys, ratio_rows, weight_rows}``.
    Emits only cells deviating from defaults. The export's key-sets must match
    the config (guards against pasting a stale export onto changed config).
    Validates via the single-source consistency predicates BEFORE returning:
    ratio keys through ``override_unknown_items`` (A5), weight keys through
    ``weight_key_arity_mismatch`` (A9b) + ``weight_unknown_items`` (A9c) — the
    probe declares the *real* ``sample_weight_keys`` so A9c actually inspects
    the product component instead of short-circuiting.
    """
    cfg_group = (parameters.get("dataset", {}) or {}).get("sample_group_keys", [])
    cfg_weight = (parameters.get("training", {}) or {}).get("sample_weight_keys") or []
    exp_group = export.get("sample_group_keys", [])
    exp_weight = export.get("sample_weight_keys", [])
    if list(exp_group) != list(cfg_group):
        raise ValueError(
            f"export sample_group_keys {exp_group} != config {cfg_group}; "
            "re-profile against the current config before pasting.")
    if list(exp_weight) != list(cfg_weight):
        raise ValueError(
            f"export sample_weight_keys {exp_weight} != config {cfg_weight}; "
            "re-profile against the current config before pasting.")

    label_col = get_schema(parameters)["label"]
    overrides: dict[str, float] = {}
    for row in export.get("ratio_rows", []):
        ratio = float(row["ratio"])
        if ratio == default_ratio:
            continue
        # Walk the full group_keys order: label position -> "0", every other
        # position -> the next value from this row's ratio_dims keys.
        vals = iter(row["keys"])
        parts = [_NEG_LABEL if k == label_col else str(next(vals))
                 for k in cfg_group]
        overrides["|".join(parts)] = ratio
    weights: dict[str, float] = {}
    for row in export.get("weight_rows", []):
        weight = float(row["weight"])
        if weight != default_weight:
            weights["|".join(str(v) for v in row["keys"])] = weight

    probe = {**parameters}
    probe["dataset"] = {
        **(parameters.get("dataset", {}) or {}),
        "sample_ratio_overrides": overrides,
    }
    probe["training"] = {
        **(parameters.get("training", {}) or {}),
        "sample_weights": weights,
        "sample_weight_keys": list(cfg_weight),
    }
    arity_bad = weight_key_arity_mismatch(probe)
    if arity_bad:
        raise ValueError(
            f"weight key(s) {arity_bad} do not have "
            f"{len(cfg_weight)} '|'-segment(s) to match sample_weight_keys "
            f"{cfg_weight}; fix before paste."
        )
    bad = sorted(
        set(override_unknown_items(probe))
        | set(weight_unknown_items(probe))
    )
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
 td.warn{{background:#fff3cd;color:#8a4b00;font-weight:bold}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
 details{{background:#eef6ff;border:1px solid #cde;padding:.5rem 1rem;
  margin:.6rem 0;max-width:60rem}}
 details summary{{cursor:pointer;font-weight:bold}}
 details code{{background:#fff;padding:0 .25rem;border:1px solid #ddd}}
 #tabs{{margin:.6rem 0}}
 #tabs button.active{{background:#cde;font-weight:bold}}
 #flt{{margin:.5rem 0;padding:.35rem;width:22rem}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<details open><summary>各欄是什麼？用途是什麼？（點此展開/收合）</summary>
<p><b>兩個面（分頁切換，各自獨立）</b>：<code>ratio 面</code>依
<code>sample_group_keys</code>（label 以外的任意維度）調抽樣下採樣；<code>weight 面</code>依
<code>sample_weight_keys</code>調訓練樣本權重。兩組 keys 可不同，匯出時各以自己的
key-set 驗證。</p>
<p><b>負樣本倍率 — 目標 neg:pos（ratio 面主旋鈕，可編輯）。</b>
設定每列希望的負:正樣本倍數 R（每列預設 <code>{target_neg_pos}</code>）。保留<b>全部</b>
正樣本，下採負樣本逼近此倍率。</p>
<p><b>ratio — 負樣本保留率（唯讀，由倍率推導）。</b>
<code>ratio = clamp(倍率 × n_pos / n_neg, 0, 1)</code>，即匯出值（key
<code>segment|item|0</code>，label 固定 0）。<code>ratio = {default_ratio}</code> = 不下採。
n_pos = 0 的冷門列因 neg:pos 無定義，改為在 ratio 欄直接填保留率（預設 1.0 = 全留負樣本）。</p>
<p><b>實際倍率（唯讀）。</b>下採後實際 neg:pos；負樣本不足以達標時 ratio 夾到 1.0、
此欄低於目標並以琥珀底 ⚠ 標示（已全留）。</p>
<p><b>weight 面 n_neg / pos_rate（唯讀，連動下採樣後）。</b>weight 作用在下採樣後的
訓練資料；正樣本全留故 <code>n_pos</code> 不變，負樣本依 ratio 面設定上捲，故此面的
n_neg/pos_rate 反映實際訓練分佈。</p>
<p><b>weight — 冷門加權</b>（訓練時 loss 權重）。建議值 =
clamp((median_pos / n_pos) ^ <code>{alpha}</code>, 1.0, <code>{w_max}</code>)，
median 取 weight 面各列 n_pos 中位數。<code>weight = 1.0</code> = 不加權。匯出對應
<code>training.sample_weights</code>。</p>
</details>
<div id="tabs">
<button id="tb_ratio" class="active" onclick="setTab('ratio')">ratio 面 (sample_group_keys)</button>
<button id="tb_weight" onclick="setTab('weight')">weight 面 (sample_weight_keys)</button>
</div>
<div id="note"></div>
<div id="sumbox"><label>分組試算（下採後）：<select id="grp" onchange="renderSummary()"></select></label>
<div id="summary"></div></div>
<input id="flt" placeholder="篩選…" oninput="flt()">
<table id="g"><thead></thead><tbody></tbody></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const STATS={stats_json};
const GKEYS={gkeys_json};
const GROUP_KEYS={group_keys_json};
const LABEL="{label_col}";
const WKEYS={wkeys_json};
const DR={default_ratio};
const R={target_neg_pos};
const ALPHA={alpha};
const WMAX={w_max};
const SEP='\\u0001';
function median(arr){{ const s=arr.slice().sort((a,b)=>a-b),n=s.length;
 return n?(n%2?s[(n-1)/2]:(s[n/2-1]+s[n/2])/2):1; }}
function suggestWeight(np,med,a,wmax){{ if(np<=0) return wmax;
 return Math.min(wmax,Math.max(1,Math.pow(med/np,a))); }}
function esc(s){{ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
 .replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }}
function keepRate(nm,np,nn){{ if(np<=0||nn<=0) return 1;
 return Math.min(1,Math.max(0,nm*np/nn)); }}
// ratio store: one row per ratio_dims tuple; neg_mult editable, default R.
function buildRatio(){{
 const m=new Map();
 STATS.forEach(s=>{{ const keys=GKEYS.map(k=>s[k]),ks=keys.join(SEP);
  const a=m.get(ks)||{{keys:keys,n_pos:0,n_neg:0}};
  a.n_pos+=s.n_pos; a.n_neg+=s.n_neg; m.set(ks,a); }});
 const rows=[...m.values()];
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.suggested_neg_mult=R; r.ratio_direct=1;
  r.keys.forEach((v,j)=>r['k'+j]=v); }});
 return rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
const RATIO=buildRatio();
let WEIGHT=[];
// effective keep-rate per ratio_dims tuple from current neg_mult edits.
function ratioByKey(){{
 const m=new Map();
 RATIO.forEach(r=>m.set(r.keys.join(SEP),
  keepRate(parseFloat(r.suggested_neg_mult),r.n_pos,r.n_neg)));
 return m;
}}
// weight store: aggregate STATS to WKEYS tuple; n_neg post-downsample via the
// projected ratio_dims ratio. user weight edits preserved by key.
function rebuildWeight(){{
 if(!WKEYS.length){{ WEIGHT=[]; return; }}
 const prev=new Map(WEIGHT.map(w=>[w.keyStr,w.weight]));
 const rbk=ratioByKey(),m=new Map();
 STATS.forEach(s=>{{ const wk=WKEYS.map(k=>s[k]),ks=wk.join('|');
  const rk=GKEYS.map(k=>s[k]).join(SEP);
  const a=m.get(ks)||{{keys:wk,keyStr:ks,n_pos:0,_nn:0}};
  a.n_pos+=s.n_pos; a._nn+=s.n_neg*rbk.get(rk); m.set(ks,a); }});
 const rows=[...m.values()],med=median(rows.map(r=>r.n_pos));
 rows.forEach(r=>{{ r.n_neg_post=Math.round(r._nn);
  const t=r.n_pos+r.n_neg_post; r.pos_rate_post=(t>0?r.n_pos/t:0);
  r.suggested_weight=+suggestWeight(r.n_pos,med,ALPHA,WMAX).toFixed(4);
  r.weight=prev.has(r.keyStr)?prev.get(r.keyStr):r.suggested_weight; }});
 WEIGHT=rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
let tab='ratio',sortKey=null,sortAsc=true;
function rows(){{ return tab==='ratio'?RATIO:WEIGHT; }}
function preview(r,nm){{
 if(r.n_pos<=0){{ let kr=parseFloat(r.ratio_direct);
   if(isNaN(kr)) kr=1; kr=Math.min(1,Math.max(0,kr));
   return {{ratio:kr.toFixed(4),kn:String(Math.round(r.n_neg*kr)),pr:'0.0000',
   clamped:false,achieved:0,noNeg:false,noPos:true}}; }}
 if(isNaN(nm)) return {{ratio:'—',kn:'—',pr:'—',clamped:false,achieved:0,noNeg:false,noPos:false}};
 if(r.n_neg<=0) return {{ratio:'1.0000',kn:'0',pr:(r.n_pos>0?1:0).toFixed(4),
   clamped:false,achieved:0,noNeg:true,noPos:false}};
 const raw=nm*r.n_pos/r.n_neg,ratio=Math.min(1,Math.max(0,raw));
 const keptNeg=Math.round(r.n_neg*ratio),total=r.n_pos+keptNeg;
 return {{ratio:ratio.toFixed(4),kn:String(keptNeg),
  pr:(total>0?r.n_pos/total:0).toFixed(4),clamped:raw>1,
  achieved:(r.n_pos>0?keptNeg/r.n_pos:0),noNeg:false,noPos:false}};
}}
function achMult(pv){{
 if(pv.ratio==='—') return {{cls:'calc',html:'—',title:''}};
 if(pv.noPos) return {{cls:'calc',html:'—',title:'無正樣本，neg:pos 無定義；保留率可直接設定'}};
 if(pv.noNeg) return {{cls:'calc',html:'0.0',title:'無負樣本，不下採樣'}};
 if(pv.clamped) return {{cls:'warn',html:pv.achieved.toFixed(1)+' ⚠',
  title:'負樣本不足以達到目標倍率 '+R+'，已全留'}};
 return {{cls:'calc',html:pv.achieved.toFixed(1),title:''}};
}}
function syncEdits(){{
 document.querySelectorAll('#g td.edit').forEach(td=>{{
  const v=parseFloat(td.textContent); if(isNaN(v)) return;
  const r=rows()[+td.dataset.i];
  if(td.dataset.k==='neg_mult') r.suggested_neg_mult=v;
  else if(td.dataset.k==='ratio_direct') r.ratio_direct=v;
  else r.weight=v;
 }});
}}
function recalc(td){{
 const r=rows()[+td.dataset.i],tr=td.closest('tr'),v=parseFloat(td.textContent);
 const editingRatio=td.dataset.k==='ratio_direct';
 if(editingRatio) r.ratio_direct=v; else r.suggested_neg_mult=v;
 const pv=preview(r,parseFloat(r.suggested_neg_mult)),am=achMult(pv);
 if(!editingRatio) tr.querySelector('td.rt').textContent=pv.ratio;
 const a=tr.querySelector('td.am'); a.className=am.cls+' am';
 a.innerHTML=am.html; a.title=am.title;
 tr.querySelector('td.kn').textContent=pv.kn;
 tr.querySelector('td.pr').textContent=pv.pr;
 renderSummary();
}}
function renderRatio(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+GKEYS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat onclick="sortBy('n_neg')">n_neg ⇅</th>`+
  `<th class=stat>pos_rate</th><th>負樣本倍率</th><th class=calc>ratio</th>`+
  `<th class=calc>實際倍率</th><th class=calc>kept_neg</th>`+
  `<th class=calc>new_pos_rate</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],pv=preview(r,parseFloat(r.suggested_neg_mult));
  const am=achMult(pv),tr=document.createElement('tr'),noPos=r.n_pos<=0;
  const negMultCell=noPos
   ?`<td class=calc title="無正樣本，倍率無定義；改用 ratio 欄直接設保留率">—</td>`
   :`<td class=edit contenteditable data-k=neg_mult data-i=${{i}} oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`;
  const ratioCell=noPos
   ?`<td class="edit rt" contenteditable data-k=ratio_direct data-i=${{i}} oninput="recalc(this)">${{r.ratio_direct}}</td>`
   :`<td class="calc rt">${{pv.ratio}}</td>`;
  tr.innerHTML=r.keys.map(v=>`<td>${{esc(v)}}</td>`).join('')+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   negMultCell+ratioCell+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`;
  tb.appendChild(tr); }});
}}
function renderWeight(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+WKEYS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat>n_neg(後)</th><th class=stat>pos_rate(後)</th>`+
  `<th>weight</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],tr=document.createElement('tr');
  tr.innerHTML=r.keys.map(v=>`<td>${{esc(v)}}</td>`).join('')+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg_post}}</td>`+
   `<td class=stat>${{r.pos_rate_post.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=weight data-i=${{i}}>${{r.weight}}</td>`;
  tb.appendChild(tr); }});
}}
function render(){{
 const data=rows();
 data.forEach(r=>{{ if(r.keys) r.keys.forEach((v,j)=>r['k'+j]=v); }});
 const cols=(tab==='ratio'?GKEYS:WKEYS).map((_,j)=>'k'+j);
 const q=(document.getElementById('flt').value||'').toLowerCase();
 let idx=data.map((_,i)=>i);
 if(q) idx=idx.filter(i=>cols.map(c=>data[i][c]).join(' ').toLowerCase().indexOf(q)>=0);
 if(sortKey) idx.sort((a,b)=>{{ let x=data[a][sortKey],y=data[b][sortKey];
  if(typeof x==='string'){{x=x.toLowerCase();y=y.toLowerCase();}}
  return (x<y?-1:x>y?1:0)*(sortAsc?1:-1); }});
 if(tab==='ratio'){{ renderRatio(data,idx); renderSummary(); }}
 else renderWeight(data,idx);
}}
function sortBy(k){{ syncEdits(); if(sortKey===k){{sortAsc=!sortAsc;}}
 else{{sortKey=k;sortAsc=true;}} render(); }}
function flt(){{ syncEdits(); render(); }}
function setTab(t){{
 syncEdits();
 if(t==='weight' && !WKEYS.length){{
  document.getElementById('note').textContent=
   'sample_weight_keys 為空，無 weight 面可編輯。'; return; }}
 tab=t; sortKey=null;
 document.getElementById('tb_ratio').className=(t==='ratio'?'active':'');
 document.getElementById('tb_weight').className=(t==='weight'?'active':'');
 document.getElementById('note').textContent=
  (t==='weight'?'n_neg(後)/pos_rate(後) 反映 ratio 面目前的下採樣設定。':'');
 if(t==='weight') rebuildWeight();
 render();
}}
function initSummary(){{
 const sel=document.getElementById('grp');
 sel.innerHTML='<option value="">（全部）</option>'+
  GKEYS.map((k,j)=>`<option value="k${{j}}">${{k}}</option>`).join('');
}}
function renderSummary(){{
 const box=document.getElementById('sumbox');
 if(tab!=='ratio'){{ box.style.display='none'; return; }}
 box.style.display='';
 syncEdits();
 const by=document.getElementById('grp').value;
 const agg=new Map();
 RATIO.forEach(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  const np=r.n_pos,kn=parseInt(pv.kn)||0;
  const g=(by===''?'（全部）':String(r[by]));
  const a=agg.get(g)||{{np:0,kn:0}}; a.np+=np; a.kn+=kn; agg.set(g,a); }});
 let h='<table><thead><tr><th>分組</th><th>n_pos</th><th>n_neg(後)</th>'+
  '<th>總數</th><th>pos_rate</th></tr></thead><tbody>';
 [...agg.entries()].sort().forEach(([g,a])=>{{ const t=a.np+a.kn;
  h+=`<tr><td>${{esc(g)}}</td><td>${{a.np}}</td><td>${{a.kn}}</td>`+
   `<td>${{t}}</td><td>${{(t>0?a.np/t:0).toFixed(4)}}</td></tr>`; }});
 h+='</tbody></table>';
 document.getElementById('summary').innerHTML=h;
}}
function ratioKey(keys){{
 let it=0;
 return GROUP_KEYS.map(k=>k===LABEL?'0':String(keys[it++])).join('|');
}}
function exp(kind){{
 syncEdits(); rebuildWeight();
 const ratio_rows=RATIO.map(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  return {{keys:r.keys,ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))}}; }});
 const weight_rows=WEIGHT.map(r=>({{keys:r.keys,weight:parseFloat(r.weight)}}));
 const o={{sample_group_keys:GROUP_KEYS,sample_weight_keys:WKEYS,
  ratio_rows:ratio_rows,weight_rows:weight_rows}};
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a'); a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json'; a.click();
 }}else{{
  const ov={{}},sw={{}};
  ratio_rows.forEach(r=>{{ if(r.ratio!==DR) ov[ratioKey(r.keys)]=r.ratio; }});
  weight_rows.forEach(r=>{{ if(r.weight!==1.0) sw[r.keys.join('|')]=r.weight; }});
  document.getElementById('out').textContent=
   '# -> conf/base/parameters_dataset.yaml (under dataset:)\\n'+
   'sample_ratio_overrides:\\n'+
   Object.entries(ov).map(([k,v])=>'  "'+k+'": '+v).join('\\n')+
   '\\n\\n# -> conf/base/parameters_training.yaml (under training:)\\n'+
   'sample_weights:\\n'+
   Object.entries(sw).map(([k,v])=>'  "'+k+'": '+v).join('\\n');
 }}
}}
initSummary();
setTab('ratio');
</script></body></html>"""


def render_html(
    stats: list[dict],
    *,
    ratio_dims: list,
    group_keys: list,
    label_col: str,
    weight_keys: list,
    default_ratio: float,
    target_neg_pos: float = 5.0,
    alpha: float = 0.5,
    w_max: float = 5.0,
) -> str:
    """Render a self-contained two-tab HTML editor (pure stdlib, no assets).

    ``stats`` are union-granularity dict rows from profile_stats; the browser
    mirrors aggregate_surfaces in JS to build the ratio and weight surfaces
    live. ``ratio_dims`` (= sample_group_keys minus label) keys the ratio
    surface; ``group_keys`` (the full sample_group_keys incl. label, in order)
    is used to reconstruct override keys on export. The tuning knobs are
    surfaced in the help text so it reflects the configured values.
    """
    return _HTML_TEMPLATE.format(
        stats_json=json.dumps(stats),
        gkeys_json=json.dumps(ratio_dims),
        group_keys_json=json.dumps(group_keys),
        label_col=label_col,
        wkeys_json=json.dumps(weight_keys),
        default_ratio=default_ratio,
        target_neg_pos=target_neg_pos,
        alpha=alpha,
        w_max=w_max,
    )


def profile_stats(
    df,
    snap_dates: list,
    *,
    union_dims: list,
    label_col: str,
    time_col: str,
) -> list[dict]:
    """Spark groupBy -> per-(union_dims) (n_pos, n_neg) over snap_dates.

    ``union_dims`` is the finest granularity = (sample_group_keys ∪
    sample_weight_keys) \\ {label}; label is the count source, never a group-by
    column. Single Spark action (one .collect of a tiny grouped frame). No UDF.
    Returns a list of dicts: one per group, each union dim value plus n_pos /
    n_neg. Raises ValueError if any union dim is absent from the frame (usually
    a weight key not added to dataset.carry_columns).
    """
    from pyspark.sql import functions as F

    missing = [c for c in union_dims if c not in df.columns]
    if missing:
        raise ValueError(
            f"profiling columns {missing} not in source columns {df.columns}; "
            "weight keys must be identity/label/carry columns present in "
            "sample_pool (add them to dataset.carry_columns)."
        )
    rows = (
        df.filter(F.col(time_col).isin(list(snap_dates)))
        .groupBy(*union_dims)
        .agg(
            F.sum(F.col(label_col)).alias("n_pos"),
            F.sum(F.lit(1) - F.col(label_col)).alias("n_neg"),
        )
        .collect()
    )
    return [
        {**{d: r[d] for d in union_dims},
         "n_pos": int(r["n_pos"]), "n_neg": int(r["n_neg"])}
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
    train_params: Path = typer.Option(
        Path("conf/base/parameters_training.yaml"),
        help="training params yaml — source of sample_weight_keys"),
    base_params: Path = typer.Option(
        Path("conf/base/parameters.yaml"),
        help="base params yaml — source of schema.columns"),
    target_neg_pos: float = typer.Option(5.0, help="downsample target neg:pos R"),
    alpha: float = typer.Option(0.5, help="cold-weight damping exponent"),
    w_max: float = typer.Option(5.0, help="cold-weight cap"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds = cfg.get("dataset", cfg)
    training_cfg = yaml.safe_load(train_params.read_text()).get("training", {}) or {}
    schema_cfg = yaml.safe_load(base_params.read_text()).get("schema", {})
    snap_dates = ds["train_snap_dates"]
    try:
        keys = resolve_keys(ds, training_cfg, schema_cfg)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f"[1/4] config: {len(snap_dates)} snap date(s) from {params}; "
        f"ratio_dims={keys['ratio_dims']} label={keys['label_col']} "
        f"weight_keys={keys['weight_keys']} union_dims={keys['union_dims']}"
    )
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    typer.echo(
        "[2/4] starting SparkSession + reading source… "
        "(client-template-local local[*] is far faster for this script)"
    )
    df = _load_spark_df(source)
    typer.echo("[3/4] profiling: Spark groupBy + single collect over snap dates…")
    try:
        stats = profile_stats(
            df, snaps, union_dims=keys["union_dims"],
            label_col=keys["label_col"], time_col=keys["time_col"])
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"[3/4] {len(stats)} union-granularity cell(s) profiled")
    typer.echo("[4/4] rendering self-contained HTML…")
    html = render_html(
        stats, ratio_dims=keys["ratio_dims"],
        group_keys=list(ds.get("sample_group_keys", [])),
        label_col=keys["label_col"], weight_keys=keys["weight_keys"],
        default_ratio=float(ds.get("sample_ratio", 1.0)),
        target_neg_pos=target_neg_pos, alpha=alpha, w_max=w_max,
    )
    PROFILING_DIR.mkdir(parents=True, exist_ok=True)
    out = PROFILING_DIR / "sampling_overrides_editor.html"
    out.write_text(html)
    typer.echo(f"Wrote {out} ({len(stats)} cells). Open it in a browser.")


@app.command("to-yaml")
def to_yaml(
    export_json: Path = typer.Argument(..., help="browser JSON export"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="dataset params yaml"),
    train_params: Path = typer.Option(
        Path("conf/base/parameters_training.yaml"),
        help="training params yaml — source of sample_weight_keys"),
    base_params: Path = typer.Option(
        Path("conf/base/parameters.yaml"),
        help="base params yaml — source of schema for A5/A9"),
) -> None:
    ds = yaml.safe_load(params.read_text())
    ds_cfg = ds.get("dataset", ds)
    # Assemble a single parameters dict the consistency predicates accept:
    # schema from base_params (or an inline schema in --params, for tests),
    # dataset from --params, training from --train-params.
    merged: dict = {}
    if base_params.exists():
        merged.update(yaml.safe_load(base_params.read_text()) or {})
    if "schema" in ds:
        merged["schema"] = ds["schema"]
    merged["dataset"] = ds_cfg
    merged["training"] = yaml.safe_load(train_params.read_text()).get("training", {}) or {}
    if not (merged.get("schema") or {}).get("columns") and not (merged.get("schema") or {}).get("categorical_values"):
        typer.echo(
            "WARNING: no schema resolved from --base-params / --params; "
            "unknown-product validation (A5/A9c) may be skipped silently.",
            err=True)
    export = json.loads(export_json.read_text())
    default_ratio = float(ds_cfg.get("sample_ratio", 1.0))
    try:
        out = grid_to_yaml(export, merged, default_ratio)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo("# --> conf/base/parameters_dataset.yaml (under dataset:)")
    typer.echo(out["sample_ratio_overrides_yaml"])
    typer.echo("# --> conf/base/parameters_training.yaml (under training:)")
    typer.echo(out["sample_weights_yaml"])


if __name__ == "__main__":
    app()
