"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool, write a self-contained HTML
                     editor to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5/A9-validated) for manual paste into config.

Self-contained dev tool (logic + CLI in one file), mirroring the
scripts/promote_model.py / scripts/suggest_categorical_cols.py convention —
unit-tested via ``from scripts.sampling_overrides_editor import ...`` in
tests/scripts/. Not part of the production DAG. Implements the D8
sampling-overrides formulas.

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


def build_grid(
    stats: list[tuple[str, str, int, int]],
    target_neg_pos: float,
    alpha: float,
    w_max: float,
) -> list[dict]:
    """Turn per-(segment,product) (n_pos, n_neg) stats into editor grid rows.

    ``median_pos`` is the per-cell median of n_pos across the whole grid
    (D8). The editor's primary knob is ``suggested_neg_mult`` — the target
    neg:pos multiplier R, defaulted uniformly to ``target_neg_pos`` per cell;
    the browser derives the read-only downsample ratio from it live. We still
    carry ``suggested_ratio`` (= the derived ratio at the default multiplier)
    for any consumer that wants the starting ratio without re-deriving.
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
            "suggested_neg_mult": target_neg_pos,
            "suggested_ratio": suggest_ratio(n_pos, n_neg, target_neg_pos),
            "suggested_weight": suggest_weight(n_pos, median_pos, alpha, w_max),
        })
    return grid


def resolve_keys(dataset_cfg: dict, training_cfg: dict, schema_cfg: dict) -> dict:
    """Resolve ratio dims, weight keys, and the finest profiling granularity.

    ``item``/``label``/``time`` come from ``schema.columns``; ``segment`` is the
    single ``dataset.sample_group_keys`` entry that is neither item nor label
    (the ratio surface is segment x item, label fixed to 0 on export). The
    weight surface is keyed by ``training.sample_weight_keys`` (arbitrary
    available columns). ``union_dims`` is the finest granularity to profile at:
    ``(sample_group_keys ∪ sample_weight_keys) \\ {label}``, ratio dims first.

    Fails fast unless ``sample_group_keys`` is exactly one segment + item +
    label, and unless ``label`` is absent from ``sample_weight_keys`` (the
    editor's per-group n_pos/n_neg model splits on label via sum(label), so a
    label weight key is self-contradictory — hand-write those weights instead).
    """
    cols = schema_cfg.get("columns", {})
    try:
        item_col, label_col, time_col = cols["item"], cols["label"], cols["time"]
    except KeyError as exc:
        raise ValueError(
            f"schema.columns is missing {exc}; cannot resolve profiling "
            "columns. Check the base parameters yaml."
        ) from exc
    group_keys = list(dataset_cfg.get("sample_group_keys", []))
    segments = [k for k in group_keys if k not in (item_col, label_col)]
    if len(group_keys) != 3 or len(segments) != 1:
        raise ValueError(
            "sampling editor expects sample_group_keys = [segment, "
            f"{item_col!r}, {label_col!r}] (one segment + item + label); "
            f"got {group_keys}."
        )
    weight_keys = list(training_cfg.get("sample_weight_keys") or [])
    if label_col in weight_keys:
        raise ValueError(
            f"sampling editor cannot edit weights keyed by the label column "
            f"{label_col!r} (per-group n_pos/n_neg is derived by splitting on "
            f"label). Remove it from sample_weight_keys, or hand-write those "
            f"sample_weights."
        )
    # union dims: ratio dims (group_keys minus label, in order) then any extra
    # weight-key columns not already present. label is already excluded from
    # weight_keys by the guard above, so the extension only needs a dedup check.
    union_dims: list[str] = [k for k in group_keys if k != label_col]
    for k in weight_keys:
        if k not in union_dims:
            union_dims.append(k)
    return {
        "segment_col": segments[0],
        "item_col": item_col,
        "label_col": label_col,
        "time_col": time_col,
        "weight_keys": weight_keys,
        "union_dims": union_dims,
    }


def aggregate_surfaces(*args, **kwargs):  # implemented in Task 3
    raise NotImplementedError


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

    # Reuse the single-source consistency predicates (A5 + A9). The editor
    # emits "<segment>|<product>" weight keys, so the probe must declare the
    # matching sample_weight_keys = [segment, item] for A9c (weight_unknown_items)
    # to validate the product component — without it the predicate short-circuits
    # on an empty key list and unknown products slip through silently. Item at
    # index 1 mirrors the fixed seg|prod emission order above.
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    group_keys = (parameters.get("dataset", {}) or {}).get("sample_group_keys", [])
    segments = [k for k in group_keys if k not in (item_col, label_col)]
    segment_col = segments[0] if segments else "segment"
    probe = {**parameters}
    probe.setdefault("dataset", {})
    probe["dataset"] = {**probe["dataset"], "sample_ratio_overrides": overrides}
    probe["training"] = {
        **probe.get("training", {}),
        "sample_weights": weights,
        "sample_weight_keys": [segment_col, item_col],
    }
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
 td.warn{{background:#fff3cd;color:#8a4b00;font-weight:bold}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
 details{{background:#eef6ff;border:1px solid #cde;padding:.5rem 1rem;
  margin:.6rem 0;max-width:60rem}}
 details summary{{cursor:pointer;font-weight:bold}}
 details code{{background:#fff;padding:0 .25rem;border:1px solid #ddd}}
 #mode{{margin:.6rem 0;padding:.5rem;background:#f0f4ff;border:1px solid #cde;
  font-weight:bold}}
 #mode label{{font-weight:normal;margin-right:1.2rem;cursor:pointer}}
 #flt{{margin:.5rem 0;padding:.35rem;width:22rem}}
 #bulk{{margin:.5rem 0;padding:.5rem;background:#f7f7f7;border:1px solid #ddd;
  display:inline-block}}
 #bulk select,#bulk input{{margin:0 .25rem;padding:.2rem}}
 #bulk select[multiple]{{min-width:11rem;vertical-align:top}}
 #bulk input[type=number]{{width:6rem}}
 #bulk #bm{{margin-left:.6rem;color:#060}}
 tfoot td{{background:#fff7e6;font-weight:bold;border-top:2px solid #999}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<details open><summary>負樣本倍率 / ratio / 實際倍率 / weight 是什麼？用途是什麼？（點此展開/收合）</summary>
<p><b>加權粒度（三選一，替代關係）</b>：頂端選 (1) <code>依 segment</code>、
(2) <code>依 product</code>、(3) <code>segment × product</code>（逐格，預設）。
三者<b>互斥</b>——同時只編輯、只匯出一張表，<b>彼此不會連動</b>。(1)/(2) 模式把每個
segment / product 視為一個彙總單位，倍率與試算都套在它的彙總 n_pos / n_neg 上。
下游怎麼吃這三種粒度由另一處決定，本工具只負責選粒度、編輯、試算、匯出。</p>
<p><b>負樣本倍率 — 目標 neg:pos（主旋鈕，可編輯）。</b>
設定每列希望的負:正樣本倍數 R。保留<b>全部</b>正樣本(label=1)，把負樣本(label=0)
隨機抽樣，使 neg:pos 逼近此倍率。每列預設 = <code>{target_neg_pos}</code>。
<b>用途</b>：壓低類別極不平衡處的負樣本量，縮短訓練、避免模型被海量負樣本淹沒。</p>
<p><b>ratio — 負樣本下採樣保留率（唯讀，由倍率推導）。</b>
<code>ratio = clamp(倍率 × n_pos / n_neg, 0, 1)</code>，這個 ratio 才是匯出的值
（逐格模式 key 格式 <code>segment|product|0</code>，label 固定 0 因為只下採樣負樣本；
依 segment / 依 product 模式則為單鍵 <code>segment</code> / <code>product</code>）。
<code>ratio = {default_ratio}</code>（= default）代表不下採樣、全留。</p>
<p><b>實際倍率 — 下採樣後實際 neg:pos（唯讀）。</b>
= kept_neg / n_pos，與你設的「負樣本倍率」目標同單位、可直接對照（設 5、得幾）。
一般等於目標；當負樣本不足以達到目標倍率時，ratio 會夾到 1.0（全留），此欄會
低於目標並以琥珀底 ⚠ 標示——代表再怎麼留也達不到你要的倍率。</p>
<p><b>weight — 冷門加權</b>（訓練時該列樣本的 loss 權重）。建議值 =
clamp((median_pos / n_pos) ^ <code>{alpha}</code>, 1.0, <code>{w_max}</code>)，
median_pos = <b>當前模式</b>各列 n_pos 的中位數。<code>weight = 1.0</code> 代表不加權。
<b>用途</b>：正樣本稀少的冷門列容易被熱門壓過，提高其權重讓模型別忽略長尾。
匯出對應 <code>training.sample_weights</code>。</p>
<p><b>kept_neg / new_pos_rate — 下採樣後試算</b>（綠底，唯讀）。編輯
<code>負樣本倍率</code> 時即時更新：<code>kept_neg</code> = round(n_neg × ratio)
為下採樣後保留的負樣本筆數，<code>new_pos_rate</code> =
n_pos / (n_pos + kept_neg)。<b>用途</b>：填數字前先看到平衡效果。</p>
<p><b>建議預設值</b>：倍率 = R；weight = 上述冷門加權公式，median 取<b>當前模式</b>
各列 n_pos 的中位數（邏輯三模式一致）。<b>批次設定</b>(多選一次設定多列)僅在逐格
模式顯示。表頭可點擊排序，上方輸入框即時篩選，編輯值在排序/篩選後保留。</p>
</details>
<div id="mode">加權粒度（三選一，替代關係，同時只編輯一張）：
<label><input type=radio name=md value=cell checked onchange="setMode('cell')"> segment × product（逐格）</label>
<label><input type=radio name=md value=segment onchange="setMode('segment')"> 依 segment</label>
<label><input type=radio name=md value=product onchange="setMode('product')"> 依 product</label>
</div>
<input id="flt" placeholder="篩選…" oninput="flt()">
<div id="bulk">批次設定：by
<select id="bk" onchange="fillBulk()"><option value="product">product</option>
<option value="segment">segment</option></select> =
<select id="bv" multiple size=6 title="⌘/Ctrl-click 可複選"></select>
→ set <select id="sk"><option value="neg_mult">負樣本倍率</option>
<option value="weight">weight</option></select> =
<input id="sv" type="number" step="any" placeholder="e.g. 3">
<button onclick="bulkSet()">套用</button><span id="bm"></span></div>
<table id="g"><thead></thead><tbody></tbody><tfoot><tr id="foot"></tr></tfoot></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const GRID={grid_json};
const DR={default_ratio};
const R={target_neg_pos};
const ALPHA={alpha};
const WMAX={w_max};
function median(arr){{ const s=arr.slice().sort((a,b)=>a-b),n=s.length;
 return n?(n%2?s[(n-1)/2]:(s[n/2-1]+s[n/2])/2):1; }}
function suggestWeight(np,med,a,wmax){{
 // mirrors scripts suggest_weight: clamp((med/np)^a, 1, wmax); np<=0 -> wmax.
 if(np<=0) return wmax;
 return Math.min(wmax,Math.max(1,Math.pow(med/np,a)));
}}
// Build the two aggregate stores (segment / product). Each is an INDEPENDENT
// editable store derived once from the per-cell grid; the three modes are
// mutually exclusive (替代關係) so editing one never writes another.
function aggStore(key){{
 const m=new Map();
 GRID.forEach(r=>{{ const a=m.get(r[key])||{{n_pos:0,n_neg:0}};
  a.n_pos+=r.n_pos; a.n_neg+=r.n_neg; m.set(r[key],a); }});
 const rows=[...m.entries()].map(([k,a])=>({{[key]:k,n_pos:a.n_pos,n_neg:a.n_neg,
  pos_rate:(a.n_pos+a.n_neg>0?a.n_pos/(a.n_pos+a.n_neg):0)}}));
 const med=median(rows.map(r=>r.n_pos));
 rows.forEach(r=>{{ r.suggested_neg_mult=R;
  r.suggested_weight=+suggestWeight(r.n_pos,med,ALPHA,WMAX).toFixed(4); }});
 return rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
const MODES={{
 cell:{{rows:GRID,cols:['segment','product']}},
 segment:{{rows:aggStore('segment'),cols:['segment']}},
 product:{{rows:aggStore('product'),cols:['product']}},
}};
let mode='cell',sortKey=null,sortAsc=true;
function rows(){{ return MODES[mode].rows; }}
function preview(r,nm){{
 // primary knob is the neg:pos multiplier nm; derive the read-only keep-rate
 // ratio = clamp(nm*n_pos/n_neg,0,1), then post-downsample preview: keep all
 // positives, keep n_neg*ratio negatives.
 if(isNaN(nm)) return {{ratio:'—',kn:'—',pr:'—',clamped:false,achieved:0,noNeg:false}};
 if(r.n_neg<=0)
  return {{ratio:'1.0000',kn:'0',pr:(r.n_pos>0?1:0).toFixed(4),
   clamped:false,achieved:0,noNeg:true}};
 const raw=nm*r.n_pos/r.n_neg;
 const ratio=Math.min(1,Math.max(0,raw));
 const keptNeg=Math.round(r.n_neg*ratio),total=r.n_pos+keptNeg;
 return {{ratio:ratio.toFixed(4),kn:String(keptNeg),
  pr:(total>0?r.n_pos/total:0).toFixed(4),clamped:raw>1,
  achieved:(r.n_pos>0?keptNeg/r.n_pos:0),noNeg:false}};
}}
function achMult(pv){{
 // read-only achieved-multiplier cell (neg:pos after downsample, same unit as
 // the knob): green when target reached; amber + ⚠ when negatives ran out
 // (ratio clamped to 1.0 so the target can't be met).
 if(pv.ratio==='—') return {{cls:'calc',html:'—',title:''}};
 if(pv.noNeg) return {{cls:'calc',html:'0.0',title:'無負樣本，不下採樣'}};
 if(pv.clamped) return {{cls:'warn',html:pv.achieved.toFixed(1)+' ⚠',
  title:'負樣本不足以達到目標倍率 '+R+'，已全留'}};
 return {{cls:'calc',html:pv.achieved.toFixed(1),title:''}};
}}
function syncEdits(){{
 // sync the active mode's editable cells back into its store (by row index).
 document.querySelectorAll('#g td.edit').forEach(td=>{{
  const v=parseFloat(td.textContent);
  if(!isNaN(v)) rows()[+td.dataset.i][
   td.dataset.k==='neg_mult'?'suggested_neg_mult':'suggested_weight']=v;
 }});
}}
function recalc(td){{
 const r=rows()[+td.dataset.i],tr=td.closest('tr');
 const nm=parseFloat(td.textContent);
 r.suggested_neg_mult=nm;
 const pv=preview(r,nm),am=achMult(pv);
 tr.querySelector('td.rt').textContent=pv.ratio;
 const a=tr.querySelector('td.am');
 a.className=am.cls+' am'; a.innerHTML=am.html; a.title=am.title;
 tr.querySelector('td.kn').textContent=pv.kn;
 tr.querySelector('td.pr').textContent=pv.pr;
 recalcTotals();
}}
function recalcTotals(){{
 // totals over the WHOLE active store (not filter-aware).
 let np=0,nn=0,kn=0;
 rows().forEach(r=>{{ np+=r.n_pos; nn+=r.n_neg;
  const pv=preview(r,parseFloat(r.suggested_neg_mult));
  kn+=(pv.kn==='—'?r.n_neg:+pv.kn); }});
 const span=MODES[mode].cols.length;
 document.getElementById('foot').innerHTML=
  `<td>總計</td>`+(span>1?'<td>—</td>':'')+
  `<td class=stat>${{np}}</td><td class=stat>${{nn}}</td>`+
  `<td class=stat>${{(np+nn>0?np/(np+nn):0).toFixed(4)}}</td>`+
  `<td>—</td><td>—</td>`+
  `<td class=calc>${{(np>0?kn/np:0).toFixed(1)}}</td>`+
  `<td class=calc>${{kn}}</td>`+
  `<td class=calc>${{(np+kn>0?np/(np+kn):0).toFixed(4)}}</td><td>—</td>`;
}}
function render(){{
 const data=rows(),cols=MODES[mode].cols;
 // precompute derived fields so columns can be sorted on them too
 data.forEach(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  r._ratio=pv.ratio; r._am=achMult(pv); r._kn=pv.kn; r._pr=pv.pr;
  r._ach=pv.achieved||0; r._kept=(pv.kn==='—'?r.n_neg:+pv.kn); r._npr=+pv.pr||0; }});
 const q=(document.getElementById('flt').value||'').toLowerCase();
 let idx=data.map((_,i)=>i);
 if(q) idx=idx.filter(i=>cols.map(c=>data[i][c]).join(' ').toLowerCase().indexOf(q)>=0);
 if(sortKey) idx.sort((a,b)=>{{ let x=data[a][sortKey],y=data[b][sortKey];
  if(typeof x==='string'){{x=x.toLowerCase();y=y.toLowerCase();}}
  return (x<y?-1:x>y?1:0)*(sortAsc?1:-1); }});
 const nameHdr=cols.map(c=>`<th onclick="sortBy('${{c}}')">${{c}} ⇅</th>`).join('');
 document.querySelector('#g thead').innerHTML=`<tr>${{nameHdr}}`+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat onclick="sortBy('n_neg')">n_neg ⇅</th>`+
  `<th class=stat onclick="sortBy('pos_rate')">pos_rate ⇅</th>`+
  `<th>負樣本倍率</th><th class=calc>ratio</th>`+
  `<th class=calc onclick="sortBy('_ach')">實際倍率 ⇅</th>`+
  `<th class=calc onclick="sortBy('_kept')">kept_neg ⇅</th>`+
  `<th class=calc onclick="sortBy('_npr')">new_pos_rate ⇅</th>`+
  `<th>weight</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{
  const r=data[i],tr=document.createElement('tr');
  const names=cols.map(c=>`<td>${{r[c]}}</td>`).join('');
  tr.innerHTML=names+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=neg_mult data-i=${{i}} `+
   `oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`+
   `<td class="calc rt">${{r._ratio}}</td>`+
   `<td class="${{r._am.cls}} am" title="${{r._am.title}}">${{r._am.html}}</td>`+
   `<td class="calc kn">${{r._kn}}</td><td class="calc pr">${{r._pr}}</td>`+
   `<td class=edit contenteditable data-k=weight data-i=${{i}}>`+
   `${{r.suggested_weight}}</td>`;
  tb.appendChild(tr);
 }});
 recalcTotals();
}}
function sortBy(k){{
 syncEdits();
 if(sortKey===k){{sortAsc=!sortAsc;}}else{{sortKey=k;sortAsc=true;}}
 render();
}}
function flt(){{ syncEdits(); render(); }}
function setMode(m){{
 // switch granularity; each mode keeps its own edits -> no cross-table coupling.
 syncEdits(); mode=m; sortKey=null;
 document.getElementById('bulk').style.display=(m==='cell'?'inline-block':'none');
 render();
}}
function fillBulk(){{
 // multi-select (cell mode) populated with distinct values of the chosen dim.
 const bk=document.getElementById('bk').value;
 const vals=[...new Set(GRID.map(r=>r[bk]))].sort();
 document.getElementById('bv').innerHTML=
  vals.map(v=>`<option value="${{v}}">${{v}}</option>`).join('');
}}
function bulkSet(){{
 // cell mode only: overwrite neg_mult / weight for every selected segment/product.
 syncEdits();
 const bk=document.getElementById('bk').value;
 const bv=[...document.getElementById('bv').selectedOptions].map(o=>o.value);
 const sk=document.getElementById('sk').value;
 const sv=parseFloat(document.getElementById('sv').value);
 const msg=document.getElementById('bm');
 if(!bv.length||isNaN(sv)){{ msg.textContent='請選擇至少一個值並填目標值'; return; }}
 const tf=sk==='neg_mult'?'suggested_neg_mult':'suggested_weight';
 let n=0;
 GRID.forEach(r=>{{ if(bv.includes(r[bk])){{ r[tf]=sv; n++; }} }});
 msg.textContent='已更新 '+n+' 筆 ('+bk+'='+bv.join(',')+', '+sk+'='+sv+')';
 render();
}}
function exp(kind){{
 // export ONLY the active mode's overrides; key format depends on granularity.
 syncEdits();
 const data=rows(),cols=MODES[mode].cols;
 const keyOf=r=>cols.map(c=>r[c]).join('|');
 const ov={{}},sw={{}};
 data.forEach(r=>{{
  const pv=preview(r,parseFloat(r.suggested_neg_mult));
  const ratio=pv.ratio==='—'?DR:parseFloat(pv.ratio);
  const k=keyOf(r);
  if(ratio!==DR) ov[(mode==='cell'?k+'|0':k)]=ratio;
  if(parseFloat(r.suggested_weight)!==1.0) sw[k]=parseFloat(r.suggested_weight);
 }});
 if(kind==='json'){{
  // cell mode keeps the array shape the to-yaml CLI consumes; others tag mode.
  let o;
  if(mode==='cell') o=data.map(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
   return {{segment:r.segment,product:r.product,neg_mult:r.suggested_neg_mult,
    ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio)),weight:r.suggested_weight}}; }});
  else o={{mode:mode,sample_ratio_overrides:ov,sample_weights:sw}};
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json';a.click();
 }}else{{
  const note=mode==='cell'?'':' (mode='+mode+'; 下游消費方式待另一處定義)';
  document.getElementById('out').textContent=
   '# -> sample_ratio_overrides'+note+'\\n'+
   'sample_ratio_overrides:\\n'+
   Object.entries(ov).map(([k,v])=>'  "'+k+'": '+v).join('\\n')+
   '\\n\\n# -> sample_weights'+note+'\\n'+
   'sample_weights:\\n'+
   Object.entries(sw).map(([k,v])=>'  "'+k+'": '+v).join('\\n');
 }}
}}
setMode('cell');
fillBulk();
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
    base_params: Path = typer.Option(
        Path("conf/base/parameters.yaml"),
        help="base params yaml — source of schema.columns"),
    target_neg_pos: float = typer.Option(5.0, help="downsample target neg:pos R"),
    alpha: float = typer.Option(0.5, help="cold-weight damping exponent"),
    w_max: float = typer.Option(5.0, help="cold-weight cap"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds = cfg.get("dataset", cfg)
    schema_cfg = yaml.safe_load(base_params.read_text()).get("schema", {})
    snap_dates = ds["train_snap_dates"]
    try:
        cols = resolve_columns(ds, schema_cfg)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f"[1/4] config: {len(snap_dates)} snap date(s) from {params}; "
        f"columns segment={cols['segment_col']} item={cols['item_col']} "
        f"label={cols['label_col']} time={cols['time_col']}"
    )
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    typer.echo(
        "[2/4] starting SparkSession + reading source… "
        "(standalone client-template/spark init can take a few minutes; "
        "client-template-local local[*] is far faster for this script)"
    )
    df = _load_spark_df(source)
    typer.echo("[3/4] profiling: Spark groupBy + single collect over snap dates…")
    stats = profile_stats(df, snaps, **cols)
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
