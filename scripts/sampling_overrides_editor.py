"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool at union(sample_group_keys ∪
                     sample_weight_keys)\\{label} granularity, write a
                     self-contained two-tab HTML editor (ratio surface keyed by
                     sample_group_keys, weight surface keyed by
                     sample_weight_keys) to data/profiling/. ratio surface
                     downsamples BOTH classes -- a direct positive keep-rate
                     plus neg-mult / keep-rate input modes for negatives --
                     with group/batch row selection; weight surface supports a
                     couple/decouple (global phi) base toggle.
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
import math
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

# Override key is '|'-joined sample_group_keys. Each ratio cell can emit two
# entries -- the label component distinguishes them: negatives "0", positives
# "1". Both classes are cost knobs (downsampling), never ranking intent; ranking
# lift is the weight surface's job.
_NEG_LABEL = "0"
_POS_LABEL = "1"

# Yellow-flag floor for a cell's post-downsample positive count. A judgment
# number, not a derived one: the only hard failure is n_pos_post == 0 (the cell
# drops out of the two-factor weight model entirely, silently looking like
# "needs no weighting"). Override per run with --pos-warn-min.
DEFAULT_POS_WARN_MIN = 30


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


def floor_weight(n_pos: float, n_neg_post: float, t: float) -> float:
    """Negative-down-weight v that lifts a cell's effective pos-rate to ``t``.

    ``v = n_pos·(1−t) / (t·n_neg_post)`` on the POST-downsample negative count,
    so the resulting floor depends only on ``t`` and is independent of how much
    the (cost-only) downsampling already removed (φ is folded into n_neg_post).

    Edge cases keep negatives untouched (v=1.0): n_pos<=0 (cold cell with no
    positives to lift) or n_neg_post<=0 (no negatives to weight).
    """
    if n_pos <= 0 or n_neg_post <= 0:
        return 1.0
    return n_pos * (1.0 - t) / (t * n_neg_post)


def two_factor_weights(
    n_pos: float, n_neg_post: float, *, t: float, alpha: float, m_min: float
) -> dict:
    """Per-cell positive/negative weights = attention A × floor factor.

    ``W = A · (1 if positive else v)`` where:

    - ``v = floor_weight(...)`` lifts the cell's effective pos-rate to ``t``.
    - ``m = n_pos + n_neg_post·v`` is the floor-weighted mass (= n_pos/t for a
      lifted cell, so ``m ∝ n_pos``); ``A = (m_min/m)**alpha`` normalises
      attention by positive count (the least-positive cell gets A=1; hotter
      cells are down-weighted, A≤1 — never up-weighting scarce positives).

    ``m_min`` is the smallest ``m`` among the cells WITH positives (caller-
    supplied so every cell shares one reference). A zero-positive cell does not
    participate in attention: it returns A=1, v=1, w_pos=w_neg=1.

    Returns ``{w_pos, w_neg, v, A, m, eff_pos_rate}``.
    """
    if n_pos <= 0:
        return {"w_pos": 1.0, "w_neg": 1.0, "v": 1.0, "A": 1.0,
                "m": float(n_neg_post), "eff_pos_rate": 0.0}
    v = floor_weight(n_pos, n_neg_post, t)
    m = n_pos + n_neg_post * v
    A = (m_min / m) ** alpha if m > 0 else 1.0
    w_pos = A
    w_neg = A * v
    eff = n_pos / (n_pos + n_neg_post * v) if (n_pos + n_neg_post * v) else 0.0
    return {"w_pos": w_pos, "w_neg": w_neg, "v": v, "A": A, "m": m,
            "eff_pos_rate": eff}


def resolve_keys(dataset_cfg: dict, training_cfg: dict, schema_cfg: dict) -> dict:
    """Resolve ratio dims, weight keys, and the finest profiling granularity.

    The ratio surface is keyed by ``ratio_dims`` = ``sample_group_keys`` minus
    the label column (order preserved); it may be any length (0, 1, or many) —
    matching what the framework's key-selecting nodes support. ``label`` MUST be a
    ``sample_group_key``: the editor splits each cell into n_pos/n_neg via
    sum(label) and fixes the label component to "0" on export, so a group-key
    set without label is incompatible (hand-write those overrides instead).

    The weight surface is keyed by ``weight_dims`` = ``training.sample_weight_keys``
    minus label (arbitrary available columns); ``label`` MUST be present in
    ``sample_weight_keys`` when it is non-empty, as the pos/neg split axis of the
    two-factor weight model (w_pos vs w_neg). ``union_dims`` is the finest
    granularity to profile at: ``(sample_group_keys ∪ sample_weight_keys) \\
    {label}``, ratio dims first.
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
    if weight_keys and label_col not in weight_keys:
        raise ValueError(
            f"sampling editor requires the label column {label_col!r} in "
            f"sample_weight_keys (it is the pos/neg split axis for the two-factor "
            f"weight model: w_pos vs w_neg); got {weight_keys}. Add it, or leave "
            "sample_weight_keys empty to skip the weight surface."
        )
    # ratio dims = group keys minus label; weight dims = weight keys minus label.
    # Order preserved; either may be empty. label is the pos/neg split axis on
    # both surfaces, never a grouping dim.
    ratio_dims = [k for k in group_keys if k != label_col]
    weight_dims = [k for k in weight_keys if k != label_col]
    # union dims: ratio dims first, then any extra weight dims.
    union_dims: list[str] = list(ratio_dims)
    for k in weight_dims:
        if k not in union_dims:
            union_dims.append(k)
    return {
        "ratio_dims": ratio_dims,
        "weight_dims": weight_dims,
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
    weight_dims: list,
    alpha: float,
    t: float,
    default_neg_mult: float | None,
    pos_ratios: dict | None = None,
    default_keep_rate: float = 1.0,
    neg_base: str = "coupled",
    phi: float = 1.0,
) -> dict:
    """Roll finest-granularity stats up into the ratio and weight surfaces.

    Both classes can be downsampled, and both downsamples are *cost* knobs --
    ranking lift stays the weight surface's job.

    ``default_keep_rate`` is the config's ``dataset.sample_ratio``: the
    pipeline's own fallback for an un-overridden key, and therefore what BOTH
    columns fall back to. Anything else would make the preview disagree with
    what the draw actually does whenever ``sample_ratio < 1``.

    ``pos_ratios`` maps a ``tuple(row[d] for d in ratio_dims)`` -> positive
    keep-rate (missing -> ``default_keep_rate``). ``neg_mults`` maps the same
    tuple -> target neg:pos multiplier; a missing key (or an explicit ``None``,
    and ``default_neg_mult=None`` for the un-listed rest) is "no multiplier
    typed", which falls back to ``default_keep_rate`` -- mirroring the editor's
    empty multiplier cell, which is how every row starts.

    The multiplier is defined on the POST-downsample positive count, so a cell's
    realized neg:pos is what the user typed however hard positives were cut::

        n_pos_post = n_pos * r_pos
        ratio      = clamp(neg_mult * n_pos_post / n_neg, 0, 1)

    ``neg_base`` selects what the weight floor sees: ``"coupled"`` (default) uses
    this surface's post-downsample counts, so the realized pos-rate lands exactly
    at ``t``; ``"decoupled"`` ignores the ratio surface on BOTH sides -- raw
    ``n_pos`` and ``Sum n_neg * phi`` (``phi=1`` means raw negatives). The JS
    mirror in ``_HTML_TEMPLATE`` (``rebuildWeight``) must apply the same branch.

    Pure: no Spark, no I/O. ``stats`` are union-granularity dict rows from
    profile_stats. ``ratio_dims`` is the (possibly empty) list of ratio-surface
    dimensions (sample_group_keys minus label).

    Weight surface (two-factor, ranking lift): aggregate to the ``weight_dims``
    tuple over post-downsample counts; each cell gets the floor factor ``v``
    (lifts effective pos-rate to ``t``) and attention ``A = (m_min/m)^alpha``
    (m = n_pos_post + n_neg_post*v, proportional to n_pos_post; m_min over cells
    with positives), so ``w_pos = A``, ``w_neg = A*v``. A cell whose positives
    are cut to zero goes neutral (``v = A = 1``) -- it leaves the weight model
    without erroring, which is why ``ratio_rows`` carries ``n_pos_post`` for the
    caller to flag.

    Both ratio_rows and weight_rows carry a variable-length ``keys`` list.
    """
    pos_ratios = pos_ratios or {}
    # --- ratio surface: aggregate to ratio_dims tuple ---
    racc: dict = {}
    for s in stats:
        k = tuple(s[d] for d in ratio_dims)
        a = racc.setdefault(k, [0, 0])
        a[0] += s["n_pos"]
        a[1] += s["n_neg"]
    pos_ratio_by_key: dict = {}
    ratio_by_key: dict = {}
    ratio_rows: list[dict] = []
    for key, (npos, nneg) in racc.items():
        raw_pos = pos_ratios.get(key)
        if raw_pos is None:
            raw_pos = default_keep_rate
        r_pos = min(1.0, max(0.0, float(raw_pos)))
        pos_ratio_by_key[key] = r_pos
        npos_post = round(npos * r_pos)
        mult = neg_mults.get(key, default_neg_mult)
        # Exact mirror of the editor's preview()/keepRate() branch order.
        if npos_post == 0:
            # No positives left, so neg:pos means nothing and suggest_ratio
            # would return 0 -- silently deleting the cell's whole negative set.
            # Fall back to the draw's own default, NOT to "keep all": with
            # sample_ratio < 1 the draw really does thin these negatives.
            ratio = min(1.0, max(0.0, float(default_keep_rate)))
        elif nneg == 0:
            ratio = 1.0                       # nothing to downsample
        elif mult is None:
            ratio = min(1.0, max(0.0, float(default_keep_rate)))
        else:
            ratio = suggest_ratio(npos_post, nneg, float(mult))
        ratio_by_key[key] = ratio
        kept = round(nneg * ratio)
        total = npos_post + kept
        ratio_rows.append({
            "keys": list(key), "n_pos": npos, "n_neg": nneg,
            "pos_rate": (npos / (npos + nneg) if npos + nneg else 0.0),
            "pos_ratio": r_pos, "n_pos_post": npos_post,
            "neg_mult": mult, "ratio": ratio, "kept_neg": kept,
            "new_pos_rate": (npos_post / total if total else 0.0),
        })
    ratio_rows.sort(key=lambda r: tuple(str(x) for x in r["keys"]))

    # --- weight surface: two-factor (floor v + attention A) per weight_dims ---
    weight_rows: list[dict] = []
    if weight_dims:
        wacc: dict = {}
        for s in stats:
            wk = tuple(s[d] for d in weight_dims)
            rk = tuple(s[d] for d in ratio_dims)
            # [raw n_pos, n_pos_post (fractional), n_neg_post (fractional)]
            a = wacc.setdefault(wk, [0, 0.0, 0.0])
            a[0] += s["n_pos"]
            if neg_base == "decoupled":
                a[1] += s["n_pos"]                        # raw, ratio-independent
                a[2] += s["n_neg"] * phi                  # raw * phi
            else:
                a[1] += s["n_pos"] * pos_ratio_by_key[rk]
                a[2] += s["n_neg"] * ratio_by_key[rk]
        # attention reference: smallest floor-weighted mass among cells WITH
        # positives (m = n_pos_post/t, proportional to n_pos_post), so the
        # least-positive cell gets A=1.
        masses = [npp + nnp * floor_weight(npp, nnp, t)
                  for _, npp, nnp in wacc.values() if npp > 0]
        m_min = min(masses) if masses else 1.0
        for wk, (npos_raw, npos_post, nneg_post) in wacc.items():
            tf = two_factor_weights(npos_post, nneg_post, t=t, alpha=alpha,
                                    m_min=m_min)
            nat_logit = (math.log(npos_post / nneg_post)
                         if npos_post > 0 and nneg_post > 0 else float("-inf"))
            weight_rows.append({
                "keys": list(wk), "n_pos": npos_raw,
                "n_pos_post": round(npos_post), "n_neg_post": round(nneg_post),
                "v": tf["v"], "A": tf["A"],
                "w_pos": round(tf["w_pos"], 6), "w_neg": round(tf["w_neg"], 6),
                "floored_neg_mass": round(nneg_post * tf["v"]),
                "eff_pos_rate": tf["eff_pos_rate"],
                "nat_logit": nat_logit,
                "attn_mass": tf["A"] * tf["m"],
                "dropped_from_model": npos_post <= 0,
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
    Each ratio row carries ``ratio`` (negatives) and ``ratio_pos`` (positives)
    and so emits up to two override entries, distinguished by the label
    component -- ``"0"`` and ``"1"``. A pre-two-class export has no
    ``ratio_pos``; it read as "positives untouched", which is exactly
    ``default_ratio``, so it round-trips to the same YAML it used to produce.
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
        # Two independent cost knobs on one cell; each is emitted only when it
        # deviates from the config default the pipeline would otherwise apply.
        for lbl, rkey in ((_NEG_LABEL, "ratio"), (_POS_LABEL, "ratio_pos")):
            ratio = float(row.get(rkey, default_ratio))
            if ratio == default_ratio:
                continue
            # Walk the full group_keys order: label position -> this class's
            # component, every other position -> the next value from this row's
            # ratio_dims keys.
            vals = iter(row["keys"])
            parts = [lbl if k == label_col else str(next(vals))
                     for k in cfg_group]
            overrides["|".join(parts)] = ratio
    # Each weight cell emits two label-aware entries (two-factor model):
    # positive rows (label component "1") -> w_pos, negative ("0") -> w_neg.
    # Reconstruct each key by walking the full sample_weight_keys, substituting
    # the label slot (handles label at any position).
    weights: dict[str, float] = {}
    for row in export.get("weight_rows", []):
        for lbl, wkey in (("1", "w_pos"), (_NEG_LABEL, "w_neg")):
            weight = float(row[wkey])
            if weight == default_weight:
                continue
            vals = iter(row["keys"])
            parts = [lbl if k == label_col else str(next(vals)) for k in cfg_weight]
            weights["|".join(parts)] = weight

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
 td.dead{{background:#f8d7da;color:#842029;font-weight:bold}}
 tr.deadrow td{{opacity:.7}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
 details{{background:#eef6ff;border:1px solid #cde;padding:.5rem 1rem;
  margin:.6rem 0;max-width:60rem}}
 details summary{{cursor:pointer;font-weight:bold}}
 details code{{background:#fff;padding:0 .25rem;border:1px solid #ddd}}
 #tabs{{margin:.6rem 0}}
 #tabs button.active{{background:#cde;font-weight:bold}}
 #flt{{margin:.5rem 0;padding:.35rem;width:22rem}}
 #totrow td{{background:#dbeeff;font-weight:bold;border-top:2px solid #69f}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<details open><summary>各欄速覽（完整概念框架、推導與範例見 docs，見下方連結）</summary>
<p><b>兩個面（分頁切換，各自獨立匯出、各自的 key-set）。</b>
<code>ratio 面</code>＝訓練<b>成本</b>：依 <code>sample_group_keys</code> 下採樣本
（→ <code>sample_ratio_overrides</code>）。<b>正、負樣本各有一個保留率</b>，兩欄預設都＝設定檔的
<code>sample_ratio</code>——那才是 pipeline 對沒被 override 的 key 真正會用的值。
<code>weight 面</code>＝排序<b>抬升</b>：依 <code>sample_weight_keys</code> 調樣本權重
（→ <code>sample_weights</code>；雙因子 <code>v×A</code>，全域旋鈕 <code>t</code>、<code>α</code>）。</p>
<p><b>砍正樣本只為省成本，不為調排序</b>——排序抬升是 weight 面 <code>A</code> 因子的事。
<code>n_pos(後)</code> 欄會標色：低於警告門檻標黃、<b>歸零標紅</b>。歸零的格在 weight 面
<code>v=A=1</code>，看起來像「不用調」，其實是「整格沒被算進權重模型」。</p>
<p><b>匯出與驗證提醒。</b>ratio 面每格最多出兩筆：<code>…|1</code>＝正樣本保留率、
<code>…|0</code>＝負樣本保留率；等於預設值的不匯出（稀疏）。
<code>w_pos = A</code>（key <code>…|1</code>）、<code>w_neg = A·v</code>（key <code>…|0</code>）；
<code>sample_weight_keys</code> 須含 <code>label</code> 當正/負切分軸。
綠欄是加權後驗證：eff pos_rate(後) 每列應 = <code>t</code>、地板 logit(後) 每列應相同。</p>
<details><summary>公式速查（保留率 / v / A / w）</summary>
<p><code>n_pos(後) = round(n_pos × 正樣本保留率)</code>——先砍正樣本，後面每個數字都以它為準。<br>
<code>負樣本保留率 = clamp(倍率 × n_pos(後) / n_neg, 0, 1)</code>——倍率的分子是<b>下採後</b>的
正樣本，所以「實際倍率」欄會等於你填的值；正樣本砍一半，負樣本跟著砍一半。<br>
<code>n_pos(後)=0</code> 的格：neg:pos 無定義，負樣本欄改成直接填保留率。<br>
<code>v = n_pos(1−t) / (t · n_neg)</code> — <b>地板</b>：降負樣本權重把有效正樣本率墊到 <code>t</code>。<br>
<code>A = (m_min / m)^α</code>，<code>m = n_pos + n_neg·v</code> — <b>注意力</b>：把各 item 的
loss 佔比拉向等權（最輕者 A=1、越熱越小、≤1）。<br>
<code>w_pos = A</code>、<code>w_neg = A·v</code>。<br>
基數：<b>連動</b>(預設) 用 ratio 面下採後的正負數 → 套用後 pos-rate 精確 = <code>t</code>；
<b>不連動</b>正負都回到原始數（負樣本再 × φ）。</p>
</details>
<p><b>完整概念與範例</b>：<code>docs/operations/user-guides/sampling-overrides-editor.md</code>
（<a href="../../docs/operations/user-guides/sampling-overrides-editor.md">相對連結</a>，若從 repo 的
<code>data/profiling/</code> 開啟此頁可直接點開）。</p>
</details>
<div id="tabs">
<button id="tb_ratio" class="active" onclick="setTab('ratio')">ratio 面 (sample_group_keys)</button>
<button id="tb_weight" onclick="setTab('weight')">weight 面 (sample_weight_keys)</button>
</div>
<div id="rmodebar">ratio 面負樣本輸入模式：
 <label><input type="radio" name="rmode" value="mult" checked onclick="setRmode('mult')"> 依負樣本倍率</label>
 <label><input type="radio" name="rmode" value="keep" onclick="setRmode('keep')"> 依保留率</label>
 · 正樣本過少警告門檻 <input id=poswarn type=number step=1 min=0 value="{pos_warn_min}" oninput="onPosWarn()">
 <span class=stat>（n_pos(後) 低於此值標黃；歸零一律標紅）</span></div>
<div id="knobs">weight 面旋鈕：
 t（目標正樣本率）<input id=t type=number step=0.01 min=0 max=1 value="{t}" oninput="onKnob()">
 · α（注意力阻尼）<input id=alpha type=number step=0.1 min=0 value="{alpha}" oninput="onKnob()">
 · 基數
 <label><input type=radio name=wbase value=couple checked onclick="setWbase('couple')"> 連動 ratio 面</label>
 <label><input type=radio name=wbase value=decouple onclick="setWbase('decouple')"> 不連動（正負都回原始）</label>
 φ <input id=wphi type=number step=0.05 min=0 max=1 value="1.00" disabled oninput="onKnob()"></div>
<div id="note"></div>
<div id="sumbox"><label>分組試算（下採後）：<select id="grp" onchange="renderSummary()"></select></label>
<div id="summary"></div></div>
<div id="grpsel">依群組選取：維度
 <select id="gdim" onchange="fillGroupVals()"></select> =
 <select id="gval"></select>
 <button onclick="groupSelect('add')">加入選取</button>
 <button onclick="groupSelect('only')">只選此群組</button></div>
<div id="batchsel">批次套用：對 <b id="rselcount">0</b> 個選取列設
 正樣本保留率 <input id="rbatchpos" type="number" step="0.05" min="0" max="1" placeholder="不改">
 · <span id="rbatchneglab">負樣本倍率</span> <input id="rbatchneg" type="number" step="0.5" min="0" placeholder="不改">
 <button onclick="applyBatch()">套用到選取列</button>
 <button onclick="selectAllVisible(true)">全選（可見）</button>
 <button onclick="clearRsel()">清除選取</button>
 <label><input type="checkbox" id="rclearafter"> 套用後清除</label></div>
<input id="flt" placeholder="篩選…" oninput="flt()">
<table id="g"><thead></thead><tbody></tbody></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const STATS={stats_json};
const GKEYS={gkeys_json};
const GROUP_KEYS={group_keys_json};
const WDIMS={wdims_json};
const LABEL="{label_col}";
const WKEYS={wkeys_json};
const DR={default_ratio};   // config 的 sample_ratio：正負兩欄共同的預設保留率
const R={target_neg_pos};
let T={t};
let ALPHA={alpha};
let POSWARN={pos_warn_min};
let WBASE='couple';   // 'couple'=連動 ratio 面(下採後) | 'decouple'=不連動(原始，負樣本×PHI)
let PHI=1;            // 不連動時的全域負樣本保留率
function setWbase(b){{ WBASE=b;
 const el=document.getElementById('wphi'); if(el) el.disabled=(b!=='decouple');
 rebuildWeight(); if(tab==='weight') render(); }}
const SEP='\\u0001';
function clamp01(x){{ return Math.min(1,Math.max(0,x)); }}
// two-factor weight (mirror of Python two_factor_weights/floor_weight)
function floorWeight(np,nnp,t){{ if(np<=0||nnp<=0) return 1;
 return np*(1-t)/(t*nnp); }}
function twoFactor(np,nnp,t,a,mmin){{
 if(np<=0) return {{w_pos:1,w_neg:1,v:1,A:1,m:nnp,eff:0}};
 const v=floorWeight(np,nnp,t),m=np+nnp*v,A=Math.pow(mmin/m,a);
 return {{w_pos:A,w_neg:A*v,v:v,A:A,m:m,eff:np/(np+nnp*v)}}; }}
function esc(s){{ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
 .replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }}
// 正樣本先砍：後面每個數字（含負樣本倍率的分子）都以 n_pos(後) 為準。
function posRatio(r){{ let v=parseFloat(r.rpos); if(isNaN(v)) v=DR; return clamp01(v); }}
function nPosPost(r){{ return Math.round(r.n_pos*posRatio(r)); }}
// 鏡像 Python aggregate_surfaces 的兩個 guard：n_neg=0 或 n_pos(後)=0 → 全留負樣本。
function keepRate(nm,npPost,nn){{ if(nn<=0||npPost<=0) return 1;
 if(isNaN(nm)) return DR;   // 沒填倍率 → 落回設定檔預設保留率
 return clamp01(nm*npPost/nn); }}
// ratio store: one row per ratio_dims tuple; 兩個保留率預設都是 DR。
function buildRatio(){{
 const m=new Map();
 STATS.forEach(s=>{{ const keys=GKEYS.map(k=>s[k]),ks=keys.join(SEP);
  const a=m.get(ks)||{{keys:keys,n_pos:0,n_neg:0}};
  a.n_pos+=s.n_pos; a.n_neg+=s.n_neg; m.set(ks,a); }});
 const rows=[...m.values()];
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.rpos=DR; r.suggested_neg_mult=''; r.ratio_direct=DR;
  r.keys.forEach((v,j)=>r['k'+j]=v); }});
 return rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
const RATIO=buildRatio();
let WEIGHT=[];
// 兩個保留率 per ratio_dims tuple，直接讀 preview()，不另寫第二份算式。
function surfaceByKey(){{
 const neg=new Map(),pos=new Map();
 RATIO.forEach(r=>{{ const k=r.keys.join(SEP);
  neg.set(k,preview(r).ratio); pos.set(k,posRatio(r)); }});
 return {{neg:neg,pos:pos}};
}}
// weight store: aggregate STATS to WDIMS tuple; 正負兩邊都吃下採後的數。
function rebuildWeight(){{
 if(!WDIMS.length){{ WEIGHT=[]; return; }}
 const sbk=surfaceByKey(),m=new Map();
 STATS.forEach(s=>{{ const wk=WDIMS.map(k=>s[k]),ks=wk.join(SEP);
  const rk=GKEYS.map(k=>s[k]).join(SEP);
  const a=m.get(ks)||{{keys:wk,n_pos_raw:0,n_neg_raw:0,_np:0,_nn:0}};
  a.n_pos_raw+=s.n_pos; a.n_neg_raw+=s.n_neg;
  if(WBASE==='decouple'){{ a._np+=s.n_pos; a._nn+=s.n_neg*PHI; }}
  else {{ a._np+=s.n_pos*sbk.pos.get(rk); a._nn+=s.n_neg*sbk.neg.get(rk); }}
  m.set(ks,a); }});
 const cells=[...m.values()];
 const masses=cells.filter(c=>c._np>0)
  .map(c=>c._np+c._nn*floorWeight(c._np,c._nn,T));
 const mmin=masses.length?Math.min.apply(null,masses):1;
 const FL=Math.log(T/(1-T));
 let hot=-Infinity;
 cells.forEach(c=>{{ c.nat_logit=(c._np>0&&c._nn>0)?Math.log(c._np/c._nn):-Infinity;
  if(c._np>0&&c.nat_logit>hot) hot=c.nat_logit; }});
 cells.forEach(c=>{{ const tf=twoFactor(c._np,c._nn,T,ALPHA,mmin);
  c.n_pos_post=Math.round(c._np); c.n_neg_post=Math.round(c._nn);
  c.dropped=(c._np<=0); c.v=tf.v; c.A=tf.A;
  c.w_pos=+tf.w_pos.toFixed(6); c.w_neg=+tf.w_neg.toFixed(6);
  c.floored_neg_mass=Math.round(c._nn*tf.v); c.eff_pos_rate=tf.eff;
  c.floor_logit=(c._np>0?FL:c.nat_logit); c.attn_mass=tf.A*tf.m;
  c.nat_gap=(c._np>0?hot-c.nat_logit:NaN);
  c.keys.forEach((v,j)=>c['k'+j]=v); }});
 WEIGHT=cells.sort((x,y)=>y.n_pos_post-x.n_pos_post);
}}
let tab='ratio',sortKey=null,sortAsc=true;
let RMODE='mult';   // 'mult'=依負樣本倍率(算保留率) | 'keep'=依保留率(算倍率)
function setRmode(m){{ syncEdits(); RMODE=m;
 document.getElementById('rbatchneglab').textContent=
  (m==='keep'?'負樣本保留率':'負樣本倍率');
 rebuildWeight(); if(tab==='ratio') render(); }}
function onPosWarn(){{ const v=parseFloat(document.getElementById('poswarn').value);
 POSWARN=(isNaN(v)||v<0)?0:v; if(tab==='ratio') render(); }}
// ---- ratio 選取集（以 RATIO 索引識別）＋群組/批次套用 ----
const RSEL=new Set();
function rselCount(){{ const el=document.getElementById('rselcount'); if(el) el.textContent=RSEL.size; }}
function visIdx(){{
 const q=(document.getElementById('flt').value||'').toLowerCase();
 const cols=GKEYS.map((_,j)=>'k'+j);
 return RATIO.map((_,i)=>i).filter(i=>!q ||
   cols.map(c=>RATIO[i][c]).join(' ').toLowerCase().indexOf(q)>=0);
}}
function toggleRow(i,on){{ if(on) RSEL.add(i); else RSEL.delete(i); rselCount(); }}
function selectAllVisible(on){{ visIdx().forEach(i=>{{ if(on===false) RSEL.delete(i); else RSEL.add(i); }});
 if(tab==='ratio') render(); }}
function clearRsel(){{ RSEL.clear(); if(tab==='ratio') render(); }}
function initGroupSel(){{
 const gd=document.getElementById('gdim');
 gd.innerHTML=GKEYS.map((k,j)=>`<option value="${{j}}">${{esc(k)}}</option>`).join('');
 fillGroupVals();
}}
function fillGroupVals(){{
 const j=+document.getElementById('gdim').value;
 const vals=[...new Set(RATIO.map(r=>r['k'+j]))].sort();
 document.getElementById('gval').innerHTML=
   vals.map(v=>`<option value="${{esc(v)}}">${{esc(v)}}</option>`).join('');
}}
function groupSelect(kind){{
 const j=+document.getElementById('gdim').value, val=document.getElementById('gval').value;
 if(kind==='only') RSEL.clear();
 RATIO.forEach((r,i)=>{{ if(String(r['k'+j])===String(val)) RSEL.add(i); }});
 if(tab==='ratio') render();
}}
function applyBatch(){{
 const rp=document.getElementById('rbatchpos').value;
 const rn=document.getElementById('rbatchneg').value;
 if(rp===''&&rn==='') return;
 let skipped=0;
 RSEL.forEach(i=>{{ const r=RATIO[i];
   if(rp!=='') r.rpos=rp;
   if(rn!==''){{
     // 正樣本先套，再看這列還有沒有正樣本：倍率對 n_pos(後)=0 的列無定義，
     // 硬寫進保留率欄會把「倍率 5」讀成「保留率 5」→ clamp 成全留，是靜默誤解。
     if(RMODE==='keep') r.ratio_direct=rn;
     else if(nPosPost(r)<=0) skipped++;
     else r.suggested_neg_mult=rn;
   }} }});
 if(document.getElementById('rclearafter').checked) RSEL.clear();
 if(tab==='ratio') render(); else rebuildWeight();
 if(skipped) alert(`已套用；負樣本值略過 ${{skipped}} 個 n_pos(後)=0 的列（倍率無定義，請改保留率模式）`);
}}
function rows(){{ return tab==='ratio'?RATIO:WEIGHT; }}
function preview(r){{
 const npPost=nPosPost(r);
 // keep 模式或正樣本已歸零 → 直填負樣本保留率；否則由倍率推算。
 const useDirect=(RMODE==='keep')||(npPost<=0);
 const nm=parseFloat(r.suggested_neg_mult);
 let kr;
 if(useDirect){{ kr=parseFloat(r.ratio_direct); if(isNaN(kr)) kr=DR; kr=clamp01(kr); }}
 else kr=keepRate(nm,npPost,r.n_neg);
 const kept=Math.round(r.n_neg*kr),total=npPost+kept;
 return {{npPost:npPost,ratio:kr,kn:kept,total:total,
  pr:(total>0?npPost/total:0),
  achieved:(npPost>0?kept/npPost:0),
  clamped:(!useDirect && r.n_neg>0 && nm*npPost/r.n_neg>1),
  noNeg:r.n_neg<=0,noPos:npPost<=0}};
}}
function achMult(pv){{
 if(pv.noPos) return {{cls:'calc',html:'—',title:'n_pos(後)=0，neg:pos 無定義；負樣本保留率可直接設定'}};
 if(pv.noNeg) return {{cls:'calc',html:'0.0',title:'無負樣本，不下採樣'}};
 if(pv.clamped) return {{cls:'warn',html:pv.achieved.toFixed(1)+' ⚠',
  title:'負樣本不足以達到目標倍率，已全留'}};
 return {{cls:'calc',html:pv.achieved.toFixed(1),title:''}};
}}
function posCls(npPost){{ return npPost<=0?'dead':(npPost<POSWARN?'warn':'calc'); }}
function posTitle(npPost){{
 if(npPost<=0) return '正樣本被砍光：這格在 weight 面 v=A=1，整格退出權重模型（不是「不用調」）';
 if(npPost<POSWARN) return '正樣本低於警告門檻 '+POSWARN+'，這格可能學不動';
 return '';
}}
function syncEdits(){{
 document.querySelectorAll('#g td.edit').forEach(td=>{{
  const v=td.textContent.trim();
  const r=rows()[+td.dataset.i];
  if(td.dataset.k==='rpos') r.rpos=v;
  else if(td.dataset.k==='neg_mult') r.suggested_neg_mult=v;
  else if(td.dataset.k==='ratio_direct') r.ratio_direct=v;
 }});
}}
function recalc(td){{
 const r=rows()[+td.dataset.i],tr=td.closest('tr'),v=td.textContent.trim();
 const k=td.dataset.k;
 if(k==='rpos') r.rpos=v;
 else if(k==='ratio_direct') r.ratio_direct=v;
 else r.suggested_neg_mult=v;
 const pv=preview(r),am=achMult(pv);
 const np=tr.querySelector('td.np');
 np.className=posCls(pv.npPost)+' np'; np.textContent=pv.npPost;
 np.title=posTitle(pv.npPost);
 // 改正樣本保留率會翻轉「倍率/保留率」哪一欄可編（n_pos(後) 跨過 0），
 // 這種結構性變化重繪整列不划算也會吃掉游標，交給下一次 render()。
 const rt=tr.querySelector('td.rt');
 if(!rt.isContentEditable) rt.textContent=pv.ratio.toFixed(4);
 const a=tr.querySelector('td.am'); a.className=am.cls+' am';
 a.innerHTML=am.html; a.title=am.title;
 tr.querySelector('td.kn').textContent=pv.kn;
 tr.querySelector('td.pr').textContent=pv.pr.toFixed(4);
 renderSummary();
}}
function renderRatio(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+`<th><input type=checkbox onclick="selectAllVisible(this.checked)"></th>`+
  GKEYS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat onclick="sortBy('n_neg')">n_neg ⇅</th>`+
  `<th class=stat>pos_rate</th>`+
  `<th>正樣本<br>保留率</th><th class=calc>n_pos<br>(後)</th>`+
  `<th>負樣本倍率</th><th class=calc>負樣本<br>保留率</th>`+
  `<th class=calc>實際倍率</th><th class=calc>kept_neg</th>`+
  `<th class=calc>new_pos_rate</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],pv=preview(r);
  const am=achMult(pv),tr=document.createElement('tr');
  if(pv.noPos) tr.className='deadrow';
  const editKeep=(RMODE==='keep')||pv.noPos;
  const negMultCell=editKeep
   ?`<td class=calc title="保留率模式／n_pos(後)=0：未使用倍率">—</td>`
   :`<td class=edit contenteditable data-k=neg_mult data-i=${{i}} oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`;
  const ratioCell=editKeep
   ?`<td class="edit rt" contenteditable data-k=ratio_direct data-i=${{i}} oninput="recalc(this)">${{r.ratio_direct}}</td>`
   :`<td class="calc rt">${{pv.ratio.toFixed(4)}}</td>`;
  tr.innerHTML=`<td><input type=checkbox ${{RSEL.has(i)?'checked':''}} onclick="toggleRow(${{i}},this.checked)"></td>`+
   r.keys.map(v=>`<td>${{esc(v)}}</td>`).join('')+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=rpos data-i=${{i}} oninput="recalc(this)">${{r.rpos}}</td>`+
   `<td class="${{posCls(pv.npPost)}} np" title="${{posTitle(pv.npPost)}}">${{pv.npPost}}</td>`+
   negMultCell+ratioCell+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr.toFixed(4)}}</td>`;
  tb.appendChild(tr); }});
 rselCount();
}}
function renderWeight(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+WDIMS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos_raw')">n_pos<br>(原始) ⇅</th>`+
  `<th class=calc onclick="sortBy('n_pos_post')">n_pos<br>(下採後) ⇅</th>`+
  `<th class=stat>n_neg<br>(原始)</th><th class=stat>n_neg<br>(下採後)</th>`+
  `<th class=stat>自然<br>logit</th>`+
  `<th class=calc>nat 差距</th><th class=calc>odds<br>(e^Δ)</th>`+
  `<th class=calc>v</th><th class=calc>A</th>`+
  `<th class=calc>w_pos</th><th class=calc>w_neg</th>`+
  `<th class=calc>有效負<br>樣本質量</th><th class=calc>eff pos<br>_rate(後)</th>`+
  `<th class=calc>地板<br>logit(後)</th><th class=calc>A·m</th>`+
  `<th class=calc>狀態</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],tr=document.createElement('tr'),zero=r.dropped;
  if(zero) tr.className='deadrow';
  const e=zero?0:Math.exp(r.nat_gap);
  const gap=zero?'':r.nat_gap.toFixed(2);
  const odds=zero?'':(e>=10?String(Math.round(e)):e.toFixed(1));
  tr.innerHTML=r.keys.map(v=>`<td>${{esc(v)}}</td>`).join('')+
   `<td class=stat>${{r.n_pos_raw}}</td>`+
   `<td class="${{zero?'dead':'calc'}}">${{r.n_pos_post}}</td>`+
   `<td class=stat>${{r.n_neg_raw}}</td>`+
   `<td class=stat>${{r.n_neg_post}}</td>`+
   `<td class=stat>${{zero?'—':r.nat_logit.toFixed(3)}}</td>`+
   `<td class=calc>${{gap}}</td><td class=calc>${{odds}}</td>`+
   `<td class=calc>${{(+r.v).toPrecision(3)}}</td><td class=calc>${{r.A.toFixed(4)}}</td>`+
   `<td class=calc>${{r.w_pos}}</td><td class=calc>${{r.w_neg}}</td>`+
   `<td class=calc>${{r.floored_neg_mass}}</td>`+
   `<td class=calc>${{r.eff_pos_rate.toFixed(4)}}</td>`+
   `<td class=calc>${{zero?'—':r.floor_logit.toFixed(3)}}</td>`+
   `<td class=calc>${{Math.round(r.attn_mass)}}</td>`+
   `<td class="${{zero?'dead':'calc'}}">${{zero?'退出權重模型 (v=A=1)':'正常'}}</td>`;
  tb.appendChild(tr); }});
}}
function render(){{
 const data=rows();
 data.forEach(r=>{{ if(r.keys) r.keys.forEach((v,j)=>r['k'+j]=v); }});
 const cols=(tab==='ratio'?GKEYS:WDIMS).map((_,j)=>'k'+j);
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
 if(t==='weight' && !WDIMS.length){{
  document.getElementById('note').textContent=
   'sample_weight_keys 為空（或只有 label），無 weight 面可編輯。'; return; }}
 tab=t; sortKey=null;
 document.getElementById('tb_ratio').className=(t==='ratio'?'active':'');
 document.getElementById('tb_weight').className=(t==='weight'?'active':'');
 document.getElementById('knobs').style.display=(t==='weight'?'':'none');
 document.getElementById('rmodebar').style.display=(t==='ratio'?'':'none');
 document.getElementById('grpsel').style.display=(t==='ratio'?'':'none');
 document.getElementById('batchsel').style.display=(t==='ratio'?'':'none');
 document.getElementById('note').textContent=
  (t==='weight'
   ?'雙因子：v 把地板墊到 t、A 壓 loss 佔比；正負兩邊都連動 ratio 面的下採。eff/地板欄每列應相同；n_pos(下採後)=0 的列已退出模型。'
   :'');
 if(t==='weight') rebuildWeight();
 render();
}}
function onKnob(){{
 const t=parseFloat(document.getElementById('t').value);
 const a=parseFloat(document.getElementById('alpha').value);
 if(!isNaN(t)&&t>0&&t<1) T=t;
 if(!isNaN(a)&&a>=0) ALPHA=a;
 const p=parseFloat(document.getElementById('wphi').value);
 if(!isNaN(p)&&p>=0&&p<=1) PHI=p;
 rebuildWeight(); if(tab==='weight') render();
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
 let tpo=0,tp=0,tno=0,tn=0;
 RATIO.forEach(r=>{{ const pv=preview(r);
  const g=(by===''?'（全部）':String(r[by]));
  const a=agg.get(g)||{{npo:0,np:0,nno:0,nn:0}};
  a.npo+=r.n_pos; a.np+=pv.npPost; a.nno+=r.n_neg; a.nn+=pv.kn; agg.set(g,a);
  tpo+=r.n_pos; tp+=pv.npPost; tno+=r.n_neg; tn+=pv.kn; }});
 let h='<table><thead><tr><th>分組</th><th>n_pos(原)</th><th>n_pos(後)</th>'+
  '<th>n_neg(原)</th><th>n_neg(後)</th><th>總數(後)</th><th>pos_rate(後)</th>'+
  '</tr></thead><tbody>';
 [...agg.entries()].sort().forEach(([g,a])=>{{ const t=a.np+a.nn;
  h+=`<tr><td>${{esc(g)}}</td><td>${{a.npo}}</td><td>${{a.np}}</td>`+
   `<td>${{a.nno}}</td><td>${{a.nn}}</td><td>${{t}}</td>`+
   `<td>${{(t>0?a.np/t:0).toFixed(4)}}</td></tr>`; }});
 const before=tpo+tno,after=tp+tn;
 const cut=(before>0?(1-after/before)*100:0);
 h+=`<tr id=totrow><td>總計　訓練列數</td><td>${{tpo}}</td><td>${{tp}}</td>`+
  `<td>${{tno}}</td><td>${{tn}}</td>`+
  `<td>${{before}} → ${{after}}（−${{cut.toFixed(1)}}%）</td>`+
  `<td>${{(after>0?tp/after:0).toFixed(4)}}</td></tr>`;
 h+='</tbody></table>';
 document.getElementById('summary').innerHTML=h;
}}
function ratioKey(keys,lbl){{
 let it=0;
 return GROUP_KEYS.map(k=>k===LABEL?lbl:String(keys[it++])).join('|');
}}
function weightKey(keys,lbl){{
 let it=0;
 return WKEYS.map(k=>k===LABEL?lbl:String(keys[it++])).join('|');
}}
function exp(kind){{
 syncEdits(); rebuildWeight();
 const ratio_rows=RATIO.map(r=>{{ const pv=preview(r);
  return {{keys:r.keys,ratio:pv.ratio,ratio_pos:posRatio(r)}}; }});
 const weight_rows=WEIGHT.map(r=>({{keys:r.keys,w_pos:r.w_pos,w_neg:r.w_neg}}));
 const o={{sample_group_keys:GROUP_KEYS,sample_weight_keys:WKEYS,
  ratio_rows:ratio_rows,weight_rows:weight_rows}};
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a'); a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json'; a.click();
 }}else{{
  const ov={{}},sw={{}};
  ratio_rows.forEach(r=>{{ if(r.ratio_pos!==DR) ov[ratioKey(r.keys,'1')]=r.ratio_pos;
   if(r.ratio!==DR) ov[ratioKey(r.keys,'0')]=r.ratio; }});
  weight_rows.forEach(r=>{{ if(r.w_pos!==1.0) sw[weightKey(r.keys,'1')]=r.w_pos;
   if(r.w_neg!==1.0) sw[weightKey(r.keys,'0')]=r.w_neg; }});
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
initGroupSel();
setTab('ratio');
</script></body></html>"""


def render_html(
    stats: list[dict],
    *,
    ratio_dims: list,
    group_keys: list,
    label_col: str,
    weight_keys: list,
    weight_dims: list,
    default_ratio: float,
    t: float = 1 / 6,
    alpha: float = 0.5,
    target_neg_pos: float = 5.0,
    pos_warn_min: float = DEFAULT_POS_WARN_MIN,
) -> str:
    """Render a self-contained two-tab HTML editor (pure stdlib, no assets).

    ``stats`` are union-granularity dict rows from profile_stats; the browser
    mirrors aggregate_surfaces in JS to build the ratio and weight surfaces
    live. ``ratio_dims`` (= sample_group_keys minus label) keys the ratio
    surface, where each cell carries two keep-rates -- positives and negatives,
    both cost knobs, both defaulting to ``default_ratio`` (the config's
    ``dataset.sample_ratio``, which is what the draw falls back to for an
    un-overridden key). ``group_keys`` (full, incl. label) reconstructs the
    ``...|1`` / ``...|0`` ratio override keys. ``weight_dims`` (= sample_weight_keys
    minus label) keys the two-factor weight surface, driven by the global knobs
    ``t`` (target pos-rate floor) and ``alpha`` (attention damping);
    ``weight_keys`` (full, incl. label) reconstructs the weight keys on export.
    ``target_neg_pos`` is only the ratio surface's cost-downsample reference.
    ``pos_warn_min`` is the yellow-flag floor for ``n_pos(post)``; zero positives
    is always flagged red because such a cell silently leaves the weight model.
    """
    return _HTML_TEMPLATE.format(
        stats_json=json.dumps(stats),
        gkeys_json=json.dumps(ratio_dims),
        group_keys_json=json.dumps(group_keys),
        wdims_json=json.dumps(weight_dims),
        label_col=label_col,
        wkeys_json=json.dumps(weight_keys),
        default_ratio=default_ratio,
        target_neg_pos=target_neg_pos,
        alpha=alpha,
        t=t,
        pos_warn_min=pos_warn_min,
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
    t: float = typer.Option(
        1 / 6, help="weight floor target pos-rate (lift all products to this)"),
    alpha: float = typer.Option(
        0.5, help="weight attention damping (0=off, 1=equalize per-product loss)"),
    target_neg_pos: float = typer.Option(
        5.0, help="ratio surface cost-downsample reference neg:pos (default keep-all)"),
    pos_warn_min: float = typer.Option(
        DEFAULT_POS_WARN_MIN,
        help="yellow-flag a cell whose post-downsample n_pos falls below this "
             "(zero is always flagged red); adjustable in the browser too"),
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
        f"ratio_dims={keys['ratio_dims']} weight_dims={keys['weight_dims']} "
        f"label={keys['label_col']} union_dims={keys['union_dims']} "
        f"(t={t} alpha={alpha} default keep-rate={ds.get('sample_ratio', 1.0)} "
        f"pos_warn_min={pos_warn_min})"
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
        weight_dims=keys["weight_dims"],
        default_ratio=float(ds.get("sample_ratio", 1.0)),
        t=t, alpha=alpha, target_neg_pos=target_neg_pos,
        pos_warn_min=pos_warn_min,
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
