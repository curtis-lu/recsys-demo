"""Pair ledger（壓制帳本，框架診斷項目 7；spec §3 Phase 4b）。

在診斷抽樣上（driver-side numpy）做兩件互補的傷害歸因會計：

- ``pair_ledger``（傘函數，node 只呼叫它）：對每個正例列，列舉同 query
  排其上方的**負例** item，記交換兩列名次會讓該 query 的 AP 貢獻總和變
  多少（|ΔAP|，lambdarank 的 λ 梯度定義——這裡只做會計、不訓練）→
  「壓制者 × 受害者」矩陣＋傷害 × segment 分組；並內含 substitution。
- ``substitution_ablation``：逐 item 把分數換成該 item base rate 的
  logit 常數、重算參數化 macro mAP（O(M) 次）→ 每 item 個性化分數的
  淨貢獻／淨傷害（delta_vs_current 負＝淨貢獻、正＝淨傷害）。

設計要點（計畫「設計定案」節的落地）：
- 排序與 ``positive_row_contributions`` 完全同款 lexsort；k 截斷語意
  跟 ``evaluation.metric.k`` 一致。
- |ΔAP| 是 query 層 AP 貢獻和的精確變化量，**不是** macro per-item mAP
  的全式變化（per-item 分母跨 query）——判讀用相對量。
- 全樣本不切折：描述性會計、無擬合搜尋，無過擬合疑慮。
- 注入（debug_inject_offsets）語意與 offset_sweep 一致（_common 共用）：
  一切計算之前加到 logit 分數上，map_current＝注入後現狀。
- segment 欄由上游 join 進 eval_predictions，這裡只消費欄位（spec 明文
  邊界：不 import evaluation/segments.py）。
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import (
    _CLIP_EPS, apply_injection, metric_params, parse_injection, to_logit,
)
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)


def _arrays(sample_pdf: pd.DataFrame, parameters: dict):
    """共用前處理：groups／items／y／z（含注入）＋notes。"""
    schema = get_schema(parameters)
    query_cols = [schema["time"], *schema["entity"]]
    notes: list[str] = []
    groups = (
        sample_pdf.groupby(query_cols, sort=False, dropna=False)
        .ngroup()
        .to_numpy()
    )
    items = sample_pdf[schema["item"]].astype(str).to_numpy()
    y = (sample_pdf[schema["label"]].to_numpy() == 1)
    z, z_notes = to_logit(sample_pdf[schema["score"]].to_numpy())
    notes.extend(z_notes)
    inject = parse_injection(parameters)
    z, inj_notes = apply_injection(z, items, inject)
    notes.extend(inj_notes)
    return groups, items, y, z, inject, schema, notes


def substitution_ablation(
    sample_pdf: pd.DataFrame, parameters: dict
) -> dict:
    mp = metric_params(parameters)
    out: dict = {"map_current": None, "substitution": {}, "notes": []}
    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——substitution ablation 未執行")
        return out
    groups, items, y, z, _inject, _schema, notes = _arrays(
        sample_pdf, parameters
    )
    out["notes"] = notes
    yf = y.astype(np.float64)
    map_current = float(
        compute_macro_per_item_map(groups, items, yf, z, **mp)
    )
    out["map_current"] = map_current
    for it in sorted(set(items.tolist())):
        mask = items == it
        base_rate = float(yf[mask].mean())
        p = min(max(base_rate, _CLIP_EPS), 1.0 - _CLIP_EPS)
        base_logit = float(np.log(p / (1.0 - p)))
        z_sub = z.copy()
        z_sub[mask] = base_logit
        m = float(compute_macro_per_item_map(groups, items, yf, z_sub, **mp))
        out["substitution"][it] = {
            "base_rate": base_rate,
            "base_logit": base_logit,
            "map_substituted": m,
            "delta_vs_current": m - map_current,
        }
    return out


def pair_ledger(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    schema = get_schema(parameters)
    mp = metric_params(parameters)
    k = mp["k"]
    seg_cols_cfg = list(
        (parameters.get("evaluation", {}) or {}).get("segment_columns", [])
        or []
    )
    notes: list[str] = []
    out: dict = {
        "enabled": True,
        "score_col_used": schema["score"],
        "metric_params": mp,
        "injected_offsets": {},
        "n_queries": 0,
        "n_pos_rows": 0,
        "n_mis_ordered_pairs": 0,
        "matrix": {},
        "by_suppressor": {},
        "by_victim": {},
        "map_current": None,
        "substitution": {},
        "by_segment": {},
        "notes": notes,
    }
    if len(sample_pdf) == 0:
        notes.append("診斷抽樣為空——pair ledger 未執行")
        return out

    groups, items, y, z, inject, _schema, arr_notes = _arrays(
        sample_pdf, parameters
    )
    notes.extend(arr_notes)
    out["injected_offsets"] = inject

    seg_cols = [c for c in seg_cols_cfg if c in sample_pdf.columns]
    missing = [c for c in seg_cols_cfg if c not in sample_pdf.columns]
    if missing:
        notes.append(f"segment 欄不在抽樣中，by_segment 略過：{missing}")

    # ---- pair 枚舉（與 positive_row_contributions 同款 lexsort）----
    sort_idx = np.lexsort((-z, groups))
    g_s = groups[sort_idx]
    y_s = y[sort_idx].astype(np.float64)
    it_s = items[sort_idx]
    boundaries = np.concatenate([
        [0], np.flatnonzero(np.diff(g_s)) + 1, [len(g_s)],
    ])

    pair_count: dict = {}
    dap_sum: dict = {}
    row_harm = np.zeros(len(sample_pdf), dtype=np.float64)
    row_suppressed = np.zeros(len(sample_pdf), dtype=bool)
    n_pairs = 0
    n_pos_rows = 0
    for qi in range(len(boundaries) - 1):
        s, e = boundaries[qi], boundaries[qi + 1]
        yq = y_s[s:e]
        n_pos_rows += int(yq.sum())
        if yq.sum() == 0:
            continue
        L = e - s
        ranks = np.arange(1, L + 1, dtype=np.float64)
        k_eff = float(k) if k is not None else float(L)
        cum = np.cumsum(yq)
        contrib = np.where(ranks <= k_eff, cum / ranks, 0.0)
        s_prefix = np.cumsum(
            np.where((yq == 1) & (ranks <= k_eff), 1.0 / ranks, 0.0)
        )
        pos_pos = np.flatnonzero(yq == 1)
        neg_pos = np.flatnonzero(yq == 0)
        for b in pos_pos:
            if b == 0:
                continue
            above = neg_pos[neg_pos < b]
            if len(above) == 0:
                continue
            a_rank = above + 1.0
            new_c = np.where(
                a_rank <= k_eff, (cum[above] + 1.0) / a_rank, 0.0
            )
            spill = s_prefix[b - 1] - s_prefix[above]
            dap = new_c - contrib[b] + spill
            victim = str(it_s[s + b])
            orig_row = sort_idx[s + b]
            row_harm[orig_row] += float(dap.sum())
            row_suppressed[orig_row] = True
            n_pairs += len(above)
            for j, d in zip(above, dap):
                key = (str(it_s[s + j]), victim)
                pair_count[key] = pair_count.get(key, 0) + 1
                dap_sum[key] = dap_sum.get(key, 0.0) + float(d)

    out["n_queries"] = int(len(boundaries) - 1)
    out["n_pos_rows"] = int(n_pos_rows)
    out["n_mis_ordered_pairs"] = int(n_pairs)

    total = float(sum(dap_sum.values()))
    matrix: dict = {}
    for (sup, vic), c in pair_count.items():
        matrix.setdefault(sup, {})[vic] = {
            "pair_count": c, "dap_sum": dap_sum[(sup, vic)],
        }
    out["matrix"] = {
        sup: dict(sorted(v.items())) for sup, v in sorted(matrix.items())
    }

    def _marginal(axis: int) -> dict:
        agg: dict = {}
        for key, c in pair_count.items():
            a = agg.setdefault(
                key[axis], {"pair_count": 0, "dap_sum": 0.0}
            )
            a["pair_count"] += c
            a["dap_sum"] += dap_sum[key]
        for a in agg.values():
            a["dap_share"] = (a["dap_sum"] / total) if total > 0 else None
        return {k_: agg[k_] for k_ in sorted(agg)}

    out["by_suppressor"] = _marginal(0)
    out["by_victim"] = _marginal(1)

    # ---- by_segment（傷害集中在誰身上）----
    pos_mask = y
    for c in seg_cols:
        raw = sample_pdf[c].to_numpy()
        vals = np.where(pd.isna(raw), "null", raw.astype(str))
        block: dict = {}
        for v in sorted(set(vals[pos_mask].tolist())):
            m = pos_mask & (vals == v)
            dsum = float(row_harm[m].sum())
            block[v] = {
                "n_pos_rows": int(m.sum()),
                "n_suppressed_pos_rows": int((m & row_suppressed).sum()),
                "dap_sum": dsum,
                "dap_share": (dsum / total) if total > 0 else None,
            }
        out["by_segment"][c] = block

    # ---- substitution（傘函數併入；notes 去重保序）----
    sub = substitution_ablation(sample_pdf, parameters)
    out["map_current"] = sub["map_current"]
    out["substitution"] = sub["substitution"]
    for n in sub["notes"]:
        if n not in notes:
            notes.append(n)

    logger.info(
        "pair ledger: %d queries, %d mis-ordered pairs, "
        "%d suppressors, map_current=%s",
        out["n_queries"], n_pairs, len(out["by_suppressor"]),
        out["map_current"],
    )
    return out
