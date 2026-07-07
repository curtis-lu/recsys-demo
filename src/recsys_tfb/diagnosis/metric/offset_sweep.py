"""Offset sweep（分流閥，框架診斷項目 6；spec §3 Phase 4）。

在診斷抽樣上（driver-side numpy）對每個 item 的 logit 分數加常數 δ_j，
座標下降搜尋讓參數化 macro per-item mAP 最大的 δ*。判讀語意：折外
mAP(δ*) − mAP(0) ＝「純水準（per-item 平移）可收復的指標缺口」；收不回
的部分＝條件判別力缺口（必須動訓練）。

設計要點（計畫「設計定案」節的落地）：
- δ 單位 log-odds：排序分數先 logit 變換再平移，與對帳層 offset 同尺度。
  整欄超出 (0,1) 時略過 logit（直接平移原始分數）＋ notes 註記。
- holdout：query 層切折（RandomState(sample.seed) permutation），δ 只在
  fit 折搜尋、mAP 兩折分開報告——防「收復缺口」只是擬合驗證雜訊。
- 收縮＋平手偏 0：座標選擇目標 = mAP_fit − shrink_lambda·g²/M，候選按
  |g| 升冪、僅嚴格改善才換——乾淨資料 δ* 恰為 0。
- debug_inject_offsets（僅驗收/測試）：在一切計算之前加到 logit 分數上，
  模擬已知水準錯位；mAP(0) 是注入後的現狀。scope 僅本模組。
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)

_CLIP_EPS = 1e-12
_TIE_EPS = 1e-12


def _diag_cfg(parameters: dict) -> dict:
    return ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})


def _metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    k = m.get("k")
    return {
        "k": int(k) if k is not None else None,
        "weight_alpha": float(m.get("weight_alpha", 0.0)),
        "min_positives": int(m.get("min_positives", 0)),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0)),
    }


def _logit_scores(scores: np.ndarray) -> tuple[np.ndarray, list[str]]:
    s = np.asarray(scores, dtype=np.float64)
    if len(s) and (s.min() < 0.0 or s.max() > 1.0):
        return s.copy(), [
            "score 超出 (0,1)——略過 logit 變換，δ 單位為原始分數尺度"
        ]
    z = np.clip(s, _CLIP_EPS, 1.0 - _CLIP_EPS)
    return np.log(z / (1.0 - z)), []


def _grid(cfg: dict) -> np.ndarray:
    g = cfg.get("grid", {}) or {}
    lo = float(g.get("lo", -2.0))
    hi = float(g.get("hi", 2.0))
    step = float(g.get("step", 0.05))
    n = int(round((hi - lo) / step))
    grid = np.round(lo + step * np.arange(n + 1), 10)
    if not np.any(grid == 0.0):
        grid = np.sort(np.append(grid, 0.0))
    return grid


def _split_queries(
    groups: np.ndarray, holdout_fraction: float, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    """query 層切折。groups 必須是連續碼 0..n-1（groupby().ngroup()）。"""
    n = int(groups.max()) + 1 if len(groups) else 0
    rng = np.random.RandomState(seed)
    perm = rng.permutation(n)
    n_hold = min(int(round(n * holdout_fraction)), n - 1) if n else 0
    hold_flag = np.zeros(n, dtype=bool)
    if n_hold > 0:
        hold_flag[perm[:n_hold]] = True
    hold_mask = hold_flag[groups] if len(groups) else np.zeros(0, dtype=bool)
    return ~hold_mask, hold_mask


def sweep(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    schema = get_schema(parameters)
    query_cols = [schema["time"], *schema["entity"]]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    diag = _diag_cfg(parameters)
    cfg = diag.get("offset_sweep", {}) or {}
    shrink_lambda = float(cfg.get("shrink_lambda", 0.1))
    holdout_fraction = float(cfg.get("holdout_fraction", 0.5))
    max_rounds = int(cfg.get("max_rounds", 5))
    seed = int((diag.get("sample", {}) or {}).get("seed", 42))
    mp = _metric_params(parameters)
    inject = {
        str(k): float(v)
        for k, v in (diag.get("debug_inject_offsets", {}) or {}).items()
    }
    notes: list[str] = []

    out: dict = {
        "enabled": True,
        "score_col_used": score_col,
        "params": {
            "shrink_lambda": shrink_lambda,
            "holdout_fraction": holdout_fraction,
            "max_rounds": max_rounds,
            "grid": dict(cfg.get("grid", {}) or {}),
        },
        "metric_params": mp,
        "injected_offsets": inject,
        "items": [],
        "delta_star": {},
        "per_item": {},
        "n_rounds_run": 0,
        "converged": False,
        "map_fit": {"zero": None, "star": None},
        "map_holdout": {"zero": None, "star": None},
        "recovered_gap_holdout": None,
        "interaction_residual_holdout": None,
        "n_queries_fit": 0,
        "n_queries_holdout": 0,
        "notes": notes,
    }
    if len(sample_pdf) == 0:
        notes.append("診斷抽樣為空——sweep 未執行")
        return out

    groups = sample_pdf.groupby(query_cols, sort=False).ngroup().to_numpy()
    items = sample_pdf[item_col].astype(str).to_numpy()
    y = sample_pdf[label_col].to_numpy()
    z, z_notes = _logit_scores(sample_pdf[score_col].to_numpy())
    notes.extend(z_notes)

    if inject:
        z = z + pd.Series(items).map(inject).fillna(0.0).to_numpy()
        notes.append(
            f"debug_inject_offsets 生效（僅本節點；mAP(0) 為注入後現狀）："
            f"{inject}"
        )
        unknown = sorted(set(inject) - set(items.tolist()))
        if unknown:
            notes.append(f"注入鍵不在抽樣 item 中（無作用）：{unknown}")

    fit_mask, hold_mask = _split_queries(groups, holdout_fraction, seed)
    out["n_queries_fit"] = int(len(np.unique(groups[fit_mask])))
    out["n_queries_holdout"] = int(len(np.unique(groups[hold_mask])))
    if out["n_queries_holdout"] == 0:
        notes.append("holdout 折為空（query 數過少）——折外指標無法報告")

    unique_items = sorted(set(items.tolist()))
    masks = {it: items == it for it in unique_items}
    n_items = len(unique_items)
    grid = _grid(cfg)
    # 候選按 |g| 升冪：平手時偏向 0（kind=stable 保同 |g| 的負值先於正值）
    grid_by_abs = grid[np.argsort(np.abs(grid), kind="stable")]

    def _map_on(mask: np.ndarray, off: np.ndarray):
        if not mask.any():
            return None
        return float(compute_macro_per_item_map(
            groups[mask], items[mask], y[mask], (z + off)[mask], **mp
        ))

    delta = {it: 0.0 for it in unique_items}
    off = np.zeros(len(z), dtype=np.float64)
    converged = False
    n_rounds_run = 0
    for _ in range(max_rounds):
        n_rounds_run += 1
        changed = False
        for it in unique_items:
            base_off = off - delta[it] * masks[it]
            best_g, best_obj = delta[it], -np.inf
            for g in grid_by_abs:
                m_fit = _map_on(fit_mask, base_off + g * masks[it])
                if m_fit is None:
                    continue
                obj = m_fit - shrink_lambda * (g ** 2) / n_items
                if obj > best_obj + _TIE_EPS:
                    best_obj, best_g = obj, float(g)
            if best_g != delta[it]:
                changed = True
                delta[it] = best_g
            off = base_off + delta[it] * masks[it]
        if not changed:
            converged = True
            break
    out["n_rounds_run"] = n_rounds_run
    out["converged"] = converged

    zero_off = np.zeros(len(z), dtype=np.float64)
    out["items"] = unique_items
    out["delta_star"] = dict(delta)
    out["map_fit"] = {
        "zero": _map_on(fit_mask, zero_off),
        "star": _map_on(fit_mask, off),
    }
    out["map_holdout"] = {
        "zero": _map_on(hold_mask, zero_off),
        "star": _map_on(hold_mask, off),
    }
    mh = out["map_holdout"]
    if mh["zero"] is not None and mh["star"] is not None:
        out["recovered_gap_holdout"] = mh["star"] - mh["zero"]

    per_item: dict = {}
    for it in unique_items:
        d = delta[it]
        loo = None
        if d != 0.0 and mh["star"] is not None:
            m_wo = _map_on(hold_mask, off - d * masks[it])
            if m_wo is not None:
                loo = mh["star"] - m_wo
        per_item[it] = {"delta_star": d, "loo_contribution_holdout": loo}
    out["per_item"] = per_item
    if out["recovered_gap_holdout"] is not None:
        loo_sum = sum(
            v["loo_contribution_holdout"] or 0.0 for v in per_item.values()
        )
        out["interaction_residual_holdout"] = (
            out["recovered_gap_holdout"] - loo_sum
        )
    logger.info(
        "offset sweep: %d items, rounds=%d converged=%s, "
        "holdout mAP %s -> %s",
        n_items, n_rounds_run, converged, mh["zero"], mh["star"],
    )
    return out
