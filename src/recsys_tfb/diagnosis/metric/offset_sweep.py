"""Offset sweep（分流閥，框架診斷項目 6；spec §3 Phase 4）。

在診斷抽樣上（driver-side numpy）對每個 item 的 logit 分數加常數 δ_j，
座標下降搜尋讓參數化 macro per-item mAP 最大的 δ*。判讀語意：折外
mAP(δ*) − mAP(0) ＝「純水準（per-item 平移）可收復的指標缺口」；收不回
的部分＝條件判別力缺口（必須動訓練）。

設計要點（計畫「設計定案」節的落地）：
- δ 單位 log-odds：排序分數先 logit 變換再平移，與對帳層 offset 同尺度。
  整欄超出 (0,1) 時略過 logit（直接平移原始分數）＋ notes 註記。
- holdout：query 層切折（query key CRC32 hash 分桶，列序無關），δ 只在
  fit 折搜尋、mAP 兩折分開報告——防「收復缺口」只是擬合驗證雜訊。
- 收縮＋平手偏 0：座標選擇目標 = mAP_fit − shrink_lambda·g²/M，候選按
  |g| 升冪、僅嚴格改善才換——乾淨資料 δ* 恰為 0。
- debug_inject_offsets（僅驗收/測試）：在一切計算之前加到 logit 分數上，
  模擬已知水準錯位；mAP(0) 是注入後的現狀。scope 僅本模組。
- gauge（規範自由度）：對所有 item 加同一常數不改變排序、mAP 不變——
  δ* 只有相對差可識別。跨執行比較用 ``delta_star_centered``（去均值）。
"""
from __future__ import annotations

import logging
import zlib

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import (
    _HASH_BUCKETS, apply_injection, diag_cfg, metric_params, parse_injection,
    to_logit,
)
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)

_TIE_EPS = 1e-12
_FOLD_SITE = "offset_sweep_fold"


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


def _fold_split(
    sample_pdf: pd.DataFrame, query_cols: list, holdout_fraction: float,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """query 層 hash 折別（列序無關；opus N1 修，2026-07-08）。

    對 query key 字串 CRC32 分桶：bucket < fraction*BUCKETS → holdout。
    同 query 各列 key 相同 → 折別與 toPandas() 列序無關。近似比例
    （非精確 round(n×fraction)）。fit 折空時把 bucket 最小的 query 移入
    fit（確定性），保 fit 非空；holdout 空由呼叫端既有路徑處理。
    """
    parts = []
    for c in query_cols:
        s = sample_pdf[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            parts.append(s.dt.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            parts.append(s.astype(str))
    keys = parts[0] if len(parts) == 1 else parts[0].str.cat(parts[1:], sep="|")
    token = f"|{_FOLD_SITE}|{seed}"
    buckets = keys.map(
        lambda k_: zlib.crc32(f"{k_}{token}".encode()) % _HASH_BUCKETS
    ).to_numpy()
    threshold = int(round(holdout_fraction * _HASH_BUCKETS))
    hold_mask = buckets < threshold
    fit_mask = ~hold_mask
    if len(buckets) and not fit_mask.any():
        rescue = buckets == buckets.min()
        fit_mask, hold_mask = rescue, ~rescue
    return fit_mask, hold_mask


def sweep(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    schema = get_schema(parameters)
    query_cols = [schema["time"], *schema["entity"]]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    diag = diag_cfg(parameters)
    cfg = diag.get("offset_sweep", {}) or {}
    shrink_lambda = float(cfg.get("shrink_lambda", 0.1))
    holdout_fraction = float(cfg.get("holdout_fraction", 0.5))
    max_rounds = int(cfg.get("max_rounds", 5))
    seed = int((diag.get("sample", {}) or {}).get("seed", 42))
    mp = metric_params(parameters)
    inject = parse_injection(parameters)
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
        "delta_star_centered": {},
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

    # dropna=False：query 鍵含 null 的列自成一組（預設 dropna=True 會給
    # ngroup 代碼 -1，讓 hold_flag[-1] 靜默繞到最後一組的折別）。
    groups = (
        sample_pdf.groupby(query_cols, sort=False, dropna=False)
        .ngroup()
        .to_numpy()
    )
    items = sample_pdf[item_col].astype(str).to_numpy()
    y = sample_pdf[label_col].to_numpy()
    z, z_notes = to_logit(sample_pdf[score_col].to_numpy())
    notes.extend(z_notes)

    z, inj_notes = apply_injection(z, items, inject)
    notes.extend(inj_notes)

    fit_mask, hold_mask = _fold_split(
        sample_pdf, query_cols, holdout_fraction, seed
    )
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
    # gauge：對所有 item 加同一常數不改變任何 query 內排序（mAP 不變），
    # δ* 只有相對差可識別——跨執行比較請用去均值後的 centered 值，
    # 原始值會被最佳化路徑選到的共同平移污染。
    mean_delta = float(np.mean(list(delta.values()))) if delta else 0.0
    out["delta_star_centered"] = {
        it: float(d - mean_delta) for it, d in delta.items()
    }
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
        per_item[it] = {
            "delta_star": d,
            "delta_star_centered": out["delta_star_centered"][it],
            "loo_contribution_holdout": loo,
        }
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
