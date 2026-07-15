"""從 trials + search_space 算 convergence / boundary / importances 摘要。"""

from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


def compute_convergence(trials: list[dict], *, patience: int) -> dict:
    """答「需不需要再繼續 trial」。只看 COMPLETE 且 value 非 None 的 trial：
    找最後一次刷新最佳（maximize）的 trial，算距今幾個完成 trial 未再進步；
    plateau = 未進步數 >= patience。"""
    completed = [
        t for t in trials
        if t.get("state") == "COMPLETE" and t.get("value") is not None
    ]
    if not completed:
        return {
            "best_value": None, "best_trial_number": None, "n_completed": 0,
            "last_improvement_trial": None, "trials_since_improvement": None,
            "plateau": False, "note": "尚無完成的 trial。",
        }
    best_val = None
    best_num = None
    last_improve_idx = 0
    for idx, t in enumerate(completed):
        if best_val is None or t["value"] > best_val:
            best_val, best_num, last_improve_idx = t["value"], t["number"], idx
    since = len(completed) - 1 - last_improve_idx
    plateau = since >= patience
    note = (
        f"近 {since} 個完成的 trial 未再刷新最佳；已達 plateau 提示閾值"
        f"（patience={patience}），可考慮停止。"
        if plateau else
        f"最佳在第 {best_num} 號 trial；距今 {since} 個未進步，未達 plateau"
        f"閾值（patience={patience}），可能還有空間。"
    )
    return {
        "best_value": best_val, "best_trial_number": best_num,
        "n_completed": len(completed), "last_improvement_trial": best_num,
        "trials_since_improvement": since, "plateau": plateau, "note": note,
    }


def _rel_position(value, low, high, log: bool) -> Optional[float]:
    try:
        lo, hi, v = float(low), float(high), float(value)
    except (TypeError, ValueError):
        return None
    if log:
        if lo <= 0 or hi <= 0 or v <= 0 or hi == lo:
            return None
        return (math.log(v) - math.log(lo)) / (math.log(hi) - math.log(lo))
    if hi == lo:
        return None
    return (v - lo) / (hi - lo)


def compute_boundary(
    best_params: dict, search_space: list, *, hi_thresh: float, lo_thresh: float
) -> dict:
    """答「search range 要不要調」。對每個數值型搜尋參數，看最佳值離 search_space 邊界多近。"""
    best_params = best_params or {}
    out: dict = {}
    for spec in search_space:
        name = spec["name"]
        ptype = spec.get("type")
        if name not in best_params:
            continue
        if ptype not in ("int", "float"):
            out[name] = {"type": ptype, "suggestion": "n/a",
                         "note": "categorical，不做邊界建議。"}
            continue
        value = best_params[name]
        low, high = spec.get("low"), spec.get("high")
        log = bool(spec.get("log", False))
        rel = _rel_position(value, low, high, log)
        scale = "log" if log else ptype
        if rel is None:
            out[name] = {"best_value": value, "low": low, "high": high,
                         "scale": scale, "rel_position": None,
                         "at_low": False, "at_high": False, "suggestion": "ok",
                         "note": "無法計算相對位置（範圍退化），略過建議。"}
            continue
        at_high = rel >= hi_thresh
        at_low = rel <= lo_thresh
        if at_high:
            suggestion = "widen_high"
            note = f"最佳值貼近上界（相對位置 {rel:.3f}），建議放寬上界。"
        elif at_low:
            suggestion = "widen_low"
            note = f"最佳值貼近下界（相對位置 {rel:.3f}），建議放寬下界。"
        else:
            suggestion = "ok"
            note = f"最佳值在範圍內（相對位置 {rel:.3f}），範圍看似足夠。"
        out[name] = {"best_value": value, "low": low, "high": high,
                     "scale": scale, "rel_position": rel,
                     "at_low": at_low, "at_high": at_high,
                     "suggestion": suggestion, "note": note}
    return out
