"""對帳層（spec §3 Phase 2）：理論偏移 vs 實測校準差距。

理論偏移通式（per 完整 group-key cell）：
    offset = ln((r_pos * w_pos) / (r_neg * w_neg))
r＝抽樣保留率（dataset.sample_ratio_overrides，缺項用 dataset.sample_ratio）、
w＝訓練權重（training.sample_weights，缺項 1.0）。退化案例：只砍負類保留 r
→ offset = −ln r（手冊3 Ch10 的 logQ 校正）。label 不在對應 keys 裡 →
該側對 label 對稱、貢獻恆 0。

誠實限制（spec 明定）：overrides 是多維 key（如 cust_segment_typ|prod_name|
label），item 層的單一 offset 是聚合近似——本模組按完整 group key 細列、
item 層只給 {min, max, mean} 摘要帶並標 approx，verdict 用帶不用單值。

抽樣 cell 與權重 cell 的維度可能不同（``sample_group_keys`` 與
``sample_weight_keys`` 各自宣告，不保證對齊）。維度相同時按完整 cell key
直接疊乘；維度不同時權重 cell 是「跨該 item 全部抽樣 cell 都適用」的效應，
廣播疊加到同 item 的每個抽樣 cell 上（無抽樣 cell 覆蓋的 item 才單獨列出
權重 cell）。
"""
from __future__ import annotations

import logging
import math

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def _split_label_dim(keys: list[str], label_col: str):
    """回傳 (label 維 index, 非 label 維名稱 list)；label 不在 keys → (None, keys)。"""
    if label_col not in keys:
        return None, list(keys)
    idx = keys.index(label_col)
    return idx, [k for k in keys if k != label_col]


def _pair_by_cell(table: dict, keys: list[str], label_col: str,
                   default: float) -> dict[str, dict[str, float]]:
    """把 {'a|b|0': v} 形式的 config 表整理成
    {非label鍵: {'pos': v1, 'neg': v0}}；缺項補 default。
    label 不在 keys → 空 dict（該側對 label 對稱）。"""
    idx, _ = _split_label_dim(keys, label_col)
    if idx is None:
        return {}
    cells: dict[str, dict[str, float]] = {}
    for key, val in (table or {}).items():
        parts = key.split("|")
        if len(parts) != len(keys):
            continue  # 段數不符（A9b/A5 是它們的守門，這裡靜默略過）
        label_val = parts[idx]
        rest = "|".join(p for i, p in enumerate(parts) if i != idx)
        slot = "pos" if label_val == "1" else "neg"
        cells.setdefault(rest, {})[slot] = float(val)
    for cell in cells.values():
        cell.setdefault("pos", default)
        cell.setdefault("neg", default)
    return cells


def _item_of(cell_key: str, keys: list[str], label_col: str,
             item_col: str) -> str | None:
    """從某個 cell key（keys 的非 label 維順序串接）取出 item 值；
    item_col 不在該 keys 集合中 → None（該 cell 無法歸戶到單一 item）。"""
    _, rest_keys = _split_label_dim(keys, label_col)
    if item_col not in rest_keys:
        return None
    return cell_key.split("|")[rest_keys.index(item_col)]


def theoretical_offsets(parameters: dict) -> dict:
    """讀採樣／加權 config，回傳 per-cell 理論偏移＋per-item 摘要帶（JSON-ready）。"""
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {}) or {}
    tr = parameters.get("training", {}) or {}
    sample_ratio = float(ds.get("sample_ratio", 1.0))
    group_keys = list(ds.get("sample_group_keys", []) or [])
    weight_keys = list(tr.get("sample_weight_keys", []) or [])

    notes: list[str] = []
    ratio_cells = _pair_by_cell(
        ds.get("sample_ratio_overrides", {}), group_keys, label_col,
        default=sample_ratio,
    )
    if label_col not in group_keys and (ds.get("sample_ratio_overrides") or {}):
        notes.append(
            f"sample_group_keys 不含 {label_col}——抽樣對 label 對稱，"
            f"理論上不移動 level，抽樣側貢獻為 0。"
        )
    weight_cells = _pair_by_cell(
        tr.get("sample_weights", {}), weight_keys, label_col, default=1.0,
    )
    if label_col not in weight_keys and (tr.get("sample_weights") or {}):
        notes.append(
            f"sample_weight_keys 不含 {label_col}——權重對 label 對稱，"
            f"權重側貢獻為 0。"
        )

    # Step 1：抽樣 cell 先細列（各自維度＝group_keys 的非 label 維）。
    cells: dict[str, dict] = {}
    for cell_key, rw in ratio_cells.items():
        offset = math.log(rw["pos"] / rw["neg"])
        cells[cell_key] = {
            "source": "sampling",
            "r_pos": rw["pos"], "r_neg": rw["neg"],
            "w_pos": 1.0, "w_neg": 1.0,
            "offset": offset,
            "item": _item_of(cell_key, group_keys, label_col, item_col),
        }

    # Step 2：權重 cell 併入。維度相同（同一組非 label 鍵）→ 同 cell key 直接
    # 疊乘；維度不同 → 權重 cell 是該 item 的全域效應，廣播疊加到同 item 的
    # 每個抽樣 cell 上（沒有抽樣 cell 覆蓋的 item 才單獨列權重 cell）。
    _, group_dims = _split_label_dim(group_keys, label_col)
    _, weight_dims = _split_label_dim(weight_keys, label_col)
    same_dims = group_dims == weight_dims

    for cell_key, ww in weight_cells.items():
        w_offset = math.log(ww["pos"] / ww["neg"])
        item = _item_of(cell_key, weight_keys, label_col, item_col)

        if same_dims and cell_key in cells:
            matched = cells[cell_key]
            matched["w_pos"], matched["w_neg"] = ww["pos"], ww["neg"]
            matched["offset"] += w_offset
            matched["source"] = "sampling+weights"
            continue

        targets = [
            c for c in cells.values()
            if c["item"] == item and c["source"].startswith("sampling")
        ]
        if targets:
            for t in targets:
                t["w_pos"], t["w_neg"] = ww["pos"], ww["neg"]
                t["offset"] += w_offset
                if "weights" not in t["source"]:
                    t["source"] += "+weights"
        else:
            cells[cell_key] = {
                "source": "weights",
                "r_pos": 1.0, "r_neg": 1.0,
                "w_pos": ww["pos"], "w_neg": ww["neg"],
                "offset": w_offset,
                "item": item,
            }

    by_item: dict[str, dict] = {}
    for cell in cells.values():
        it = cell["item"]
        if it is None:
            continue
        agg = by_item.setdefault(
            it, {"min": math.inf, "max": -math.inf, "_sum": 0.0, "n_cells": 0}
        )
        agg["min"] = min(agg["min"], cell["offset"])
        agg["max"] = max(agg["max"], cell["offset"])
        agg["_sum"] += cell["offset"]
        agg["n_cells"] += 1
    for it, agg in by_item.items():
        agg["mean"] = agg.pop("_sum") / agg["n_cells"]
        agg["approx"] = True  # item 層是跨 cell 聚合近似（見模組 docstring）

    return {"cells": cells, "by_item": by_item, "notes": notes}


def _logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def calibration_gap_by_item(
    sdf: SparkDataFrame, parameters: dict, score_col: str,
) -> dict[str, dict]:
    """per item 的 logit(p̄) − logit(ȳ)（先平均再 logit，spec 釘的公式）。

    Spark 只做 groupBy 聚合（無 UDF）；logit 在 driver 端對 22 個 item 級的
    小 dict 計算。ȳ 或 p̄ ∉ (0,1) → gap=None＋reason（不炸）。
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]

    rows = (
        sdf.groupBy(item_col)
        .agg(
            F.mean(F.col(score_col).cast("double")).alias("p_mean"),
            F.mean(F.col(label_col).cast("double")).alias("y_rate"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    out: dict[str, dict] = {}
    for r in rows:
        p, y = float(r["p_mean"]), float(r["y_rate"])
        entry: dict = {"p_mean": p, "y_rate": y, "n_rows": int(r["n_rows"])}
        if not (0.0 < y < 1.0):
            entry["gap"] = None
            entry["reason"] = f"y_rate={y} 使 logit 未定義（全正或全負）"
        elif not (0.0 < p < 1.0):
            entry["gap"] = None
            entry["reason"] = f"p_mean={p} 不在 (0,1)——score 欄可能不是機率"
        else:
            entry["gap"] = _logit(p) - _logit(y)
        out[str(r[item_col])] = entry
    return out


def reconcile(eval_predictions: SparkDataFrame, parameters: dict) -> dict:
    """對帳表：理論帶 × 實測 gap → residual → verdict（JSON-ready）。"""
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("reconciliation", {}) or {})
    score_col = str(cfg.get("score_col", "score_uncalibrated"))
    threshold = float(cfg.get("explained_threshold", 0.3))
    schema = get_schema(parameters)
    base_score_col = schema["score"]

    fallback = False
    if score_col not in eval_predictions.columns:
        logger.warning(
            "reconcile: %s 欄不存在（monitoring 路徑無校準前分數）——"
            "退回 %s，理論對帳將包含校準層效應", score_col, base_score_col,
        )
        score_col, fallback = base_score_col, True

    theory = theoretical_offsets(parameters)
    gaps = calibration_gap_by_item(eval_predictions, parameters, score_col)
    gaps_cal = (
        calibration_gap_by_item(eval_predictions, parameters, base_score_col)
        if score_col != base_score_col else None
    )

    by_item: dict[str, dict] = {}
    all_explained = True
    for item, g in sorted(gaps.items()):
        band = theory["by_item"].get(item)
        t_min = band["min"] if band else 0.0
        t_max = band["max"] if band else 0.0
        entry = {
            "theory_min": t_min, "theory_max": t_max,
            "theory_approx": bool(band and band.get("approx")),
            "gap": g["gap"], "p_mean": g["p_mean"], "y_rate": g["y_rate"],
            "n_rows": g["n_rows"],
        }
        if gaps_cal is not None:
            entry["gap_calibrated"] = gaps_cal.get(item, {}).get("gap")
        if g["gap"] is None:
            entry["residual"] = None
            entry["verdict"] = "無法評估"
            entry["reason"] = g.get("reason")
        else:
            clipped = min(max(g["gap"], t_min), t_max)
            entry["residual"] = g["gap"] - clipped
            entry["verdict"] = (
                "可解釋" if abs(entry["residual"]) <= threshold else "不可解釋"
            )
            if entry["verdict"] != "可解釋":
                all_explained = False
        by_item[item] = entry

    return {
        "enabled": True,
        "score_col_used": score_col,
        "fallback": fallback,
        "explained_threshold": threshold,
        "theory": theory,
        "by_item": by_item,
        "all_explained": all_explained,
    }
