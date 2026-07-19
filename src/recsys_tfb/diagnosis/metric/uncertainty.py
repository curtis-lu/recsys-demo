"""per-item AP 與 macro 的 cluster bootstrap CI（spec §3 Phase 1）.

cluster＝entity（cust_id）：同一客戶跨期整批重抽。關鍵簡化：重抽整個
cluster 不改變任何 query 內的排序，所以每列正例貢獻
（``evaluation.metrics.positive_row_contributions``）只算一次；每個
replicate 只是帶 cluster 乘數的重新聚合（bincount with weights），
n_boot=200 在 driver 端 numpy 上是毫秒級。

CI＝percentile bootstrap（2.5 / 97.5）。某 item 在部分 replicate 中可能
沒有正例列（該客戶群沒被抽到）——以 NaN 略過（nanpercentile）。
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Optional

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import (
    align_positive_row_weights,
    macro_from_per_item,
    positive_row_contributions,
)


def bootstrap_per_item_ci(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    """在診斷抽樣上估 per-item AP 與 macro 的 CI。回傳可直接 JSON 序列化的 dict。

    點估計與每個 replicate 都套 ``evaluation.metric`` 的參數家族
    （weight_alpha / min_positives / shrinkage_k；k＝截斷）。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    eval_params = parameters.get("evaluation", {}) or {}
    metric_cfg = eval_params.get("metric", {}) or {}
    k = metric_cfg.get("k", None)
    metric_params = {
        "weight_alpha": float(metric_cfg.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(metric_cfg.get("min_positives", 0) or 0),
        "shrinkage_k": float(metric_cfg.get("shrinkage_k", 0) or 0.0),
    }
    diag_cfg = eval_params.get("diagnosis", {}) or {}
    n_boot = int((diag_cfg.get("ci", {}) or {}).get("n_boot", 200))
    seed = int((diag_cfg.get("sample", {}) or {}).get("seed", 42))

    # query id（time × entity）與 cluster id（entity only）
    query_key = (
        sample_pdf[time_col].astype(str)
        + "|"
        + sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    )
    groups = pd.factorize(query_key)[0]
    cluster_key = sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    clusters = pd.factorize(cluster_key)[0]

    y = sample_pdf[label_col].to_numpy()
    score = sample_pdf[score_col].to_numpy(dtype=np.float64)
    items = sample_pdf[item_col].astype(str).to_numpy()

    contrib, row_idx = positive_row_contributions(groups, y, score, k)
    if len(contrib) == 0:
        return {
            "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
            "metric_params": metric_params,
            "per_item": {}, "macro": None,
        }

    item_of = items[row_idx]
    cluster_of = clusters[row_idx]
    uniq_items, item_inv = np.unique(item_of, return_inverse=True)
    n_items = len(uniq_items)
    n_clusters = int(clusters.max()) + 1

    # ---- 點估計 ----
    sums = np.bincount(item_inv, weights=contrib, minlength=n_items)
    counts = np.bincount(item_inv, minlength=n_items).astype(np.float64)
    point = sums / counts
    macro_point = macro_from_per_item(point, counts, **metric_params)

    # ---- bootstrap：重抽 cluster、帶乘數重新聚合 ----
    rng = np.random.RandomState(seed)
    boot_items = np.full((n_boot, n_items), np.nan)
    boot_macro = np.full(n_boot, np.nan)
    for b in range(n_boot):
        draw = rng.randint(0, n_clusters, n_clusters)
        mult = np.bincount(draw, minlength=n_clusters).astype(np.float64)
        w = mult[cluster_of]
        s = np.bincount(item_inv, weights=contrib * w, minlength=n_items)
        c = np.bincount(item_inv, weights=w, minlength=n_items)
        present = c > 0
        vals = np.divide(s, c, out=np.full(n_items, np.nan), where=present)
        boot_items[b] = vals
        m = macro_from_per_item(
            vals[present], c[present], **metric_params
        )
        if m is not None:
            boot_macro[b] = m

    lo = np.nanpercentile(boot_items, 2.5, axis=0)
    hi = np.nanpercentile(boot_items, 97.5, axis=0)

    per_item = {
        str(uniq_items[j]): {
            "ap": float(point[j]),
            "ci_low": float(lo[j]),
            "ci_high": float(hi[j]),
            "n_pos": int(counts[j]),
        }
        for j in range(n_items)
    }
    macro = None
    if macro_point is not None and not np.all(np.isnan(boot_macro)):
        macro = {
            "ap": float(macro_point),
            "ci_low": float(np.nanpercentile(boot_macro, 2.5)),
            "ci_high": float(np.nanpercentile(boot_macro, 97.5)),
        }
    return {
        "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
        "metric_params": metric_params,
        "per_item": per_item, "macro": macro,
    }


#: ``frame`` 的正規欄名。刻意不吃 ``parameters``／schema：這個函式只做統計，
#: 由呼叫端把 schema 角色欄（snap_date / cust_id / prod_name …）投影成這五欄。
_FRAME_COLS = ("group", "cluster", "item", "label", "score")


def _weighted_macro(
    contrib: np.ndarray,
    item_inv: np.ndarray,
    n_items: int,
    w_pos: np.ndarray,
    metric_kwargs: dict,
) -> float:
    """加權 per-item 平均 → macro。回傳 NaN 表示這個 replicate 沒有可用的 item。

    公式與 ``evaluation.metrics.compute_macro_per_item_map`` 的加權路徑
    **逐項相同**（分母是權重和而非列數，macro 合併維持等權重）。這裡另寫一份
    的唯一理由是效能：``compute_macro_per_item_map`` 每次呼叫都會重跑
    ``positive_row_contributions`` 的 ``lexsort``，而 cluster 重抽不改變任何
    query 內的排序，所以 contributions 在 bootstrap 迴圈外只算一次就夠
    （本模組 docstring 開頭的核心簡化）。n_boot=200 × 兩側 ＝ 400 次
    lexsort，在公司規模樣本上是分鐘級 vs 毫秒級的差別。
    ``test_paired_aggregation_matches_compute_macro_per_item_map`` 把這份
    聚合釘在公開實作上，避免兩份公式日後漂移。
    """
    sums = np.bincount(item_inv, weights=contrib * w_pos, minlength=n_items)
    counts = np.bincount(item_inv, weights=w_pos, minlength=n_items)
    present = counts > 0
    if not present.any():
        return float("nan")
    vals = sums[present] / counts[present]
    macro = macro_from_per_item(
        vals,
        counts[present],
        weight_alpha=float(metric_kwargs.get("weight_alpha", 0.0) or 0.0),
        min_positives=int(metric_kwargs.get("min_positives", 0) or 0),
        shrinkage_k=float(metric_kwargs.get("shrinkage_k", 0.0) or 0.0),
    )
    return float("nan") if macro is None else float(macro)


def _row_offsets(frame: pd.DataFrame, shift) -> np.ndarray:
    """把 ``shift`` 正規化成每列一個要從分數扣掉的位移。"""
    n_rows = len(frame)
    if shift is None or (isinstance(shift, Mapping) and not shift):
        return np.zeros(n_rows, dtype=np.float64)
    if isinstance(shift, Mapping):
        return (
            frame["item"].map(shift).fillna(0.0).to_numpy(dtype=np.float64)
        )
    off = np.asarray(shift, dtype=np.float64)
    if off.shape != (n_rows,):
        raise ValueError(
            f"shift must be a mapping item -> offset, or a row-aligned array "
            f"of shape ({n_rows},); got shape {off.shape}"
        )
    return off


def paired_bootstrap_delta(
    frame: pd.DataFrame,
    metric_kwargs: dict,
    shift,
    n_boot: int = 200,
    seed: int = 42,
    strata_col: str = "stratum",
    weight_col: str = "inclusion_weight",
    _record_draws: Optional[list] = None,
) -> tuple[float, float]:
    """分層配對 cluster bootstrap：``mAP(F) − mAP(F − shift)`` 的百分位 CI。

    ``frame`` 需帶 :data:`_FRAME_COLS` 五欄（``group`` ＝ query id、
    ``cluster`` ＝ 重抽單位（entity）、``item`` / ``label`` / ``score``），
    以及可選的 ``strata_col`` 與 ``weight_col``（即 ``draw_diagnosis_sample``
    產出的 ``stratum`` / ``inclusion_weight``）。兩欄缺席時視為單層、全 1
    權重——舊呼叫端不必改就能用，**不 raise**。

    ``shift`` 是 ``{item: offset}``（未列出的 item 位移 0），或一條與 frame
    等長、逐列對齊的位移向量（供位移依 context 而非只依 item 決定的診斷用）。
    ``{}`` ＝ 零位移，此時兩側分數完全相同、Δ 恆為 0。

    三個設計決定與各自的理由：

    1. **配對**：兩個 mAP 在**同一組**重抽樣本上計算再相減。兩者高度相關
       （多數 query 的名次根本沒被 shift 改動），分開各自 bootstrap 再相減
       等於把相關性丟掉，CI 會寬到任何真實效果都測不出來。
    2. **層內重抽**：重抽在每個 stratum 內獨立進行，且各層的 cluster 抽取
       數維持原值。分層抽樣下各層的納入機率不同，跨層一起重抽會讓層的相對
       比重隨機漂移，等於在估計裡混進一個抽樣設計造成的假變異。
    3. **重抽單位是 ``(stratum, cluster)`` 而非 cluster**：stratum 是
       query 級的屬性，同一個 entity 可能有些期落在 ``take_all``、有些落在
       ``hash_ratio``。若硬把 cluster 指派到單一層，被指派走的那些列會讓另一
       層的 cluster 數改變——正是第 2 點要避免的事。代價是橫跨兩層的 entity
       在兩層拿到獨立的乘數（層間相關性被切斷），這在診斷用途下遠比扭曲層
       比重無害。

    每列的最終權重 ＝ ``inclusion_weight × 該 (層, cluster) 的重抽次數``，
    經 :func:`~recsys_tfb.evaluation.metrics.align_positive_row_weights`
    對齊到正例列後餵進加權聚合——不修正的話，被降抽的那一層會被系統性低估。

    ``_record_draws`` 是測試 hook：非 None 時，每個 replicate append 一個
    ``{stratum: 該層抽到的 cluster 總數}``。
    """
    n_rows = len(frame)
    if n_rows == 0:
        return (0.0, 0.0)

    k = metric_kwargs.get("k", None)
    groups = pd.factorize(frame["group"])[0]
    cluster_codes = pd.factorize(frame["cluster"])[0]
    items = frame["item"].astype(str).to_numpy()
    y = frame["label"].to_numpy()
    score = frame["score"].to_numpy(dtype=np.float64)
    score_shifted = score - _row_offsets(frame, shift)

    base_w = (
        frame[weight_col].to_numpy(dtype=np.float64)
        if weight_col in frame.columns
        else np.ones(n_rows, dtype=np.float64)
    )
    strata = (
        frame[strata_col].astype(str).to_numpy()
        if strata_col in frame.columns
        else np.full(n_rows, "__all__")
    )

    # 重抽單位 ＝ (stratum, cluster)，編碼成單一 int 以便 bincount。
    uniq_strata, stratum_codes = np.unique(strata, return_inverse=True)
    n_clusters = int(cluster_codes.max()) + 1
    unit_code = stratum_codes.astype(np.int64) * n_clusters + cluster_codes
    uniq_units, unit_inv = np.unique(unit_code, return_inverse=True)
    n_units = len(uniq_units)
    unit_stratum = uniq_units // n_clusters
    strata_units = [
        np.flatnonzero(unit_stratum == s) for s in range(len(uniq_strata))
    ]

    # contributions 與 item 索引空間在迴圈外算好——cluster 重抽不改變 query
    # 內的排序，所以每側只需算一次（見 _weighted_macro 的 docstring）。
    contrib_base, ridx_base = positive_row_contributions(groups, y, score, k)
    contrib_shift, ridx_shift = positive_row_contributions(
        groups, y, score_shifted, k
    )
    if len(contrib_base) == 0 or len(contrib_shift) == 0:
        return (0.0, 0.0)
    uniq_items, item_codes = np.unique(items, return_inverse=True)
    n_items = len(uniq_items)
    inv_base = item_codes[ridx_base]
    inv_shift = item_codes[ridx_shift]

    rng = np.random.default_rng(seed)
    deltas = np.full(n_boot, np.nan)
    for b in range(n_boot):
        mult = np.zeros(n_units, dtype=np.float64)
        record: dict[str, int] = {}
        for s in range(len(uniq_strata)):
            members = strata_units[s]  # 層內重抽：抽取池限定在本層的單位
            m = len(members)
            picks = members[rng.integers(0, m, m)]
            drawn = np.bincount(picks, minlength=n_units).astype(np.float64)
            mult += drawn
            record[str(uniq_strata[s])] = int(drawn[strata_units[s]].sum())
        if _record_draws is not None:
            _record_draws.append(record)

        w_row = base_w * mult[unit_inv]
        macro_base = _weighted_macro(
            contrib_base, inv_base, n_items,
            align_positive_row_weights(w_row, n_rows, ridx_base),
            metric_kwargs,
        )
        macro_shift = _weighted_macro(
            contrib_shift, inv_shift, n_items,
            align_positive_row_weights(w_row, n_rows, ridx_shift),
            metric_kwargs,
        )
        if not (np.isnan(macro_base) or np.isnan(macro_shift)):
            deltas[b] = macro_base - macro_shift

    if np.all(np.isnan(deltas)):
        return (0.0, 0.0)
    return (
        float(np.nanpercentile(deltas, 2.5)),
        float(np.nanpercentile(deltas, 97.5)),
    )
