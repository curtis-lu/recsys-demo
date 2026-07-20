"""suppression 計算層：壓制帳本（同一 query 內誰排在誰前面）＋交叉購買 lift。

回答的兩個問題
--------------
1. **壓制帳本**：對每個被壓制的正例列（同一 query 內有 label=0 的 item 排
   在它前面），把它的 AP 缺口（``1 − 目前的 precision 貢獻``）按「換掉哪個
   負例最能救回多少 AP」的比例分攤給排在它上面的每個負例，彙總成
   ``(positive_item, suppressor_item)`` 的壓制帳本。
2. **交叉購買**：``item_j``／``item_k`` 這一對，買 j 的人有多常也買 k
   （``p_k_given_j``），以及這個條件機率相對 k 的基礎購買率高多少
   （``lift``）。

移植來源：``scripts/suppression_ledger_diagnosis.py:388-757`` 的
``analyze_suppression``（壓制帳本部分）。輸出鍵沿用它的 ``:727-757`` 那份
return dict——那是唯一真實來源。``cross_purchase_stats`` 是全新函式，語意
取代（不是移植）``cross_purchase.py`` 的 Spark 版本。

向量化：內層迴圈必須消失
-------------------------
腳本原版是三層迴圈：外層走每個 query、次層走每個正例列、內層
（``:519`` 的 ``for a, raw_d, gap_d in zip(above, raw_severity,
allocated_gap)``）走每一個「排在該正例上面的負例」。內層次數 ＝ 成對數
（公司規模估算 250 萬–500 萬次），是這項診斷最慢的地方。

這裡保留 query 迴圈與正例列迴圈（次數 ＝ 正例列數，約 25–50 萬，與 query
迴圈同一個量級），**拿掉內層**：每一輪正例列改成把 ``above``／
``raw_severity``／``allocated_gap``／``score_margin``／正例列原始索引／
兩邊的 item 名稱以陣列形式存進 list，迴圈結束後 ``np.concatenate`` 成一張
扁平的成對表（``pos_row``／``sup_row``／``pos_item``／``sup_item``／
``gap``／``raw``／``margin``），剩下的統計（每組壓制帳本、per-suppressor
彙總、examples top-K）全部用 pandas ``groupby``／``nlargest`` 一次算完，
取代原版逐筆維護 dict-of-set 與 heapq 的做法。**拿掉外層**（query／正例列）
是另一個更難的問題（要用 ``np.repeat`` ＋ ragged-range 建成對索引），不在
本模組範圍。

⚠ 記憶體：成對表列數 ＝ 上面估的 250 萬–500 萬，7 欄（含 int64／float64／
object），driver 上可接受但必須知道實際跑出多少——:func:`compute` 會用
``logger.info`` 印出實際的 ``n_pairs`` 與 ``n_positive_rows``。

cross_purchase_stats：lift 而非裸條件機率
-------------------------------------------
熱門 item k 對**任何** j 的 ``P(k|j)`` 都高——只給條件機率的話，矩陣會退化
成「熱門那幾行整片亮」，那張圖畫的是 item 的熱門度，不是關聯。
``lift = p_k_given_j / (n_k / n_units)`` 把 k 自己的基礎購買率除掉，
``lift ≈ 1`` 代表在這份樣本上兩者近似獨立。

⚠ **共現單位 ＝ query 單位（``time`` × ``entity``），不是單純 entity**。
與被取代的 ``cross_purchase.py:33-47``（該檔 join key 就是
``[schema["time"], *schema["entity"]]``）一致——同一個 entity 在不同
``snap_date`` 買同一個 item 不是同一件事，不能併成一次共現。這與
:func:`compute` 裡壓制帳本的 ``groups``（同一個 query 單位）是同一種切法，
兩張圖才能在同一個「一格 = 一個 query 單位」的基準上對照。

⚠ **母體變了，這是本 Plan 唯一的語意變更**：舊的 ``cross_purchase_matrix``
（``cross_purchase.py``，本模組不刪它）吃的是 Spark 的 ``label_table``
**全量**；這裡的 :func:`cross_purchase_stats` 吃的是 **診斷抽樣**
（``diagnosis_sample``，pandas，抽樣後的）。改的理由：壓制矩陣與交叉購買
矩陣並排對照是這項診斷的全部價值（「模型讓 k 壓制 j」對上「買 j 的人本來
就常買 k」），兩張圖必須算在同一個母體上，讀者才不必在每次對照時夾帶一個
沒說出口的「這兩個母體其實不一樣」的假設。代價：抽樣是分層的
（``stratum``／``inclusion_weight``），樣本內共買頻率不是母體共買頻率的
無偏估計——這件事寫進 ``suppression.SCOPE.blind_to``（Task 5.3），這裡只
記一筆。

不下結論
--------
本模組只輸出數字、分布與對照點。**沒有** severity／verdict／建議動作，也
沒有把連續量切成離散類別的門檻——判斷留給讀者。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import (
    diag_cfg, metric_params, per_item_ap, query_key, to_logit,
)

logger = logging.getLogger(__name__)

#: 唯一可用的分數欄。與家族其餘模組一致：壓制帳本的名次順序雖然是單調不變
#: 的量（校準是單調變換，不改排序），但 score_margin 的單位需要與家族其餘
#: 診斷一致（logit 空間），且腳本原版本來就硬性要求這一欄，這裡沿用不改。
SCORE_COL = "score_uncalibrated"

#: 每個非顯然欄位一句話定義，跟著 JSON 走。純定義，不含判讀（見模組
#: docstring「不下結論」）。
FIELD_NOTES: dict[str, str] = {
    "n_suppressed_positive_rows": "至少有一個 label=0 item 排在它前面的正例列數。",
    "suppressed_positive_rate": "n_suppressed_positive_rows / n_positive_rows。",
    "mean_negatives_above_positive": "n_misordered_pairs / n_positive_rows（未加權）。",
    "n_misordered_pairs": "(正例列, 排在它上面的負例列) 這種成對關係的總數。",
    "total_ap_gap_allocated_to_suppressors": (
        "所有被壓制正例列的 AP 缺口，依換掉哪個負例最能救回多少 AP 的比例，"
        "分攤給各個壓制者之後的總和。"
    ),
    "target_summary": "以正例 item 為單位的彙總——見各欄位說明。",
    "pair_ledger": (
        "以 (positive_item, suppressor_item) 為單位的壓制帳本——這個負例 "
        "item 總共讓這個正例 item 的正例列損失多少 AP、影響幾列。"
    ),
    "by_suppressor": "以壓制者 item 為單位的彙總（pair_ledger 的另一個視角）。",
    "examples": (
        "gap／raw_severity 最大的具體 (正例列, 壓制負例列) 樣本，供逐案核對，"
        "不做聚合證據用。"
    ),
    "matrices": (
        "target_gap_share／affected_positive_rate／mean_logit_margin／"
        "suppressor_target_gap_share 四張矩陣，列為正例 item、欄為壓制者 "
        "item（suppressor_target_gap_share 相反）。"
    ),
    "axis_order": (
        "出現在成對表裡的 item 名稱排序後的清單。壓制矩陣與交叉購買資料"
        "共用這組順序，兩張圖才能同軸對照。"
    ),
}

#: cross_purchase_stats 的欄位說明，跟著它回傳的 list[dict] 走。
CROSS_PURCHASE_FIELD_NOTES: dict[str, str] = {
    "n_joint": "同一個 query 單位（time × entity）上 j 與 k 皆 label=1 的單位數。",
    "n_j": "j 為 label=1 的 query 單位數。",
    "n_k": "k 為 label=1 的 query 單位數。",
    "p_k_given_j": "n_joint / n_j——買 j 的 query 單位裡有多少比例也買 k。",
    "lift": (
        "p_k_given_j / (n_k / n_units)。lift ≈ 1 代表在這份抽樣樣本上 j、k "
        "近似獨立；> 1 代表比獨立時更常同時出現。"
    ),
}


def _validate(pdf: pd.DataFrame, schema: dict) -> None:
    if SCORE_COL not in pdf.columns:
        raise ValueError(
            f"suppression 需要 {SCORE_COL!r} 欄，但輸入沒有這一欄。與家族其餘"
            "模組一致，不退回 schema.score。"
        )
    query_cols = [schema["time"], *schema["entity"]]
    required = [*query_cols, schema["item"], schema["label"]]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"suppression 輸入缺必要欄位：{missing}")


def _pct(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    return float(np.nanpercentile(np.asarray(values, dtype=np.float64), q))


def _mean(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return float(np.mean(np.asarray(values, dtype=np.float64)))


def _rank_display(rank_value: Optional[float], query_size: Optional[float]) -> Optional[str]:
    if rank_value is None or query_size is None:
        return None

    def compact(x: float) -> str:
        return str(int(round(x))) if abs(x - round(x)) < 1e-9 else f"{x:.1f}"

    return f"{compact(float(rank_value))} of {compact(float(query_size))}"


def _top_targets_summary(rows: list[dict], limit: int = 3) -> str:
    shown = rows[:limit]
    if not shown:
        return ""
    return ", ".join(str(r["positive_item"]) for r in shown)


def cross_purchase_stats(sample_pdf: pd.DataFrame, schema: dict) -> list[dict]:
    """每組 (item_j, item_k) 的交叉購買 lift。

    共現單位 ＝ query 單位（``schema["time"]`` × ``schema["entity"]``，與
    :func:`compute` 裡的 ``groups``、與被取代的 ``cross_purchase.py:33-47``
    一致），只看 ``label == 1`` 的列。母體是傳入的 ``sample_pdf``（診斷
    抽樣，見模組 docstring「母體變了」）。

    Returns:
        list of dict，每個 dict 含 ``item_j``／``item_k``／``n_joint``／
        ``n_j``／``n_k``／``p_k_given_j``／``lift``。``lift`` 在 k 的
        query-單位基礎購買率為 0 時（結構上不可能發生：k 若出現在輸出裡
        代表它在某組合裡是分母，其 n_k 必然 > 0，這裡仍防禦性地回 None）
        才會是 None。
    """
    query_cols = [schema["time"], *schema["entity"]]
    item_col = schema["item"]
    label_col = schema["label"]

    unit_key_all = query_key(sample_pdf, query_cols)
    n_units = int(unit_key_all.nunique())
    if n_units == 0:
        return []

    pos_mask = sample_pdf[label_col].to_numpy() == 1
    if not pos_mask.any():
        return []

    pos = pd.DataFrame({
        "_unit": unit_key_all.to_numpy()[pos_mask],
        "_item": sample_pdf.loc[pos_mask, item_col].astype(str).to_numpy(),
    }).drop_duplicates()

    item_units: dict[str, set] = {
        str(item): set(sub["_unit"]) for item, sub in pos.groupby("_item")
    }
    items = sorted(item_units)
    base_rate = {item: len(units) / n_units for item, units in item_units.items()}

    out: list[dict[str, Any]] = []
    for j in items:
        units_j = item_units[j]
        n_j = len(units_j)
        for k in items:
            if k == j:
                continue
            units_k = item_units[k]
            n_k = len(units_k)
            n_joint = len(units_j & units_k)
            p_k_given_j = 0.0 if n_j == 0 else n_joint / n_j
            k_rate = base_rate[k]
            lift = None if k_rate <= 0.0 else p_k_given_j / k_rate
            out.append({
                "item_j": j,
                "item_k": k,
                "n_joint": int(n_joint),
                "n_j": int(n_j),
                "n_k": int(n_k),
                "p_k_given_j": float(p_k_given_j),
                "lift": None if lift is None else float(lift),
            })
    return out


def compute(diagnosis_sample: tuple[pd.DataFrame, dict], parameters: dict) -> dict:
    """回傳 JSON-safe dict（會直接被 JSONDataset 寫檔）。

    ``diagnosis_sample`` ＝ ``sample.draw_diagnosis_sample`` 的 ``(sample_pdf,
    sample_meta)``。所有 numpy 純量在出口轉成 python ``float``／``int``。

    **三條 return 路徑（停用／空樣本／完整）的 key set 完全相同**，未計算的
    值留 ``None``／空容器，照抄 ``config_shift``／``item_ability`` 的契約。

    ⚠ 這裡的「空樣本」只擋 ``len(sample_pdf) == 0``（抽樣沒抽到任何列）；
    「有列但沒有任何正例」（見 ``test_empty_sample_returns_stub_without_
    raising``）不特判——讓下面的向量化計算自然流過去，因為 per-query 迴圈
    本來就用 ``if yq.sum() == 0: continue`` 跳過沒有正例的 query，算出來的
    ``n_positive_rows=0``／``pair_ledger=[]``／``axis_order=[]`` 已經是正確
    答案，特判反而是多一條沒人驗過的路徑。
    """
    sample_pdf, sample_meta = diagnosis_sample
    schema = get_schema(parameters)
    diag = diag_cfg(parameters)
    cfg = diag.get("suppression", {}) or {}
    mp = metric_params(parameters)

    out: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled", True)),
        "score_col_used": SCORE_COL,
        "metric_params": mp,
        "logit_notes": [],
        "top_examples": int(cfg.get("top_examples", 50)),
        "n_rows": 0,
        "n_queries": 0,
        "n_entities": 0,
        "n_items": 0,
        "n_positive_rows": 0,
        "n_suppressed_positive_rows": 0,
        "suppressed_positive_rate": None,
        "mean_negatives_above_positive": None,
        "n_misordered_pairs": 0,
        "macro_per_item_map": None,
        "total_ap_gap_allocated_to_suppressors": 0.0,
        "target_summary": [],
        "top_suppressors_by_target": [],
        "pair_ledger": [],
        "by_suppressor": [],
        "examples": [],
        "matrices": {
            "target_gap_share": {},
            "affected_positive_rate": {},
            "mean_logit_margin": {},
            "suppressor_target_gap_share": {},
        },
        "axis_order": [],
        "sample_meta": dict(sample_meta or {}),
        "field_notes": FIELD_NOTES,
        "cross_purchase_field_notes": CROSS_PURCHASE_FIELD_NOTES,
        "notes": [],
    }
    if not out["enabled"]:
        out["notes"].append("evaluation.diagnosis.suppression.enabled = false——未計算。")
        return out

    _validate(sample_pdf, schema)

    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——壓制帳本均未計算。")
        return out

    entity_cols = schema["entity"]
    query_cols = [schema["time"], *entity_cols]
    item_col = schema["item"]
    label_col = schema["label"]
    top_examples = out["top_examples"]

    groups = pd.factorize(query_key(sample_pdf, query_cols))[0]
    query_keys = query_key(sample_pdf, query_cols).astype(str).to_numpy()
    clusters = pd.factorize(query_key(sample_pdf, entity_cols))[0]
    items = sample_pdf[item_col].astype(str).to_numpy()
    y = sample_pdf[label_col].to_numpy(dtype=np.int64)
    z, logit_notes = to_logit(sample_pdf[SCORE_COL].to_numpy(dtype=np.float64))
    out["logit_notes"] = logit_notes
    out["notes"].extend(logit_notes)

    with log_step(logger, "suppression.per_item_ap"):
        ap_by_item, _n_pos_ap, macro_map = per_item_ap(groups, items, y, z, mp)
    out["macro_per_item_map"] = macro_map

    unique_items = sorted(set(items.tolist()))
    target_stats: dict[str, dict[str, Any]] = {}
    for item in unique_items:
        item_mask = items == item
        pos_mask_item = item_mask & (y == 1)
        target_stats[item] = {
            "positive_item": item,
            "ap": ap_by_item.get(item),
            "n_pos": int(pos_mask_item.sum()),
            "suppressed_positive_rows": 0,
            "total_suppression_pairs": 0,
            "allocated_ap_gap": 0.0,
            "_positive_ranks": [],
            "_positive_query_sizes": [],
            "_positive_rank_percentiles": [],
            "_negative_above_counts": [],
        }

    n = len(sample_pdf)
    sort_idx = np.lexsort((-z, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y[sort_idx].astype(np.float64)
    item_sorted = items[sort_idx]
    z_sorted = z[sort_idx]
    boundaries = np.concatenate([
        [0], np.flatnonzero(np.diff(g_sorted)) + 1, [n],
    ])

    rank = np.full(n, -1, dtype=np.int64)
    rank_pct = np.full(n, np.nan, dtype=np.float64)

    # 扁平成對表的暫存 list——內層迴圈拿掉後，這是取代 dict-of-set／heapq 的
    # 唯一累積結構。迴圈結束後一次 np.concatenate，統計全部交給 groupby。
    pos_row_parts: list[np.ndarray] = []
    sup_row_parts: list[np.ndarray] = []
    pos_item_parts: list[np.ndarray] = []
    sup_item_parts: list[np.ndarray] = []
    gap_parts: list[np.ndarray] = []
    raw_parts: list[np.ndarray] = []
    margin_parts: list[np.ndarray] = []

    n_misordered_pairs = 0

    with log_step(logger, f"suppression.enumerate_pairs（{n} 列）"):
        for qi in range(len(boundaries) - 1):
            s, e = boundaries[qi], boundaries[qi + 1]
            local_len = e - s
            orig = sort_idx[s:e]
            rank[orig] = np.arange(1, local_len + 1, dtype=np.int64)
            rank_pct[orig] = rank[orig] / float(local_len)

            yq = y_sorted[s:e]
            if yq.sum() == 0:
                continue

            ranks_arr = np.arange(1, local_len + 1, dtype=np.float64)
            k_eff = float(mp["k"]) if mp["k"] is not None else float(local_len)
            cum = np.cumsum(yq)
            contrib = np.where(ranks_arr <= k_eff, cum / ranks_arr, 0.0)
            pos_recip_prefix = np.cumsum(
                np.where((yq == 1) & (ranks_arr <= k_eff), 1.0 / ranks_arr, 0.0)
            )
            pos_positions = np.flatnonzero(yq == 1)
            neg_positions = np.flatnonzero(yq == 0)

            for b in pos_positions:
                pos_orig = int(sort_idx[s + b])
                positive_item = str(item_sorted[b + s])
                tstat = target_stats[positive_item]
                tstat["_positive_ranks"].append(float(rank[pos_orig]))
                tstat["_positive_query_sizes"].append(float(local_len))
                tstat["_positive_rank_percentiles"].append(float(rank_pct[pos_orig]))

                above = neg_positions[neg_positions < b]
                if len(above) == 0:
                    tstat["_negative_above_counts"].append(0)
                    continue

                tstat["suppressed_positive_rows"] += 1
                tstat["total_suppression_pairs"] += int(len(above))
                tstat["_negative_above_counts"].append(int(len(above)))
                n_misordered_pairs += int(len(above))

                a_rank = above + 1.0
                new_contrib = np.where(
                    a_rank <= k_eff, (cum[above] + 1.0) / a_rank, 0.0,
                )
                intermediate_pos_gain = pos_recip_prefix[b - 1] - pos_recip_prefix[above]
                raw_severity = new_contrib - contrib[b] + intermediate_pos_gain
                raw_total_for_row = float(raw_severity.sum())
                row_ap_gap = max(0.0, 1.0 - float(contrib[b]))
                allocated_gap = (
                    raw_severity / raw_total_for_row * row_ap_gap
                    if raw_total_for_row > 0.0 and row_ap_gap > 0.0
                    else np.zeros_like(raw_severity)
                )

                n_above = len(above)
                pos_row_parts.append(np.full(n_above, pos_orig, dtype=np.int64))
                sup_row_parts.append(sort_idx[s + above])
                pos_item_parts.append(np.full(n_above, positive_item, dtype=object))
                sup_item_parts.append(item_sorted[s + above].astype(str))
                gap_parts.append(allocated_gap)
                raw_parts.append(raw_severity)
                margin_parts.append(z_sorted[s + above] - z_sorted[s + b])

    if pos_row_parts:
        pairs_df = pd.DataFrame({
            "pos_row": np.concatenate(pos_row_parts),
            "sup_row": np.concatenate(sup_row_parts),
            "pos_item": np.concatenate(pos_item_parts),
            "sup_item": np.concatenate(sup_item_parts),
            "gap": np.concatenate(gap_parts),
            "raw": np.concatenate(raw_parts),
            "margin": np.concatenate(margin_parts),
        })
    else:
        pairs_df = pd.DataFrame({
            "pos_row": pd.Series(dtype=np.int64),
            "sup_row": pd.Series(dtype=np.int64),
            "pos_item": pd.Series(dtype=object),
            "sup_item": pd.Series(dtype=object),
            "gap": pd.Series(dtype=np.float64),
            "raw": pd.Series(dtype=np.float64),
            "margin": pd.Series(dtype=np.float64),
        })

    logger.info(
        "suppression: n_pairs=%d n_positive_rows=%d n_misordered_pairs=%d",
        len(pairs_df), int(y.sum()), n_misordered_pairs,
    )

    total_allocated_gap = float(pairs_df["gap"].sum()) if len(pairs_df) else 0.0

    if len(pairs_df) > 0:
        with log_step(logger, f"suppression.aggregate_pairs（{len(pairs_df)} 對）"):
            pair_agg = pairs_df.groupby(["pos_item", "sup_item"], sort=False).agg(
                allocated_ap_gap=("gap", "sum"),
                affected_positive_rows=("pos_row", "nunique"),
                mean_score_margin=("margin", "mean"),
                median_score_margin=("margin", "median"),
            ).reset_index()
            alloc_by_target = pairs_df.groupby("pos_item")["gap"].sum()
            sup_agg = pairs_df.groupby("sup_item").agg(
                allocated_ap_gap=("gap", "sum"),
                affected_positive_rows=("pos_row", "nunique"),
                affected_positive_items=("pos_item", "nunique"),
                mean_score_margin=("margin", "mean"),
            ).reset_index()
    else:
        pair_agg = pd.DataFrame(columns=[
            "pos_item", "sup_item", "allocated_ap_gap",
            "affected_positive_rows", "mean_score_margin", "median_score_margin",
        ])
        alloc_by_target = pd.Series(dtype=np.float64)
        sup_agg = pd.DataFrame(columns=[
            "sup_item", "allocated_ap_gap", "affected_positive_rows",
            "affected_positive_items", "mean_score_margin",
        ])

    for item, t in target_stats.items():
        t["allocated_ap_gap"] = float(alloc_by_target.get(item, 0.0))

    pair_rows: list[dict[str, Any]] = []
    for r in pair_agg.itertuples(index=False):
        positive_item = str(r.pos_item)
        suppressor_item = str(r.sup_item)
        n_pos_target = target_stats[positive_item]["n_pos"]
        target_gap = float(alloc_by_target.get(positive_item, 0.0))
        mean_margin = float(r.mean_score_margin) if pd.notna(r.mean_score_margin) else None
        median_margin = float(r.median_score_margin) if pd.notna(r.median_score_margin) else None
        pair_rows.append({
            "positive_item": positive_item,
            "suppressor_item": suppressor_item,
            "affected_positive_rows": int(r.affected_positive_rows),
            "affected_positive_rate": (
                None if n_pos_target == 0
                else r.affected_positive_rows / n_pos_target
            ),
            "mean_score_margin": mean_margin,
            "median_score_margin": median_margin,
            "allocated_ap_gap": float(r.allocated_ap_gap),
            "target_ap_gap_share": (
                None if target_gap <= 0.0
                else float(r.allocated_ap_gap) / target_gap
            ),
            "overall_ap_gap_share": (
                None if total_allocated_gap <= 0.0
                else float(r.allocated_ap_gap) / total_allocated_gap
            ),
        })
    pair_rows.sort(key=lambda r: (
        -float(r["allocated_ap_gap"]), r["positive_item"], r["suppressor_item"],
    ))

    target_rows: list[dict[str, Any]] = []
    top_suppressors_by_target: list[dict[str, Any]] = []
    for item, t in target_stats.items():
        ranks = t.pop("_positive_ranks")
        query_sizes = t.pop("_positive_query_sizes")
        ranks_list = t.pop("_positive_rank_percentiles")
        neg_above = t.pop("_negative_above_counts")
        suppressed_positive_rate = (
            None if t["n_pos"] == 0 else t["suppressed_positive_rows"] / t["n_pos"]
        )
        mean_neg_above = _mean([float(x) for x in neg_above])
        ap_gap = None if t["ap"] is None else 1.0 - float(t["ap"])
        ap_gap_from_suppressors = (
            None if t["n_pos"] == 0 else float(t["allocated_ap_gap"]) / t["n_pos"]
        )
        unexplained_ap_gap = (
            None if ap_gap is None or ap_gap_from_suppressors is None
            else max(0.0, float(ap_gap) - float(ap_gap_from_suppressors))
        )
        overall_ap_gap_share = (
            None if total_allocated_gap <= 0.0
            else float(t["allocated_ap_gap"]) / total_allocated_gap
        )
        median_rank = _pct(ranks, 50)
        median_query_size = _pct(query_sizes, 50)
        median_rank_display = _rank_display(median_rank, median_query_size)
        median_rank_pct = _pct(ranks_list, 50)
        item_pairs = sorted(
            [p for p in pair_rows if p["positive_item"] == item],
            key=lambda r: -float(r["allocated_ap_gap"]),
        )
        top_suppressor = ""
        if item_pairs:
            top_suppressor = str(item_pairs[0]["suppressor_item"])
            for rank_i, p in enumerate(item_pairs[:3], start=1):
                top_suppressors_by_target.append({
                    "positive_item": item,
                    "suppressor_rank": rank_i,
                    "suppressor_item": p["suppressor_item"],
                    "target_ap_gap_share": p["target_ap_gap_share"],
                    "overall_ap_gap_share": p["overall_ap_gap_share"],
                    "affected_positive_rows": p["affected_positive_rows"],
                    "affected_positive_rate": p["affected_positive_rate"],
                    "mean_score_margin": p["mean_score_margin"],
                })
        target_rows.append({
            "positive_item": item,
            "ap": t["ap"],
            "ap_gap": ap_gap,
            "n_pos": t["n_pos"],
            "ap_gap_from_suppressors": ap_gap_from_suppressors,
            "unexplained_ap_gap": unexplained_ap_gap,
            "overall_ap_gap_share": overall_ap_gap_share,
            "suppressed_positive_rate": suppressed_positive_rate,
            "mean_negatives_above_positive": mean_neg_above,
            "median_positive_rank_display": median_rank_display,
            "median_positive_rank_percentile": median_rank_pct,
            "top_suppressor": top_suppressor,
        })
    target_rows.sort(key=lambda r: (
        -(0.0 if r["overall_ap_gap_share"] is None else float(r["overall_ap_gap_share"])),
        float("inf") if r["ap"] is None else float(r["ap"]),
        r["positive_item"],
    ))

    suppressor_rows: list[dict[str, Any]] = []
    for r in sup_agg.itertuples(index=False):
        suppressor_item = str(r.sup_item)
        sup_pairs = sorted(
            [p for p in pair_rows if p["suppressor_item"] == suppressor_item],
            key=lambda x: -float(x["allocated_ap_gap"]),
        )
        mean_margin = float(r.mean_score_margin) if pd.notna(r.mean_score_margin) else None
        suppressor_rows.append({
            "suppressor_item": suppressor_item,
            "affected_positive_items": int(r.affected_positive_items),
            "affected_positive_rows": int(r.affected_positive_rows),
            "overall_ap_gap_share": (
                None if total_allocated_gap <= 0.0
                else float(r.allocated_ap_gap) / total_allocated_gap
            ),
            "mean_score_margin": mean_margin,
            "top_positive_items": _top_targets_summary(sup_pairs, 3),
        })
    suppressor_rows.sort(key=lambda r: (
        -(0.0 if r["overall_ap_gap_share"] is None else float(r["overall_ap_gap_share"])),
        r["suppressor_item"],
    ))

    examples: list[dict[str, Any]] = []
    if top_examples > 0 and len(pairs_df) > 0:
        top_df = pairs_df.nlargest(top_examples, ["gap", "raw"])
        for row in top_df.itertuples(index=False):
            pos_row = int(row.pos_row)
            sup_row = int(row.sup_row)
            examples.append({
                "query": str(query_keys[pos_row]),
                "positive_item": str(row.pos_item),
                "suppressor_item": str(row.sup_item),
                "positive_score": float(z[pos_row]),
                "suppressor_score": float(z[sup_row]),
                "positive_rank": int(rank[pos_row]),
                "suppressor_rank": int(rank[sup_row]),
                "score_margin": float(row.margin),
                "allocated_ap_gap": float(row.gap),
            })

    target_gap_share_matrix: dict[str, dict[str, float]] = {}
    affected_rate_matrix: dict[str, dict[str, float]] = {}
    mean_margin_matrix: dict[str, dict[str, float]] = {}
    suppressor_target_gap_share_matrix: dict[str, dict[str, float]] = {}
    suppressor_allocated_gap = {
        str(r.sup_item): float(r.allocated_ap_gap) for r in sup_agg.itertuples(index=False)
    }
    axis_items: set = set()
    for p in pair_rows:
        axis_items.add(p["positive_item"])
        axis_items.add(p["suppressor_item"])
        target_gap_share_matrix.setdefault(p["positive_item"], {})[p["suppressor_item"]] = float(
            p["target_ap_gap_share"] or 0.0
        )
        affected_rate_matrix.setdefault(p["positive_item"], {})[p["suppressor_item"]] = float(
            p["affected_positive_rate"] or 0.0
        )
        mean_margin_matrix.setdefault(p["positive_item"], {})[p["suppressor_item"]] = float(
            p["mean_score_margin"] or 0.0
        )
        sup_total = suppressor_allocated_gap.get(p["suppressor_item"], 0.0)
        suppressor_target_gap_share_matrix.setdefault(p["suppressor_item"], {})[p["positive_item"]] = (
            0.0 if sup_total <= 0.0 else float(p["allocated_ap_gap"]) / sup_total
        )

    n_suppressed_positive_rows = int(
        sum(r["suppressed_positive_rows"] for r in target_stats.values())
    )

    out["target_summary"] = target_rows
    out["top_suppressors_by_target"] = top_suppressors_by_target
    out["pair_ledger"] = pair_rows
    out["by_suppressor"] = suppressor_rows
    out["examples"] = examples
    out["matrices"] = {
        "target_gap_share": target_gap_share_matrix,
        "affected_positive_rate": affected_rate_matrix,
        "mean_logit_margin": mean_margin_matrix,
        "suppressor_target_gap_share": suppressor_target_gap_share_matrix,
    }
    out["axis_order"] = sorted(axis_items)
    out["n_rows"] = int(n)
    out["n_queries"] = int(len(boundaries) - 1)
    out["n_entities"] = int(len(np.unique(clusters)))
    out["n_items"] = int(len(unique_items))
    out["n_positive_rows"] = int(y.sum())
    out["n_suppressed_positive_rows"] = n_suppressed_positive_rows
    out["suppressed_positive_rate"] = (
        None if int(y.sum()) == 0 else n_suppressed_positive_rows / int(y.sum())
    )
    out["mean_negatives_above_positive"] = (
        None if int(y.sum()) == 0 else int(n_misordered_pairs) / int(y.sum())
    )
    out["n_misordered_pairs"] = int(n_misordered_pairs)
    out["total_ap_gap_allocated_to_suppressors"] = total_allocated_gap

    logger.info(
        "suppression: %d queries, %d entities, %d items, "
        "n_suppressed_positive_rows=%d, total_ap_gap_allocated_to_suppressors=%.6f",
        out["n_queries"], out["n_entities"], out["n_items"],
        n_suppressed_positive_rows, total_allocated_gap,
    )
    return out
