"""item_ability 計算層：模型能不能在同一個 query 內分辨誰會買哪個 item。

回答的問題（只有這一個）：**對每個 item，模型是不是真的在同一位客戶的候選
清單裡把它排到前面，而不是只因為某些客戶整體分數較高**。

兩個 AUC，為什麼要兩個
-----------------------
對每個 item j，在 item j 的所有列上把正例（該客戶買了 j）與負例（沒買）算
一次加權 AUC：

    raw_within_item_auc      = AUC(logit(score_uncalibrated), label)
    query_centered_auc       = AUC(logit(score_uncalibrated) − query 平均, label)

``query_centered_auc`` 先用 :func:`query_center_scores` 把每個 query 的平均
logit 分數扣掉，只留「同一位客戶的候選之間誰比較被看好」這個相對訊號；
``raw_within_item_auc`` 沒扣，混進了「這位客戶整體分數水準」（同一 query 內
所有候選的平均分數，跟這個 item 專屬的排序無關）。

    auc_gap_raw_minus_centered = raw_within_item_auc − query_centered_auc

**這個差值的方向**（raw 減 centered，不是絕對值、不是反過來）**量的是客戶整體
分數水準對 raw AUC 的貢獻**：正值＝raw AUC 被「買家恰好是整體分數較高的客戶」
撐高（看起來的能力有一部分不是 item 專屬的）、負值＝相反（買家整體分數較低、
raw AUC 反而被拉低）。方向本身就是訊號，散點圖偏離對角線的方向就是它。取絕對
值或反號都不會讓任何數值測試轉紅，只有方向測試
（``test_auc_gap_is_raw_minus_centered_not_absolute``）守得住。

sort-once bootstrap
--------------------
每個 item 的 AUC CI 都要靠分層 cluster bootstrap（見
``recsys_tfb.diagnosis.metric.uncertainty.iter_stratified_cluster_multipliers``
——本模組不寫第二份重抽迴圈，直接共用那份骨架）。試作腳本
（``scripts/item_ability_diagnosis.py``）每次呼叫 ``weighted_auc`` 都重新排序
一次分數，公司規模（≈25 萬 query × 22 item ≈ 550 萬列）下這是
``n_items × (n_boot + 2)`` 次排序，慢到無法接受。

拆法：:func:`presort_by_score` 把「排序＋抓同分組邊界」獨立出來，每個 item
的 raw／centered 分數**各排一次**（在 bootstrap 迴圈**外**）；
:func:`weighted_auc_presorted` 只做那個線性掃描，**內部不得再排序**。
bootstrap 迴圈內只做「拿乘數 → 乘上 inclusion_weight → 依既定的 order 重排
→ 呼叫 weighted_auc_presorted」。``test_bootstrap_sorts_once_per_item_regardless_of_n_boot``
把這個效能宣稱釘死：排序次數固定是 ``2 × n_items``，與 ``n_boot`` 無關。

三個相對於試作腳本（``scripts/item_ability_diagnosis.py``）的行為修正
-----------------------------------------------------------------------
1. **讀不到 ``score_uncalibrated`` 直接 raise，不退回 ``score``。** 理由與
   ``config_shift`` 相同：raw／centered AUC 是在模型輸出的 logit 空間上算
   的，校準後的分數是另一個空間的量，兩者不能混。
2. **點估計也吃 ``inclusion_weight``。** 試作腳本的 ``weighted_auc(z_i, yy)``
   點估計沒有帶權重（``weight=None`` → 全 1）。診斷抽樣是分層的，不加權的
   話某一層（例如 hash_ratio 降抽層）的客戶會被系統性低估——與
   ``config_shift``／``uncertainty`` 對 ``inclusion_weight`` 的處理方式一致。
   ``test_inclusion_weight_changes_the_auc`` 把這條釘在點估計（``n_boot=0``）
   上，不是只釘在 CI 上。
3. **bootstrap 改用分層 cluster 重抽，不是試作腳本的全域 SRS。** 試作腳本的
   ``_bootstrap_item_auc`` 用 ``RandomState.randint`` 對全體客戶做無分層的
   簡單隨機重抽，且完全不管 ``inclusion_weight``。這裡改用
   :func:`~recsys_tfb.diagnosis.metric.uncertainty.iter_stratified_cluster_multipliers`
   （層內獨立重抽、各層單位數維持原值），與本套診斷家族其他 CI 一致。

不下結論
--------
本模組只輸出數字、分布與對照點。**沒有** severity／verdict／建議動作，也沒有
把連續量切成離散類別的門檻——判斷留給讀者。試作腳本裡用顏色標記
「bottom-right 高 raw 低 centered」的 SVG 散點圖邏輯刻意沒有移植過來（那是
呈現層的事，且已經在往「這是壞的」的方向下判斷）。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import (
    diag_cfg, metric_params, per_item_ap, query_key, sample_arrays, to_logit,
)
from recsys_tfb.diagnosis.metric.uncertainty import iter_stratified_cluster_multipliers

logger = logging.getLogger(__name__)

#: 唯一可用的分數欄。見模組 docstring 修正 1——不設 fallback 是刻意的。
SCORE_COL = "score_uncalibrated"

#: 每個非顯然欄位一句話定義，跟著 JSON 走。純定義，不含判讀（見模組 docstring
#: 「不下結論」）。
FIELD_NOTES: dict[str, str] = {
    "raw_within_item_auc": (
        "item j 的正例列 vs 負例列，在 logit(score_uncalibrated) 上直接算的"
        "加權 AUC（inclusion_weight 加權，同分給 0.5 分）。未扣掉 query 內的"
        "平均分數，客戶整體分數水準與 item 專屬的排序能力混在一起。"
    ),
    "query_centered_auc": (
        "與 raw_within_item_auc 同樣的 AUC 計算，但分數先扣掉各自 query 的"
        "平均 logit(score_uncalibrated)（見 query_center_scores）——把 query "
        "內的整體水準移除，只留 item 相對於同一 query 其他候選的排序能力。"
    ),
    "auc_gap_raw_minus_centered": (
        "raw_within_item_auc − query_centered_auc（方向固定：raw 減 centered，"
        "不取絕對值）。量的是 raw AUC 裡有多少來自「客戶整體分數水準」（同一 "
        "query 內所有候選的平均分數）而非 item 在 query 內的相對排序：正值＝raw "
        "被『買家恰好是整體分數較高的客戶』撐高、負值＝相反。無論正負，"
        "query_centered_auc 才是移除這個因素後 item 自己的排序能力。"
    ),
    "raw_within_item_auc_ci_low": "raw_within_item_auc 的 2.5 百分位（分層 cluster bootstrap）。",
    "raw_within_item_auc_ci_high": "raw_within_item_auc 的 97.5 百分位（分層 cluster bootstrap）。",
    "query_centered_auc_ci_low": "query_centered_auc 的 2.5 百分位（分層 cluster bootstrap）。",
    "query_centered_auc_ci_high": "query_centered_auc 的 97.5 百分位（分層 cluster bootstrap）。",
    "mean_relative_score_pos": "該 item 正例列的 query-centered 分數平均。",
    "mean_relative_score_neg": "該 item 負例列的 query-centered 分數平均。",
    "relative_score_gap": "mean_relative_score_pos − mean_relative_score_neg。",
    "median_positive_rank": (
        "該 item 正例列在各自 query 內的名次（1＝分數最高、排最前；越大越靠後）"
        "之中位數。未加權。"
    ),
    "p10_positive_rank": "正例名次的第 10 百分位數（較好的一端），未加權。",
    "p25_positive_rank": "正例名次的第 25 百分位數，未加權。",
    "p75_positive_rank": "正例名次的第 75 百分位數，未加權。",
    "p90_positive_rank": "正例名次的第 90 百分位數（較差的一端），未加權。",
    "positive_ranks": (
        "該 item 每一個正例列在各自 query 內的名次（1-based，1＝排最前）原始值"
        "列表。"
    ),
    "ap": (
        "該 item 的 average precision（依 evaluation.metric 的 k／shrinkage "
        "設定），未經 inclusion_weight 加權。"
    ),
    "n_pos": "該 item 的正例列數（原始列數，未加權）。",
    "n_neg": "該 item 的負例列數（原始列數，未加權）。",
    "n_pos_ap": (
        "positive_row_contributions 實際採計進 ap 的正例列數（可能因 k 截斷"
        "而少於 n_pos）。"
    ),
    "macro_per_item_map": (
        "全體 item 的 macro per-item mAP 點估計（未經 inclusion_weight 加"
        "權，與 ap 同一計算路徑）。"
    ),
}


def query_center_scores(groups: np.ndarray, z: np.ndarray) -> np.ndarray:
    """逐 query 減掉該 query 的平均分數。"""
    sums = np.bincount(groups, weights=z)
    counts = np.bincount(groups)
    return z - (sums / counts)[groups]


def descending_ranks(groups: np.ndarray, score: np.ndarray) -> np.ndarray:
    """One-based rank within each query, highest score = rank 1; lower is better.

    回傳原始名次（不除以 query size）：名次直接讀得懂（「排第 3」），而百分位
    （rank ÷ query size）在 query 候選數不固定時才需要，且「0.125」這種數字讀
    者難以直覺對上「排第 1」。候選數本身另外在 ``candidates_per_query`` 交代，
    讓讀者知道名次是「幾中選幾」。
    """
    out = np.full(len(score), np.nan, dtype=np.float64)
    if len(score) == 0:
        return out
    order = np.lexsort((-score, groups))
    g_sorted = groups[order]
    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        idx = order[s:e]
        n = e - s
        out[idx] = np.arange(1, n + 1, dtype=np.float64)
    return out


def presort_by_score(score: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """對分數排序一次，回傳排序索引與同分組邊界。

    ``order`` ＝ 依分數遞增排序的索引（``kind="mergesort"``，穩定排序，與
    原版 ``weighted_auc`` 用的排序法一致）。``tie_starts`` ＝ 排序後每個同分
    組的起始索引，含結尾哨兵——例如排序後分數是 ``[1,1,2,3,3,3]`` 則
    ``tie_starts = [0,2,3,6]``（第一組 ``[0,2)``、第二組 ``[2,3)``、第三組
    ``[3,6)``）。

    只在這裡排序一次：bootstrap 迴圈只換權重、重排到這個既定的 ``order``，
    不再呼叫排序——這是本模組 sort-once 效能宣稱的唯一依據。
    """
    s = np.asarray(score, dtype=np.float64)
    order = np.argsort(s, kind="mergesort")
    n = len(order)
    if n == 0:
        return order, np.zeros(1, dtype=np.int64)
    s_sorted = s[order]
    boundaries = np.flatnonzero(np.diff(s_sorted)) + 1
    tie_starts = np.concatenate([[0], boundaries, [n]]).astype(np.int64)
    return order, tie_starts


def weighted_auc_presorted(
    labels: np.ndarray,
    weights: np.ndarray,
    tie_starts: np.ndarray,
) -> Optional[float]:
    """已排序資料的加權 AUC 線性掃描；同分組給 0.5 分。

    ``labels``／``weights`` 必須已依 :func:`presort_by_score` 回傳的
    ``order`` 重排過。**內部不得再排序**——那正是 sort-once 效能宣稱的意義。

    權重為 0 的列自然貢獻 0（正例／負例權重和的加總本來就不含它），所以不需
    要另外過濾掉權重為 0 的列——原版 ``weighted_auc``（``scripts/
    item_ability_diagnosis.py``）的 ``keep = w > 0`` 過濾在這裡拿掉了：過濾
    會位移陣列索引，跟 ``tie_starts`` 記錄的絕對位置對不上。兩者在權重非負
    時數學上等價（``inclusion_weight`` 與 bootstrap 乘數都不會是負值）。
    """
    n = len(labels)
    if n == 0:
        return None
    yy = np.asarray(labels, dtype=np.int64)
    w = np.asarray(weights, dtype=np.float64)

    pos_total = float(w[yy == 1].sum())
    neg_total = float(w[yy == 0].sum())
    if pos_total <= 0.0 or neg_total <= 0.0:
        return None

    numer = 0.0
    neg_before = 0.0
    for i in range(len(tie_starts) - 1):
        start, end = int(tie_starts[i]), int(tie_starts[i + 1])
        yy_g = yy[start:end]
        w_g = w[start:end]
        pos_w = float(w_g[yy_g == 1].sum())
        neg_w = float(w_g[yy_g == 0].sum())
        numer += pos_w * (neg_before + 0.5 * neg_w)
        neg_before += neg_w
    return float(numer / (pos_total * neg_total))


def _ci_bounds(boot: np.ndarray) -> tuple[Optional[float], Optional[float]]:
    if len(boot) == 0 or np.all(np.isnan(boot)):
        return None, None
    return (
        float(np.nanpercentile(boot, 2.5)),
        float(np.nanpercentile(boot, 97.5)),
    )


def _validate(pdf: pd.DataFrame, schema: dict) -> None:
    if SCORE_COL not in pdf.columns:
        raise ValueError(
            f"item_ability 需要 {SCORE_COL!r} 欄，但輸入沒有這一欄。這裡刻意"
            "不退回 schema.score：raw／query-centered AUC 是在 "
            f"logit({SCORE_COL}) 空間上算的，校準後的分數是另一個空間的量，"
            "用它算出來的 AUC 不是同一件事的估計值。"
        )
    query_cols = [schema["time"], *schema["entity"]]
    required = [*query_cols, schema["item"], schema["label"]]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"item_ability 輸入缺必要欄位：{missing}")


def compute(diagnosis_sample: tuple[pd.DataFrame, dict], parameters: dict) -> dict:
    """回傳 JSON-safe dict（會直接被 JSONDataset 寫檔）。

    ``diagnosis_sample`` ＝ ``sample.draw_diagnosis_sample`` 的 ``(sample_pdf,
    sample_meta)``。所有 numpy 純量在出口轉成 python ``float``／``int``。

    **三條 return 路徑（停用／空樣本／完整）的 key set 完全相同**，未計算的
    值留 ``None``／空容器——呼叫端（``render``）因此不必為每個鍵寫存在性
    判斷（照抄 ``config_shift.compute`` 的契約）。
    """
    sample_pdf, sample_meta = diagnosis_sample
    schema = get_schema(parameters)
    diag = diag_cfg(parameters)
    cfg = diag.get("item_ability", {}) or {}
    ci_cfg = diag.get("ci", {}) or {}
    mp = metric_params(parameters)
    ci_info = {
        "enabled": bool(ci_cfg.get("enabled", True)),
        "n_boot": int(ci_cfg.get("n_boot", 200)),
        "seed": int((diag.get("sample", {}) or {}).get("seed", 42)),
    }

    out: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled", True)),
        "score_col_used": SCORE_COL,
        "metric_params": mp,
        "logit_notes": [],
        "top_n": int(cfg.get("top_n", 30)),
        "n_rows": 0,
        "n_queries": 0,
        "n_entities": 0,
        "n_items": 0,
        "n_positive_rows": 0,
        "macro_per_item_map": None,
        "candidates_per_query": None,
        "ci": ci_info,
        "per_item": [],
        "sample_meta": dict(sample_meta or {}),
        "field_notes": FIELD_NOTES,
        "notes": [],
    }
    if not out["enabled"]:
        out["notes"].append("evaluation.diagnosis.item_ability.enabled = false——未計算。")
        return out

    # 欄位檢查即使樣本為空也要做——空樣本代表「抽樣沒抽到東西」，欄位缺失
    # 代表「上游沒把該帶的欄帶出來」，兩者是不同的錯誤，順序不能顛倒。
    _validate(sample_pdf, schema)

    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——per-item AUC 均未計算。")
        return out

    entity_cols = schema["entity"]

    groups, items, y, _, ht_weight = sample_arrays(sample_pdf, schema)
    # item_ability 只需要「缺席時填 1」的權重（見 sample_arrays docstring 的
    # ht_weights／row_weights 區分），丟掉可為 None 的 ht_weights 那一個。
    # clusters 這裡要**已經 factorize** 過的連續 int 陣列（下面
    # iter_stratified_cluster_multipliers 拿它直接當索引），sample_arrays
    # 刻意不回傳 clusters（config_shift 要的是另一種型別），故自己現組。
    clusters = pd.factorize(query_key(sample_pdf, entity_cols))[0]
    n_entities = int(clusters.max()) + 1

    # strata 同一套來源，與 config_shift／uncertainty 一致：缺欄（未分層抽樣）
    # 時退回單一層 "__all__"。
    strata = (
        sample_pdf["stratum"].astype(str).to_numpy()
        if "stratum" in sample_pdf.columns
        else np.full(len(sample_pdf), "__all__")
    )

    with log_step(logger, "item_ability.base_arrays"):
        z, logit_notes = to_logit(sample_pdf[SCORE_COL].to_numpy(dtype=np.float64))
        out["logit_notes"] = logit_notes
        out["notes"].extend(logit_notes)
        rel = query_center_scores(groups, z)
        rank = descending_ranks(groups, z)
        ap_by_item, n_pos_ap, macro_map = per_item_ap(groups, items, y, z, mp)
    out["macro_per_item_map"] = macro_map

    # 每個 query 的候選數（＝該 query 內的列數）：名次要「幾中選幾」才讀得懂，
    # 這是名次的分母。min==max 代表候選數固定（例如每位客戶都對全部 item 評分）。
    query_sizes = np.bincount(groups)
    out["candidates_per_query"] = {
        "min": int(query_sizes.min()),
        "median": float(np.median(query_sizes)),
        "max": int(query_sizes.max()),
    }

    n_boot = ci_info["n_boot"] if ci_info["enabled"] else 0
    seed = ci_info["seed"]
    unique_items = sorted(set(items.tolist()))
    n_items_total = len(unique_items)

    per_item: list[dict[str, Any]] = []
    # per-item 迴圈是本模組最貴的一段（item 數 × 分層 cluster bootstrap）。
    # 公司規模下這段會安靜跑很久，看起來像卡住，所以逐項印進度，不是只包一個
    # log_step——只包外層的話，使用者看到的仍是「開始」與「結束」之間一段
    # 長時間沒有任何輸出。
    with log_step(logger, f"item_ability.per_item_auc（{n_items_total} 項）"):
        for idx, item in enumerate(unique_items, start=1):
            mask = items == item
            yy = y[mask]
            z_i = z[mask]
            rel_i = rel[mask]
            w_i = ht_weight[mask]
            cl_i = clusters[mask]
            strata_i = strata[mask]
            pos_mask = yy == 1
            neg_mask = yy == 0

            # 排序只在這裡做一次（raw／centered 各一次），bootstrap 迴圈內
            # 只換權重重排到這個既定的 order——sort-once 效能宣稱的落實處。
            order_raw, ts_raw = presort_by_score(z_i)
            order_cen, ts_cen = presort_by_score(rel_i)
            yy_raw = yy[order_raw]
            yy_cen = yy[order_cen]

            raw_auc = weighted_auc_presorted(yy_raw, w_i[order_raw], ts_raw)
            centered_auc = weighted_auc_presorted(yy_cen, w_i[order_cen], ts_cen)

            boot_raw = np.full(n_boot, np.nan, dtype=np.float64)
            boot_centered = np.full(n_boot, np.nan, dtype=np.float64)
            if n_boot > 0 and len(cl_i) > 0:
                for b, mult in enumerate(
                    iter_stratified_cluster_multipliers(cl_i, strata_i, n_boot, seed)
                ):
                    w_b = mult * w_i
                    auc_r = weighted_auc_presorted(yy_raw, w_b[order_raw], ts_raw)
                    if auc_r is not None:
                        boot_raw[b] = auc_r
                    auc_c = weighted_auc_presorted(yy_cen, w_b[order_cen], ts_cen)
                    if auc_c is not None:
                        boot_centered[b] = auc_c

            raw_ci_low, raw_ci_high = _ci_bounds(boot_raw)
            centered_ci_low, centered_ci_high = _ci_bounds(boot_centered)
            gap = (
                None if raw_auc is None or centered_auc is None
                else float(raw_auc - centered_auc)
            )

            pos_rel = rel_i[pos_mask]
            neg_rel = rel_i[neg_mask]
            pos_rank = rank[mask][pos_mask]
            mean_pos = float(np.mean(pos_rel)) if len(pos_rel) else None
            mean_neg = float(np.mean(neg_rel)) if len(neg_rel) else None

            per_item.append({
                "item": item,
                "ap": ap_by_item.get(item),
                "n_pos": int(pos_mask.sum()),
                "n_neg": int(neg_mask.sum()),
                "query_centered_auc": centered_auc,
                "query_centered_auc_ci_low": centered_ci_low,
                "query_centered_auc_ci_high": centered_ci_high,
                "raw_within_item_auc": raw_auc,
                "raw_within_item_auc_ci_low": raw_ci_low,
                "raw_within_item_auc_ci_high": raw_ci_high,
                "auc_gap_raw_minus_centered": gap,
                "mean_relative_score_pos": mean_pos,
                "mean_relative_score_neg": mean_neg,
                "relative_score_gap": (
                    None if mean_pos is None or mean_neg is None
                    else float(mean_pos - mean_neg)
                ),
                "median_positive_rank": (
                    None if len(pos_rank) == 0 else float(np.nanmedian(pos_rank))
                ),
                "p10_positive_rank": (
                    None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 10))
                ),
                "p25_positive_rank": (
                    None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 25))
                ),
                "p75_positive_rank": (
                    None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 75))
                ),
                "p90_positive_rank": (
                    None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 90))
                ),
                "positive_ranks": [int(r) for r in pos_rank],
                "n_pos_ap": int(n_pos_ap.get(item, 0)),
            })
            logger.info(
                "item_ability per-item %d/%d (item=%s, raw_auc=%s, centered_auc=%s)",
                idx, n_items_total, item, raw_auc, centered_auc,
            )

    per_item.sort(key=lambda r: (
        float("inf") if r["ap"] is None else float(r["ap"]),
        -(r["n_pos"]),
        r["item"],
    ))
    out["per_item"] = per_item
    out["n_rows"] = int(len(sample_pdf))
    out["n_queries"] = int(len(np.unique(groups)))
    out["n_entities"] = n_entities
    out["n_items"] = int(len(per_item))
    out["n_positive_rows"] = int(y.sum())

    logger.info(
        "item_ability: %d queries, %d entities, %d items, macro_per_item_map=%.6f",
        out["n_queries"], out["n_entities"], out["n_items"], macro_map,
    )
    return out
