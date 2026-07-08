"""結構層 Gain 帳本：LightGBM booster 跨樹按 item 記帳（完整集合遍歷變體）。

從每棵樹的 root 走訪，把切點 gain 分成兩帳：item-id 切點的 gain（isolate-by-item 的
成本）與已經過 item-id 切點「conditioned」之後的 context 切點 gain（item 隔出來之後
還花了多少 gain 精修判別力）。「item 隔出來之後幾乎沒有後續 context gain」＝該 item
葉預算餓死的結構鐵證——診斷框架項目 8。

雙層結構（可測性）：``_ledger_from_trees`` 是純 pandas/dict 核心（只吃
``booster.trees_to_dataframe()`` 的 DataFrame，不碰 model/preprocessor，單元測試直接
餵手工 DataFrame）；``compute_gain_ledger`` 是 thin wrapper——讀 config/schema、解析
booster 與 preprocessor 的 item 值映射後轉呼叫核心；映射缺席時降級為粗帳本。
"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.model.attribution import _resolve_booster

logger = logging.getLogger(__name__)


def _total_gain(trees: pd.DataFrame) -> float:
    """全部切點 split_gain 總和（leaf 的 NaN 視為 0，負值截為 0）。"""
    return float(
        pd.to_numeric(trees["split_gain"], errors="coerce").fillna(0).clip(lower=0).sum()
    )


def _tree_index_summary(tree_indices: list) -> dict:
    """item 切點 tree_index 的分位數摘要（min/max 為 int、p25/p50/p75 為 float）；空 list 全回 None。"""
    if not tree_indices:
        return {"min": None, "p25": None, "p50": None, "p75": None, "max": None}
    arr = np.asarray(sorted(tree_indices), dtype=float)
    return {
        "min": int(arr.min()),
        "p25": float(np.percentile(arr, 25)),
        "p50": float(np.percentile(arr, 50)),
        "p75": float(np.percentile(arr, 75)),
        "max": int(arr.max()),
    }


def _item_id_block(trees: pd.DataFrame, item_feature: str, total_gain: float) -> dict:
    """item-id 帳：對 ``split_feature == item_feature`` 的切點直接篩選加總。

    刻意與遍歷/reachable 無關——任何一個 item 切點，不論落在哪個（甚至已被上游條件
    排除到不可能命中的）分支之下，其本身的 gain 仍算入 item-id 帳；這讓粗帳本降級
    路徑可以重用同一段邏輯（完全不需要 categories）。
    """
    item_rows = trees[trees["split_feature"] == item_feature]
    gain_sum = float(
        pd.to_numeric(item_rows["split_gain"], errors="coerce").fillna(0).clip(lower=0).sum()
    )
    gain_share = (gain_sum / total_gain) if total_gain > 0 else None
    tree_indices = [int(t) for t in item_rows["tree_index"].tolist()]
    return {
        "split_count": int(len(item_rows)),
        "gain_sum": gain_sum,
        "gain_share": gain_share,
        "tree_index_summary": _tree_index_summary(tree_indices),
    }


def _decode_threshold(threshold, categories: list) -> tuple:
    """類別碼集合字串（如 ``"2||3||4"``，單碼如 ``"1"``）→ (item 值集合, 超出範圍的原始碼字串集合)。

    碼＝categories 的 list 索引。
    """
    values: set = set()
    unknown: set = set()
    for token in str(threshold).split("||"):
        token = token.strip()
        try:
            code = int(float(token))
        except ValueError:
            unknown.add(token)
            continue
        if 0 <= code < len(categories):
            values.add(categories[code])
        else:
            unknown.add(token)
    return values, unknown


def _ledger_from_trees(trees: pd.DataFrame, item_feature: str, categories: list) -> dict:
    """純 pandas/dict 核心：從 ``booster.trees_to_dataframe()`` 的 DataFrame 記帳。

    對每棵樹從 root 走訪（iterative stack），攜帶 ``reachable``（該節點可達的 item 值
    集合，root＝全 item）與 ``conditioned``（路徑上是否已經過 ≥1 個 item 切點）：

    - **item 切點**（``split_feature == item_feature``）：碼經 ``categories[code]`` 映成
      item 值 S；左子 ``reachable ∩ S``、右子 ``reachable - S``，兩側 ``conditioned=True``。
      gain 記入 item-id 帳（見 ``_item_id_block``，與遍歷無關）；對「當時 reachable」
      （進入此節點時攜帶的集合）內每個 item 記 ``isolating_split_count``/
      ``trees_touched``/``first_tree_index``。threshold 出現超出 categories 範圍的碼
      → 忽略該碼並彙總記一筆 note，不炸。
    - **context 切點**（其他特徵）：若 ``conditioned``，對 reachable 內每個 item 記
      ``context_split_count``/``context_gain``（``len(reachable)==1`` 時另記
      ``context_gain_isolated``），同時全域 context 帳的 ``split_count``/``gain_sum``
      累加一次（不論 reachable 內有幾個 item，只算一次——避免依 item 數重複計）。未
      conditioned 的全域切點（常見於 root 段）不進任何帳。
    - 任一節點自身的 ``reachable`` 為空集合時：仍完成該節點自己的帳（item-id 帳本就
      與遍歷無關；context 帳的 per-item 迴圈對空集合是 no-op，不會多記），但不再遞迴
      進它的子節點——因為子樹內任何進一步的切點都不可能命中任何 item，繼續走訪沒有
      意義。
    """
    n_trees = int(trees["tree_index"].nunique())
    n_items = len(categories)
    all_items = list(dict.fromkeys(categories))

    total_gain = _total_gain(trees)
    item_block = _item_id_block(trees, item_feature, total_gain)

    isolating_split_count = {it: 0 for it in all_items}
    context_split_count = {it: 0 for it in all_items}
    context_gain = {it: 0.0 for it in all_items}
    context_gain_isolated = {it: 0.0 for it in all_items}
    trees_touched: dict = {it: set() for it in all_items}
    first_tree_index: dict = {it: None for it in all_items}

    context_global_split_count = 0
    context_global_gain_sum = 0.0
    unknown_codes: set = set()
    numeric_item_splits = 0

    for _, tdf in trees.groupby("tree_index"):
        tdf = tdf.set_index("node_index")
        roots = tdf.index[tdf["parent_index"].isna()]
        if len(roots) == 0:
            continue  # 防禦性：畸形樹（無 root）直接跳過，不炸
        stack = [(roots[0], set(all_items), False)]
        while stack:
            node, reachable, conditioned = stack.pop()
            row = tdf.loc[node]
            feat = row["split_feature"]
            if not isinstance(feat, str):
                continue  # leaf：split_feature 非字串（NaN）
            gain = row["split_gain"]
            gain = 0.0 if pd.isna(gain) else float(gain)
            t_idx = int(row["tree_index"])

            if feat == item_feature:
                if row["decision_type"] != "==":
                    # item 欄出現非類別切點（欄位可能未宣告 categorical）——不解
                    # 類別碼、不動 reachable、不記 per-item 帳，只計異常（防呆，
                    # 審查修復 2026-07-08；spec 定案明文 decision_type == "=="）。
                    numeric_item_splits += 1
                    stack.append((row["left_child"], reachable, conditioned))
                    stack.append((row["right_child"], reachable, conditioned))
                    continue
                for it in reachable:
                    isolating_split_count[it] += 1
                    trees_touched[it].add(t_idx)
                    if first_tree_index[it] is None or t_idx < first_tree_index[it]:
                        first_tree_index[it] = t_idx
                if not reachable:
                    continue
                values, unknown = _decode_threshold(row["threshold"], categories)
                unknown_codes |= unknown
                stack.append((row["left_child"], reachable & values, True))
                stack.append((row["right_child"], reachable - values, True))
            else:
                if conditioned:
                    context_global_split_count += 1
                    context_global_gain_sum += gain
                    for it in reachable:
                        context_split_count[it] += 1
                        context_gain[it] += gain
                        trees_touched[it].add(t_idx)
                        if len(reachable) == 1:
                            context_gain_isolated[it] += gain
                if not reachable:
                    continue
                stack.append((row["left_child"], reachable, conditioned))
                stack.append((row["right_child"], reachable, conditioned))

    notes = []
    if unknown_codes:
        notes.append(
            "item 切點 threshold 出現超出 categories 範圍的碼(已忽略): "
            f"{sorted(unknown_codes)}"
        )
    if numeric_item_splits:
        notes.append(
            f"item 欄出現 {numeric_item_splits} 筆非類別切點（decision_type != '=='）"
            "——該欄可能未宣告 categorical；這些切點不參與 per-item 帳"
            "（item_id 帳按特徵名仍納入）"
        )

    sum_context_gain = sum(context_gain.values())
    per_item = {}
    for it in sorted(all_items):
        cg = context_gain[it]
        share = (cg / sum_context_gain) if sum_context_gain > 0 else None
        per_item[it] = {
            "isolating_split_count": isolating_split_count[it],
            "context_split_count": context_split_count[it],
            "context_gain": cg,
            "context_gain_isolated": context_gain_isolated[it],
            "context_gain_share": share,
            "first_tree_index": first_tree_index[it],
            "trees_touched": sorted(trees_touched[it]),
        }

    context_gain_share = (
        (context_global_gain_sum / total_gain) if total_gain > 0 else None
    )

    logger.info(
        "gain_ledger: n_trees=%d n_items=%d item_id.split_count=%d context.split_count=%d",
        n_trees, n_items, item_block["split_count"], context_global_split_count,
    )

    return {
        "enabled": True,
        "item_feature": item_feature,
        "n_trees": n_trees,
        "n_items": n_items,
        "total_gain": total_gain,
        "item_id": item_block,
        "context": {
            "split_count": context_global_split_count,
            "gain_sum": context_global_gain_sum,
            "gain_share": context_gain_share,
        },
        "per_item": per_item,
        "fallback": False,
        "notes": notes,
    }


def _coarse_ledger(trees: pd.DataFrame, item_feature: str, n_trees: int) -> dict:
    """粗帳本降級：preprocessor 缺 item 值映射時，只留 item-id 帳（by-feature 篩選即得，
    不需遍歷/categories）；``context``/``per_item`` 缺席（``None``），``fallback: True``。
    """
    total_gain = _total_gain(trees)
    item_block = _item_id_block(trees, item_feature, total_gain)
    return {
        "enabled": True,
        "item_feature": item_feature,
        "n_trees": n_trees,
        "n_items": None,
        "total_gain": total_gain,
        "item_id": item_block,
        "context": None,
        "per_item": None,
        "fallback": True,
        "notes": [
            "preprocessor 缺 category_mappings[item_col]，降級為粗帳本"
            "（無法拆解 per-item context/isolating 帳）"
        ],
    }


def compute_gain_ledger(model, preprocessor: dict, parameters: dict) -> dict:
    """Thin wrapper：讀 config/schema，解析 booster 與 item 值映射後轉呼叫 ``_ledger_from_trees``。

    ``diagnostics.gain_ledger.enabled``（預設 True）關閉時直接回 ``{"enabled": False}``，
    不觸碰 model。preprocessor 缺 ``category_mappings[item_col]`` 時降級為粗帳本。
    """
    cfg = (parameters.get("diagnostics", {}) or {}).get("gain_ledger", {}) or {}
    if not cfg.get("enabled", True):
        return {"enabled": False}

    item_col = get_schema(parameters)["item"]
    booster = _resolve_booster(model)
    trees = booster.trees_to_dataframe()
    n_trees = int(booster.num_trees())

    categories = (preprocessor or {}).get("category_mappings", {}).get(item_col)
    if not categories:
        logger.warning(
            "gain_ledger: preprocessor 缺 category_mappings[%s]，降級為粗帳本", item_col
        )
        return _coarse_ledger(trees, item_col, n_trees)

    return _ledger_from_trees(trees, item_col, list(categories))
