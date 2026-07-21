"""gain_ledger 結構層帳本單元測試（手算錨 fixture + 真 booster 契約）。"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.model import gain_ledger

ITEM_COL = "prod_code"
CATEGORIES = ["A", "B", "C", "D"]  # 碼 = list 索引


# node_depth 是 trees_to_dataframe() 一定有的欄（root=1）；手算 fixture 補上它，
# 預設 1，只有測 item 切點深度的 fixture 才給真實深度。
def _leaf(tree_index, node_index, parent_index, node_depth=1):
    return {
        "tree_index": tree_index, "node_index": node_index, "node_depth": node_depth,
        "left_child": np.nan, "right_child": np.nan, "parent_index": parent_index,
        "split_feature": np.nan, "split_gain": np.nan, "threshold": np.nan,
        "decision_type": np.nan,
    }


def _split(tree_index, node_index, parent_index, left, right, feature, gain,
           threshold, decision_type, node_depth=1):
    return {
        "tree_index": tree_index, "node_index": node_index, "node_depth": node_depth,
        "left_child": left, "right_child": right, "parent_index": parent_index,
        "split_feature": feature, "split_gain": gain, "threshold": threshold,
        "decision_type": decision_type,
    }


def _tree0_rows(s0_threshold="0||2"):
    """單棵樹手算錨：
    S0(item,S={A,C},gain10) -- 左 S1(context f_age,gain6)/右 S2(item,S={B},gain4)
                                                     右 S2 -- 左 leaf / 右 S3(context f_inc,gain2, reachable={D})
    """
    return [
        _split(0, "0-S0", np.nan, "0-S1", "0-S2", ITEM_COL, 10.0, s0_threshold, "=="),
        _split(0, "0-S1", "0-S0", "0-L0", "0-L1", "f_age", 6.0, 0.5, "<="),
        _leaf(0, "0-L0", "0-S1"),
        _leaf(0, "0-L1", "0-S1"),
        _split(0, "0-S2", "0-S0", "0-L2", "0-S3", ITEM_COL, 4.0, "1", "=="),
        _leaf(0, "0-L2", "0-S2"),
        _split(0, "0-S3", "0-S2", "0-L3", "0-L4", "f_inc", 2.0, 1000.0, "<="),
        _leaf(0, "0-L3", "0-S3"),
        _leaf(0, "0-L4", "0-S3"),
    ]


def _tree1_unconditioned_context_root():
    """第二棵樹：root 是未 conditioned 的全域 context 切點（gain=9），不應進任何帳。"""
    return [
        _split(1, "1-S0", np.nan, "1-L0", "1-L1", "f_region", 9.0, 2.5, "<="),
        _leaf(1, "1-L0", "1-S0"),
        _leaf(1, "1-L1", "1-S0"),
    ]


def _df(rows):
    return pd.DataFrame(rows)


# ---- _ledger_from_trees 手算錨 ----

def test_ledger_hand_calc_item_id_and_total_gain():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["item_id"]["split_count"] == 2
    assert result["item_id"]["gain_sum"] == pytest.approx(14.0)
    assert result["total_gain"] == pytest.approx(22.0)
    assert result["item_id"]["gain_share"] == pytest.approx(14.0 / 22.0)
    assert result["item_id"]["tree_index_summary"]["min"] == 0


def test_ledger_hand_calc_context_block():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["context"]["split_count"] == 2  # S1, S3
    assert result["context"]["gain_sum"] == pytest.approx(8.0)  # 6 + 2


def test_ledger_hand_calc_per_item_context_gain():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    per_item = result["per_item"]
    assert per_item["A"]["context_gain"] == pytest.approx(6.0)
    assert per_item["C"]["context_gain"] == pytest.approx(6.0)
    assert per_item["B"]["context_gain"] == pytest.approx(0.0)
    assert per_item["D"]["context_gain"] == pytest.approx(2.0)
    assert per_item["D"]["context_gain_isolated"] == pytest.approx(2.0)
    assert per_item["A"]["context_gain_isolated"] == pytest.approx(0.0)
    assert per_item["A"]["context_split_count"] == 1
    assert per_item["B"]["context_split_count"] == 0


def test_context_split_isolated_count():
    """私有 context 切點數（該 item 唯一可達時的 context 切點）——與
    context_gain_isolated 對稱的計數版。tree0 的 S3（reachable={D}，gain2）是
    唯一的私有 context 切點。"""
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    pi = result["per_item"]
    assert pi["D"]["context_split_isolated"] == 1
    assert pi["A"]["context_split_isolated"] == 0
    assert pi["B"]["context_split_isolated"] == 0
    # 與 gain 版一致：有私有 gain 才有私有 split
    assert (pi["D"]["context_gain_isolated"] > 0) == (pi["D"]["context_split_isolated"] > 0)


def test_ledger_hand_calc_isolating_split_count():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    per_item = result["per_item"]
    assert [per_item[k]["isolating_split_count"] for k in ["A", "B", "C", "D"]] == [1, 2, 1, 2]


def test_ledger_hand_calc_context_gain_share():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    per_item = result["per_item"]
    assert per_item["A"]["context_gain_share"] == pytest.approx(6.0 / 14.0)
    assert per_item["B"]["context_gain_share"] == 0.0


def test_ledger_hand_calc_trees_touched_and_first_tree_index():
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    per_item = result["per_item"]
    for item in ["A", "B", "C", "D"]:
        assert per_item[item]["first_tree_index"] == 0
        assert per_item[item]["trees_touched"] == [0]


# ---- 追加測試 1：全域（未 conditioned）切點不記帳 ----

def test_unconditioned_root_split_not_booked():
    trees = _df(_tree0_rows() + _tree1_unconditioned_context_root())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["per_item"]["A"]["context_gain"] == pytest.approx(6.0)
    assert result["context"]["gain_sum"] == pytest.approx(8.0)  # 不含 tree1 的 9.0
    assert result["total_gain"] == pytest.approx(31.0)  # 22 + 9


# ---- 新輸出：total_split_count（Route A，補三分表的 split 總數）----

def test_total_split_count_counts_all_non_leaf_nodes():
    """tree0 有 4 個切點（S0/S1/S2/S3），葉不算。"""
    trees = _df(_tree0_rows())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["total_split_count"] == 4
    # 未分配 split ＝ total − item_id − context（分帳殘差）
    unacc = (result["total_split_count"]
             - result["item_id"]["split_count"]
             - result["context"]["split_count"])
    assert unacc == 0  # tree0 全部切點都歸到 item 或 context


def test_coarse_ledger_still_has_total_split_count():
    """粗帳本降級沒有 reachable 走訪，但總 split 數只需數非葉節點，仍要有。"""
    trees = _df(_tree0_rows())
    result = gain_ledger._coarse_ledger(trees, ITEM_COL, n_trees=1)
    assert result["total_split_count"] == 4
    assert result["pre_item"] is None            # 需走訪，粗帳本無從算
    assert result["first_item_split_depth"] is None


# ---- 新輸出：pre_item 按特徵拆解（Q3-#1）----

def test_pre_item_breakdown_reconciles_to_unaccounted():
    """未分配（pre-item）＝item 切點之前的非 item 切點。其 gain 加總必須等於
    total − item_id − context（分帳殘差），且按特徵記得出是誰。tree1 的 root
    context f_region(gain9) 是唯一的 pre-item 切點。"""
    trees = _df(_tree0_rows() + _tree1_unconditioned_context_root())
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    pre = result["pre_item"]
    unacc_gain = (result["total_gain"]
                  - result["item_id"]["gain_sum"]
                  - result["context"]["gain_sum"])
    assert pre["gain_sum"] == pytest.approx(unacc_gain)   # 9.0
    assert pre["gain_sum"] == pytest.approx(9.0)
    assert pre["split_count"] == 1
    assert pre["by_feature"]["f_region"]["gain"] == pytest.approx(9.0)
    assert pre["by_feature"]["f_region"]["split_count"] == 1


def test_pre_item_by_feature_sorted_by_gain_desc():
    """兩個 pre-item 特徵時，by_feature 要 gain 遞減排序（讀者先看吃最多的）。"""
    rows = [
        # 兩棵各一個 pre-item root context 切點，gain 3 與 9
        _split(0, "0-S0", np.nan, "0-L0", "0-L1", "f_small", 3.0, 0.5, "<="),
        _leaf(0, "0-L0", "0-S0"), _leaf(0, "0-L1", "0-S0"),
        _split(1, "1-S0", np.nan, "1-L0", "1-L1", "f_big", 9.0, 0.5, "<="),
        _leaf(1, "1-L0", "1-S0"), _leaf(1, "1-L1", "1-S0"),
    ]
    result = gain_ledger._ledger_from_trees(_df(rows), ITEM_COL, CATEGORIES)
    feats = list(result["pre_item"]["by_feature"].keys())
    assert feats == ["f_big", "f_small"], f"未按 gain 遞減：{feats}"


# ---- 新輸出：first_item_split_depth（Q3-#2）----

def test_first_item_split_depth_summary():
    """每棵樹最淺 item 切點的深度（node_depth，root=1）。
    treeA：item 切點在 root（depth1）。treeB：root 是 context（depth1），item
    切點在 depth2。→ 兩棵的最淺 item 深度 [1, 2]，且只算「有 item 切點」的樹。"""
    rows = [
        # treeA: 根就是 item 切點（depth1）
        _split(0, "0-S0", np.nan, "0-L0", "0-L1", ITEM_COL, 5.0, "0", "==", node_depth=1),
        _leaf(0, "0-L0", "0-S0", node_depth=2), _leaf(0, "0-L1", "0-S0", node_depth=2),
        # treeB: root 是 context(depth1)，其下才有 item 切點(depth2)
        _split(1, "1-S0", np.nan, "1-S1", "1-L2", "f_age", 4.0, 0.5, "<=", node_depth=1),
        _split(1, "1-S1", "1-S0", "1-L0", "1-L1", ITEM_COL, 3.0, "1", "==", node_depth=2),
        _leaf(1, "1-L0", "1-S1", node_depth=3), _leaf(1, "1-L1", "1-S1", node_depth=3),
        _leaf(1, "1-L2", "1-S0", node_depth=2),
    ]
    result = gain_ledger._ledger_from_trees(_df(rows), ITEM_COL, CATEGORIES)
    d = result["first_item_split_depth"]
    assert d["min"] == 1 and d["max"] == 2
    assert d["p50"] == pytest.approx(1.5)
    assert d["n_trees_with_item_split"] == 2


# ---- 追加測試 2：未知碼忽略但不炸 ----

def test_unknown_code_ignored_with_note():
    trees = _df(_tree0_rows(s0_threshold="0||2||9"))
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["per_item"]["A"]["context_gain"] == pytest.approx(6.0)
    assert any("9" in n for n in result["notes"])


# ---- 追加測試 3：空側跳過不炸 ----

def test_empty_side_skipped_without_crash():
    trees = _df(_tree0_rows(s0_threshold="0||1||2||3"))  # S=全 item → 右側 reachable 為空
    result = gain_ledger._ledger_from_trees(trees, ITEM_COL, CATEGORIES)
    assert result["item_id"]["split_count"] == 2


# ---- wrapper: compute_gain_ledger ----

def test_compute_gain_ledger_disabled():
    parameters = {"diagnostics": {"gain_ledger": {"enabled": False}}}
    result = gain_ledger.compute_gain_ledger(None, {}, parameters)
    assert result == {"enabled": False}


def _tiny_real_booster():
    import lightgbm as lgb

    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    rng = np.random.RandomState(0)
    n = 40
    prod_code = rng.randint(0, 4, size=n).astype(float)
    f_age = rng.randn(n)
    f_inc = rng.randn(n)
    X = np.column_stack([prod_code, f_age, f_inc])
    y = (prod_code >= 2).astype(float)
    ds = lgb.Dataset(
        X, label=y, feature_name=["prod_code", "f_age", "f_inc"],
        categorical_feature=["prod_code"], free_raw_data=False,
    )
    params = {
        "objective": "binary", "verbosity": -1, "num_leaves": 7, "seed": 0,
        "min_data_in_leaf": 1, "num_iterations": 3, "early_stopping_rounds": 0,
    }
    adapter = LightGBMAdapter()
    adapter.train(X, y, None, None, params, train_dataset=ds)
    return adapter


def test_compute_gain_ledger_real_booster_contract():
    adapter = _tiny_real_booster()
    preprocessor = {"category_mappings": {"prod_code": ["A", "B", "C", "D"]}}
    parameters = {"schema": {"columns": {"item": "prod_code"}}}
    result = gain_ledger.compute_gain_ledger(adapter, preprocessor, parameters)
    assert result["enabled"] is True
    assert result["fallback"] is False
    assert result["n_trees"] == adapter.booster.num_trees()
    assert set(result["per_item"].keys()) == {"A", "B", "C", "D"}
    for key in ("item_id", "context", "total_gain", "notes"):
        assert key in result


def test_compute_gain_ledger_missing_category_mappings_falls_back():
    adapter = _tiny_real_booster()
    preprocessor = {"category_mappings": {}}
    parameters = {"schema": {"columns": {"item": "prod_code"}}}
    result = gain_ledger.compute_gain_ledger(adapter, preprocessor, parameters)
    assert result["fallback"] is True
    assert result["per_item"] is None
    assert result["context"] is None
    assert any("category_mappings" in n for n in result["notes"])


# ---- 審查修復（2026-07-08）：item 欄非類別切點防呆 ----

def test_numeric_item_split_not_booked_but_noted():
    """item 欄出現 decision_type != "==" 的數值切點：不解類別碼、不動 reachable、
    不記 per-item 帳；notes 記異常。（審查發現 1：spec 明文 decision_type == "=="）"""
    rows = _tree0_rows() + [
        # 第二棵樹：root 是 item 欄的「數值」切點（threshold 1.5，不可解類別碼），
        # 其下掛一個 context 切點——因 root 未 conditioned 且不是合法 item 切點，
        # 該 context 切點不得進任何帳。
        _split(2, "2-S0", np.nan, "2-S1", "2-L0", ITEM_COL, 5.0, 1.5, "<="),
        _split(2, "2-S1", "2-S0", "2-L1", "2-L2", "f_age", 3.0, 0.7, "<="),
        _leaf(2, "2-L0", "2-S0"),
        _leaf(2, "2-L1", "2-S1"),
        _leaf(2, "2-L2", "2-S1"),
    ]
    out = gain_ledger._ledger_from_trees(_df(rows), ITEM_COL, CATEGORIES)
    p = out["per_item"]
    # per-item 帳與純手算錨完全相同（數值切點與其子樹零貢獻）
    assert p["A"]["context_gain"] == pytest.approx(6.0)
    assert p["D"]["context_gain"] == pytest.approx(2.0)
    assert [p[i]["isolating_split_count"] for i in "ABCD"] == [1, 2, 1, 2]
    # 全域 conditioned context 帳也不含 3.0
    assert out["context"]["gain_sum"] == pytest.approx(8.0)
    # item_id 帳按特徵名仍納入（與遍歷解耦的既有語意）
    assert out["item_id"]["split_count"] == 3
    assert out["item_id"]["gain_sum"] == pytest.approx(19.0)
    assert any("非類別切點" in n for n in out["notes"])
