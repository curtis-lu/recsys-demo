"""Tests for category-level extension of metrics_spark."""

import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(enabled=True, unmapped="singleton"):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]},
        },
        "evaluation": {
            "item_categories": {
                "enabled": enabled, "unmapped": unmapped,
                "mapping": {"fund": ["fund_stock", "fund_bond", "fund_mix"]},
            },
            "segment_columns": ["cust_segment_typ"],
        },
    }


def test_disabled_returns_none():
    p = _params(enabled=False)
    assert ms._build_category_mapping(p) is None


def test_mapping_with_singleton_unmapped():
    m = ms._build_category_mapping(_params())
    assert m["fund_stock"] == "fund"
    assert m["fund_bond"] == "fund"
    assert m["exchange_fx"] == "exchange_fx"   # unmapped -> singleton
    assert m["lonely"] == "lonely"


def test_unknown_product_in_mapping_fails_loud():
    p = _params()
    p["evaluation"]["item_categories"]["mapping"]["x"] = ["not_a_product"]
    with pytest.raises(ValueError, match="not_a_product"):
        ms._build_category_mapping(p)


def _raw(spark):
    # c1 wants fund (via fund_bond) ; c1 fund_stock is top score
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_stock", 0.9, 0, "mass"),
            ("20240331", "c1", "fund_bond",  0.4, 1, "mass"),
            ("20240331", "c1", "exchange_fx", 0.7, 0, "mass"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label",
                "cust_segment_typ"],
    )


def test_collapse_to_categories_grain(spark):
    p = _params()
    p["schema"]["categorical_values"]["prod_name"] = [
        "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]
    collapsed = ms.collapse_to_categories(
        _raw(spark), p, segment_columns=["cust_segment_typ"]
    )
    rows = {r["prod_name"]: r for r in collapsed.collect()}
    # category column reuses item_col name so downstream stays uniform
    assert set(rows) == {"fund", "exchange_fx"}
    # fund score = max(child score) = max(0.9, 0.4) = 0.9
    assert rows["fund"]["score"] == pytest.approx(0.9)
    # fund label = max(child label) = max(0, 1) = 1
    assert rows["fund"]["label"] == 1
    # segment carried
    assert rows["fund"]["cust_segment_typ"] == "mass"


# ---------------------------------------------------------------------------
# 宣告 event 時的分類面（#378 回歸測試）
# ---------------------------------------------------------------------------


def _event_category_params():
    p = _params()
    p["schema"]["columns"]["event"] = "imp_id"
    p["evaluation"]["segment_columns"] = []
    return p


def _event_raw(spark):
    """同一組同一 item 多列，而且橫跨會被併成同一個大類的兩個 item。"""
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_bond", "i1", 0.9, 1),
            ("20240331", "c1", "fund_bond", "i2", 0.7, 0),
            ("20240331", "c1", "fund_stock", "i3", 0.5, 0),
            ("20240331", "c1", "exchange_fx", "i4", 0.2, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )


def test_category_pass_works_when_event_is_declared(spark):
    """collapse_to_categories 以 (query group, 大類) 聚合並取 max(score)，所以
    它產出的每一對只有一列、event 欄被聚掉了。分類面若沿用逐列那一面的決勝欄，
    Spark 會說 Column 'imp_id' does not exist——而且是在 training 跑到一半才炸
    （#378 實跑抓到的）。

    這一條釘的是「兩面的粒度不同、決勝欄也不同」，不是某個數值。
    """
    out = ms.compute_all_metrics(_event_raw(spark), _event_category_params())
    assert "category" in out
    # 大類面把 fund_bond 與 fund_stock 併成 fund，加上 exchange_fx，共 2 個。
    assert out["category"]["dataset_overview"]["totals"]["n_items"] == 2
    # 逐列那一面仍然是 4 列 3 個 item。
    assert out["dataset_overview"]["totals"]["n_rows"] == 4
    assert out["dataset_overview"]["totals"]["n_items"] == 3


def test_both_faces_count_ties_on_their_own_grain(spark):
    """同分列佔比在兩面都是同一個問題——「這一列的名次是決勝規則決定的，不是
    分數決定的」——但兩面的列不是同一批。逐列那一面數的是同組同分的候選列，
    大類面數的是聚合後同組同分的大類。這裡的資料兩面都沒有同分，所以兩邊都是
    0；釘的是「兩邊各自算在自己的表上」而不是某個特定數值。"""
    out = ms.compute_all_metrics(_event_raw(spark), _event_category_params())
    assert out["dataset_overview"]["totals"]["n_tied_rows"] == 0
    assert out["category"]["dataset_overview"]["totals"]["n_tied_rows"] == 0


def test_tied_rows_are_counted_on_the_row_face(spark):
    """判別性的那一條：上一條的兩個 0 也會被一個「永遠回 0」的實作通過。"""
    df = spark.createDataFrame(
        [
            ("20240331", "c1", "fund_bond", "i1", 0.9, 1),
            ("20240331", "c1", "fund_bond", "i2", 0.9, 0),
            ("20240331", "c1", "exchange_fx", "i3", 0.2, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )
    out = ms.compute_all_metrics(df, _event_category_params())
    assert out["dataset_overview"]["totals"]["n_tied_rows"] == 2
