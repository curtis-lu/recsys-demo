"""top_slot_share／suppression_counts：水準軸傷害的直接觀測。"""

import pytest

from recsys_tfb.diagnosis.metric.occupancy_spark import (
    suppression_counts,
    top_slot_share,
)


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
    }


def _df(spark, rows):
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def test_top_slot_share_counts_topk_queries(spark):
    # 2 個 query：query1 A 排 1、B 排 2；query2 B 排 1、A 排 2。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1), ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C1", "B", 0.8, 1, 1), ("20240331", "C1", "A", 0.4, 0, 2),
    ]
    out = top_slot_share(_df(spark, rows), _params(), k=1)
    assert out["n_queries"] == 2 and out["k"] == 1
    assert out["by_item"]["A"]["top_share"] == pytest.approx(0.5)
    assert out["by_item"]["A"]["n_top"] == 1
    assert out["by_item"]["A"]["y_rate"] == pytest.approx(0.0)
    assert out["by_item"]["B"]["y_rate"] == pytest.approx(1.0)


def test_top_slot_share_k2_counts_both_slots(spark):
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1), ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C1", "B", 0.8, 1, 1), ("20240331", "C1", "A", 0.4, 0, 2),
    ]
    out = top_slot_share(_df(spark, rows), _params(), k=2)
    assert out["by_item"]["A"]["top_share"] == pytest.approx(1.0)


def test_top_slot_share_rank_compared_numerically_not_lexicographically(spark):
    # rank 為 StringType 時字典序 "10" <= "2" 為真；若不 cast 成數值，k=2 會把
    # 數值上排在第 10 名（C）的列誤算進 top-2。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, "1"),
        ("20240331", "C0", "B", 0.5, 1, "2"),
        ("20240331", "C0", "C", 0.3, 0, "10"),
    ]
    out = top_slot_share(_df(spark, rows), _params(), k=2)
    assert out["by_item"]["A"]["n_top"] == 1
    assert out["by_item"]["B"]["n_top"] == 1
    assert out["by_item"]["C"]["n_top"] == 0


def test_suppression_counts_negatives_above_first_positive(spark):
    # query1：A(負) 排 1、B(正) 排 2、C(負) 排 3 → 首位正例 rank=2 →
    #   只有 A 壓制（rank 1 < 2）；C 在其下、不算。
    # query2：全負 → min_pos_rank null → 不貢獻。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1),
        ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C0", "C", 0.3, 0, 3),
        ("20240331", "C1", "A", 0.9, 0, 1),
        ("20240331", "C1", "B", 0.5, 0, 2),
    ]
    out = suppression_counts(_df(spark, rows), _params())
    assert out["by_item"]["A"]["suppression_count"] == 1
    assert "B" not in out["by_item"] and "C" not in out["by_item"]
    assert out["n_pos_queries"] == 1


def test_suppression_positive_above_positive_not_counted(spark):
    # 正例壓正例不算（只記「以負例身分」的壓制）。
    rows = [
        ("20240331", "C0", "A", 0.9, 1, 1),
        ("20240331", "C0", "B", 0.5, 1, 2),
    ]
    out = suppression_counts(_df(spark, rows), _params())
    assert out["by_item"] == {}


def test_suppression_counts_rank_compared_numerically_not_lexicographically(spark):
    # rank 為 StringType 時字典序 "10" < "2"；若不 cast 成數值會把 rank 10 的
    # 負例（C，數值上排在正例 B 之下）誤算成壓制 B 之上。
    # query：A(負,rank"1")、B(正,rank"2")、C(負,rank"10")→ 首位正例 rank=2 →
    #   只有 A 壓制（數值 1<2）；C 數值 10 不 <2、不算。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, "1"),
        ("20240331", "C0", "B", 0.5, 1, "2"),
        ("20240331", "C0", "C", 0.3, 0, "10"),
    ]
    out = suppression_counts(_df(spark, rows), _params())
    assert out["by_item"]["A"]["suppression_count"] == 1
    assert "C" not in out["by_item"]
