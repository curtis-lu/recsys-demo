"""cross_purchase_matrix：P(買 k｜買 j)，label_table 自 join。"""

import pytest

from recsys_tfb.diagnosis.metric.cross_purchase import cross_purchase_matrix


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


def _label_df(spark, rows):
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "label"],
    )


def test_conditional_probabilities_and_diagonal(spark):
    # A 買家 {C1, C2}、B 買家 {C1, C3}；C2 的 B 列 label=0 必須被濾掉。
    rows = [
        ("20240331", "C1", "A", 1), ("20240331", "C1", "B", 1),
        ("20240331", "C2", "A", 1), ("20240331", "C2", "B", 0),
        ("20240331", "C3", "B", 1),
    ]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.loc["A", "A"] == pytest.approx(1.0)
    assert prob.loc["A", "B"] == pytest.approx(0.5)   # A 買家 2 人中 1 人也買 B
    assert prob.loc["B", "A"] == pytest.approx(0.5)
    assert int(n_buyers["A"]) == 2 and int(n_buyers["B"]) == 2


def test_cross_snap_date_not_co_purchase(spark):
    # 同客戶不同 snap_date 的購買不算共現（join 鍵含 time）。
    rows = [
        ("20240331", "C1", "A", 1),
        ("20240630", "C1", "B", 1),
    ]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.loc["A", "B"] == pytest.approx(0.0)
    assert prob.loc["B", "A"] == pytest.approx(0.0)


def test_empty_positive_labels_returns_empty(spark):
    rows = [("20240331", "C1", "A", 0)]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.empty and n_buyers.empty
