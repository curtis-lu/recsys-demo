"""Tests for comparison.restrict — restrict_to_common."""

import pytest
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common


def _params() -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1", "p2", "p3", "p4"]},
        },
    }


@pytest.fixture
def a_df(spark):
    """A has cust=c1,c2,c3, prod=p1,p2,p3,p4 — and a label column already."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c1", "2026-01-31", "p2", 0.7, 2, 0),
            ("c1", "2026-01-31", "p4", 0.5, 3, 0),  # p4 not in B
            ("c2", "2026-01-31", "p1", 0.8, 1, 0),
            ("c2", "2026-01-31", "p3", 0.6, 2, 1),
            ("c3", "2026-01-31", "p1", 0.7, 1, 0),  # c3 not in B
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )


@pytest.fixture
def b_df(spark):
    """B has cust=c1,c2, prod=p1,p2,p3 — no label column."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.6),
            ("c1", "2026-01-31", "p2", 0.8),
            ("c1", "2026-01-31", "p3", 0.5),
            ("c2", "2026-01-31", "p1", 0.9),
            ("c2", "2026-01-31", "p3", 0.7),
        ],
        ["cust_id", "snap_date", "prod_name", "score"],
    )


@pytest.fixture
def label_table(spark):
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 1),
            ("c1", "2026-01-31", "p2", 0),
            ("c1", "2026-01-31", "p3", 0),
            ("c2", "2026-01-31", "p1", 0),
            ("c2", "2026-01-31", "p3", 1),
        ],
        ["cust_id", "snap_date", "prod_name", "label"],
    )


def test_restricts_to_common_cust_and_prod(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    a_rows = sorted((r["cust_id"], r["prod_name"]) for r in a_c.collect())
    b_rows = sorted((r["cust_id"], r["prod_name"]) for r in b_c.collect())
    # common cust = {c1, c2}; common prod = {p1, p2, p3}
    expected = sorted([("c1", "p1"), ("c1", "p2"), ("c1", "p3"),
                       ("c2", "p1"), ("c2", "p3")])
    # A had no (c1, p3) — so A_common has it missing too; check A's reduced set
    a_expected = sorted([("c1", "p1"), ("c1", "p2"), ("c2", "p1"), ("c2", "p3")])
    assert a_rows == a_expected
    assert b_rows == expected


def test_rank_recomputed_within_common(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    # B for c1 in common prods: scores p1=0.6, p2=0.8, p3=0.5 → ranks 2, 1, 3
    b_c1 = {r["prod_name"]: r["rank"] for r in b_c.filter("cust_id='c1'").collect()}
    assert b_c1 == {"p2": 1, "p1": 2, "p3": 3}


def test_b_gets_label_via_left_join(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    assert "label" in b_c.columns
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    assert b_labels[("c1", "p1")] == 1
    assert b_labels[("c2", "p3")] == 1
    assert b_labels[("c1", "p2")] == 0


def test_b_missing_label_fillna_zero(a_df, b_df):
    spark = a_df.sparkSession
    sparse_labels = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 1)],
        ["cust_id", "snap_date", "prod_name", "label"],
    )
    _, b_c = restrict_to_common(a_df, b_df, sparse_labels, _params())
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    # p2/p3 not in sparse_labels — must fill 0
    assert b_labels[("c1", "p2")] == 0
    assert b_labels[("c1", "p3")] == 0


def test_a_preserves_existing_label(a_df, b_df, label_table):
    a_c, _ = restrict_to_common(a_df, b_df, label_table, _params())
    a_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in a_c.collect()}
    # A's c1,p1 label was 1 in source fixture — preserved (not re-joined)
    assert a_labels[("c1", "p1")] == 1
