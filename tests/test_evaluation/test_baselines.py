"""Tests for evaluation.baselines — Spark popularity baseline."""

import pandas as pd


def _parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {},
    }


def _label_table(spark):
    # History before 2025-01-31: A bought 3x, B 1x, C 0x.
    rows = []
    for snap, a, b, c in [("2024-06-30", 2, 1, 0), ("2024-12-31", 1, 0, 0)]:
        for i in range(3):
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "A", "label": 1 if i < a else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "B", "label": 1 if i < b else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "C", "label": 1 if i < c else 0})
    return spark.createDataFrame(pd.DataFrame(rows))


def test_purchase_counts_window_excludes_snap_date_and_after(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 12, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    # 12-month window [2024-01-31, 2025-01-31): both history snaps included.
    assert by_prod["A"] == 3
    assert by_prod["B"] == 1
    assert by_prod["C"] == 0


def test_purchase_counts_lookback_limits_window(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # 3-month window [2024-10-31, 2025-01-31): only the 2024-12-31 snap.
    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 3, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


def test_purchase_counts_fallback_when_no_history(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # snap_date before all history -> empty window -> fallback to full table.
    counts = compute_purchase_counts(
        _label_table(spark), ["2024-01-01"], 12, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    assert by_prod["A"] == 3  # full table


def test_build_baseline_frame_replaces_score_and_drops_model_cols(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "cust_id": ["c1", "c1", "c2", "c2"],
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
        "score": [0.9, 0.1, 0.2, 0.8],
        "rank": [1, 2, 2, 1],
        "model_version": ["v1"] * 4,
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [5, 2],
    }))

    frame = build_baseline_frame(eval_pred, counts, _parameters())
    cols = set(frame.columns)
    assert "rank" not in cols and "model_version" not in cols
    assert "score" in cols and "label" in cols

    by_key = {(r["cust_id"], r["prod_name"]): r["score"] for r in frame.collect()}
    # Every customer gets the same per-product popularity score.
    assert by_key[("c1", "A")] == 5 and by_key[("c2", "A")] == 5
    assert by_key[("c1", "B")] == 2 and by_key[("c2", "B")] == 2


def test_build_baseline_frame_fills_missing_product_with_zero(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
        "score": [0.9, 0.1],
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"], "prod_name": ["A"], "score": [5],
    }))
    frame = build_baseline_frame(eval_pred, counts, _parameters())
    by_prod = {r["prod_name"]: r["score"] for r in frame.collect()}
    assert by_prod["A"] == 5
    assert by_prod["B"] == 0
