"""Tests for the slim metrics path: compute_overall_per_item."""

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
        "evaluation": {"k_values": [1, 2, 3]},
    }


def _eval_predictions(spark):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 6,
        "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
        "prod_name": ["A", "B", "C", "A", "B", "C"],
        "label": [1, 0, 1, 0, 1, 0],
        "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
    })
    return spark.createDataFrame(pdf)


def test_returns_only_overall_and_per_item(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    result = compute_overall_per_item(_eval_predictions(spark), _parameters())

    assert set(result.keys()) == {"overall", "per_item"}


def test_matches_compute_all_metrics_subset(spark):
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_overall_per_item,
    )

    params = _parameters()
    df = _eval_predictions(spark)

    slim = compute_overall_per_item(df, params)
    full = compute_all_metrics(_eval_predictions(spark), params)

    assert slim["overall"] == full["overall"]
    assert slim["per_item"] == full["per_item"]


def test_empty_when_no_positive_queries(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [0, 0],
        "score": [0.9, 0.1],
    })
    result = compute_overall_per_item(spark.createDataFrame(pdf), _parameters())

    assert result == {"overall": {}, "per_item": {}}
