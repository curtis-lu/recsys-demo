"""Tests for comparison.sources — load_compare_predictions."""

import pytest
from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions


def _params_for_mv(mv: str, snap: str = "2026-01-31") -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1", "p2", "p3"]},
        },
        "evaluation": {
            "snap_date": snap,
            "compare": {"kind": "model_version", "model_version": mv, "label": "L"},
        },
    }


@pytest.fixture
def ranked_predictions_view(spark):
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, "MV_A"),
            ("c1", "2026-01-31", "p2", 0.7, "MV_A"),
            ("c1", "2026-01-31", "p1", 0.8, "MV_B"),
            ("c1", "2025-12-31", "p1", 0.5, "MV_A"),  # different snap_date
        ],
        ["cust_id", "snap_date", "prod_name", "score", "model_version"],
    )
    df.createOrReplaceTempView("ranked_predictions")
    yield
    spark.catalog.dropTempView("ranked_predictions")


def test_model_version_filters_correctly(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    out = load_compare_predictions(p, spark)
    rows = sorted((r["cust_id"], r["prod_name"], r["score"]) for r in out.collect())
    assert rows == [("c1", "p1", 0.9), ("c1", "p2", 0.7)]


def test_model_version_unknown_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_GHOST")
    with pytest.raises(DataConsistencyError, match="MV_GHOST"):
        load_compare_predictions(p, spark)


def test_unknown_kind_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    p["evaluation"]["compare"]["kind"] = "parquet"
    with pytest.raises(RuntimeError, match="parquet"):
        load_compare_predictions(p, spark)


def test_missing_compare_key_raises(spark):
    p = _params_for_mv("MV_A")
    del p["evaluation"]["compare"]
    with pytest.raises(RuntimeError, match="compare"):
        load_compare_predictions(p, spark)
