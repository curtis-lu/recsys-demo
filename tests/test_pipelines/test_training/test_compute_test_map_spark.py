"""Tests for compute_test_mAP_spark — Spark-native mAP over training_eval_predictions."""

import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_compute_test_mAP_spark")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def _make_parameters() -> dict:
    return {
        "schema": {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
        "evaluation": {"k_values": ["all"]},
        "training": {"calibration": {"method": "isotonic"}},
    }


def _make_df(spark, rows):
    """rows: list of dicts with cust_id, snap_date, prod_name, score,
    score_uncalibrated, label.
    """
    import pandas as pd
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf)


def test_compute_mAP_spark_no_calibration_returns_flat_dict(spark):
    """When score == score_uncalibrated for every row, the result has NO
    'uncalibrated' sub-dict and NO 'calibration_method' key.
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP_spark

    rows = [
        # cust c1 — positives on prod_A (correct top rank)
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.9, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.1, "label": 0},
        # cust c2 — positives on prod_B (correct top rank)
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.2, "score_uncalibrated": 0.2, "label": 0},
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.8, "score_uncalibrated": 0.8, "label": 1},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "prods": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 4}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # Both customers ranked their positives at top -> overall mAP == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    assert "per_product_ap" in result
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 0


def test_compute_mAP_spark_with_calibration_emits_uncalibrated_subdict(spark):
    """When score != score_uncalibrated for any row, the result has an
    'uncalibrated' sub-dict and a 'calibration_method' string (from
    parameters['training']['calibration']['method']).
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP_spark

    # Calibrated scores agree with labels; uncalibrated DISagree (worse mAP)
    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.1, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.9, "label": 0},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "prods": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 2}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" in result
    assert result["calibration_method"] == "isotonic"
    # Calibrated ranks c1's positive at top -> calibrated overall_map == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    # Uncalibrated ranks c1's negative at top -> uncalibrated mAP < calibrated
    assert result["uncalibrated"]["overall_map"] < result["overall_map"]
