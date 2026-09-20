"""Tests for compute_test_mAP_spark — Spark-native mAP over training_eval_predictions.

The `spark` fixture is the session-scoped one from tests/conftest.py — do not
re-declare a local one (it would shadow conftest's, and an early teardown via
`.stop()` would kill the shared session mid-suite).
"""

import pytest


def _make_parameters() -> dict:
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
        "evaluation": {"k_values": ["all"]},
    }


def _make_df(spark, rows):
    """rows: list of dicts with cust_id, snap_date, prod_name, score,
    score_uncalibrated, label.
    """
    import pandas as pd
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf)


def test_compute_mAP_spark_returns_one_flat_dict(spark):
    """One set of metrics, always.

    There used to be a second, "before calibration" set, emitted when `score`
    and `score_uncalibrated` disagreed. #411 removed calibration — the only
    thing that could make them disagree — so the node no longer reads
    `score_uncalibrated` and no longer emits the second set.
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
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 4}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # Both customers ranked their positives at top -> overall mAP == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    assert "per_item_map_attr" in result
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 0


def test_a_disagreeing_score_uncalibrated_column_is_ignored(spark):
    """The discriminating half: a table whose deprecated column disagrees with
    `score` gets the same flat result, scored on `score` alone.

    Such a table can only come from a pre-#411 run. Reading it would revive the
    second metric set under a model that was never calibrated.
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP_spark

    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.1, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.9, "label": 0},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 2}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # `score` puts c1's positive on top, so the metric is the calibrated-side
    # number of the old two-set result — i.e. it scored `score`, not the other.
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
