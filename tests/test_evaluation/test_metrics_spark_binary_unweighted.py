"""The two raw-score average precisions without a weight column (#577): the
``--prediction-quality`` mode's monitoring rows carry none, every row
weighing 1. The weighted path is held to the val functions in
``tests/test_pipelines/test_training/test_compute_test_metrics.py``.

Worked example (item, score, label)::

    A 0.9 1 | A 0.8 0 | B 0.7 0 | A 0.65 1 | B 0.6 1 | A 0.5 0

* pooled, every row in one ranking: positives at ranks 1, 4, 5 ->
  precision 1/1, 2/4, 3/5 -> AP = (1 + 0.5 + 0.6) / 3 = 0.7.
* per item: A's positives at A-ranks 1 and 3 -> (1 + 2/3) / 2 = 5/6;
  B's at B-rank 2 -> 1/2; macro (5/6 + 1/2) / 2 = 2/3.
"""

import pytest

from recsys_tfb.evaluation.metrics_spark import (
    compute_macro_per_item_average_precision,
    compute_pooled_average_precision,
)

PARAMS = {"schema": {"columns": {
    "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
    "label": "label", "score": "score", "rank": "rank"}}}

ROWS = [("c1", "A", 0.9, 1), ("c1", "B", 0.7, 0), ("c2", "A", 0.8, 0),
        ("c2", "B", 0.6, 1), ("c3", "A", 0.65, 1), ("c4", "A", 0.5, 0)]


def _frame(spark):
    return spark.createDataFrame(ROWS, ["cust_id", "prod_name", "score",
                                        "label"])


def test_pooled_ap_without_a_weight_column_weighs_every_row_one(spark):
    assert compute_pooled_average_precision(
        _frame(spark), PARAMS, weighted=False) == pytest.approx(0.7)


def test_macro_per_item_ap_without_a_weight_column(spark):
    assert compute_macro_per_item_average_precision(
        _frame(spark), PARAMS, weighted=False) == pytest.approx(2 / 3)


def test_the_weighted_path_still_refuses_rows_without_the_column(spark):
    """Training's call: no weight column means the test table was not
    written under a positive ratio, and counting each row once would score
    another population."""
    with pytest.raises(ValueError, match="no such column"):
        compute_pooled_average_precision(_frame(spark), PARAMS)
