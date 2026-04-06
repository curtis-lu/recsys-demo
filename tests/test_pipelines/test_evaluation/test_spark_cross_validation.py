"""Cross-validation: Spark metrics vs pandas metrics on the same data.

Requires a SparkSession (provided by conftest.py).
"""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.evaluation.nodes_pandas import (
    compute_metrics as compute_metrics_pandas,
    prepare_eval_data as prepare_eval_data_pandas,
)
from recsys_tfb.pipelines.evaluation.nodes_spark import (
    compute_metrics as compute_metrics_spark,
    prepare_eval_data as prepare_eval_data_spark,
)

PRODUCTS = ["prod_a", "prod_b", "prod_c", "prod_d"]
CUSTOMERS = [f"C{i:04d}" for i in range(1, 31)]
SNAP_DATE = "20240131"


def _make_parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "evaluation": {
            "snap_date": SNAP_DATE,
            "k_values": [3, "all"],
            "segment_columns": [],
            "segment_sources": {},
            "baseline": {"type": "global_popularity"},
            "report": {
                "include_baseline_comparison": False,
                "include_calibration": False,
                "include_distributions": False,
            },
        },
    }


@pytest.fixture
def synthetic_data():
    rng = np.random.default_rng(123)
    pred_rows = []
    for cust_id in CUSTOMERS:
        scores = rng.random(len(PRODUCTS))
        ranks = np.argsort(-scores) + 1
        for i, prod in enumerate(PRODUCTS):
            pred_rows.append({
                "snap_date": SNAP_DATE,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": float(scores[i]),
                "rank": int(ranks[i]),
            })
    ranked_predictions = pd.DataFrame(pred_rows)

    label_rows = []
    for cust_id in CUSTOMERS:
        for prod in PRODUCTS:
            label_rows.append({
                "snap_date": SNAP_DATE,
                "cust_id": cust_id,
                "prod_name": prod,
                "label": int(rng.random() < 0.3),
            })
    label_table = pd.DataFrame(label_rows)

    return ranked_predictions, label_table


class TestSparkPandasCrossValidation:
    def test_metrics_match(self, spark, synthetic_data):
        """Spark and pandas backends should produce matching metrics."""
        ranked_predictions_pd, label_table_pd = synthetic_data
        parameters = _make_parameters()

        # Pandas path
        eval_data_pd = prepare_eval_data_pandas(
            ranked_predictions_pd, label_table_pd, parameters
        )
        metrics_pd = compute_metrics_pandas(eval_data_pd, parameters)

        # Spark path
        ranked_predictions_spark = spark.createDataFrame(ranked_predictions_pd)
        label_table_spark = spark.createDataFrame(label_table_pd)
        eval_data_spark = prepare_eval_data_spark(
            ranked_predictions_spark, label_table_spark, parameters
        )
        metrics_spark = compute_metrics_spark(eval_data_spark, parameters)

        # Compare overall metrics
        for metric_name in metrics_pd["overall"]:
            pd_val = metrics_pd["overall"][metric_name]
            spark_val = metrics_spark["overall"].get(metric_name)
            assert spark_val is not None, f"Missing {metric_name} in spark metrics"
            assert abs(pd_val - spark_val) < 1e-6, (
                f"{metric_name}: pandas={pd_val:.6f}, spark={spark_val:.6f}"
            )

    def test_per_product_metrics_match(self, spark, synthetic_data):
        """Per-product metrics should match between backends."""
        ranked_predictions_pd, label_table_pd = synthetic_data
        parameters = _make_parameters()

        eval_data_pd = prepare_eval_data_pandas(
            ranked_predictions_pd, label_table_pd, parameters
        )
        metrics_pd = compute_metrics_pandas(eval_data_pd, parameters)

        ranked_predictions_spark = spark.createDataFrame(ranked_predictions_pd)
        label_table_spark = spark.createDataFrame(label_table_pd)
        eval_data_spark = prepare_eval_data_spark(
            ranked_predictions_spark, label_table_spark, parameters
        )
        metrics_spark = compute_metrics_spark(eval_data_spark, parameters)

        # Same products
        assert set(metrics_pd["per_product"].keys()) == set(
            metrics_spark["per_product"].keys()
        )

        for prod in metrics_pd["per_product"]:
            for metric_name in metrics_pd["per_product"][prod]:
                pd_val = metrics_pd["per_product"][prod][metric_name]
                spark_val = metrics_spark["per_product"][prod].get(metric_name)
                assert spark_val is not None
                assert abs(pd_val - spark_val) < 1e-6, (
                    f"{prod}/{metric_name}: pandas={pd_val:.6f}, spark={spark_val:.6f}"
                )

    def test_query_counts_match(self, spark, synthetic_data):
        """n_queries and n_excluded_queries should match."""
        ranked_predictions_pd, label_table_pd = synthetic_data
        parameters = _make_parameters()

        eval_data_pd = prepare_eval_data_pandas(
            ranked_predictions_pd, label_table_pd, parameters
        )
        metrics_pd = compute_metrics_pandas(eval_data_pd, parameters)

        ranked_predictions_spark = spark.createDataFrame(ranked_predictions_pd)
        label_table_spark = spark.createDataFrame(label_table_pd)
        eval_data_spark = prepare_eval_data_spark(
            ranked_predictions_spark, label_table_spark, parameters
        )
        metrics_spark = compute_metrics_spark(eval_data_spark, parameters)

        assert metrics_pd["n_queries"] == metrics_spark["n_queries"]
        assert metrics_pd["n_excluded_queries"] == metrics_spark["n_excluded_queries"]
