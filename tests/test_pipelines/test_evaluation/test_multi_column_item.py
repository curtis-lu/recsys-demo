"""A multi-column ``item`` through the evaluation entries (#394, ADR-0027).

Predictions are the framework's own table and carry the combined ``item``;
``label_table``, ``sample_pool`` and an external comparison table are the
user's and carry ``campaign``/``fmt``. Each entry combines them on read, so
every answer below is the single-column answer with the item renamed.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from tests.test_pipelines.test_evaluation import test_nodes as single_column
from tests.test_pipelines.test_evaluation.test_nodes import (
    _no_segments,
    _prepare_eval_data,
)
from tests.test_pipelines.test_evaluation.test_popularity_rate import (
    _call,
    _history,
    _node,
    _plan,
)

pytestmark = pytest.mark.spark

_SOURCES = ["campaign", "fmt"]


def _combined(value: str) -> str:
    """``A`` in the single-column fixtures is ``A-x`` here."""
    return f"{value}-x"


def _split_item(df, item="prod_name"):
    """The user's shape: the item in two columns, as the SQL no longer
    combines it."""
    return (df.withColumn("campaign", F.col(item))
              .withColumn("fmt", F.lit("x"))
              .drop(item))


def _multi(params: dict) -> dict:
    params["schema"]["columns"]["item"] = list(_SOURCES)
    values = params["schema"].pop("categorical_values", {}).get("prod_name")
    if values:
        params["schema"]["categorical_values"] = {"item": [_combined(v) for v in values]}
    return params


def _to_combined_item(df):
    """Framework-written rows: the combined ``item``, already."""
    return (df.withColumn("item", F.concat(F.col("prod_name"), F.lit("-x")))
              .drop("prod_name"))


class TestPrepareEvalData:
    def test_labels_join_the_prediction_of_their_combination(self, spark):
        predictions = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c1", "c2", "c2"],
            "snap_date": ["2025-01-31"] * 4,
            "item": ["A-x", "B-x", "A-x", "B-x"],
            "score": [0.9, 0.1, 0.2, 0.8],
        }))
        labels = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c2"],
            "snap_date": ["2025-01-31"] * 2,
            "campaign": ["A", "B"],
            "fmt": ["x", "x"],
            "label": [1, 1],
        }))
        params = {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": list(_SOURCES),
            }},
            "model_version": "v1",
            "evaluation": {"snap_date": "2025-01-31"},
        }
        result = _prepare_eval_data(predictions, labels, params)
        got = {(r["cust_id"], r["item"]): r["label"] for r in result.collect()}
        assert got == {
            ("c1", "A-x"): 1, ("c1", "B-x"): 0, ("c2", "A-x"): 0, ("c2", "B-x"): 1,
        }
        assert "campaign" not in result.columns


def _baseline_params(score="count"):
    params = _multi(single_column.TestComputeBaselineMetrics._parameters())
    params["evaluation"]["baseline"]["score"] = score
    return params


def _eval_predictions(spark, params):
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        stamp_partition_fingerprint,
    )

    frame = _to_combined_item(
        single_column.TestComputeBaselineMetrics._eval_predictions(spark).drop(
            "eval_partition_fingerprint"))
    return stamp_partition_fingerprint(frame, params, [])


class TestPopularityBaseline:
    """The single-column fixtures' answers (``test_popularity_rate``), item
    renamed: the combined value is the item everywhere downstream."""

    def test_count_mode_counts_per_combined_item(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

        params = _baseline_params("count")
        _pool, labels = _history(spark)
        result = compute_baseline_metrics(
            _eval_predictions(spark, params), _split_item(labels),
            _no_segments(params), params)
        assert result["purchase_counts"] == {"A-x": 3, "B-x": 1}
        count_map = ((1 + 2 / 3) / 2 + 1 / 2) / 2
        assert result["overall"]["map@3"] == pytest.approx(count_map)

    def test_rate_mode_counts_candidates_per_combined_item(self, spark):
        params = _baseline_params("rate")
        pool, labels = _history(spark)
        plan = _plan(["2024-06-30"])
        counts = _call(
            _node("build_popularity_period_counts", post_training=True,
                  baseline_rate=True),
            sample_pool=_split_item(pool), label_table=_split_item(labels),
            parameters=params, popularity_period_counts_month_plan=plan)
        result = _call(
            _node("compute_baseline_metrics", post_training=True,
                  baseline_rate=True),
            enriched_eval_predictions=_eval_predictions(spark, params),
            label_table=_split_item(labels),
            evaluation_segment_columns=_no_segments(params),
            parameters=params, popularity_period_counts=counts,
            popularity_period_counts_month_plan=plan)
        block = result["popularity_rate"]
        assert block["candidates"] == {"A-x": 10, "B-x": 2, "C-x": 5}
        assert block["positives"] == {"A-x": 3, "B-x": 1, "C-x": 0}
        rate_map = ((1 / 2 + 2 / 3) / 2 + 1) / 2
        assert result["overall"]["map@3"] == pytest.approx(rate_map)


def _ext_params() -> dict:
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": list(_SOURCES),
            },
            "categorical_values": {"item": ["A-x", "B-x"]},
        },
        "evaluation": {
            "snap_date": "2026-01-31",
            "compare": {
                "kind": "external_hive",
                "table": "ext_proj.preds",
                "label": "ExtX",
                "columns": {
                    "cust_id": "customer_id",
                    "snap_date": "as_of_date",
                    "campaign": "ext_campaign",
                    "fmt": "ext_format",
                    "score": "pred_score",
                },
                "prod_mapping": {"ea-banner": "A-x", "eb-banner": "B-x"},
            },
        },
    }


class TestExternalComparisonTable:
    def test_combined_before_prod_mapping(self, spark, monkeypatch):
        """``prod_mapping``'s keys are combined external values."""
        from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
            load_compare_predictions,
        )

        ext = spark.createDataFrame(
            [("c1", "2026-01-31", "ea", "banner", 0.9),
             ("c1", "2026-01-31", "eb", "banner", 0.4)],
            ["customer_id", "as_of_date", "ext_campaign", "ext_format", "pred_score"],
        )
        monkeypatch.setattr(spark, "table", lambda t: ext)
        out = load_compare_predictions(_ext_params(), spark)
        assert sorted((r["item"], r["score"]) for r in out.collect()) == [
            ("A-x", 0.9), ("B-x", 0.4),
        ]

    def test_a11_asks_for_the_source_columns_not_item(self):
        from recsys_tfb.core.consistency import compare_source_well_formed_errors

        params = _ext_params()
        params["evaluation"]["compare_sources"] = {
            "x": params["evaluation"].pop("compare"),
        }
        assert compare_source_well_formed_errors(params) == []
        del params["evaluation"]["compare_sources"]["x"]["columns"]["fmt"]
        errors = compare_source_well_formed_errors(params)
        assert any("['fmt']" in e for e in errors), errors
