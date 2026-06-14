"""Tests for evaluation pipeline Spark nodes."""

from unittest.mock import MagicMock

import pytest


class TestPrepareEvalDataModelVersionFilter:
    """prepare_eval_data filters predictions to parameters['model_version']."""

    @pytest.fixture
    def parameters(self):
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
            "model_version": "20260511_153000",
        }

    def test_filter_applied_with_model_version(self, spark, parameters):
        from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data

        predictions = MagicMock(name="predictions_sdf")
        predictions.columns = ["model_version"]
        filtered = MagicMock(name="filtered_sdf")
        predictions.filter.return_value = filtered

        labels = MagicMock(name="label_sdf")
        labels.sparkSession = MagicMock()
        filtered.join.return_value = MagicMock(name="eval_predictions")
        filtered.select.return_value.distinct.return_value = MagicMock()

        try:
            prepare_eval_data(predictions, labels, parameters)
        except Exception:
            pass  # we only care that .filter was called

        assert predictions.filter.call_count == 1
        filter_arg = predictions.filter.call_args[0][0]
        # Spark Column repr includes both column name and literal value
        filter_repr = str(filter_arg)
        assert "model_version" in filter_repr
        assert "20260511_153000" in filter_repr

    def test_raises_when_model_version_missing(self, parameters):
        from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data

        params_no_mv = dict(parameters)
        del params_no_mv["model_version"]

        predictions = MagicMock(name="predictions_sdf")
        labels = MagicMock(name="label_sdf")

        with pytest.raises(RuntimeError, match="model_version"):
            prepare_eval_data(predictions, labels, params_no_mv)


def test_prepare_eval_data_injects_rank_when_missing(spark):
    """When the predictions input lacks a `rank` column (post-training mode
    sourced from training_eval_predictions after T3 schema change),
    prepare_eval_data must add it via rank_within_query so downstream
    nodes (generate_report) still find `rank`.

    The predictions input carries a `label` column but no `model_version`:
    HiveTableDataset already filters that static partition and drops the
    constant column on load. prepare_eval_data must still produce a
    non-ambiguous result.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31"] * 4,
        "prod_name": ["A", "B", "A", "B"],
        "score": [0.9, 0.1, 0.2, 0.8],
        "label": [1, 0, 0, 1],
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31"] * 4,
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)

    parameters = {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
                "identity_columns": ["cust_id", "snap_date", "prod_name"],
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters)
    cols = set(result.columns)
    assert "rank" in cols

    # Verify rank is 1-based and ordered by score desc within (cust, snap)
    result_pdf = result.toPandas().sort_values(["cust_id", "rank"])
    c1_rows = result_pdf[result_pdf["cust_id"] == "c1"]
    # c1 has score 0.9 on A and 0.1 on B -> A is rank 1
    assert list(c1_rows.sort_values("rank")["prod_name"]) == ["A", "B"]
    assert list(c1_rows.sort_values("rank")["rank"]) == [1, 2]


def test_prepare_eval_data_preserves_existing_rank_column(spark):
    """When the predictions input already has a `rank` column (non-post-training
    mode sourced from ranked_predictions), prepare_eval_data must NOT re-rank
    or overwrite — the upstream rank is authoritative.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "rank": [99, 100],  # upstream-provided rank, not recomputable from score
        "model_version": ["v1"] * 2,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "label": [1, 0],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)

    parameters = {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
                "identity_columns": ["cust_id", "snap_date", "prod_name"],
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters).toPandas()
    # rank values are preserved as-is, NOT recomputed from score
    a_row = result[result["prod_name"] == "A"].iloc[0]
    b_row = result[result["prod_name"] == "B"].iloc[0]
    assert a_row["rank"] == 99
    assert b_row["rank"] == 100


def test_prepare_eval_data_dedupes_label_when_predictions_carry_it(spark):
    """In --post-training mode the predictions source (training_eval_predictions)
    already carries a `label` column. The merge join keys on identity_cols only,
    so without dedup `label` survives on both sides -> AnalysisException:
    reference 'label' is ambiguous. prepare_eval_data must drop the label_table
    side's `label` and keep the predictions' own label.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    # Post-training predictions: carry `label` (training_eval_predictions schema).
    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "label": [1, 0],  # authoritative — scored against at training time
        "model_version": ["v1"] * 2,
    })
    # label_table: has its own `label` (deliberately different values, to prove
    # which side wins).
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "label": [0, 1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)

    parameters = {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
                "identity_columns": ["cust_id", "snap_date", "prod_name"],
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters)

    # Exactly one `label` column survives -> no ambiguous reference.
    assert result.columns.count("label") == 1
    result_pdf = result.select("prod_name", "label").toPandas()
    # Predictions' own label is kept (label_table's differing values discarded).
    by_prod = result_pdf.set_index("prod_name")["label"]
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


def test_prepare_eval_data_joins_segment_sources(spark):
    """segment_sources Hive tables are left-joined onto eval_predictions (after
    the predictions x labels join), enriching it with the segment column
    without changing its row count."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions = spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "rank": [1, 2],
        "model_version": ["v1"] * 2,
    }))
    labels = spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "label": [1, 0],
    }))
    # sample_pool-like source: finer-grained (one row per product).
    spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "cust_segment_typ": ["mass", "mass"],
        "prod_name": ["A", "B"],
    })).createOrReplaceTempView("seg_pool")

    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {
            "snap_date": "2025-01-31",
            "segment_sources": {"cust_segment_typ": {
                "table": "seg_pool",
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "cust_segment_typ"}},
        },
    }
    result = prepare_eval_data(predictions, labels, parameters).toPandas()
    assert len(result) == 2  # no fan-out from the finer-grained source
    assert set(result["cust_segment_typ"]) == {"mass"}


def test_prepare_eval_data_filters_to_configured_snap_date(spark):
    """prepare_eval_data keeps only rows at evaluation.snap_date, dropping the
    other snapshots that share the same model_version in the table."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"],
        "prod_name": ["A", "B", "A", "B"],
        "score": [0.9, 0.1, 0.2, 0.8],
        "rank": [1, 2, 2, 1],
        "model_version": ["v1"] * 4,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"],
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters).toPandas()
    assert set(result["snap_date"]) == {"2025-01-31"}
    assert len(result) == 2


def test_prepare_eval_data_raises_when_snap_date_absent(spark):
    """When evaluation.snap_date matches no predictions row, prepare_eval_data
    raises ValueError and the message names the snap_dates actually present."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "rank": [1, 2],
        "model_version": ["v1"] * 2,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2099-12-31"},
    }

    with pytest.raises(ValueError, match="2025-01-31"):
        prepare_eval_data(predictions, labels, parameters)


def test_prepare_eval_data_raises_when_snap_date_unset(spark):
    """When evaluation.snap_date is not configured, prepare_eval_data raises
    ValueError rather than silently evaluating the whole table."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1"], "snap_date": ["2025-01-31"], "prod_name": ["A"],
        "score": [0.9], "rank": [1], "model_version": ["v1"],
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1"], "snap_date": ["2025-01-31"], "prod_name": ["A"],
        "label": [1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {},
    }

    with pytest.raises(ValueError, match="snap_date not configured"):
        prepare_eval_data(predictions, labels, parameters)


def test_prepare_eval_data_left_joins_labels_and_fills_missing_with_zero(spark):
    """prepare_eval_data must LEFT JOIN predictions with labels so that
    predictions for (cust, prod) pairs with no label_table row are kept,
    with `label` filled as 0 ("not bought"). The previous INNER JOIN
    silently dropped those rows, collapsing the per-customer candidate
    set to whichever per-group cust_pool subset label_table covered.

    Setup: monitoring-mode predictions (no `label` column) with 2 custs ×
    3 prods = 6 rows. label_table only covers ccard (c1 with ccard_ins=1)
    -> 1 row. INNER would drop 5; LEFT must keep all 6 with label=0 for
    the unmatched ones.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    # Monitoring mode shape: predictions carry rank, no label.
    predictions_pdf = pd.DataFrame({
        "cust_id":       ["c1", "c1", "c1", "c2", "c2", "c2"],
        "snap_date":     ["2025-01-31"] * 6,
        "prod_name":     ["exchange_usd", "ccard_ins", "fund_stock",
                          "exchange_usd", "ccard_ins", "fund_stock"],
        "score":         [0.9, 0.7, 0.3, 0.8, 0.4, 0.2],
        "rank":          [1, 2, 3, 1, 2, 3],
        "model_version": ["v1"] * 6,
    })
    # label_table per-group cust_pool: c1 in ccard pool, c2 in nothing.
    labels_pdf = pd.DataFrame({
        "cust_id":   ["c1"],
        "snap_date": ["2025-01-31"],
        "prod_name": ["ccard_ins"],
        "label":     [1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)

    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters)
    result_pdf = result.select(
        "cust_id", "prod_name", "label"
    ).toPandas().sort_values(["cust_id", "prod_name"]).reset_index(drop=True)

    # All 6 prediction rows survive (LEFT JOIN, not INNER).
    assert len(result_pdf) == 6, (
        f"expected 6 rows (full LEFT JOIN), got {len(result_pdf)} — "
        f"INNER JOIN regression?\n{result_pdf}"
    )

    # Matched row keeps label=1.
    by_key = {
        (r.cust_id, r.prod_name): r.label
        for r in result_pdf.itertuples()
    }
    assert by_key[("c1", "ccard_ins")] == 1

    # All unmatched rows get label=0 (not None / not NaN).
    for key in [
        ("c1", "exchange_usd"), ("c1", "fund_stock"),
        ("c2", "exchange_usd"), ("c2", "ccard_ins"), ("c2", "fund_stock"),
    ]:
        assert by_key[key] == 0, (
            f"key={key} should have label=0 (LEFT JOIN miss), got {by_key[key]}"
        )


class TestComputeBaselineMetrics:
    """compute_baseline_metrics: slim baseline metrics from eval_predictions."""

    @staticmethod
    def _parameters(baseline_section=True):
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
            "evaluation": {
                "k_values": [1, 2, 3],
                "baseline": {"lookback_months": 12},
                "report": {"sections": {"baseline": baseline_section}},
            },
        }

    @staticmethod
    def _eval_predictions(spark):
        import pandas as pd
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 6,
            "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
            "prod_name": ["A", "B", "C", "A", "B", "C"],
            "label": [1, 0, 1, 0, 1, 0],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
            "rank": [1, 2, 3, 3, 1, 2],
        }))

    @staticmethod
    def _label_table(spark):
        import pandas as pd
        rows = []
        for i in range(3):
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "A", "label": 1})
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "B", "label": 1 if i < 1 else 0})
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "C", "label": 0})
        return spark.createDataFrame(pd.DataFrame(rows))

    def test_returns_overall_and_per_item(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            self._parameters(),
        )
        assert set(result.keys()) == {"overall", "per_item", "purchase_counts"}
        assert "A" in result["per_item"]
        # purchase_counts comes from _label_table fixture (snap=2024-06-30
        # falls inside the [2024-01-31, 2025-01-31) lookback window for the
        # 2025-01-31 eval snap). A=3 positives (h0/h1/h2 all label=1),
        # B=1 (only h0 label=1), C=0.
        assert result["purchase_counts"] == {"A": 3, "B": 1, "C": 0}

    def test_returns_none_when_section_disabled(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            self._parameters(baseline_section=False),
        )
        assert result is None
