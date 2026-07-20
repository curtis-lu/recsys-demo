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


def test_compute_metric_ci_disabled_returns_stub(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_metric_ci
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"ci": {"enabled": False}}},
    }
    assert compute_metric_ci(None, params) == {"enabled": False}


def test_compute_metric_ci_end_to_end_small(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_metric_ci,
        draw_diagnosis_sample_node,
    )
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.1, 0),
            ("20240331", "C1", "A", 0.1, 1),
            ("20240331", "C1", "B", 0.9, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {
            "metric": {"weight_alpha": 0.0, "k": None,
                       "min_positives": 0, "shrinkage_k": 0},
            "diagnosis": {
                "sample": {"max_queries": 100,
                           "min_pos_queries_per_item": 1, "seed": 42},
                "ci": {"enabled": True, "n_boot": 20},
            },
        },
    }
    # The sample is now drawn once by draw_diagnosis_sample_node and passed in.
    sample = draw_diagnosis_sample_node(df, params)
    out = compute_metric_ci(sample, params)
    assert out["enabled"] is True
    assert "A" in out["per_item"] and "macro" in out and "sample" in out
    assert out["sample"]["n_queries_sampled"] == 2


def test_compute_offset_sweep_disabled_writes_stub(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_offset_sweep
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"offset_sweep": {"enabled": False}}},
    }
    assert compute_offset_sweep(None, params) == {"enabled": False}


def test_compute_offset_sweep_requires_eval_predictions_when_enabled(spark):
    import pytest as _pytest
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_offset_sweep
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"offset_sweep": {"enabled": True}}},
    }
    with _pytest.raises(ValueError, match="compute_offset_sweep"):
        compute_offset_sweep(None, params)


def test_compute_metric_ci_raises_when_enabled_but_sample_none(spark):
    import pytest as _pytest
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_metric_ci
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"ci": {"enabled": True}}},
    }
    # gate/consumer drift guard: an enabled consumer must raise a clear
    # ValueError (not AttributeError on None) if the shared sample is None.
    with _pytest.raises(ValueError, match="compute_metric_ci"):
        compute_metric_ci(None, params)


def test_compute_pair_ledger_raises_when_enabled_but_sample_none(spark):
    import pytest as _pytest
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_pair_ledger
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"pair_ledger": {"enabled": True}}},
    }
    with _pytest.raises(ValueError, match="compute_pair_ledger"):
        compute_pair_ledger(None, params)


class TestSampleConsumerFlags:
    def test_defaults_all_true(self):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _sample_consumer_flags,
        )
        assert _sample_consumer_flags({}) == (True, True, True)

    def test_respects_disabled(self):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _sample_consumer_flags,
        )
        params = {"evaluation": {"diagnosis": {
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": True},
            "pair_ledger": {"enabled": False},
        }}}
        assert _sample_consumer_flags(params) == (False, True, False)


class TestDrawDiagnosisSampleNode:
    @staticmethod
    def _params():
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            }},
            "evaluation": {"diagnosis": {"sample": {
                "max_queries": 10, "min_pos_queries_per_item": 2, "seed": 42,
            }}},
        }

    @staticmethod
    def _eval_predictions(spark):
        rows = []
        for cust in ["H1", "H2", "H3", "H4"]:
            rows.append(("20240331", cust, "hot", 0.9, 1))
            rows.append(("20240331", cust, "cold", 0.1, 0))
        rows.append(("20240331", "C1", "hot", 0.9, 0))
        rows.append(("20240331", "C1", "cold", 0.1, 1))
        return spark.createDataFrame(
            rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
        )

    def test_returns_none_and_skips_draw_when_all_disabled(self, spark):
        from unittest.mock import patch
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
            # registry 診斷（contract.DIAGNOSES）預設也是 enabled，所以「全部
            # 停用」必須連它們一起關——只關舊三項的話樣本仍然該抽。
            **{name: {"enabled": False} for name in DIAGNOSES},
        })
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample"
        ) as spy:
            result = nodes_spark.draw_diagnosis_sample_node(
                self._eval_predictions(spark), params
            )
        assert result is None
        assert spy.call_count == 0

    def test_draws_when_one_enabled(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": True},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
        })
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
            return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
        ) as spy:
            nodes_spark.draw_diagnosis_sample_node(None, params)
        # exact args, not just count: guards against a regression forwarding a
        # limited/mutated eval_predictions or a copied params.
        spy.assert_called_once_with(None, params)

    def test_node_output_equals_direct_draw(self, spark):
        # Faithfulness / behaviour-preservation: the node is a pass-through of
        # draw_diagnosis_sample. Same seed -> identical content.
        from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all three consumers default-enabled
        direct_pdf, direct_meta = draw_diagnosis_sample(
            self._eval_predictions(spark), params
        )
        node_pdf, node_meta = nodes_spark.draw_diagnosis_sample_node(
            self._eval_predictions(spark), params
        )
        assert node_meta == direct_meta
        assert (
            node_pdf.sort_values(list(node_pdf.columns))
            .reset_index(drop=True)
            .equals(
                direct_pdf.sort_values(list(direct_pdf.columns))
                .reset_index(drop=True)
            )
        )

    def test_draw_diagnosis_sample_called_once_across_three_consumers(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all three default-enabled
        stub_pdf = pd.DataFrame({
            "snap_date": ["20240331"], "cust_id": ["H1"],
            "prod_name": ["hot"], "score": [0.9], "label": [1],
        })
        stub = (stub_pdf, {"n_queries_sampled": 1})
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
            return_value=stub,
        ) as spy, patch(
            "recsys_tfb.diagnosis.metric.uncertainty.bootstrap_per_item_ci",
            return_value={"n_boot": 1},
        ), patch(
            "recsys_tfb.diagnosis.metric.offset_sweep.sweep", return_value={},
        ), patch(
            "recsys_tfb.diagnosis.metric.pair_ledger.pair_ledger",
            return_value={},
        ):
            sample = nodes_spark.draw_diagnosis_sample_node(None, params)
            nodes_spark.compute_metric_ci(sample, params)
            nodes_spark.compute_offset_sweep(sample, params)
            nodes_spark.compute_pair_ledger(sample, params)
        # exactly one draw, with the node's own inputs — the three consumers
        # must NOT re-draw (they consume the shared sample).
        spy.assert_called_once_with(None, params)

    def test_node_logs_free_pandas_data_volume(self, spark, caplog):
        import logging
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all enabled
        with caplog.at_level(logging.INFO):
            nodes_spark.draw_diagnosis_sample_node(
                self._eval_predictions(spark), params
            )
        vols = [
            r.volume for r in caplog.records
            if getattr(r, "event", None) == "data_volume"
            and getattr(r, "volume", {}).get("name") == "diagnosis.sample_pdf"
        ]
        assert vols, "expected a data_volume event for diagnosis.sample_pdf"
        # Free pandas measurement (rows populated), NOT a Spark count.
        assert vols[0]["kind"] == "pandas"
        assert vols[0]["rows"] is not None


class TestRegistryDiagnosisEnabled:
    """``_registry_diagnosis_enabled`` — registry 診斷的抽樣閘門。

    為什麼跟 ``_sample_consumer_flags`` 分開而不是把 3-tuple 擴成 4-tuple：
    舊三項（ci／offset_sweep／pair_ledger）是即將被取代的既有診斷，新五項走
    ``contract.DIAGNOSES``。合在一個 tuple 裡的話，registry 每加一項診斷都要
    改所有解包點——而「新增診斷不必改接線」正是 registry 存在的目的。
    """

    def test_defaults_true(self):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _registry_diagnosis_enabled,
        )
        assert _registry_diagnosis_enabled({}) is True

    def test_false_only_when_every_registry_diagnosis_disabled(self):
        import importlib

        from recsys_tfb.diagnosis.metric import contract
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _registry_diagnosis_enabled,
        )
        all_off = {"evaluation": {"diagnosis": {
            name: {"enabled": False} for name in DIAGNOSES
        }}}
        assert _registry_diagnosis_enabled(all_off) is False

        # 任一「吃共用抽樣」的診斷開著就是 True。逐項單開，避免哪天 registry
        # 只剩一項時這條測試退化成「跟上一條測同一件事」。
        #
        # ``model_capacity``（Plan 2 Task 4）刻意排除在這個迴圈之外：它的
        # ``INPUTS`` 沒有 ``diagnosis_sample``（只讀 ``gain_ledger``），單獨
        # 開啟它不該觸發抽樣——這正是 ``_registry_diagnosis_enabled`` 的判準
        # 本身（見該函式 docstring），下面另外斷言這個反例。
        sample_consumers = [
            name for name in DIAGNOSES
            if "diagnosis_sample" in contract.inputs_for(
                importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
            )
        ]
        assert sample_consumers, "registry 裡至少要有一項吃共用抽樣的診斷，否則這條測試是空的"
        for name in sample_consumers:
            params = {"evaluation": {"diagnosis": {
                other: {"enabled": other == name} for other in DIAGNOSES
            }}}
            assert _registry_diagnosis_enabled(params) is True, name

        non_sample_consumers = [n for n in DIAGNOSES if n not in sample_consumers]
        for name in non_sample_consumers:
            params = {"evaluation": {"diagnosis": {
                other: {"enabled": other == name} for other in DIAGNOSES
            }}}
            assert _registry_diagnosis_enabled(params) is False, (
                f"{name} 不吃 diagnosis_sample，單獨開啟不該觸發抽樣閘門"
            )


def test_draw_diagnosis_sample_node_draws_for_registry_only_consumer():
    """關掉舊三項、只開 config_shift → 仍然必須抽樣。

    這是本次接線最容易靜默失效的地方：抽樣閘門若只看舊三項，使用者關掉它們
    之後 ``diagnosis_sample`` 是 None，config_shift 節點就撞 fail-loud 的
    ValueError——一個純粹由接線遺漏造成、使用者無從自救的失敗。
    """
    from unittest.mock import patch
    import pandas as pd
    from recsys_tfb.pipelines.evaluation import nodes_spark
    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
        }},
        "evaluation": {"diagnosis": {
            "sample": {"max_queries": 10, "min_pos_queries_per_item": 2,
                       "seed": 42},
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
            "config_shift": {"enabled": True},
        }},
    }
    with patch(
        "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
        return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
    ) as spy:
        result = nodes_spark.draw_diagnosis_sample_node(None, params)
    assert result is not None, (
        "sample gate ignored the registry diagnoses — config_shift is enabled "
        "but no sample was drawn"
    )
    spy.assert_called_once_with(None, params)


def test_sample_not_drawn_when_only_non_sample_diagnoses_enabled(
    caplog, monkeypatch
):
    """只開不吃抽樣的診斷時不得抽樣。

    斷言落在「回傳 None ＋ log 說了 skipping」，不能只斷言沒呼叫 Spark——
    後者被「正確跳過」與「根本沒走到這段」同時滿足（本專案踩過的假綠形態，
    見 known-pitfalls 的教訓 3）。

    目前 ``DIAGNOSES`` 還沒有不吃抽樣的成員（``model_capacity`` 是下一個
    task），這裡用 monkeypatch 造一個假的——``INPUTS`` 裡沒有
    ``diagnosis_sample``，只讀 ``gain_ledger``。``contract.DIAGNOSES`` 走模組
    屬性存取（見 ``_registry_diagnosis_enabled`` 的 docstring），所以這裡對
    ``contract`` 模組本身 patch 屬性即可生效。
    """
    import logging
    import sys
    import types
    from unittest.mock import patch

    from recsys_tfb.diagnosis.metric import contract
    from recsys_tfb.pipelines.evaluation import nodes_spark

    fake = types.ModuleType("recsys_tfb.diagnosis.metric.fake_capacity")
    fake.INPUTS = ("gain_ledger", "parameters")
    fake.compute = lambda gain_ledger, parameters: {}
    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_capacity", fake)
    monkeypatch.setattr(contract, "DIAGNOSES", ("fake_capacity",))

    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
        }},
        "evaluation": {"diagnosis": {
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
            "fake_capacity": {"enabled": True},
        }},
    }

    with caplog.at_level(logging.INFO), patch(
        "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample"
    ) as spy:
        result = nodes_spark.draw_diagnosis_sample_node(None, params)

    assert result is None
    assert spy.call_count == 0
    assert any(
        "skip" in record.getMessage().lower() for record in caplog.records
    ), "expected a log message explaining the sample draw was skipped"


def test_generated_node_writes_stub_when_disabled():
    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    params = {"evaluation": {"diagnosis": {"config_shift": {"enabled": False}}}}
    assert node_fn(None, params) == {"enabled": False}


def test_generated_node_raises_when_enabled_but_sample_none():
    import pytest as _pytest

    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    with _pytest.raises(ValueError, match="draw_diagnosis_sample_node"):
        node_fn(None, {})


def test_generated_node_delegates_to_the_named_module(monkeypatch):
    """轉呼叫的是**以名字查到的**模組，不是寫死的 config_shift。

    工廠若把模組名寫死，registry 只有一項時**每一條測試都會照樣綠**——
    Plan 2 加第二項才會爆，而症狀是「第二項診斷的頁面印出第一項的數字」，
    每頁看起來都很正常。所以這裡注入一個假模組，用它有沒有被呼叫到來證明
    查表這件事真的發生了。
    """
    import sys
    import types

    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    called = {}
    fake = types.ModuleType("recsys_tfb.diagnosis.metric.fake_diag")

    def _compute(diagnosis_sample, parameters):
        called["sample"] = diagnosis_sample
        return {"marker": "from_fake"}

    fake.compute = _compute
    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_diag", fake)

    node_fn = make_diagnosis_node("fake_diag")
    sample = ("pdf-sentinel", {"sampling_description": "x"})
    out = node_fn(sample, {})

    assert out == {"marker": "from_fake"}
    # compute 拿到的是整個 tuple，不是解包後的 sample_pdf——契約在
    # contract._SIGNATURES 釘住，抄形狀時最容易改壞的就是這裡。
    assert called["sample"] is sample


def test_each_diagnosis_node_gets_a_distinct_name():
    """``Node.name`` 預設取 ``func.__name__``（core/node.py:8）。

    工廠不設 ``__name__`` 的話五個 node 全叫 ``_run``：``--only-node`` 指不到
    任何一個，log 也分不出誰是誰。而 pipeline 照樣跑得完——這是靜默的。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    names = [make_diagnosis_node(n).__name__ for n in DIAGNOSES]
    assert names == [f"diagnose_{n}" for n in DIAGNOSES]
    assert len(set(names)) == len(names)
