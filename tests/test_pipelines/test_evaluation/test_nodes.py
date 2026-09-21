"""Tests for evaluation pipeline Spark nodes."""

from unittest.mock import MagicMock

import pytest


def _no_segments(parameters):
    """What prepare_eval_data lands when no segment column is configured, for
    nodes whose tests are not about segments.

    Fingerprinted with the ``parameters`` the node is called with, as a run's
    own JSON is: the readers of ``enriched_eval_predictions`` refuse a
    partition prepared under other settings.
    """
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

    return {"joined": [], "sources": {}, "missing": {},
            "config_fingerprint": fingerprint(parameters)}


def _kept_month_of(df, parameters, *_segments, **_mode):
    """Stand-in for ``restrict_to_current_eval_partitions`` in tests that pass
    no frame: marks what the node forwarded as the restricted one."""
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        EvalPartitions,
    )

    return EvalPartitions(("kept month of", df), [])


def _prepare_eval_data(predictions, labels, parameters):
    """``prepare_eval_data`` as monitoring wires it, frame only.

    For tests of the prediction/label join. They configure no segment column,
    so the population is read for nothing but its (empty) column list.
    """
    from recsys_tfb.pipelines.evaluation.nodes import (
        make_prepare_eval_data_node,
    )

    population = MagicMock(name="population_sdf", columns=[])
    frame, _segments = make_prepare_eval_data_node("inference_population")(
        predictions, labels, population, parameters)
    return frame


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

        predictions = MagicMock(name="predictions_sdf")
        predictions.columns = ["model_version"]
        filtered = MagicMock(name="filtered_sdf")
        predictions.filter.return_value = filtered

        labels = MagicMock(name="label_sdf")
        labels.sparkSession = MagicMock()
        filtered.join.return_value = MagicMock(name="eval_predictions")
        filtered.select.return_value.distinct.return_value = MagicMock()

        try:
            _prepare_eval_data(predictions, labels, parameters)
        except Exception:
            pass  # we only care that .filter was called

        assert predictions.filter.call_count == 1
        filter_arg = predictions.filter.call_args[0][0]
        # Spark Column repr includes both column name and literal value
        filter_repr = str(filter_arg)
        assert "model_version" in filter_repr
        assert "20260511_153000" in filter_repr

    def test_raises_when_model_version_missing(self, parameters):

        params_no_mv = dict(parameters)
        del params_no_mv["model_version"]

        predictions = MagicMock(name="predictions_sdf")
        labels = MagicMock(name="label_sdf")

        with pytest.raises(RuntimeError, match="model_version"):
            _prepare_eval_data(predictions, labels, params_no_mv)


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
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = _prepare_eval_data(predictions, labels, parameters)
    cols = set(result.columns)
    assert "rank" in cols

    # Verify rank is 1-based and ordered by score desc within (cust, snap)
    result_pdf = result.toPandas().sort_values(["cust_id", "rank"])
    c1_rows = result_pdf[result_pdf["cust_id"] == "c1"]
    # c1 has score 0.9 on A and 0.1 on B -> A is rank 1
    assert list(c1_rows.sort_values("rank")["prod_name"]) == ["A", "B"]
    assert list(c1_rows.sort_values("rank")["rank"]) == [1, 2]


_ENRICH_PARAMS = {
    "schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
    "model_version": "v1",
    "evaluation": {"snap_date": "2025-01-31"},
}


def _catalog_declared_type(entry: str, column: str) -> str:
    """The type ``conf/base/catalog.yaml`` declares, read rather than restated.

    The casts in ``prepare_eval_data`` copy these declarations. Reading them
    here is what turns a catalog edit that the cast does not follow into a red
    test instead of a type conflict on the next real run.
    """
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[3]
    catalog = yaml.safe_load((root / "conf" / "base" / "catalog.yaml").read_text())
    declared = {c["name"]: c["type"] for c in catalog[entry]["columns"]}
    return declared[column].lower()


def _post_training_predictions(spark):
    """``training_eval_predictions``' shape: carries label, no rank (declared types)."""
    return spark.createDataFrame(
        [("c1", "2025-01-31", "A", 0.9, 0.8, 1),
         ("c1", "2025-01-31", "B", 0.1, 0.2, 0)],
        "cust_id STRING, snap_date STRING, prod_name STRING, score DOUBLE, "
        "score_uncalibrated DOUBLE, label INT",
    )


def _monitoring_predictions(spark):
    """``ranked_predictions``' shape: carries rank (BIGINT), no label."""
    return spark.createDataFrame(
        [("c1", "2025-01-31", "A", 0.9, 0.8, 1),
         ("c1", "2025-01-31", "B", 0.1, 0.2, 2)],
        "cust_id STRING, snap_date STRING, prod_name STRING, score DOUBLE, "
        "score_uncalibrated DOUBLE, rank BIGINT",
    )


def _labels(spark, label_type="INT"):
    """``label_table`` is user-defined; the framework does not fix its label's width."""
    return spark.createDataFrame(
        [("c1", "2025-01-31", "A", 1)],
        f"cust_id STRING, snap_date STRING, prod_name STRING, label {label_type}",
    )


def test_prepare_eval_data_injected_rank_is_bigint(spark):
    """Bug 15: the rank post-training fills in has the type ``ranked_predictions``
    declares. ``row_number()`` yields INT, and both modes write the same
    ``enriched_eval_predictions``, so a mismatch fails whichever write comes
    second (next test).
    """

    result = _prepare_eval_data(
        _post_training_predictions(spark), _labels(spark), _ENRICH_PARAMS,
    )
    assert result.schema["rank"].dataType.simpleString() == \
        _catalog_declared_type("ranked_predictions", "rank")


@pytest.mark.parametrize("label_type", ["INT", "BIGINT"])
@pytest.mark.parametrize(
    "first", ["post_training", "monitoring"],
    ids=["post-training-then-monitoring", "monitoring-then-post-training"],
)
def test_both_modes_write_the_same_enriched_table_in_either_order(
    spark, first, label_type,
):
    """The same model_version written by both modes, in either order (bug 15).

    The table is ``columns: "auto"``: its schema comes from the first write,
    and ``_evolve_schema`` raises on a same-name type mismatch ("Schema
    evolution never casts"). Two columns come from different sources per mode,
    and both must end up the same type:

    * ``rank``: filled in by one mode (``row_number`` is INT), read from
      upstream by the other (BIGINT).
    * ``label``: ``training_eval_predictions`` (declared INT) in post-training,
      the user-defined ``label_table`` in monitoring, BIGINT in the example's
      synthetic data. Hence both ``label_type`` values: with INT only, this
      conflict stays invisible (it first showed up on a real monitoring run).

    Runs in its own test DB, for the reason ``test_persist_and_catalog_load_roundtrip``
    does (``known-pitfalls.md`` §14: a test once wiped the shared warehouse's
    real table).
    """
    import shutil
    from pathlib import Path

    from recsys_tfb.io.hive_table_dataset import HiveTableDataset

    db, table = "test_rank_type_across_modes", "enriched_eval_predictions"

    def _clean():
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
        raw = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
        table_dir = Path(raw[len("file:"):] if raw.startswith("file:") else raw)
        table_dir = table_dir / f"{db}.db" / table
        if table_dir.exists():
            shutil.rmtree(table_dir)

    enriched = {
        "post_training": lambda: _prepare_eval_data(
            _post_training_predictions(spark), _labels(spark, label_type),
            _ENRICH_PARAMS),
        "monitoring": lambda: _prepare_eval_data(
            _monitoring_predictions(spark), _labels(spark, label_type),
            _ENRICH_PARAMS),
    }
    second = "monitoring" if first == "post_training" else "post_training"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    _clean()
    try:
        ds = HiveTableDataset(
            database=db, table=table, columns="auto",
            partition_filter={"model_version": "MV_X"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        ds.save(enriched[first]())
        ds.save(enriched[second]())

        out = ds.load()
        assert out.schema["rank"].dataType.simpleString() == \
            _catalog_declared_type("ranked_predictions", "rank")
        assert out.schema["label"].dataType.simpleString() == \
            _catalog_declared_type("training_eval_predictions", "label")
        assert out.count() == 2
    finally:
        _clean()


@pytest.mark.parametrize(
    "predictions", [_post_training_predictions, _monitoring_predictions],
    ids=["post-training", "monitoring"],
)
def test_prepare_eval_data_raises_on_duplicated_label_keys(spark, predictions):
    """Bug 10: a label_table key held by more than one row raises, naming the
    number of duplicated keys.

    The LEFT JOIN would copy that candidate once per label row: the query's
    candidate set grows, its ranks shift, and no count or metric complains.
    Not ``dropDuplicates``: that picks one of the answers arbitrarily.

    The fixture's shape is deliberate. B is held by three rows (the count is of
    keys, not of surplus rows), and another month has a duplicate too (only the
    evaluated month is checked). Miscounting either way gives a number other
    than 2.
    """

    labels = spark.createDataFrame(
        [("c1", "2025-01-31", "A", 1),
         ("c1", "2025-01-31", "A", 1),
         ("c1", "2025-01-31", "B", 0),
         ("c1", "2025-01-31", "B", 0),
         ("c1", "2025-01-31", "B", 1),
         ("c1", "2024-12-31", "A", 1),
         ("c1", "2024-12-31", "A", 1)],
        "cust_id STRING, snap_date STRING, prod_name STRING, label INT",
    )
    with pytest.raises(ValueError, match="2 duplicated label_table key"):
        _prepare_eval_data(predictions(spark), labels, _ENRICH_PARAMS)


def test_prepare_eval_data_preserves_existing_rank_column(spark):
    """When the predictions input already has a `rank` column (non-post-training
    mode sourced from ranked_predictions), prepare_eval_data must NOT re-rank
    or overwrite — the upstream rank is authoritative.
    """
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
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = _prepare_eval_data(predictions, labels, parameters).toPandas()
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
            },
        },
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = _prepare_eval_data(predictions, labels, parameters)

    # Exactly one `label` column survives -> no ambiguous reference.
    assert result.columns.count("label") == 1
    result_pdf = result.select("prod_name", "label").toPandas()
    # Predictions' own label is kept (label_table's differing values discarded).
    by_prod = result_pdf.set_index("prod_name")["label"]
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


class TestSegmentsFollowThePopulation:
    """ADR-0020 bug 6: a segment column comes from the run mode's population
    table (``sample_pool`` for --post-training, ``inference_population`` for
    monitoring), keyed by (time, entity), unless ``segment_sources`` overrides
    it. What was actually joined lands as ``evaluation_segment_columns``, the
    node's second output, so consumers never infer it from frame columns."""

    @staticmethod
    def _parameters(**evaluation):
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank"}},
            "model_version": "v1",
            "evaluation": {"snap_date": "2025-01-31",
                           "segment_columns": ["cust_segment_typ"],
                           **evaluation},
        }

    @staticmethod
    def _inputs(spark):
        import pandas as pd
        predictions = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c1", "c2", "c2"],
            "snap_date": ["2025-01-31"] * 4,
            "prod_name": ["A", "B", "A", "B"],
            "score": [0.9, 0.1, 0.2, 0.8],
            "rank": [1, 2, 2, 1],
            "model_version": ["v1"] * 4,
        }))
        labels = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c1", "c2", "c2"],
            "snap_date": ["2025-01-31"] * 4,
            "prod_name": ["A", "B", "A", "B"],
            "label": [1, 0, 0, 1],
        }))
        return predictions, labels

    @staticmethod
    def _segments_by_customer(frame):
        return {r["cust_id"]: r["cust_segment_typ"]
                for r in frame.select("cust_id", "cust_segment_typ")
                .distinct().collect()}

    def test_monitoring_takes_segments_from_inference_population(self, spark):
        """c2 is not in the population: its segment is NULL, which the metric
        layer reports as the unmatched group."""
        import pandas as pd
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
        from recsys_tfb.pipelines.evaluation.nodes import (
            make_prepare_eval_data_node,
        )

        predictions, labels = self._inputs(spark)
        population = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"], "cust_id": ["c1"],
            "cust_segment_typ": ["from_population"],
        }))
        params = self._parameters()
        frame, segments = make_prepare_eval_data_node("inference_population")(
            predictions, labels, population, params)

        assert frame.count() == 4
        assert self._segments_by_customer(frame) == {
            "c1": "from_population", "c2": None}
        assert segments == {
            "joined": ["cust_segment_typ"],
            "sources": {"cust_segment_typ": "inference_population"},
            "missing": {},
            "config_fingerprint": fingerprint(params),
        }

    def test_post_training_sample_pool_is_finer_grained_without_fanout(self, spark):
        import pandas as pd
        from recsys_tfb.pipelines.evaluation.nodes import (
            make_prepare_eval_data_node,
        )

        predictions, labels = self._inputs(spark)
        # sample_pool holds one row per (time, entity, item).
        population = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 4,
            "cust_id": ["c1", "c1", "c2", "c2"],
            "prod_name": ["A", "B", "A", "B"],
            "cust_segment_typ": ["mass", "mass", "hnw", "hnw"],
        }))
        frame, segments = make_prepare_eval_data_node("sample_pool")(
            predictions, labels, population, self._parameters())

        assert frame.count() == 4
        assert self._segments_by_customer(frame) == {"c1": "mass", "c2": "hnw"}
        assert segments["sources"] == {"cust_segment_typ": "sample_pool"}

    def test_an_override_table_wins_over_the_population(self, spark):
        import pandas as pd
        from recsys_tfb.pipelines.evaluation.nodes import (
            make_prepare_eval_data_node,
        )

        predictions, labels = self._inputs(spark)
        population = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 2, "cust_id": ["c1", "c2"],
            "cust_segment_typ": ["from_population"] * 2,
        }))
        spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c2"], "snap_date": ["2025-01-31"] * 2,
            "cust_segment_typ": ["from_override"] * 2,
        })).createOrReplaceTempView("ext_segment_override")
        params = self._parameters(segment_sources={"cust_segment_typ": {
            "table": "ext_segment_override",
            "key_columns": ["cust_id", "snap_date"],
            "segment_column": "cust_segment_typ"}})

        frame, segments = make_prepare_eval_data_node("inference_population")(
            predictions, labels, population, params)

        assert self._segments_by_customer(frame) == {
            "c1": "from_override", "c2": "from_override"}
        assert segments["joined"] == ["cust_segment_typ"]
        assert segments["sources"] == {
            "cust_segment_typ": "ext_segment_override"}

    def test_a_population_without_the_column_skips_it_and_warns(
        self, spark, caplog
    ):
        """Not a raise: a monitoring population is a user-defined table, and
        one missing segment column must not stop the monthly report. The
        warning names the table and the column so a typo is recognisable."""
        import logging
        import pandas as pd
        from recsys_tfb.pipelines.evaluation.nodes import (
            make_prepare_eval_data_node,
        )

        predictions, labels = self._inputs(spark)
        population = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 2, "cust_id": ["c1", "c2"],
        }))
        with caplog.at_level(logging.WARNING):
            frame, segments = make_prepare_eval_data_node(
                "inference_population"
            )(predictions, labels, population, self._parameters())

        assert "cust_segment_typ" not in frame.columns
        assert segments["joined"] == []
        assert segments["sources"] == {}
        assert segments["missing"] == {
            "cust_segment_typ": "inference_population"}
        warnings = [r.getMessage() for r in caplog.records
                    if r.levelno == logging.WARNING]
        assert any("inference_population" in m and "cust_segment_typ" in m
                   for m in warnings), warnings


def test_prepare_eval_data_filters_to_configured_snap_date(spark):
    """prepare_eval_data keeps only rows at evaluation.snap_date, dropping the
    other snapshots that share the same model_version in the table."""
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
            "label": "label", "score": "score", "rank": "rank"}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = _prepare_eval_data(predictions, labels, parameters).toPandas()
    assert set(result["snap_date"]) == {"2025-01-31"}
    assert len(result) == 2


def test_prepare_eval_data_raises_when_snap_date_absent(spark):
    """When evaluation.snap_date matches no predictions row, prepare_eval_data
    raises ValueError and the message names the snap_dates actually present."""
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
            "label": "label", "score": "score", "rank": "rank"}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2099-12-31"},
    }

    with pytest.raises(ValueError, match="2025-01-31"):
        _prepare_eval_data(predictions, labels, parameters)


def test_prepare_eval_data_raises_when_snap_date_unset(spark):
    """When evaluation.snap_date is not configured, prepare_eval_data raises
    ValueError rather than silently evaluating the whole table."""
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
            "label": "label", "score": "score", "rank": "rank"}},
        "model_version": "v1",
        "evaluation": {},
    }

    with pytest.raises(ValueError, match="snap_date not configured"):
        _prepare_eval_data(predictions, labels, parameters)


def _months_frames(spark, months):
    """Predictions and labels for customers c1/c2 × items A/B in each month."""
    import pandas as pd

    keys = [(m, c, p) for m in months for c in ("c1", "c2") for p in ("A", "B")]
    predictions = spark.createDataFrame(pd.DataFrame({
        "snap_date": [m for m, _, _ in keys],
        "cust_id": [c for _, c, _ in keys],
        "prod_name": [p for _, _, p in keys],
        "score": [0.9 if p == "A" else 0.1 for _, _, p in keys],
        "rank": [1 if p == "A" else 2 for _, _, p in keys],
        "model_version": ["v1"] * len(keys),
    }))
    labels = spark.createDataFrame(pd.DataFrame({
        "snap_date": [m for m, _, _ in keys],
        "cust_id": [c for _, c, _ in keys],
        "prod_name": [p for _, _, p in keys],
        "label": [1 if p == "A" else 0 for _, _, p in keys],
    }))
    return predictions, labels


def _prepare_params(snap_date):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "model_version": "v1",
        "evaluation": {"snap_date": snap_date},
    }


def test_prepare_eval_data_stamps_every_row_with_the_partition_fingerprint(spark):
    """#374: each row carries the settings its partition is written under, so
    a reader can tell when another run rewrote that date since."""
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        PARTITION_FINGERPRINT_COLUMN,
        partition_fingerprint,
    )

    predictions, labels = _months_frames(spark, ["2025-01-31", "2025-02-28"])
    params = _prepare_params(["2025-01-31", "2025-02-28"])
    params["post_training"] = False
    result = _prepare_eval_data(predictions, labels, params)
    stamped = {r[0] for r in
               result.select(PARTITION_FINGERPRINT_COLUMN).distinct().collect()}
    # No segment column configured, so nothing joined.
    assert stamped == {partition_fingerprint(params, [])}


class TestAPartitionCarriesTheSegmentColumnsActuallyJoined:
    """#374 review: the settings alone do not say what a partition holds. A
    segment column the population table lacks is skipped (not raised), so two
    runs with identical settings write different rows when the population
    changed between them. Driven through prepare_eval_data, the real writer,
    and compute_metrics, a reader."""

    MONTHS = ["2025-01-31", "2025-02-28"]

    @staticmethod
    def _params(snap_date):
        params = _prepare_params(snap_date)
        params["evaluation"].update({"segment_columns": ["tier"],
                                     "k_values": [1, 2]})
        return params

    @staticmethod
    def _population(spark, months, with_tier):
        if with_tier:
            return spark.createDataFrame(
                [(m, c, "gold") for m in months for c in ("c1", "c2")],
                ["snap_date", "cust_id", "tier"])
        return spark.createDataFrame(
            [(m, c) for m in months for c in ("c1", "c2")],
            ["snap_date", "cust_id"])

    @classmethod
    def _run(cls, spark, snap_date, months, with_tier):
        """One prepare_eval_data run: ``(its partition rows, its JSON)``."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            make_prepare_eval_data_node,
        )

        predictions, labels = _months_frames(spark, months)
        return make_prepare_eval_data_node("inference_population")(
            predictions, labels, cls._population(spark, months, with_tier),
            cls._params(snap_date))

    def test_a_month_rewritten_without_a_joined_column_is_refused(self, spark):
        """January–February joined ``tier``; the population then lost it and
        February was re-run alone (``tier`` skipped, NULL in the partition).
        Resuming the range: its JSON still says joined=[tier] and the
        settings match, so without the joined list in the fingerprint all of
        February would land in the unmatched segment, exit 0."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        range_rows, range_segments = self._run(
            spark, self.MONTHS, self.MONTHS, with_tier=True)
        february_rows, february_segments = self._run(
            spark, "2025-02-28", ["2025-02-28"], with_tier=False)
        assert february_segments["joined"] == []
        table = range_rows.filter("snap_date = '2025-01-31'").unionByName(
            february_rows, allowMissingColumns=True)

        with pytest.raises(ValueError) as excinfo:
            compute_metrics(table, range_segments, self._params(self.MONTHS))
        message = str(excinfo.value)
        assert "2025-02-28: written under other settings" in message, message
        assert "2025-01-31" not in message, message

    def test_same_settings_and_population_over_other_dates_are_read(self, spark):
        """Same settings, same population: the month a single-date run wrote
        is the range run's month too."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        range_rows, range_segments = self._run(
            spark, self.MONTHS, self.MONTHS, with_tier=True)
        february_rows, _ = self._run(
            spark, "2025-02-28", ["2025-02-28"], with_tier=True)
        table = range_rows.filter("snap_date = '2025-01-31'").unionByName(
            february_rows)
        result = compute_metrics(table, range_segments, self._params(self.MONTHS))
        assert result["dataset_overview"]["totals"]["n_snap_dates"] == 2


def test_prepare_eval_data_keeps_every_configured_date(spark):
    """Two of three months configured (#374): both kept, whole query groups
    (2 customers × 2 items each), the third month dropped."""
    predictions, labels = _months_frames(
        spark, ["2025-01-31", "2025-02-28", "2025-03-31"])
    result = _prepare_eval_data(
        predictions, labels, _prepare_params(["2025-01-31", "2025-03-31"])
    ).toPandas()
    assert sorted(result["snap_date"].value_counts().items()) == [
        ("2025-01-31", 4), ("2025-03-31", 4)]


def test_prepare_eval_data_names_the_one_configured_date_with_no_predictions(spark):
    """Three months configured, the predictions hold two. A check on the
    filtered frame as a whole passes (it has rows), so the missing month would
    silently drop out of the evaluation."""
    predictions, labels = _months_frames(spark, ["2025-01-31", "2025-02-28"])
    params = _prepare_params(["2025-01-31", "2025-02-28", "2025-03-31"])
    with pytest.raises(ValueError) as excinfo:
        _prepare_eval_data(predictions, labels, params)
    message = str(excinfo.value)
    assert ("No predictions found for 1 of 3 evaluation.snap_date dates: "
            "['2025-03-31']") in message, message
    # The months that are there are listed as present, not as missing.
    assert "snap_dates present in predictions: ['2025-01-31', '2025-02-28']" \
        in message, message


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
        }},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = _prepare_eval_data(predictions, labels, parameters)
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
                "snap_date": "2025-01-31",
                "k_values": [1, 2, 3],
                "baseline": {"lookback_months": 12},
                "report": {"sections": {"baseline": baseline_section}},
            },
        }

    @classmethod
    def _eval_predictions(cls, spark):
        """The partition as ``prepare_eval_data`` under :meth:`_parameters`
        writes it, settings fingerprint included."""
        import pandas as pd

        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        return stamp_partition_fingerprint(spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 6,
            "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
            "prod_name": ["A", "B", "C", "A", "B", "C"],
            "label": [1, 0, 1, 0, 1, 0],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
            "rank": [1, 2, 3, 3, 1, 2],
        })), cls._parameters(), [])

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
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            _no_segments(self._parameters()),
            self._parameters(),
        )
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

        assert set(result.keys()) == {
            "overall", "per_item", "purchase_counts", "monthly_counts",
            "config_fingerprint",
        }
        assert result["config_fingerprint"] == fingerprint(self._parameters())
        assert "A" in result["per_item"]
        # purchase_counts comes from _label_table fixture (snap=2024-06-30
        # falls inside the [2024-01-31, 2025-01-31) lookback window for the
        # 2025-01-31 eval snap). A=3 positives (h0/h1/h2 all label=1),
        # B=1 (only h0 label=1), C=0.
        assert result["purchase_counts"] == {"A": 3, "B": 1, "C": 0}
        # All history sits in a single calendar month → monthly_counts breaks
        # the same totals down by "2024-06" and reconciles with purchase_counts.
        assert result["monthly_counts"] == {
            "A": {"2024-06": 3}, "B": {"2024-06": 1}, "C": {"2024-06": 0},
        }

    def test_several_dates_record_each_windows_covered_months(self, spark):
        """#374: two evaluated dates, two lookback windows. History at
        2024-06-30 falls in both; history at 2025-01-31 only in the February
        window (the January window ends before its own date). Counts sum over
        the windows; each window's months with label rows are recorded so the
        report can divide by window-months, not by one window's 12."""
        import pandas as pd

        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters()
        params["evaluation"]["snap_date"] = ["2025-01-31", "2025-02-28"]
        labels = self._label_table(spark).unionByName(spark.createDataFrame(
            pd.DataFrame({
                "snap_date": ["2025-01-31"] * 3, "cust_id": ["h0"] * 3,
                "prod_name": ["A", "B", "C"], "label": [1, 0, 0],
            })))
        result = compute_baseline_metrics(
            TestEnrichedReadersKeepTheEvaluatedMonth._two_months(spark),
            labels, _no_segments(params), params)
        assert result["window_months_covered"] == {
            "2025-01-31": 1, "2025-02-28": 2}
        # A: 3 in the January window, 3 + 1 in the February one.
        assert result["purchase_counts"] == {"A": 7, "B": 2, "C": 0}
        assert result["monthly_counts"] == {
            "A": {"2024-06": 6, "2025-01": 1},
            "B": {"2024-06": 2, "2025-01": 0},
            "C": {"2024-06": 0, "2025-01": 0},
        }

    def test_returns_a_fingerprinted_stub_when_section_disabled(self):
        """Not ``None``: a ``null`` JSON cannot carry the fingerprint, and
        ``generate_report`` would then refuse it as unfingerprinted. Returns
        before touching either DataFrame, hence no Spark session."""
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters(baseline_section=False)
        result = compute_baseline_metrics(None, None, _no_segments(params), params)
        assert result == {
            "enabled": False, "config_fingerprint": fingerprint(params),
        }


class TestComputePredictionQuality:
    """compute_prediction_quality: every evaluated row as a binary prediction
    (ADR-0024)."""

    @staticmethod
    def _parameters(section=True, **block):
        params = TestComputeBaselineMetrics._parameters()
        params["evaluation"]["report"]["sections"]["prediction_quality"] = section
        if block:
            params["evaluation"]["prediction_quality"] = block
        return params

    @staticmethod
    def _with_a_query_group_without_a_positive(spark):
        """``TestComputeBaselineMetrics``' partition plus customer c3, whose
        three rows are all negative."""
        import pandas as pd

        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        return stamp_partition_fingerprint(spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 9,
            "cust_id": ["c1"] * 3 + ["c2"] * 3 + ["c3"] * 3,
            "prod_name": ["A", "B", "C"] * 3,
            "label": [1, 0, 1, 0, 1, 0, 0, 0, 0],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3, 0.7, 0.6, 0.4],
            "rank": [1, 2, 3, 3, 1, 2, 1, 2, 3],
        })), TestComputeBaselineMetrics._parameters(), [])

    @pytest.mark.parametrize("section", [False, None])
    def test_returns_a_fingerprinted_stub_unless_switched_on(self, section):
        """Off in the framework's conf, and off when no conf says anything
        (``None`` leaves the key out). Returns before touching the frame."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters(section=section)
        if section is None:
            del params["evaluation"]["report"]["sections"]["prediction_quality"]
        result = compute_prediction_quality(None, None, params)
        assert result == {
            "enabled": False, "config_fingerprint": fingerprint(params)}

    def test_counts_the_query_group_without_a_positive(self, spark):
        """ADR-0024 decision 3: nothing between the partition and the bins
        filters on ``total_rel``; c3's three negatives are in every count."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._parameters()
        result = compute_prediction_quality(
            self._with_a_query_group_without_a_positive(spark),
            _no_segments(params), params)
        summary = result["overall"]["summary"]
        assert (summary["n"], summary["n_pos"]) == (9, 3)
        assert result["per_item"]["summary"]["A"]["n"] == 3

    def test_bins_and_budget_come_from_the_conf(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters(n_bins=8, n_display_bins=4, top_n=1)
        result = compute_prediction_quality(
            self._with_a_query_group_without_a_positive(spark),
            _no_segments(params), params)
        assert result["bins"]["n_bins"] == 8
        assert result["bins"]["n_display_bins"] == 4
        assert result["bins"]["lo"] == pytest.approx(0.1)
        assert result["bins"]["width"] == pytest.approx(0.1)
        assert len(result["per_item"]["listed"]) == 1
        assert result["per_item"]["n_items"] == 3
        assert result["enabled"] is True
        assert result["config_fingerprint"] == fingerprint(params)

    def test_the_default_bins_apply_when_the_block_is_absent(self, spark):
        from recsys_tfb.core.consistency import PREDICTION_QUALITY_DEFAULTS
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._parameters()
        result = compute_prediction_quality(
            self._with_a_query_group_without_a_positive(spark),
            _no_segments(params), params)
        assert result["bins"]["n_bins"] == PREDICTION_QUALITY_DEFAULTS["n_bins"]
        assert result["per_item"]["top_n"] == PREDICTION_QUALITY_DEFAULTS["top_n"]

    @staticmethod
    def _weighted(spark, params, c3_weight):
        """The same nine rows carrying the zero-positive group weight: 1 on the
        two groups holding a positive, ``c3_weight`` on c3's three rows (the
        kept zero-positive group; ``None`` is what a partition written under
        ratio 0 holds once the ``columns: "auto"`` table has the column)."""
        import pandas as pd
        from pyspark.sql import types as T

        from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL
        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        pdf = pd.DataFrame({
            "snap_date": ["2025-01-31"] * 9,
            "cust_id": ["c1"] * 3 + ["c2"] * 3 + ["c3"] * 3,
            "prod_name": ["A", "B", "C"] * 3,
            "label": [1, 0, 1, 0, 1, 0, 0, 0, 0],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3, 0.7, 0.6, 0.4],
            "rank": [1, 2, 3, 3, 1, 2, 1, 2, 3],
            ZERO_POSITIVE_GROUP_WEIGHT_COL: [1.0] * 6 + [c3_weight] * 3,
        })
        schema = T.StructType([
            T.StructField("snap_date", T.StringType()),
            T.StructField("cust_id", T.StringType()),
            T.StructField("prod_name", T.StringType()),
            T.StructField("label", T.LongType()),
            T.StructField("score", T.DoubleType()),
            T.StructField("rank", T.LongType()),
            T.StructField(ZERO_POSITIVE_GROUP_WEIGHT_COL, T.DoubleType()),
        ])
        rows = [tuple(None if pd.isna(v) else v for v in r)
                for r in pdf.itertuples(index=False)]
        return stamp_partition_fingerprint(
            spark.createDataFrame(rows, schema), params, [])

    def _post_training(self, **dataset):
        params = self._parameters()
        params["post_training"] = True
        params["dataset"] = dataset
        return params

    def test_post_training_weighs_every_row_by_its_group_weight(self, spark):
        """#429: a kept zero-positive group stands for 1/r groups, so each of
        c3's rows counts twice at r = 0.5 — in the totals and per item."""
        from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._post_training(test_zero_positive_group_ratio=0.5)
        result = compute_prediction_quality(
            self._weighted(spark, params, 2.0), _no_segments(params), params)
        summary = result["overall"]["summary"]
        assert (summary["n"], summary["n_pos"]) == (12, 3)
        assert result["per_item"]["summary"]["A"]["n"] == 4
        assert result["columns"]["weight"] == ZERO_POSITIVE_GROUP_WEIGHT_COL

    def test_the_weight_is_decided_by_the_config_not_by_the_column(self, spark):
        """Under ratio 0 the column can still exist — NULL, from the
        ``columns: "auto"`` table another run widened — and summing it would
        silently drop those rows. Rows count once."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._post_training()
        result = compute_prediction_quality(
            self._weighted(spark, params, None), _no_segments(params), params)
        summary = result["overall"]["summary"]
        assert (summary["n"], summary["n_pos"]) == (9, 3)

    def test_monitoring_mode_counts_rows(self, spark):
        """Monitoring reads inference output, which no dataset draw touched."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._parameters()
        params["post_training"] = False
        params["dataset"] = {"test_zero_positive_group_ratio": 0.5}
        result = compute_prediction_quality(
            self._weighted(spark, params, 2.0), _no_segments(params), params)
        assert result["overall"]["summary"]["n"] == 9


def test_compute_metric_ci_disabled_returns_stub():
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import compute_metric_ci
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"ci": {"enabled": False}}},
    }
    assert compute_metric_ci(None, params) == {
        "enabled": False, "config_fingerprint": fingerprint(params),
    }


def test_compute_report_aggregates_disabled_returns_fingerprinted_stub():
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_report_aggregates,
    )
    params = {"evaluation": {"report": {"sections": {"diagnostics": False}}}}
    assert compute_report_aggregates(None, None, params) == {
        "enabled": False, "config_fingerprint": fingerprint(params),
    }


def test_compute_metric_ci_end_to_end_small(spark):
    from recsys_tfb.pipelines.evaluation.nodes import (
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
            "snap_date": "20240331",
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
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        stamp_partition_fingerprint,
    )
    sample = draw_diagnosis_sample_node(
        stamp_partition_fingerprint(df, params, []), _no_segments(params), params)
    out = compute_metric_ci(sample, params)
    assert out["enabled"] is True
    assert "A" in out["per_item"] and "macro" in out and "sample" in out
    assert out["sample"]["n_queries_sampled"] == 2
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    assert out["config_fingerprint"] == fingerprint(params)


def test_compute_metric_ci_raises_when_enabled_but_sample_none(spark):
    import pytest as _pytest
    from recsys_tfb.pipelines.evaluation.nodes import compute_metric_ci
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


class TestConsumersSegmentByTheLandedList:
    """Every node that groups by segment reads ``evaluation_segment_columns``
    (ADR-0020 bug 6). The frame here also carries ``stale_seg``, all NULL, as
    a column the other run mode joined into the shared enriched table would
    be; it is listed first in config, so a consumer that picks "configured and
    present in the frame" segments by it and grows a fake "(unmatched)" group.
    """

    @classmethod
    def _segments(cls):
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

        return {
            "joined": ["cust_segment_typ"],
            "sources": {"cust_segment_typ": "sample_pool"},
            "missing": {"holding_combo": "sample_pool"},
            "config_fingerprint": fingerprint(cls._parameters()),
        }

    @staticmethod
    def _parameters():
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank"}},
            "evaluation": {
                "snap_date": "2025-01-31",
                "k_values": [1, 2, 3],
                "segment_columns": ["stale_seg", "cust_segment_typ",
                                    "holding_combo"],
                "baseline": {"lookback_months": 12},
                "diagnosis": {"sample": {
                    "max_queries": 10, "min_pos_queries_per_item": 1,
                    "seed": 42}},
            },
        }

    @classmethod
    def _frame(cls, spark):
        from pyspark.sql import functions as F

        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        return stamp_partition_fingerprint(
            TestComputeBaselineMetrics._eval_predictions(spark).withColumn(
                "cust_segment_typ",
                F.when(F.col("cust_id") == "c1", "mass").otherwise("hnw"),
            ).withColumn("stale_seg", F.lit(None).cast("string")),
            cls._parameters(), cls._segments()["joined"])

    def test_compute_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        result = compute_metrics(
            self._frame(spark), self._segments(), self._parameters())
        assert set(result["per_segment"]) == {"mass", "hnw"}
        assert set(result["dataset_overview"]["by_segment"]) == {"mass", "hnw"}
        # What the report says about segments travels with the metrics.
        assert result["segments"] == {
            k: self._segments()[k] for k in ("joined", "sources", "missing")}
        # It lands as metrics.json, so it records the computed settings it
        # was made with and generate_report can refuse a stale one
        # (ADR-0018 decision 2, ADR-0020 decision 2).
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

        assert result["config_fingerprint"] == fingerprint(self._parameters())

    def test_compute_baseline_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._frame(spark), TestComputeBaselineMetrics._label_table(spark),
            self._segments(), self._parameters())
        assert set(result["per_segment"]) == {"mass", "hnw"}

    def test_draw_diagnosis_sample_node(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            draw_diagnosis_sample_node,
        )

        sample_pdf, _meta = draw_diagnosis_sample_node(
            self._frame(spark), self._segments(), self._parameters())
        assert "cust_segment_typ" in sample_pdf.columns
        assert "stale_seg" not in sample_pdf.columns

    def test_generate_comparison_report(self, monkeypatch):
        """Only the model side has segment columns; the compared side is a
        prediction table of its own. No rendered section shows segments, so
        the observable is what each side is asked to segment by."""
        from recsys_tfb.pipelines.evaluation import nodes

        asked = []

        def fake_metrics(df, parameters, *, segment_columns=()):
            asked.append((df, list(segment_columns)))
            return {}

        monkeypatch.setattr(nodes, "compute_all_metrics", fake_metrics)
        monkeypatch.setattr(nodes, "build_comparison_result",
                            lambda *a, **k: {})
        monkeypatch.setattr(nodes, "assemble_comparison_report",
                            lambda *a, **k: "<html/>")
        nodes.generate_comparison_report(
            "model_side", "compared_side", {}, self._segments(),
            self._parameters())
        assert asked == [("model_side", ["cust_segment_typ"]),
                         ("compared_side", [])]


class TestEnrichedReadersKeepTheEvaluatedMonth:
    """ADR-0018 decision 1: ``enriched_eval_predictions`` holds every month a
    ``model_version`` was evaluated on, so each node reading it keeps the
    evaluated month.

    The frame here carries a second month with flipped labels and scores, the
    way the table accumulates months. Each node's result on it must equal its
    result on the evaluated month alone; a node that keeps both months differs.
    """

    @staticmethod
    def _parameters(snap_date="2025-01-31"):
        params = TestComputeBaselineMetrics._parameters()
        params["evaluation"]["snap_date"] = snap_date
        params["evaluation"]["diagnosis"] = {"sample": {
            "max_queries": 10, "min_pos_queries_per_item": 1, "seed": 42}}
        # On, so compute_prediction_quality reads the frame instead of
        # returning its stub (#381).
        params["evaluation"]["report"]["sections"]["prediction_quality"] = True
        return params

    @staticmethod
    def _one_month(spark):
        return TestComputeBaselineMetrics._eval_predictions(spark)

    @classmethod
    def _two_months(cls, spark):
        from pyspark.sql import functions as F

        one = cls._one_month(spark)
        other = (one.withColumn("snap_date", F.lit("2025-02-28"))
                 .withColumn("label", 1 - F.col("label"))
                 .withColumn("score", 1 - F.col("score")))
        return one.unionByName(other)

    def test_compute_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters()
        both = compute_metrics(self._two_months(spark), _no_segments(params),
                               params)
        alone = compute_metrics(self._one_month(spark), _no_segments(params),
                                params)
        assert both["dataset_overview"]["totals"]["n_snap_dates"] == 1
        assert both == alone

    def test_compute_baseline_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters()
        labels = TestComputeBaselineMetrics._label_table(spark)
        both = compute_baseline_metrics(
            self._two_months(spark), labels, _no_segments(params), params)
        alone = compute_baseline_metrics(
            self._one_month(spark), labels, _no_segments(params), params)
        assert both == alone

    def test_compute_report_aggregates(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_report_aggregates,
        )

        params = self._parameters()
        both = compute_report_aggregates(
            self._two_months(spark), _no_segments(params), params)
        alone = compute_report_aggregates(
            self._one_month(spark), _no_segments(params), params)
        assert both == alone

    def test_compute_prediction_quality(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._parameters()
        both = compute_prediction_quality(
            self._two_months(spark), _no_segments(params), params)
        alone = compute_prediction_quality(
            self._one_month(spark), _no_segments(params), params)
        assert both["overall"]["summary"]["n"] == 6
        assert both == alone

    def test_draw_diagnosis_sample_node(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            draw_diagnosis_sample_node,
        )

        params = self._parameters()
        sample_pdf, meta = draw_diagnosis_sample_node(
            self._two_months(spark), _no_segments(params), params)
        assert set(sample_pdf["snap_date"]) == {"2025-01-31"}
        assert meta["n_queries_sampled"] == 2

    def test_compute_metrics_refuses_a_month_with_no_rows(self, spark):
        """Postcondition: the evaluated month's partition is empty or was never
        written, and reading the table raised nothing."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date="2025-03-31")
        with pytest.raises(
            ValueError,
            match=r"compute_metrics postcondition: 0 evaluated months",
        ):
            compute_metrics(self._two_months(spark), _no_segments(params),
                            params)

    def test_compute_metrics_evaluates_every_configured_date_together(self, spark):
        """#374: two dates configured, both months' query groups go into one
        set of metrics — 2 customers × 2 months = 4 query groups."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        result = compute_metrics(self._two_months(spark), _no_segments(params),
                                 params)
        overview = result["dataset_overview"]
        assert overview["totals"]["n_snap_dates"] == 2
        assert sorted(overview["by_snap_date"]) == ["2025-01-31", "2025-02-28"]
        assert result["n_queries"] + result["n_excluded_queries"] == 4

    def test_compute_metrics_refuses_a_configured_date_with_no_rows(self, spark):
        """Two dates configured, the partition holds one of them: expected 2,
        evaluated 1."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date=["2025-01-31", "2025-03-31"])
        with pytest.raises(
            ValueError,
            match=r"compute_metrics postcondition: 1 evaluated months .*"
                  r"expected exactly 2",
        ):
            compute_metrics(self._two_months(spark), _no_segments(params),
                            params)

    # --- partitions rewritten by another run (#374) ------------------------

    _READERS = ("compute_metrics", "compute_baseline_metrics",
                "compute_report_aggregates", "draw_diagnosis_sample_node",
                "compute_prediction_quality")

    @staticmethod
    def _read(name, frame, spark, params):
        from recsys_tfb.pipelines.evaluation import nodes

        if name == "compute_baseline_metrics":
            return nodes.compute_baseline_metrics(
                frame, TestComputeBaselineMetrics._label_table(spark),
                _no_segments(params), params)
        if name == "compute_report_aggregates":
            return nodes.compute_report_aggregates(
                frame, _no_segments(params), params)
        return getattr(nodes, name)(frame, _no_segments(params), params)

    @staticmethod
    def _rewrite(frame, month, params):
        """``frame`` with ``month``'s partition as a run under ``params``
        would leave it."""
        from pyspark.sql import functions as F

        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            PARTITION_FINGERPRINT_COLUMN,
            partition_fingerprint,
        )

        return frame.withColumn(
            PARTITION_FINGERPRINT_COLUMN,
            F.when(F.col("snap_date") == month,
                   F.lit(partition_fingerprint(params, [])))
            .otherwise(F.col(PARTITION_FINGERPRINT_COLUMN)))

    @pytest.mark.parametrize("name", _READERS)
    def test_a_month_another_run_rewrote_is_refused_by_name(self, spark, name):
        """January and February evaluated together; February then rewritten
        by a post-training run of February alone. Resuming the first run from
        any reader reads the range directory's JSON, whose fingerprint still
        matches, so only the partition's own fingerprint can refuse February."""
        params = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        february_alone = self._parameters(snap_date="2025-02-28")
        february_alone["post_training"] = True
        table = self._rewrite(self._two_months(spark), "2025-02-28",
                              february_alone)
        with pytest.raises(ValueError) as excinfo:
            self._read(name, table, spark, params)
        message = str(excinfo.value)
        assert "2025-02-28: written under other settings" in message, message
        assert "2025-01-31" not in message, message

    def test_a_single_date_resume_refuses_its_month_a_range_run_rewrote(
        self, spark,
    ):
        """The other direction: February alone first, then a range run under
        other settings rewrote February; resuming February alone refuses it."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date="2025-02-28")
        range_run = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        range_run["post_training"] = True
        table = self._rewrite(self._two_months(spark), "2025-02-28", range_run)
        with pytest.raises(ValueError,
                           match=r"2025-02-28: written under other settings"):
            compute_metrics(table, _no_segments(params), params)

    def test_a_partition_written_before_fingerprints_is_refused(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            PARTITION_FINGERPRINT_COLUMN,
        )

        params = self._parameters()
        table = self._two_months(spark).drop(PARTITION_FINGERPRINT_COLUMN)
        with pytest.raises(ValueError, match=r"2025-01-31: written before"):
            compute_metrics(table, _no_segments(params), params)

    def test_a_range_resume_reads_a_month_a_same_settings_single_run_wrote(
        self, spark,
    ):
        """Same settings, other dates: not a rewrite. Stamped as February
        alone would stamp it, the range run reads February as its own."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        table = self._rewrite(self._two_months(spark), "2025-02-28",
                              self._parameters(snap_date="2025-02-28"))
        result = compute_metrics(table, _no_segments(params), params)
        assert result["dataset_overview"]["totals"]["n_snap_dates"] == 2


class TestReadersRefuseAPartitionPreparedUnderOtherSettings:
    """The partition is read back from Hive, so a slice starting after
    ``prepare_eval_data`` does not re-join it. Its landed
    ``evaluation_segment_columns`` fingerprint says which settings it was
    joined under; the segmenting readers compare it on
    ``PARTITION_CONTENT_KEYS`` before reading (the #352 sign-off, ADR-0020
    bug 6).
    """

    _READERS = ("compute_metrics", "compute_baseline_metrics",
                "draw_diagnosis_sample_node")

    @staticmethod
    def _call(name, frame, labels, segments, params):
        from recsys_tfb.pipelines.evaluation import nodes

        if name == "compute_baseline_metrics":
            return nodes.compute_baseline_metrics(
                frame, labels, segments, params)
        return getattr(nodes, name)(frame, segments, params)

    @pytest.mark.parametrize("name", _READERS)
    def test_a_changed_segment_setting_names_the_join_to_re_run(self, name):
        """Raised before the frame is touched, so no frame is passed."""
        prepared_under = TestEnrichedReadersKeepTheEvaluatedMonth._parameters()
        now = TestEnrichedReadersKeepTheEvaluatedMonth._parameters()
        now["evaluation"]["segment_columns"] = ["cust_segment_typ"]
        with pytest.raises(ValueError) as exc:
            self._call(name, None, None, _no_segments(prepared_under), now)
        msg = str(exc.value)
        assert "evaluation.segment_columns" in msg
        assert "--from-node prepare_eval_data" in msg

    @pytest.mark.parametrize("name", _READERS)
    def test_a_changed_metric_setting_does_not_stop_the_reader(self, spark, name):
        prepared_under = TestEnrichedReadersKeepTheEvaluatedMonth._parameters()
        now = TestEnrichedReadersKeepTheEvaluatedMonth._parameters()
        now["evaluation"]["k_values"] = [1, 2]
        result = self._call(
            name,
            TestEnrichedReadersKeepTheEvaluatedMonth._one_month(spark),
            TestComputeBaselineMetrics._label_table(spark),
            _no_segments(prepared_under), now)
        assert result is not None


class TestCiConsumerEnabled:
    """The gate reads the schema since #378, so every fixture here declares
    one. A bare ``{}`` now raises the missing-role error rather than answering
    — the same second gate ``get_schema`` applies to any caller that never
    went through the CLI (#274)."""

    @staticmethod
    def _params(event=None, **evaluation):
        columns = {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name"}
        if event is not None:
            columns["event"] = event
        return {"schema": {"columns": columns}, "evaluation": evaluation}

    def test_default_true(self):
        from recsys_tfb.pipelines.evaluation.nodes import (
            _ci_consumer_enabled,
        )
        assert _ci_consumer_enabled(self._params()) is True

    def test_respects_disabled(self):
        from recsys_tfb.pipelines.evaluation.nodes import (
            _ci_consumer_enabled,
        )
        params = self._params(diagnosis={"ci": {"enabled": False}})
        assert _ci_consumer_enabled(params) is False

    def test_false_when_an_optional_role_makes_it_inapplicable(self):
        """The CI bootstraps on the driver through
        ``positive_row_contributions``, which ranks within a query group by
        item alone. With ``event`` declared that order is arbitrary, and the
        interval would sit on the same report line as a Spark-computed point
        estimate that used a different one."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            _ci_consumer_enabled,
        )
        assert _ci_consumer_enabled(self._params(event="imp_id")) is False

    def test_the_registry_gate_also_sees_inapplicability(self):
        """The sample is drawn iff *either* gate says yes, so fixing only the
        CI one leaves the sample drawn for diagnoses that will refuse to run —
        and the sample frame carries no optional-role column, so deduplicating
        it by identity raises ``KeyError: ['imp_id']``. That is what the ad
        example's evaluation died on before this gate learnt the same rule."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            _registry_diagnosis_enabled,
        )
        assert _registry_diagnosis_enabled(self._params()) is True
        assert _registry_diagnosis_enabled(self._params(event="imp_id")) is False

    def test_the_gate_and_the_consumer_agree(self):
        """``compute_metric_ci`` raises when the gate says "nobody wants the
        sample" while it still wants one. Both ask the same helper, so this
        pins that they cannot drift into that state."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            _ci_consumer_enabled, compute_metric_ci,
        )
        params = self._params(event="imp_id")
        assert _ci_consumer_enabled(params) is False
        out = compute_metric_ci(None, params)
        assert out["enabled"] is False
        assert "imp_id" in out["skipped_reason"]


class TestDrawDiagnosisSampleNode:
    @staticmethod
    def _params():
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            }},
            "evaluation": {"snap_date": "20240331", "diagnosis": {"sample": {
                "max_queries": 10, "min_pos_queries_per_item": 2, "seed": 42,
            }}},
        }

    @classmethod
    def _eval_predictions(cls, spark):
        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        rows = []
        for cust in ["H1", "H2", "H3", "H4"]:
            rows.append(("20240331", cust, "hot", 0.9, 1))
            rows.append(("20240331", cust, "cold", 0.1, 0))
        rows.append(("20240331", "C1", "hot", 0.9, 0))
        rows.append(("20240331", "C1", "cold", 0.1, 1))
        return stamp_partition_fingerprint(spark.createDataFrame(
            rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
        ), cls._params(), [])

    def test_returns_none_and_skips_draw_when_all_disabled(self, spark):
        from unittest.mock import patch
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
        from recsys_tfb.pipelines.evaluation import nodes
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": False},
            # registry 診斷（contract.DIAGNOSES）預設也是 enabled，所以「全部
            # 停用」必須連它們一起關——只關 ci 的話樣本仍然該抽。
            **{name: {"enabled": False} for name in DIAGNOSES},
        })
        with patch(
            "recsys_tfb.pipelines.evaluation.nodes.draw_diagnosis_sample"
        ) as spy:
            result = nodes.draw_diagnosis_sample_node(self._eval_predictions(spark), _no_segments(params), params)
        assert result is None
        assert spy.call_count == 0

    def test_draws_when_one_enabled(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": True},
        })
        with patch(
            "recsys_tfb.pipelines.evaluation.nodes.draw_diagnosis_sample",
            return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
        ) as spy, patch.object(
            nodes, "restrict_to_current_eval_partitions", _kept_month_of
        ):
            nodes.draw_diagnosis_sample_node(None, _no_segments(params), params)
        # exact args, not just count: the draw gets the month-restricted table
        # (ADR-0018 decision 1), not the table itself, and params uncopied.
        spy.assert_called_once_with(
            ("kept month of", None), params, segment_columns=[])

    def test_node_output_equals_direct_draw(self, spark):
        # Faithfulness / behaviour-preservation: the node is a pass-through of
        # draw_diagnosis_sample. Same seed -> identical content.
        from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
        from recsys_tfb.pipelines.evaluation import nodes
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            PARTITION_FINGERPRINT_COLUMN,
        )
        params = self._params()  # both consumers default-enabled
        # The node draws from the table as read: fingerprint checked, dropped.
        direct_pdf, direct_meta = draw_diagnosis_sample(
            self._eval_predictions(spark).drop(PARTITION_FINGERPRINT_COLUMN),
            params,
        )
        node_pdf, node_meta = nodes.draw_diagnosis_sample_node(self._eval_predictions(spark), _no_segments(params), params)
        assert node_meta == direct_meta
        assert (
            node_pdf.sort_values(list(node_pdf.columns))
            .reset_index(drop=True)
            .equals(
                direct_pdf.sort_values(list(direct_pdf.columns))
                .reset_index(drop=True)
            )
        )

    def test_draw_diagnosis_sample_called_once_across_consumers(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes
        params = self._params()  # ci default-enabled
        stub_pdf = pd.DataFrame({
            "snap_date": ["20240331"], "cust_id": ["H1"],
            "prod_name": ["hot"], "score": [0.9], "label": [1],
        })
        stub = (stub_pdf, {"n_queries_sampled": 1})
        with patch(
            "recsys_tfb.pipelines.evaluation.nodes.draw_diagnosis_sample",
            return_value=stub,
        ) as spy, patch(
            "recsys_tfb.pipelines.evaluation.nodes.bootstrap_per_item_ci",
            return_value={"n_boot": 1},
        ), patch.object(
            nodes, "restrict_to_current_eval_partitions", _kept_month_of
        ):
            sample = nodes.draw_diagnosis_sample_node(None, _no_segments(params), params)
            nodes.compute_metric_ci(sample, params)
        # exactly one draw, with the node's own inputs — the consumer must NOT
        # re-draw (it consumes the shared sample).
        spy.assert_called_once_with(
            ("kept month of", None), params, segment_columns=[])

    def test_node_logs_free_pandas_data_volume(self, spark, caplog):
        import logging
        from recsys_tfb.pipelines.evaluation import nodes
        params = self._params()  # all enabled
        with caplog.at_level(logging.INFO):
            nodes.draw_diagnosis_sample_node(self._eval_predictions(spark), _no_segments(params), params)
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

    為什麼跟 ``_ci_consumer_enabled`` 分開：既有的 ci 是非 registry 的消費者、
    各診斷走 ``contract.DIAGNOSES``，兩組生命週期不同。合在一起的話，registry
    每加一項診斷都要改解包點——而「新增診斷不必改接線」正是 registry 存在的
    目的。
    """

    #: 每個 fixture 都要帶 schema：#378 之後這個閘門除了 enabled 旗標，還問
    #: 「目前宣告的 schema 下這項診斷算不算得出來」，而 ``get_schema`` 不接受
    #: 一個沒宣告任何角色的設定。
    _SCHEMA = {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
    }}

    def test_defaults_true(self):
        from recsys_tfb.pipelines.evaluation.nodes import (
            _registry_diagnosis_enabled,
        )
        assert _registry_diagnosis_enabled({"schema": self._SCHEMA}) is True

    def test_false_only_when_every_registry_diagnosis_disabled(self):
        import importlib

        from recsys_tfb.diagnosis.metric import contract
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
        from recsys_tfb.pipelines.evaluation.nodes import (
            _registry_diagnosis_enabled,
        )
        all_off = {"schema": self._SCHEMA, "evaluation": {"diagnosis": {
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
            params = {"schema": self._SCHEMA, "evaluation": {"diagnosis": {
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
    """關掉 ci、只開 config_shift → 仍然必須抽樣。

    這是本次接線最容易靜默失效的地方：抽樣閘門若只看舊的 ci，使用者關掉它
    之後 ``diagnosis_sample`` 是 None，config_shift 節點就撞 fail-loud 的
    ValueError——一個純粹由接線遺漏造成、使用者無從自救的失敗。
    """
    from unittest.mock import patch
    import pandas as pd
    from recsys_tfb.pipelines.evaluation import nodes
    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
        }},
        "evaluation": {"diagnosis": {
            "sample": {"max_queries": 10, "min_pos_queries_per_item": 2,
                       "seed": 42},
            "ci": {"enabled": False},
            "config_shift": {"enabled": True},
        }},
    }
    with patch(
        "recsys_tfb.pipelines.evaluation.nodes.draw_diagnosis_sample",
        return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
    ) as spy, patch.object(
        nodes, "restrict_to_current_eval_partitions", _kept_month_of
    ):
        result = nodes.draw_diagnosis_sample_node(None, _no_segments(params), params)
    assert result is not None, (
        "sample gate ignored the registry diagnoses — config_shift is enabled "
        "but no sample was drawn"
    )
    spy.assert_called_once_with(
        ("kept month of", None), params, segment_columns=[])


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
    from recsys_tfb.pipelines.evaluation import nodes

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
            "fake_capacity": {"enabled": True},
        }},
    }

    with caplog.at_level(logging.INFO), patch(
        "recsys_tfb.pipelines.evaluation.nodes.draw_diagnosis_sample"
    ) as spy:
        result = nodes.draw_diagnosis_sample_node(None, _no_segments(params), params)

    assert result is None
    assert spy.call_count == 0
    assert any(
        "skip" in record.getMessage().lower() for record in caplog.records
    ), "expected a log message explaining the sample draw was skipped"


def test_generated_node_writes_stub_when_disabled():
    """The stub carries the name and the fingerprint too: a disabled
    diagnosis is still a landed JSON that ``render_diagnosis_pages`` checks."""
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    params = {"evaluation": {"diagnosis": {"config_shift": {"enabled": False}}}}
    assert node_fn(None, params) == {
        "enabled": False,
        "diagnosis": "config_shift",
        "config_fingerprint": fingerprint(params, (
            "dataset.sample_group_keys", "dataset.sample_ratio",
            "dataset.sample_ratio_overrides", "training.sample_weight_keys",
            "training.sample_weights",
        )),
    }


def test_diagnosis_fingerprint_moves_only_with_its_own_extra_keys():
    """``config_shift`` reads ``dataset.sample_ratio_overrides``; ``item_ability``
    does not. Shared-key fingerprints would make one stale with the other."""
    import copy
    import importlib

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES, inputs_for
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    before = {
        "dataset": {"sample_ratio_overrides": {"k|1": 0.5}},
        "evaluation": {"diagnosis": {n: {"enabled": False} for n in DIAGNOSES}},
    }
    after = copy.deepcopy(before)
    after["dataset"]["sample_ratio_overrides"] = {"k|1": 0.25}

    def fingerprint_of(name, params):
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        upstream = [None] * (len(inputs_for(mod)) - 1)
        return make_diagnosis_node(name)(*upstream, params)["config_fingerprint"]

    assert fingerprint_of("config_shift", before) != \
        fingerprint_of("config_shift", after)
    assert fingerprint_of("item_ability", before) == \
        fingerprint_of("item_ability", after)


def test_generated_node_raises_when_enabled_but_sample_none():
    """The wiring-bug message, which must stay reachable: enabled, applicable,
    and still handed no sample means the gate really is out of sync."""
    import pytest as _pytest

    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    params = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
    }}}
    with _pytest.raises(ValueError, match="draw_diagnosis_sample_node"):
        node_fn(None, params)


def test_generated_node_stubs_instead_of_raising_when_not_applicable():
    """An optional role makes this diagnosis inapplicable, so the sample gate
    deliberately drew nothing — and the node must read that as a skip, not as
    the wiring bug above. Getting this wrong is what ended the ad example's
    evaluation with "gate out of sync with the consumer flag", pointing the
    reader at a bug that was not there."""
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    params = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "event": "imp_id",
    }}}
    out = node_fn(None, params)
    assert out["enabled"] is False
    assert "imp_id" in out["skipped_reason"]
    assert out["diagnosis"] == "config_shift"


def test_generated_node_delegates_to_the_named_module(monkeypatch):
    """轉呼叫的是**以名字查到的**模組，不是寫死的 config_shift。

    工廠若把模組名寫死，registry 只有一項時**每一條測試都會照樣綠**——
    Plan 2 加第二項才會爆，而症狀是「第二項診斷的頁面印出第一項的數字」，
    每頁看起來都很正常。所以這裡注入一個假模組，用它有沒有被呼叫到來證明
    查表這件事真的發生了。
    """
    import sys
    import types

    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    called = {}
    fake = types.ModuleType("recsys_tfb.diagnosis.metric.fake_diag")

    def _compute(diagnosis_sample, parameters):
        called["sample"] = diagnosis_sample
        return {"marker": "from_fake"}

    fake.compute = _compute
    fake.EXTRA_CONFIG_KEYS = ("dataset.only_fake_reads_this",)
    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_diag", fake)

    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

    node_fn = make_diagnosis_node("fake_diag")
    sample = ("pdf-sentinel", {"sampling_description": "x"})
    params = {"dataset": {"only_fake_reads_this": 3}}
    out = node_fn(sample, params)

    # The computed path carries the name and a fingerprint over the module's
    # own declared keys: with the key present in params, a fingerprint that
    # ignored EXTRA_CONFIG_KEYS would hash differently.
    assert out == {
        "marker": "from_fake",
        "diagnosis": "fake_diag",
        "config_fingerprint": fingerprint(
            params, ("dataset.only_fake_reads_this",)),
    }
    assert out["config_fingerprint"] != fingerprint(params)
    # compute 拿到的是整個 tuple，不是解包後的 sample_pdf——契約在
    # contract._SIGNATURES 釘住，抄形狀時最容易改壞的就是這裡。
    assert called["sample"] is sample


def _install_fake_upstream_and_downstream(monkeypatch):
    """A fake diagnosis pair shaped like ``model_capacity`` reading
    ``evaluation_item_ability``, generalised with made-up names so the
    precondition is proven generic rather than special-cased.

    ``fake_downstream.INPUTS`` names ``evaluation_fake_upstream`` — a
    landed result of another registry diagnosis — plus ``parameters``.
    ``contract.DIAGNOSES`` is patched to contain both names, which is what
    ``make_diagnosis_node._run``'s new pre-check keys off of (an
    ``evaluation_<x>`` input where ``<x>`` is itself in ``DIAGNOSES``).

    Returns the dict ``compute`` records its received upstream payload into,
    so a test can tell whether ``compute`` ran at all and with what.
    """
    import sys
    import types

    from recsys_tfb.diagnosis.metric import contract

    upstream_mod = types.ModuleType("recsys_tfb.diagnosis.metric.fake_upstream")
    upstream_mod.EXTRA_CONFIG_KEYS = ("dataset.only_upstream_reads_this",)
    upstream_mod.compute = lambda *a: {}  # never called directly in these tests

    downstream_mod = types.ModuleType(
        "recsys_tfb.diagnosis.metric.fake_downstream")
    downstream_mod.INPUTS = ("evaluation_fake_upstream", "parameters")
    received = {}

    def _compute(evaluation_fake_upstream, parameters):
        received["upstream_payload"] = evaluation_fake_upstream
        return {"marker": "downstream_computed"}

    downstream_mod.compute = _compute

    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_upstream", upstream_mod)
    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_downstream",
        downstream_mod)
    monkeypatch.setattr(
        contract, "DIAGNOSES", ("fake_upstream", "fake_downstream"))
    return received


def test_downstream_diagnosis_refuses_a_stale_upstream_result(monkeypatch):
    """(#342 F8) ``--only-node diagnose_fake_downstream`` loading a stale
    ``evaluation_fake_upstream`` JSON must raise, naming the changed key and
    the upstream's rerun node — the same freshness check
    ``render_diagnosis_pages`` runs, but here it must run *inside* the
    downstream diagnosis's own node, before it stamps the CURRENT fingerprint
    over a result actually computed on stale input."""
    import copy

    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    _install_fake_upstream_and_downstream(monkeypatch)

    old_params = {"dataset": {"only_upstream_reads_this": 1},
                  "evaluation": {"diagnosis": {}}}
    new_params = copy.deepcopy(old_params)
    new_params["dataset"]["only_upstream_reads_this"] = 2

    stale_upstream_payload = {
        "enabled": True, "diagnosis": "fake_upstream",
        "config_fingerprint": fingerprint(
            old_params, ("dataset.only_upstream_reads_this",)),
    }

    node_fn = make_diagnosis_node("fake_downstream")
    with pytest.raises(ValueError) as exc:
        node_fn(stale_upstream_payload, new_params)
    msg = str(exc.value)
    assert "dataset.only_upstream_reads_this: 1 -> 2" in msg
    assert "--from-node diagnose_fake_upstream" in msg


def test_downstream_diagnosis_computes_on_a_fresh_upstream_result(monkeypatch):
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    received = _install_fake_upstream_and_downstream(monkeypatch)

    params = {"dataset": {"only_upstream_reads_this": 1},
              "evaluation": {"diagnosis": {}}}
    fresh_upstream_payload = {
        "enabled": True, "diagnosis": "fake_upstream",
        "config_fingerprint": fingerprint(
            params, ("dataset.only_upstream_reads_this",)),
    }

    node_fn = make_diagnosis_node("fake_downstream")
    out = node_fn(fresh_upstream_payload, params)
    assert out["marker"] == "downstream_computed"
    assert received["upstream_payload"] is fresh_upstream_payload


def test_disabled_downstream_diagnosis_skips_the_upstream_check(monkeypatch):
    """A disabled downstream returns its stub before reaching compute (and
    before this precondition), so a stale upstream must not raise here —
    the existing enabled-gate ordering is unchanged."""
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    _install_fake_upstream_and_downstream(monkeypatch)

    old_params = {"dataset": {"only_upstream_reads_this": 1},
                  "evaluation": {"diagnosis": {
                      "fake_downstream": {"enabled": False}}}}
    stale_upstream_payload = {
        "enabled": True, "diagnosis": "fake_upstream",
        "config_fingerprint": fingerprint(
            old_params, ("dataset.only_upstream_reads_this",)),
    }
    # Change the key after stamping the payload, so it really is stale
    # relative to `old_params` as passed to the disabled downstream node.
    old_params["dataset"]["only_upstream_reads_this"] = 2

    node_fn = make_diagnosis_node("fake_downstream")
    out = node_fn(stale_upstream_payload, old_params)
    assert out == {
        "enabled": False, "diagnosis": "fake_downstream",
        "config_fingerprint": fingerprint(old_params),
    }


def test_each_diagnosis_node_gets_a_distinct_name():
    """``Node.name`` 預設取 ``func.__name__``（core/node.py:8）。

    工廠不設 ``__name__`` 的話五個 node 全叫 ``_run``：``--only-node`` 指不到
    任何一個，log 也分不出誰是誰。而 pipeline 照樣跑得完——這是靜默的。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.pipelines.evaluation.nodes import make_diagnosis_node

    names = [make_diagnosis_node(n).__name__ for n in DIAGNOSES]
    assert names == [f"diagnose_{n}" for n in DIAGNOSES]
    assert len(set(names)) == len(names)
