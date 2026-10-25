"""Tests for evaluation pipeline Spark nodes."""

import logging
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
            "config_fingerprint": fingerprint(parameters, "prepare_eval_data")}


def _row_facts(frame, parameters, segment_columns=None):
    """What ``compute_row_facts`` lands for ``frame``, for the nodes reading
    it: their tests pass the facts of the rows they compute on, as a run
    does."""
    from recsys_tfb.pipelines.evaluation.nodes import compute_row_facts

    return compute_row_facts(
        frame, segment_columns or _no_segments(parameters), parameters)


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
    frame, _segments, _categories = make_prepare_eval_data_node("inference_population")(
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
        frame, segments, _categories = make_prepare_eval_data_node("inference_population")(
            predictions, labels, population, params)

        assert frame.count() == 4
        assert self._segments_by_customer(frame) == {
            "c1": "from_population", "c2": None}
        assert segments == {
            "joined": ["cust_segment_typ"],
            "sources": {"cust_segment_typ": "inference_population"},
            "missing": {},
            "config_fingerprint": fingerprint(params, "prepare_eval_data"),
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
        frame, segments, _categories = make_prepare_eval_data_node("sample_pool")(
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

        frame, segments, _categories = make_prepare_eval_data_node("inference_population")(
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
            frame, segments, _categories = make_prepare_eval_data_node(
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
        rows, segments, _categories = make_prepare_eval_data_node(
            "inference_population")(
            predictions, labels, cls._population(spark, months, with_tier),
            cls._params(snap_date))
        return rows, segments

    def test_a_month_rewritten_without_a_joined_column_is_refused(self, spark):
        """January–February joined ``tier``; the population then lost it and
        February was re-run alone (``tier`` skipped, NULL in the partition).
        Resuming the range: its JSON still says joined=[tier] and the
        settings match, so without the joined list in the fingerprint all of
        February would land in the unmatched segment, exit 0."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_metrics,
            compute_row_facts,
        )

        range_rows, range_segments = self._run(
            spark, self.MONTHS, self.MONTHS, with_tier=True)
        february_rows, february_segments = self._run(
            spark, "2025-02-28", ["2025-02-28"], with_tier=False)
        assert february_segments["joined"] == []
        table = range_rows.filter("snap_date = '2025-01-31'").unionByName(
            february_rows, allowMissingColumns=True)
        params = self._params(self.MONTHS)
        # Facts counted on the range run's own (clean) rows, as landed.
        facts = _row_facts(range_rows, params, range_segments)

        for read in (
            lambda: compute_row_facts(table, range_segments, params),
            lambda: compute_metrics(table, facts, range_segments, params),
        ):
            with pytest.raises(ValueError) as excinfo:
                read()
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
        params = self._params(self.MONTHS)
        facts = _row_facts(table, params, range_segments)
        assert facts["dataset_overview"]["totals"]["n_snap_dates"] == 2
        # The metrics reader reads the same table without refusing it.
        assert compute_metrics(table, facts, range_segments, params)


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
                "report": {"sections": {"popularity_baseline": baseline_section}},
            },
        }

    @classmethod
    def _eval_predictions(cls, spark, parameters=None):
        """The partition as ``prepare_eval_data`` under ``parameters``
        (default :meth:`_parameters`) writes it, settings fingerprint
        included."""
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
        })), parameters or cls._parameters(), [])

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

        frame = self._eval_predictions(spark)
        result = compute_baseline_metrics(
            frame,
            _row_facts(frame, self._parameters()),
            self._label_table(spark),
            _no_segments(self._parameters()),
            self._parameters(),
        )
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

        # The model's whole bundle (ADR-0031 decision 8) plus the baseline's own
        # counts; no category (no mapping configured), no dataset_overview.
        assert set(result.keys()) == {
            "overall", "per_item", "macro_avg", "per_segment",
            "per_item_segment", "per_item_shared_numbers",
            "per_item_segment_shared_numbers", "population",
            "observation_items", "purchase_counts", "monthly_counts",
            "popularity_window", "config_fingerprint",
        }
        assert result["config_fingerprint"]["applied"] == fingerprint(
            self._parameters(), "compute_baseline_metrics")["applied"]
        assert set(result["config_fingerprint"]["inputs"]) == {
            "prepare_eval_data", "compute_row_facts"}
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

    def test_the_baseline_has_the_macro_the_model_has(self, spark):
        """ADR-0031 decision 8: the baseline goes through the model's metrics,
        macro included. Popularity A=3 > B=1 > C=0 ranks A, B, C in both
        groups: c1's positives A and C sit 1st and 3rd, c2's B 2nd. Per-item
        recall@1 is A 1, B 0, C 0; recall@2 is A 1, B 1, C 0."""
        from recsys_tfb.evaluation.result_readers import POPULATION_KEY
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters()
        frame = self._eval_predictions(spark)
        result = compute_baseline_metrics(
            frame, _row_facts(frame, params), self._label_table(spark),
            _no_segments(params), params)
        macro = (result.get("macro_avg") or {}).get("by_item") or {}
        assert macro.get("recall@1") == pytest.approx(1 / 3)
        assert macro.get("recall@2") == pytest.approx(2 / 3)
        assert result[POPULATION_KEY]["macro_items"] == ["A", "B", "C"]
        # The facts of these rows are the model's (decision 8).
        assert "dataset_overview" not in result

    def test_one_date_lands_its_window_as_the_report_prints_it(self, spark):
        """#536: the report reads the window instead of re-counting months
        and re-reading the settings. All history sits in 2024-06, so the
        2025-01-31 window has label rows in 1 of its 12 months: partial, and
        the per-month average divides by 1."""
        from recsys_tfb.evaluation.result_readers import POPULARITY_WINDOW_KEY
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters()
        frame = self._eval_predictions(spark)
        result = compute_baseline_metrics(
            frame, _row_facts(frame, params), self._label_table(spark),
            _no_segments(params), params)
        assert result[POPULARITY_WINDOW_KEY] == {
            "lookback_months": 12, "score": "count", "months_covered": 1,
            "window_months_covered": {"2025-01-31": 1},
            "full_window_months": 12, "per_month_divisor": 1,
            "window_partial": True,
        }

    def test_several_dates_record_each_windows_covered_months(self, spark):
        """#374: two evaluated dates, two lookback windows. History at
        2024-06-30 falls in both; history at 2025-01-31 only in the February
        window (the January window ends before its own date). Counts sum over
        the windows; each window's months with label rows are recorded, and
        the report divides by window-months, 1 + 2, not by one window's 12
        (#536: landed in the window block, the divisor too)."""
        import pandas as pd

        from recsys_tfb.evaluation.result_readers import POPULARITY_WINDOW_KEY
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
        frame = TestEnrichedReadersKeepTheEvaluatedMonth._two_months(spark)
        result = compute_baseline_metrics(
            frame, _row_facts(frame, params), labels,
            _no_segments(params), params)
        assert result[POPULARITY_WINDOW_KEY] == {
            "lookback_months": 12, "score": "count", "months_covered": 2,
            "window_months_covered": {"2025-01-31": 1, "2025-02-28": 2},
            "full_window_months": 24, "per_month_divisor": 3,
            "window_partial": True,
        }
        assert "window_months_covered" not in result   # only in the block
        # A: 3 in the January window, 3 + 1 in the February one.
        assert result["purchase_counts"] == {"A": 7, "B": 2, "C": 0}
        assert result["monthly_counts"] == {
            "A": {"2024-06": 6, "2025-01": 1},
            "B": {"2024-06": 2, "2025-01": 0},
            "C": {"2024-06": 0, "2025-01": 0},
        }

class TestCompareToBaseline:
    """The comparison layer (#536, ADR-0031 decisions 4 and 5) and its first
    node, ``compare_to_baseline``: two results as the metrics nodes compute
    them, joined cell by cell. The rows are ``TestComputeBaselineMetrics``':
    the model ranks c1 A, B, C (positives A and C) and c2 B, C, A (positive
    B); popularity A 3 > B 1 > C 0 ranks A, B, C in both."""

    @staticmethod
    def _parameters(**evaluation):
        params = TestComputeBaselineMetrics._parameters()
        params["evaluation"].update(evaluation)
        return params

    @staticmethod
    def _metrics(spark, frame, params):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        return compute_metrics(
            frame, _row_facts(frame, params), _no_segments(params), params)

    @classmethod
    def _model_and_baseline(cls, spark, params):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        frame = TestComputeBaselineMetrics._eval_predictions(spark, params)
        baseline = compute_baseline_metrics(
            frame, _row_facts(frame, params),
            TestComputeBaselineMetrics._label_table(spark),
            _no_segments(params), params)
        return cls._metrics(spark, frame, params), baseline

    def test_each_cell_is_the_model_the_baseline_and_their_difference(
            self, spark):
        """map@1: the model's c1 AP@1 is 1/2 (two positives, the first at 1)
        and c2's 1, so 0.75; the baseline's 1/2 and 0, so 0.25."""
        from recsys_tfb.pipelines.evaluation.nodes import compare_to_baseline
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters()
        metrics, baseline = self._model_and_baseline(spark, params)
        out = compare_to_baseline(metrics, baseline, params)
        cell = out["overall"]["cells"]["map@1"]
        assert cell["this"] == metrics["overall"]["map@1"] == pytest.approx(0.75)
        assert cell["reference"] == baseline["overall"]["map@1"] \
            == pytest.approx(0.25)
        assert cell["diff"] == pytest.approx(0.5)
        # Per item and in the macro too: per-item recall@1 of A, B, C is
        # 1, 1, 0 for the model and 1, 0, 0 for the baseline.
        assert out["per_item"]["cells"]["B"]["recall@1"] == {
            "this": 1.0, "reference": 0.0, "diff": 1.0}
        macro = out["macro_by_item"]["cells"]["recall@1"]
        assert macro["diff"] == pytest.approx(1 / 3)
        # It applies no setting; it records what both of its inputs did.
        assert set(out["config_fingerprint"]["inputs"]) >= {
            "compute_metrics", "compute_baseline_metrics"}

    def test_the_all_cells_pair_each_side_s_own_all(self, spark):
        """Two results whose ``"all"`` resolved to different K (3 items
        against 2): ``map@all`` pairs the model's ``map@3`` with the other's
        ``map@2``, and ``map@3``, which only one side holds, has no
        difference. The comparison layer as ``--compare`` will call it, on
        two results the metrics node computed. Without C, both groups rank
        their one positive first (AP 1); with it, c1's second positive sits
        3rd: (1 + 2/3) / 2."""
        from pyspark.sql import functions as F

        from recsys_tfb.pipelines.evaluation.steps.comparison_layer import (
            compare_metric_results,
        )

        params = self._parameters(k_values=[1, "all"])
        frame = TestComputeBaselineMetrics._eval_predictions(spark)
        three = self._metrics(spark, frame, params)
        two = self._metrics(
            spark, frame.filter(F.col("prod_name") != "C"), params)
        out = compare_metric_results(three, two)
        assert out["population"]["all_k"] == {
            "this": 3, "reference": 2, "same": False}
        cell = out["overall"]["cells"]["map@all"]
        assert cell["this"] == three["overall"]["map@3"] \
            == pytest.approx((1 + (1 + 2 / 3) / 2) / 2)
        assert cell["reference"] == two["overall"]["map@2"] == 1.0
        assert cell["diff"] == pytest.approx(cell["this"] - 1.0)
        assert out["overall"]["cells"]["map@3"] == {
            "this": three["overall"]["map@3"], "reference": None,
            "diff": None}
        # C has rows on one side only: its cells have no other side, and
        # the per-item block says how many items each side held.
        assert out["per_item"]["cells"]["C"]["recall@1"] == {
            "this": three["per_item"]["C"]["recall@1"], "reference": None,
            "diff": None}
        assert out["per_item"]["entries"] == {"this": 3, "reference": 2}

    def test_inputs_computed_under_other_settings_are_refused(self, spark):
        """Unchecked, ``--only-node compare_to_baseline`` after a setting
        changed compares the old metrics and stamps today's fingerprint on
        the result, which the report then accepts."""
        from recsys_tfb.pipelines.evaluation.nodes import compare_to_baseline

        computed_with = self._parameters()
        metrics, baseline = self._model_and_baseline(spark, computed_with)
        now = self._parameters(metric={"min_positives": 2})
        with pytest.raises(ValueError) as exc:
            compare_to_baseline(metrics, baseline, now)
        assert "evaluation.metric.min_positives" in str(exc.value)
        # The first step applying evaluation.metric.
        assert "--from-node compute_metrics," in str(exc.value)

    def test_a_ledger_landed_before_its_segment_columns_is_refused(
            self, spark):
        """A ``metrics.json`` written before #536 has no
        ``population.segment_columns``; the fingerprint passes it (no
        setting changed)."""
        from recsys_tfb.pipelines.evaluation.nodes import compare_to_baseline

        params = self._parameters()
        metrics, baseline = self._model_and_baseline(spark, params)
        del metrics["population"]["segment_columns"]
        with pytest.raises(ValueError) as exc:
            compare_to_baseline(metrics, baseline, params)
        assert "evaluation_metrics" in str(exc.value)
        assert "population.segment_columns" in str(exc.value)
        # Not compute_metrics: the row facts of a run that old lack the
        # all-positive accounts too, and the report would refuse them next.
        assert "--from-node compute_row_facts" in str(exc.value)


from recsys_tfb.pipelines.evaluation.steps.display_ks import (  # noqa: E402
    comparison_cut,
    resolve_display_k,
)


class TestResolveDisplayKClampsToItemCount:
    """bug 8 (ADR-0020): the display K list used to be an unclamped
    superset — with only 3 categories, the report still printed @4/@5,
    whose precision denominator is just K (not min(K, n_items)), so those
    columns decline purely because the denominator grew, not because the
    model did anything different. Fix is a filter over the given list
    (never regenerates it — a caller list without "all" doesn't gain one),
    keeping "all" always. The cut moved from the report to the K-cut nodes'
    module (``steps/display_ks``, #536)."""

    def test_drops_k_above_n_items_keeps_all(self):
        assert resolve_display_k([1, 3, 5, "all"], 3) == [1, 3, "all"]

    def test_keeps_everything_when_n_items_covers_it(self):
        assert resolve_display_k([1, 3, 5, "all"], 5) == [1, 3, 5, "all"]

    def test_does_not_add_all_when_caller_list_lacks_it(self):
        assert resolve_display_k([1, 3, 5], 3) == [1, 3]

    def test_skips_the_filter_when_n_items_is_zero_or_unknown(self):
        """n_items<=0 means overview data is missing (e.g. baseline's slim
        per_item bundle) — filtering would collapse every table down to
        just "@all"."""
        assert resolve_display_k([1, 3, 5, "all"], 0) == [1, 3, 5, "all"]


class TestComparisonCut:
    """The comparison report's K columns at one grain (#537): ``compare_k``
    cut at this side's "all" by the bug 8 rule, ``"all"`` written as the K
    it resolved to (a header writes the real K, ADR-0031 decision 16 item
    7), ascending, each K once — so no table has two @K columns for one K."""

    def test_all_becomes_its_k_and_k_above_it_is_cut(self):
        assert comparison_cut(3, [1, 3, 5, "all"]) == {
            "all_k": 3, "compare_k": [1, 3]}

    def test_an_int_equal_to_all_is_one_column(self):
        assert comparison_cut(5, [1, 3, 5, "all"])["compare_k"] == [1, 3, 5]

    def test_ascending_whatever_the_configured_order(self):
        # 8 first: a set of {8, 3, 1} iterates as 8, 1, 3.
        assert comparison_cut(8, ["all", 3, 1])["compare_k"] == [1, 3, 8]

    def test_a_list_without_all_does_not_gain_it(self):
        assert comparison_cut(7, [1, 3])["compare_k"] == [1, 3]


class TestResolveDisplayKs:
    """resolve_display_ks: the K each table of the main report shows, cut
    after the comparison (#536, ADR-0031 decisions 10 and 16), from what the
    metrics and comparison nodes compute. Three items, so ``"all"`` is 3 at
    the item grain; items A and B share category AB, so 2 at the category
    grain."""

    @staticmethod
    def _parameters(compare_k, k_values=None):
        """``compare_k=None`` leaves it unwritten; ``k_values=None`` keeps
        ``TestComputeBaselineMetrics``' ``[1, 2, 3]``."""
        params = TestCompareToBaseline._parameters(
            item_categories={"enabled": True, "unmapped": "singleton",
                             "mapping": {"AB": ["A", "B"]}})
        params["schema"]["categorical_values"] = {"prod_name": ["A", "B", "C"]}
        if compare_k is not None:
            params["evaluation"]["report"]["display"] = {"compare_k": compare_k}
        if k_values is not None:
            params["evaluation"]["k_values"] = k_values
        return params

    def _resolved(self, spark, params):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compare_to_baseline,
            resolve_display_ks,
        )

        metrics, baseline = TestCompareToBaseline._model_and_baseline(
            spark, params)
        return resolve_display_ks(
            metrics, params,
            baseline_comparison=compare_to_baseline(metrics, baseline, params))

    def test_each_list_drops_the_k_above_what_all_stands_for(self, spark):
        """bug 8 (ADR-0020): an int K above ``"all"`` is dropped, ``"all"``
        kept."""
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters([1, 3, 5, "all"])
        out = self._resolved(spark, params)
        # The metrics section's K: k_values [1, 2, 3] resolved ∪ "all",
        # as integers.
        assert out.get("evaluation_metrics") == {
            "all_k": 3,
            "metrics_section": [1, 2, 3],
            "category": {"all_k": 2, "metrics_section": [1, 2]}}
        # The baseline section's tables, every one at compare_k, cut at the
        # comparison's K (the model's, which its baseline shares) and at the
        # category grain's: "all" resolves to 3 and to 2, 5 is dropped.
        assert out.get("baseline_comparison") == {
            "all_k": 3, "compare_k": [1, 3],
            "category": {"all_k": 2, "compare_k": [1, 2]}}
        # Its fingerprint holds the display key it read, so a changed
        # compare_k makes it — and only it — stale.
        assert "evaluation.report.display.compare_k" in \
            out["config_fingerprint"]["applied"]

    def test_the_metrics_section_follows_k_values(self, spark):
        """ADR-0031 decision 16 / #537: the metrics section shows the K the
        run computed — ``k_values`` resolved ∪ "all", above "all" cut — not
        a fixed 1..5. ``[1, "all"]`` computes @1 and @3 (three items), @1
        and @2 at the category grain."""
        out = self._resolved(spark, self._parameters([1, "all"],
                                                     k_values=[1, "all"]))
        assert out["evaluation_metrics"]["metrics_section"] == [1, 3]
        assert out["evaluation_metrics"]["category"]["metrics_section"] == [
            1, 2]

    def test_an_unwritten_compare_k_is_k_values(self, spark):
        """``compare_k`` not written: the K the run computed
        (``evaluation.k_values``), so no table shows a K nothing computed —
        the old ``[1, 3, 5, "all"]`` asked for @1 and @3 that the default
        ``k_values`` (``[5, "all"]``) never computes."""
        out = self._resolved(spark, self._parameters(None,
                                                     k_values=[2, "all"]))
        assert out["baseline_comparison"]["compare_k"] == [2, 3]
        assert out["baseline_comparison"]["category"]["compare_k"] == [2]

    def test_configured_compare_k_falls_back_to_k_values_then_its_default(
            self):
        from recsys_tfb.core.consistency import EVALUATION_K_VALUES_DEFAULT
        from recsys_tfb.pipelines.evaluation.steps.display_ks import (
            configured_compare_k,
        )

        written = {"evaluation": {"k_values": [1, 2],
                                  "report": {"display": {"compare_k": [1]}}}}
        assert configured_compare_k(written) == [1]
        assert configured_compare_k(
            {"evaluation": {"k_values": [1, 3, "all"]}}) == [1, 3, "all"]
        assert configured_compare_k({"evaluation": {}}) == list(
            EVALUATION_K_VALUES_DEFAULT)

    def test_without_the_comparison_the_baseline_has_no_lists(self, spark):
        """The popularity-baseline section off, the pipeline wires no
        comparison (#538): the cut still lands the metrics section's K."""
        from recsys_tfb.pipelines.evaluation.nodes import resolve_display_ks

        params = TestComputeBaselineMetrics._parameters(baseline_section=False)
        frame = TestComputeBaselineMetrics._eval_predictions(spark)
        metrics = TestCompareToBaseline._metrics(spark, frame, params)
        out = resolve_display_ks(metrics, params)
        assert out.get("baseline_comparison", "absent") is None
        # The ranking-metrics section's K still land: k_values [1, 2, 3] ∪ all,
        # cut at 3.
        assert out["evaluation_metrics"]["metrics_section"] == [1, 2, 3]

    def test_a_comparison_computed_under_other_settings_is_refused(
            self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compare_to_baseline,
            resolve_display_ks,
        )

        computed_with = self._parameters([1])
        metrics, baseline = TestCompareToBaseline._model_and_baseline(
            spark, computed_with)
        comparison = compare_to_baseline(metrics, baseline, computed_with)
        now = self._parameters([1])
        now["evaluation"]["metric"] = {"min_positives": 1}
        with pytest.raises(ValueError) as exc:
            resolve_display_ks(metrics, now, baseline_comparison=comparison)
        assert "baseline_comparison (written by compare_to_baseline)" in str(
            exc.value)
        # It applies nothing itself: what moved is what its inputs applied.
        assert "settings its input from compute_metrics" in str(exc.value)
        assert "--from-node compute_metrics," in str(exc.value)


class TestComputeRowFacts:
    """compute_row_facts: what the rows hold whatever their scores
    (ADR-0031 decision 4), landed once for the model, its baseline and the
    report to read by name."""

    @staticmethod
    def _parameters(drop_all_positive=False):
        params = TestComputeBaselineMetrics._parameters()
        params["evaluation"]["query_filter"] = {
            "drop_all_positive_groups": drop_all_positive}
        return params

    @classmethod
    def _frame(cls, spark, params=None):
        """Three query groups: c1 holds a positive, c2 none, c3 is
        all-positive. B is positive in c3 only."""
        import pandas as pd

        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        return stamp_partition_fingerprint(spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 9,
            "cust_id": ["c1"] * 3 + ["c2"] * 3 + ["c3"] * 3,
            "prod_name": ["A", "B", "C"] * 3,
            "label": [1, 0, 1, 0, 0, 0, 1, 1, 1],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3, 0.4, 0.6, 0.7],
            "rank": [1, 2, 3, 3, 1, 2, 3, 2, 1],
        })), params or cls._parameters(), [])

    def test_the_metrics_record_what_they_were_computed_on(self, spark):
        """The population ledger (ADR-0031 decision 4). Dropping the
        all-positive c3 leaves c1 the one group scored, so B — positive in c3
        only — is in no macro, while ``"all"`` still resolves on every row."""
        from recsys_tfb.evaluation.result_readers import POPULATION_KEY
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_metrics,
            compute_row_facts,
        )

        params = self._parameters(drop_all_positive=True)
        frame = self._frame(spark, params)
        facts = compute_row_facts(frame, _no_segments(params), params)
        metrics = compute_metrics(frame, facts, _no_segments(params), params)
        assert metrics[POPULATION_KEY] == {
            "all_k": 3,
            "macro_items": ["A", "C"],
            "n_scored_queries_with_positive": 1,
            "n_scored_queries_without_positive": 0,
            # The segment columns it sliced by (#536): what the comparison
            # layer reads to tell a side computed without them.
            "segment_columns": [],
        }
        # The facts are the row facts' alone.
        assert not {"dataset_overview", "n_queries", "segments"} & set(metrics)

    def test_the_metrics_refuse_facts_counted_under_other_settings(
            self, spark):
        """A slice re-running compute_metrics alone reads the landed facts."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_metrics,
            compute_row_facts,
        )

        old = self._parameters()
        new = self._parameters(drop_all_positive=True)
        facts = compute_row_facts(
            self._frame(spark, old), _no_segments(old), old)
        with pytest.raises(Exception, match=r"--from-node compute_row_facts"):
            compute_metrics(self._frame(spark, new), facts,
                            _no_segments(new), new)

    def test_the_macro_items_leave_out_items_below_min_positives(self, spark):
        """The reports count ``population.macro_items`` instead of applying
        ``evaluation.metric.min_positives`` again (#536), so the rule lives
        here alone. Switch off, c1 and c3 are scored: A and C hold two
        positives each, B one (c3). min_positives 2 keeps A and C; B is still
        a per_item cell, drawn but not averaged."""
        from recsys_tfb.evaluation.result_readers import (
            MACRO_ITEMS_KEY,
            POPULATION_KEY,
        )
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_metrics,
            compute_row_facts,
        )

        params = self._parameters()
        params["evaluation"]["metric"] = {"min_positives": 2}
        frame = self._frame(spark, params)
        facts = compute_row_facts(frame, _no_segments(params), params)
        metrics = compute_metrics(frame, facts, _no_segments(params), params)
        assert metrics[POPULATION_KEY][MACRO_ITEMS_KEY] == ["A", "C"]
        assert set(metrics["per_item"]) == {"A", "B", "C"}

    def test_counts_the_rows_and_their_query_groups(self, spark):
        from recsys_tfb.evaluation.result_readers import ALL_POSITIVE_KEY
        from recsys_tfb.pipelines.evaluation.nodes import compute_row_facts
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters()
        facts = compute_row_facts(
            self._frame(spark), _no_segments(params), params)
        totals = facts["dataset_overview"]["totals"]
        assert (totals["n_rows"], totals["n_entities"], totals["n_items"],
                totals["n_positives"]) == (9, 3, 3, 5)
        assert (facts["n_queries"], facts["n_excluded_queries"],
                facts[ALL_POSITIVE_KEY]) == (3, 1, 1)
        assert facts["segments"] == {"joined": [], "sources": {}, "missing": {}}
        assert facts["config_fingerprint"]["applied"] == fingerprint(
            params, "compute_row_facts")["applied"]
        assert set(facts["config_fingerprint"]["inputs"]) == {
            "prepare_eval_data"}


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
        assert result["config_fingerprint"]["applied"] == fingerprint(
            params, "compute_prediction_quality")["applied"]
        assert set(result["config_fingerprint"]["inputs"]) == {
            "prepare_eval_data"}

    def test_lands_the_display_bins_and_the_sweep_the_report_draws(
            self, spark):
        """#536: the report draws these instead of re-deriving them from the
        fine bins. Checkable by hand whatever bin each score falls in: the 4
        display bins hold all 9 rows and 3 positives, each listed item's its
        own 3 rows and 1 positive; the lowest display edge is the data's
        minimum (0.1), so it predicts every row positive — precision 3/9,
        recall 1 — and so does the sweep's last (lowest) threshold. Read back
        through JSON, as the report reads it."""
        import json

        from recsys_tfb.evaluation.result_readers import (
            DISPLAY_BINS_KEY,
            THRESHOLD_SWEEP_KEY,
            frame_from_json,
        )
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._parameters(n_bins=8, n_display_bins=4, top_n=3)
        result = json.loads(json.dumps(compute_prediction_quality(
            self._with_a_query_group_without_a_positive(spark),
            _no_segments(params), params)))
        overall = frame_from_json(result["overall"][DISPLAY_BINS_KEY])
        assert len(overall) == 4
        assert (overall["n"].sum(), overall["n_pos"].sum()) == (9, 3)
        assert overall.loc[0, "score_from"] == pytest.approx(0.1)
        assert overall.loc[0, "precision"] == pytest.approx(3 / 9)
        assert overall.loc[0, "recall"] == pytest.approx(1.0)
        items = frame_from_json(result["per_item"][DISPLAY_BINS_KEY])
        assert items.columns[0] == "prod_name" and len(items) == 3 * 4
        by_item = items.groupby("prod_name")[["n", "n_pos"]].sum()
        assert by_item.to_dict("index") == {
            item: {"n": 3, "n_pos": 1} for item in ("A", "B", "C")}
        sweep = frame_from_json(result["overall"][THRESHOLD_SWEEP_KEY])
        assert sweep["threshold"].is_monotonic_decreasing
        lowest = sweep.iloc[-1]
        assert lowest["threshold"] == pytest.approx(0.1)
        assert (lowest["tp"], lowest["fp"]) == (3, 6)
        assert lowest["recall"] == pytest.approx(1.0)

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

    def test_a_null_weight_under_a_positive_ratio_is_refused(self, spark):
        """The conf says test kept zero-positive groups, the predictions
        say otherwise — they were written by a model whose test ratio was 0
        (NULL weights), so conf and --model-version disagree. Summing would
        drop those rows; counting them would call a filtered table weighted."""
        from recsys_tfb.core.consistency import DataConsistencyError
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._post_training(test_zero_positive_group_ratio=0.5)
        with pytest.raises(DataConsistencyError, match="were not written under"):
            compute_prediction_quality(
                self._weighted(spark, params, None), _no_segments(params), params)

    def test_a_missing_weight_column_under_a_positive_ratio_is_refused(self, spark):
        from recsys_tfb.core.consistency import (
            ZERO_POSITIVE_GROUP_WEIGHT_COL,
            DataConsistencyError,
        )
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_prediction_quality,
        )

        params = self._post_training(test_zero_positive_group_ratio=0.5)
        frame = self._weighted(spark, params, 2.0).drop(ZERO_POSITIVE_GROUP_WEIGHT_COL)
        with pytest.raises(DataConsistencyError, match="were not written under"):
            compute_prediction_quality(frame, _no_segments(params), params)

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
            "config_fingerprint": fingerprint(
                cls._parameters(), "prepare_eval_data"),
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

        frame = self._frame(spark)
        facts = _row_facts(frame, self._parameters(), self._segments())
        result = compute_metrics(
            frame, facts, self._segments(), self._parameters())
        assert set(result["per_segment"]) == {"mass", "hnw"}
        # The ledger records the list it sliced by, the landed one (#536).
        assert result["population"]["segment_columns"] == ["cust_segment_typ"]
        assert set(facts["dataset_overview"]["by_segment"]) == {"mass", "hnw"}
        # What the report says about segments travels with the row facts.
        assert facts["segments"] == {
            k: self._segments()[k] for k in ("joined", "sources", "missing")}
        # They land as JSON, so they record the settings they applied
        # and generate_report can refuse a stale one
        # (ADR-0018 decision 2, ADR-0020 decision 2); so do the metrics.
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint

        assert facts["config_fingerprint"]["applied"] == fingerprint(
            self._parameters(), "compute_row_facts")["applied"]
        assert result["config_fingerprint"]["applied"] == fingerprint(
            self._parameters(), "compute_metrics")["applied"]
        assert set(result["config_fingerprint"]["inputs"]) == {
            "prepare_eval_data", "compute_row_facts"}

    def test_compute_baseline_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        frame = self._frame(spark)
        result = compute_baseline_metrics(
            frame, _row_facts(frame, self._parameters(), self._segments()),
            TestComputeBaselineMetrics._label_table(spark),
            self._segments(), self._parameters())
        assert set(result["per_segment"]) == {"mass", "hnw"}

    @classmethod
    def _compared_frame(cls, spark):
        """The compared side as ``restrict_to_common`` hands it: c2's query
        group only (a subset of this run's side's), and its own
        ``cust_segment_typ`` holding a value this run's side does not — what
        a prediction table landed under other settings could carry. A side
        segmenting by its own column would show ``b_own``."""
        from pyspark.sql import functions as F

        return (cls._frame(spark)
                .filter(F.col("cust_id") == "c2")
                .withColumn("cust_segment_typ", F.lit("b_own"))
                .drop("stale_seg", "eval_partition_fingerprint"))

    def test_compute_compare_metrics(self, spark):
        """Both sides segment by the landed list, and the compared side takes
        this run's side's values, joined on the query group (#537): a
        segment column is the entity's attribute, whoever scored it. c2 is
        ``hnw`` on this run's side. So the two ledgers name the same segment
        columns, and the comparison's annotation says the same."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_compare_metrics
        from recsys_tfb.pipelines.evaluation.steps.comparison_layer import (
            compare_metric_results,
        )

        frame = self._frame(spark)
        facts_a, metrics_a, facts_b, metrics_b = compute_compare_metrics(
            frame, self._compared_frame(spark),
            self._segments(), self._parameters())
        assert set(metrics_a["per_segment"]) == {"mass", "hnw"}
        assert metrics_a["population"]["segment_columns"] == [
            "cust_segment_typ"]
        assert set(facts_a["dataset_overview"]["by_segment"]) == {"mass", "hnw"}
        assert set(metrics_b["per_segment"]) == {"hnw"}
        assert set(facts_b["dataset_overview"]["by_segment"]) == {"hnw"}
        assert metrics_b["population"]["segment_columns"] == [
            "cust_segment_typ"]
        population = compare_metric_results(metrics_a, metrics_b)["population"]
        assert population["segment_columns"]["same"] is True

    def test_the_pipeline_names_each_output_after_the_side_it_holds(
            self, spark):
        """The node returns four results by position and the pipeline names
        them (``pipeline._comparison_nodes``): swap two names there and
        nothing raises — A's metrics would land as the compared source's, and
        the report would draw each side under the other's label. Read through
        the node's own ``outputs``: side A holds both customers' segments,
        side B c2's only (:meth:`_compared_frame`)."""
        from recsys_tfb.pipelines.evaluation import create_pipeline

        node = next(
            n for n in create_pipeline(parameters={}, compare_source={
                "kind": "model_version", "model_version": "v0"}).nodes
            if n.name == "compute_compare_metrics")
        frame = self._frame(spark)
        landed = dict(zip(node.outputs, node.func(
            frame, self._compared_frame(spark),
            self._segments(), self._parameters())))

        assert sorted(landed) == ["compare_metrics_a", "compare_metrics_b",
                                  "compare_row_facts_a", "compare_row_facts_b"]
        by_segment = {name: set(landed[name]["dataset_overview"].get(
            "by_segment") or {}) for name in ("compare_row_facts_a",
                                              "compare_row_facts_b")}
        assert by_segment == {"compare_row_facts_a": {"mass", "hnw"},
                              "compare_row_facts_b": {"hnw"}}
        per_segment = {name: set(landed[name]["per_segment"])
                       for name in ("compare_metrics_a", "compare_metrics_b")}
        assert per_segment == {"compare_metrics_a": {"mass", "hnw"},
                               "compare_metrics_b": {"hnw"}}

    def test_compute_compare_metrics_lands_what_it_was_computed_with(
            self, spark):
        """All four results land (#536), each with the fingerprint and the
        compare source the readers check: ``compare_sources.
        compare_source_identity`` of today's parameters, ``label`` left
        out. The report tests stamp their hand-written results the same
        way, on this test's word."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_compare_metrics
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        params = self._parameters()
        params["evaluation"]["compare"] = {
            "kind": "model_version", "model_version": "v0", "label": "old"}
        frame = self._frame(spark)
        landed = compute_compare_metrics(
            frame, frame, self._segments(), params)
        assert len(landed) == 4
        for result in landed:
            assert result["config_fingerprint"]["applied"] == fingerprint(
                params, "compute_compare_metrics")["applied"]
            assert set(result["config_fingerprint"]["inputs"]) == {
                "prepare_eval_data"}
            assert result.get("compare_source") == {
                "kind": "model_version", "model_version": "v0",
                "source": "enriched_eval_predictions",
                "table": "enriched_eval_predictions"}


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
        two, one = self._two_months(spark), self._one_month(spark)
        both = compute_metrics(two, _row_facts(two, params),
                               _no_segments(params), params)
        alone = compute_metrics(one, _row_facts(one, params),
                                _no_segments(params), params)
        assert both == alone

    def test_compute_row_facts(self, spark):
        params = self._parameters()
        both = _row_facts(self._two_months(spark), params)
        alone = _row_facts(self._one_month(spark), params)
        assert both["dataset_overview"]["totals"]["n_snap_dates"] == 1
        assert both == alone

    def test_compute_baseline_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters()
        labels = TestComputeBaselineMetrics._label_table(spark)
        two, one = self._two_months(spark), self._one_month(spark)
        both = compute_baseline_metrics(
            two, _row_facts(two, params), labels, _no_segments(params), params)
        alone = compute_baseline_metrics(
            one, _row_facts(one, params), labels, _no_segments(params), params)
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

    def test_compute_row_facts_refuses_a_month_with_no_rows(self, spark):
        """Postcondition: the evaluated month's partition is empty or was never
        written, and reading the table raised nothing."""
        params = self._parameters(snap_date="2025-03-31")
        with pytest.raises(
            ValueError,
            match=r"compute_row_facts postcondition: 0 evaluated months",
        ):
            _row_facts(self._two_months(spark), params)

    def test_compute_row_facts_evaluates_every_configured_date_together(self, spark):
        """#374: two dates configured, both months' query groups go into one
        set of facts — 2 customers × 2 months = 4 query groups."""
        params = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        result = _row_facts(self._two_months(spark), params)
        overview = result["dataset_overview"]
        assert overview["totals"]["n_snap_dates"] == 2
        assert sorted(overview["by_snap_date"]) == ["2025-01-31", "2025-02-28"]
        assert result["n_queries"] + result["n_excluded_queries"] == 4

    def test_compute_row_facts_refuses_a_configured_date_with_no_rows(self, spark):
        """Two dates configured, the partition holds one of them: expected 2,
        evaluated 1."""
        params = self._parameters(snap_date=["2025-01-31", "2025-03-31"])
        with pytest.raises(
            ValueError,
            match=r"compute_row_facts postcondition: 1 evaluated months .*"
                  r"expected exactly 2",
        ):
            _row_facts(self._two_months(spark), params)

    # --- partitions rewritten by another run (#374) ------------------------

    _READERS = ("compute_row_facts", "compute_metrics",
                "compute_baseline_metrics",
                "compute_report_aggregates", "compute_prediction_quality")

    @classmethod
    def _read(cls, name, frame, spark, params):
        from recsys_tfb.pipelines.evaluation import nodes

        # The metrics readers take the facts landed from the clean table.
        if name == "compute_metrics":
            return nodes.compute_metrics(
                frame, _row_facts(cls._two_months(spark), params),
                _no_segments(params), params)
        if name == "compute_baseline_metrics":
            return nodes.compute_baseline_metrics(
                frame, _row_facts(cls._two_months(spark), params),
                TestComputeBaselineMetrics._label_table(spark),
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
        facts = _row_facts(self._two_months(spark), params)
        with pytest.raises(ValueError,
                           match=r"2025-02-28: written under other settings"):
            compute_metrics(table, facts, _no_segments(params), params)

    def test_a_partition_written_before_fingerprints_is_refused(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            PARTITION_FINGERPRINT_COLUMN,
        )

        params = self._parameters()
        table = self._two_months(spark).drop(PARTITION_FINGERPRINT_COLUMN)
        facts = _row_facts(self._two_months(spark), params)
        with pytest.raises(ValueError, match=r"2025-01-31: written before"):
            compute_metrics(table, facts, _no_segments(params), params)

    def test_a_range_resume_reads_a_month_a_same_settings_single_run_wrote(
        self, spark,
    ):
        """Same settings, other dates: not a rewrite. Stamped as February
        alone would stamp it, the range run reads February as its own."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(snap_date=["2025-01-31", "2025-02-28"])
        table = self._rewrite(self._two_months(spark), "2025-02-28",
                              self._parameters(snap_date="2025-02-28"))
        facts = _row_facts(table, params)
        assert facts["dataset_overview"]["totals"]["n_snap_dates"] == 2
        assert compute_metrics(table, facts, _no_segments(params), params)


class TestReadersRefuseAPartitionPreparedUnderOtherSettings:
    """The partition is read back from Hive, so a slice starting after
    ``prepare_eval_data`` does not re-join it. Its landed
    ``evaluation_segment_columns`` fingerprint says which settings it was
    joined under; the segmenting readers compare it on the settings
    ``prepare_eval_data`` applies before reading (ADR-0020 bug 6).
    """

    _READERS = ("compute_row_facts", "compute_metrics",
                "compute_baseline_metrics")

    @staticmethod
    def _call(name, frame, labels, segments, params, facts=None):
        """``facts``: the row facts the metrics readers take; the segment
        check comes first, so a test of it passes none."""
        from recsys_tfb.pipelines.evaluation import nodes

        if name == "compute_baseline_metrics":
            return nodes.compute_baseline_metrics(
                frame, facts, labels, segments, params)
        if name == "compute_metrics":
            return nodes.compute_metrics(frame, facts, segments, params)
        return nodes.compute_row_facts(frame, segments, params)

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
        frame = TestEnrichedReadersKeepTheEvaluatedMonth._one_month(spark)
        result = self._call(
            name,
            frame,
            TestComputeBaselineMetrics._label_table(spark),
            _no_segments(prepared_under), now,
            facts=_row_facts(frame, now, _no_segments(prepared_under)))
        assert result is not None


class TestAllPositiveQueryGroups:
    """#376: ``evaluation.query_filter.drop_all_positive_groups``.

    An all-positive query group has the same positive label on every row, so
    every per-query metric of it is the same whatever the order. The switch drops
    those groups from the measurement metrics, the popularity baseline and
    both sides of the comparison report — at each grain, decided on that
    grain's own frame. It does not reach the per-item breakdown's
    aggregates. With it off, the
    evaluation nodes warn when those groups are more than 10% of the groups
    holding a positive.

    ``ROWS`` makes the two grains disagree (fund_stock and fund_bond fold into
    the category fund):

        c1  fund_stock .9 1 | exchange_fx .2 1   all-positive at both grains
        c2  fund_stock .3 1 | fund_bond   .8 0   mixed; all-positive as one fund row
        c3  fund_bond  .9 0 | exchange_fx .1 1   mixed at both grains
        c4  fund_stock .5 0 | exchange_fx .4 0   no positive

    Each "drops" test compares the switched-on result on the whole frame with
    the switched-off result on the frame without that grain's all-positive
    groups.
    """

    ROWS = [
        ("c1", "fund_stock", 0.9, 1), ("c1", "exchange_fx", 0.2, 1),
        ("c2", "fund_stock", 0.3, 1), ("c2", "fund_bond", 0.8, 0),
        ("c3", "fund_bond", 0.9, 0), ("c3", "exchange_fx", 0.1, 1),
        ("c4", "fund_stock", 0.5, 0), ("c4", "exchange_fx", 0.4, 0),
    ]
    #: The compared side of the comparison report: another prediction table
    #: over the same groups, scored differently.
    COMPARED_ROWS = [
        ("c1", "fund_stock", 0.4, 1), ("c1", "exchange_fx", 0.6, 1),
        ("c2", "fund_stock", 0.6, 1), ("c2", "fund_bond", 0.1, 0),
        ("c3", "fund_bond", 0.8, 0), ("c3", "exchange_fx", 0.7, 1),
        ("c4", "fund_stock", 0.2, 0), ("c4", "exchange_fx", 0.5, 0),
    ]
    FINE = {"c1"}
    CATEGORY = {"c1", "c2"}
    KEY = "evaluation.query_filter.drop_all_positive_groups"
    NODES_LOGGER = "recsys_tfb.pipelines.evaluation.nodes"

    @staticmethod
    def _parameters(switch=None, *, items=("fund_stock", "fund_bond",
                                           "exchange_fx"),
                    mapping=None):
        params = {
            "schema": {
                "columns": {
                    "time": "snap_date", "entity": ["cust_id"],
                    "item": "prod_name", "label": "label", "score": "score",
                    "rank": "rank",
                },
                "categorical_values": {"prod_name": list(items)},
            },
            "evaluation": {
                "snap_date": "2025-01-31",
                "k_values": [1, "all"],
                "baseline": {"lookback_months": 12},
                "item_categories": {
                    "enabled": True, "unmapped": "singleton",
                    "mapping": (mapping if mapping is not None
                                else {"fund": ["fund_stock", "fund_bond"]}),
                },
            },
        }
        if switch is not None:
            params["evaluation"]["query_filter"] = {
                "drop_all_positive_groups": switch}
        return params

    @classmethod
    def _frame(cls, spark, parameters, without=(), rows=None):
        """The partition as ``prepare_eval_data`` under ``parameters`` writes
        it, less the query groups in ``without``; ``rank`` follows ``score``."""
        import pandas as pd

        from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
            stamp_partition_fingerprint,
        )

        kept = [r for r in (rows or cls.ROWS) if r[0] not in without]
        rank = {}
        for cust in {r[0] for r in kept}:
            ordered = sorted((r for r in kept if r[0] == cust),
                             key=lambda r: -r[2])
            for i, r in enumerate(ordered, start=1):
                rank[(r[0], r[1])] = i
        return stamp_partition_fingerprint(spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * len(kept),
            "cust_id": [r[0] for r in kept],
            "prod_name": [r[1] for r in kept],
            "score": [r[2] for r in kept],
            "label": [r[3] for r in kept],
            "rank": [rank[(r[0], r[1])] for r in kept],
        })), parameters, [])

    @staticmethod
    def _label_table(spark):
        """History for the popularity counts: fund_stock 3, fund_bond 2,
        exchange_fx 1."""
        import pandas as pd

        rows = [
            {"snap_date": "2024-06-30", "cust_id": f"h{i}", "prod_name": item,
             "label": 1}
            for item, n in (("fund_stock", 3), ("fund_bond", 2),
                            ("exchange_fx", 1))
            for i in range(n)
        ]
        return spark.createDataFrame(pd.DataFrame(rows))

    def _metrics(self, spark, switch, without=()):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = self._parameters(switch)
        frame = self._frame(spark, params, without)
        return compute_metrics(frame, _row_facts(frame, params),
                               _no_segments(params), params)

    def _baseline(self, spark, switch, without=()):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
        )

        params = self._parameters(switch)
        frame = self._frame(spark, params, without)
        return compute_baseline_metrics(
            frame, _row_facts(frame, params), self._label_table(spark),
            _no_segments(params), params)

    # ---- the measurement metrics -------------------------------------------

    def test_compute_metrics_drops_the_fine_grained_ones(self, spark):
        on = self._metrics(spark, True)
        without = self._metrics(spark, False, without=self.FINE)
        for key in ("overall", "per_item", "macro_avg"):
            assert on[key] == without[key], key
        assert on["overall"]["map@3"] == pytest.approx(0.5)

    def test_compute_metrics_drops_the_category_grain_ones(self, spark):
        on = self._metrics(spark, True)
        without = self._metrics(spark, False, without=self.CATEGORY)
        for key in ("overall", "per_item", "macro_avg"):
            assert on["category"][key] == without["category"][key], key
        assert on["category"]["overall"]["map@2"] == pytest.approx(0.5)

    # ---- the popularity baseline -------------------------------------------

    def test_compute_baseline_metrics_drops_the_fine_grained_ones(self, spark):
        """Popularity scores c1 1, c2 1, c3 1/2 at map@3: 5/6 off, 3/4 on."""
        on = self._baseline(spark, True)
        without = self._baseline(spark, False, without=self.FINE)
        assert on["overall"] == without["overall"]
        assert on["per_item"] == without["per_item"]
        assert on["overall"]["map@3"] == pytest.approx(0.75)

    def test_compute_baseline_metrics_drops_the_category_grain_ones(self, spark):
        on = self._baseline(spark, True)
        without = self._baseline(spark, False, without=self.CATEGORY)
        assert on["category"]["overall"] == without["category"]["overall"]
        assert on["category"]["per_item"] == without["category"]["per_item"]
        assert on["category"]["overall"]["map@2"] == pytest.approx(0.5)

    # ---- the comparison report ---------------------------------------------

    def test_compute_compare_metrics_drops_them_on_both_sides(self, spark):
        """The two sides must be read under the same rule, or the Δ compares
        different groups. Each side is checked against its own frame with the
        all-positive groups removed; the sides score differently (fine-grained
        map@3 without c1: 1/2 on the model side, 3/4 on the compared one)."""
        from recsys_tfb.evaluation.metrics_spark import (
            compute_all_metrics,
            compute_row_facts as count_row_facts,
        )
        from recsys_tfb.pipelines.evaluation.nodes import compute_compare_metrics

        on, off = self._parameters(True), self._parameters(False)
        _fa, metrics_a, _fb, metrics_b = compute_compare_metrics(
            self._frame(spark, on), self._frame(spark, on, rows=self.COMPARED_ROWS),
            _no_segments(on), on)
        seen = {"a": metrics_a, "b": metrics_b}

        for side, rows in (("a", self.ROWS), ("b", self.COMPARED_ROWS)):
            fine_frame = self._frame(spark, off, self.FINE, rows=rows)
            cat_frame = self._frame(spark, off, self.CATEGORY, rows=rows)
            fine = compute_all_metrics(
                fine_frame, off, count_row_facts(fine_frame, off))
            cat = compute_all_metrics(
                cat_frame, off, count_row_facts(cat_frame, off))
            assert seen[side]["overall"] == fine["overall"], side
            assert (seen[side]["category"]["overall"]
                    == cat["category"]["overall"]), side
        assert seen["a"]["overall"]["map@3"] == pytest.approx(0.5)
        assert seen["b"]["overall"]["map@3"] == pytest.approx(0.75)

    # ---- the per-item breakdown does not follow it ------------------------

    @staticmethod
    def _unfingerprinted(result):
        """The settings fingerprint names the switch (#376), so it differs
        between the two runs by design; everything else must not."""
        return {k: v for k, v in result.items() if k != "config_fingerprint"}

    def test_compute_report_aggregates_does_not_follow_it(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_report_aggregates,
        )

        def run(switch, without=()):
            params = self._parameters(switch)
            return self._unfingerprinted(compute_report_aggregates(
                self._frame(spark, params, without), _no_segments(params),
                params))

        assert run(True) == run(False)
        # The fixture can tell: without c1 the aggregates differ.
        assert run(False) != run(False, without=self.FINE)

    # ---- the report headline follows it -----------------------------------

    def test_the_report_s_macro_per_item_map_follows_it(self, spark):
        """The overview's macro per-item mAP (a model-selection metric, #537)
        is read from ``compute_metrics``'s result (``metrics.json``), so the
        switch reaches it like every other measurement metric: switched on,
        it equals the value of the frame without the all-positive groups."""
        from recsys_tfb.evaluation.report_sections import overview
        from recsys_tfb.report.types import Table

        def headline(switch, without=()):
            params = self._parameters(switch)
            metrics = self._metrics(spark, switch, without=without)
            s = overview.build({"evaluation_metrics": metrics}, params)
            (table,) = [b for b in s.blocks if isinstance(b, Table)]
            return {r.label: r.cells[0]
                    for r in table.rows}["macro per-item mAP"]

        on = headline(True)
        assert on == pytest.approx(headline(False, without=self.FINE))
        # The fixture can tell: c1's all-positive group moves the headline.
        assert on != pytest.approx(headline(False))

    def test_the_groups_scored_are_the_metrics_own_count(self, spark):
        """Switched on, the report prints how many groups the measurement
        metrics scored — in 核心概念's totals (#537). Read
        off the metrics' landed ledger since #536, it must be the number the
        report printed before, holding a positive less all-positive, here by
        hand: three groups hold a positive at either grain (all but c4); c1
        is all-positive at the fine grain, c1 and c2 at the category one."""
        from recsys_tfb.evaluation.report_sections import (
            core_concepts,
        )
        from recsys_tfb.pipelines.evaluation.nodes import (
            compare_to_baseline,
            resolve_display_ks,
        )

        params = self._parameters(True)
        frame = self._frame(spark, params)
        facts = _row_facts(frame, params)
        metrics = self._metrics(spark, True)
        results = {
            "evaluation_row_facts": facts, "evaluation_metrics": metrics,
            "prediction_quality_metrics": None,
            "evaluation_display_ks": resolve_display_ks(
                metrics, params, baseline_comparison=compare_to_baseline(
                    metrics, self._baseline(spark, True), params)),
        }
        totals = {r.label: r.cells[0] for r in core_concepts.build(
            results, params).blocks[-1].rows}
        assert totals["　其中算進指標的"] == 3 - 1

    # ---- the log -----------------------------------------------------------

    TEN_GROUPS = (
        [("c0", "A", 0.9, 1), ("c0", "B", 0.1, 1)]
        + [(f"c{i}", item, score, label)
           for i in range(1, 10)
           for item, score, label in (("A", 0.9, 1), ("B", 0.1, 0))]
    )

    def _warnings(self, caplog, spark, params, rows=None):
        with caplog.at_level(logging.INFO, logger=self.NODES_LOGGER):
            _row_facts(self._frame(spark, params, rows=rows), params)
        return [r.getMessage() for r in caplog.records
                if r.name == self.NODES_LOGGER
                and r.levelno == logging.WARNING]

    def test_switched_off_a_share_above_ten_percent_warns(self, spark, caplog):
        """One of three groups holding a positive at the fine grain, two of
        three at the category grain: each grain warns, naming the switch."""
        warned = self._warnings(caplog, spark, self._parameters())
        assert len(warned) == 2, warned
        assert all(self.KEY in m for m in warned), warned
        assert any("33.3%" in m and "fine grain" in m for m in warned), warned
        assert any("66.7%" in m and "category grain" in m for m in warned), warned
        ledger = [r.getMessage() for r in caplog.records
                  if r.name == self.NODES_LOGGER
                  and r.levelno == logging.INFO
                  and "Row facts counted" in r.getMessage()]
        assert len(ledger) == 2, ledger

    def test_exactly_ten_percent_does_not_warn(self, spark, caplog):
        params = self._parameters(items=("A", "B"), mapping={})
        params["evaluation"]["item_categories"]["enabled"] = False
        assert self._warnings(caplog, spark, params, self.TEN_GROUPS) == []

    def test_a_share_just_above_ten_percent_is_not_logged_as_ten_point_zero(
            self, spark, caplog):
        """21 of 209 groups holding a positive (10.048%): one decimal would log
        10.0% in a warning that says the share is above 10%."""
        rows = (
            [(f"a{i}", item, score, 1)
             for i in range(21) for item, score in (("A", 0.9), ("B", 0.1))]
            + [(f"m{i}", item, score, label)
               for i in range(188)
               for item, score, label in (("A", 0.9, 1), ("B", 0.1, 0))]
        )
        params = self._parameters(items=("A", "B"), mapping={})
        params["evaluation"]["item_categories"]["enabled"] = False
        warned = self._warnings(caplog, spark, params, rows)
        assert len(warned) == 1, warned
        assert "10.05%" in warned[0] and "(10.0%)" not in warned[0], warned

    def test_the_category_grain_warns_on_its_own(self, spark, caplog):
        """Fine-grained share exactly 10%: no warning. A and B fold into one
        category, so every group is one positive row there: 100%, warned."""
        params = self._parameters(items=("A", "B"), mapping={"ab": ["A", "B"]})
        warned = self._warnings(caplog, spark, params, self.TEN_GROUPS)
        assert len(warned) == 1, warned
        assert "category" in warned[0] and self.KEY in warned[0]

    def test_switched_on_does_not_warn(self, spark, caplog):
        assert self._warnings(caplog, spark, self._parameters(True)) == []

    # ---- the landed accounts (#536) ----------------------------------------

    def test_compute_compare_metrics_lands_each_side_s_ledger_and_accounts(
            self, spark):
        """The comparison report counts each side's ``population.macro_items``
        at both grains, and its row facts carry the all-positive accounts:
        they come out of ``metrics_spark.compute_row_facts``, the function
        the run's own facts come out of (#536). By hand: fund_bond is never
        positive, so the fine-grained macro holds the other two items; at the
        category grain fund and exchange_fx both hold a positive. The
        accounts are those of ``ROWS`` (switch off)."""
        from recsys_tfb.evaluation.result_readers import (
            ALL_POSITIVE_SHARE_KEY,
            ALL_POSITIVE_WARNS_KEY,
            MACRO_ITEMS_KEY,
            N_WITH_POSITIVE_KEY,
            POPULATION_KEY,
        )
        from recsys_tfb.pipelines.evaluation.nodes import compute_compare_metrics

        params = self._parameters(False)
        facts, metrics_a, _facts_b, metrics_b = compute_compare_metrics(
            self._frame(spark, params),
            self._frame(spark, params, rows=self.COMPARED_ROWS),
            _no_segments(params), params)
        seen = {"metrics_a": metrics_a, "metrics_b": metrics_b}
        for side in ("metrics_a", "metrics_b"):
            assert seen[side][POPULATION_KEY][MACRO_ITEMS_KEY] == [
                "exchange_fx", "fund_stock"], side
            assert seen[side]["category"][POPULATION_KEY][MACRO_ITEMS_KEY] == [
                "exchange_fx", "fund"], side
        for grain, share in ((facts, 1 / 3), (facts["category"], 2 / 3)):
            assert grain[N_WITH_POSITIVE_KEY] == 3
            assert grain[ALL_POSITIVE_SHARE_KEY] == pytest.approx(share)
            assert grain[ALL_POSITIVE_WARNS_KEY] is True

    @pytest.mark.parametrize("switch", [False, True])
    def test_the_row_facts_land_each_grain_s_accounts(self, spark, switch):
        """The report reads these instead of computing them (ADR-0031
        principle 1). By hand from ``ROWS``: three groups hold a positive at
        either grain (all but c4); one is all-positive at the fine grain (c1),
        two at the category grain (c1, c2). Both shares are above 10%, so
        each grain warns exactly when the switch is off."""
        from recsys_tfb.evaluation.result_readers import (
            ALL_POSITIVE_SHARE_KEY,
            ALL_POSITIVE_WARNS_KEY,
            N_WITH_POSITIVE_KEY,
        )

        params = self._parameters(switch)
        facts = _row_facts(self._frame(spark, params), params)
        for grain, share in ((facts, 1 / 3), (facts["category"], 2 / 3)):
            assert grain[N_WITH_POSITIVE_KEY] == 3
            assert grain[ALL_POSITIVE_SHARE_KEY] == pytest.approx(share)
            assert grain[ALL_POSITIVE_WARNS_KEY] is (not switch)
