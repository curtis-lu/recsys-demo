"""End-to-end tests for evaluation pipeline in compare modes."""

import pytest
from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    DataConsistencyError,
    compare_mutual_exclusive_errors,
)
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline


def test_default_pipeline_has_persist_node():
    pipeline = create_pipeline(post_training=False)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "persist_eval_predictions" in node_names
    assert "load_compare_predictions" not in node_names


def test_compare_mode_adds_three_extra_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "load_compare_predictions" in node_names
    assert "restrict_to_common" in node_names
    assert "generate_comparison_report" in node_names
    # And the four existing + persist still present
    assert "prepare_eval_data" in node_names
    assert "persist_eval_predictions" in node_names


def test_compare_only_mode_skips_compute_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src, compare_only=True)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "load_eval_predictions_from_hive" in node_names
    assert "generate_comparison_report" in node_names
    # explicitly NOT present:
    assert "compute_metrics" not in node_names
    assert "compute_baseline_metrics" not in node_names
    assert "generate_report" not in node_names
    assert "persist_eval_predictions" not in node_names
    assert "prepare_eval_data" not in node_names


def test_a13_compare_and_compare_only_mutually_exclusive():
    errs = compare_mutual_exclusive_errors("x", "y")
    assert errs and "mutually exclusive" in errs[0].lower()


def _warehouse_table_dir(spark, db: str, table: str):
    """Return the local Path for a managed Spark table, stripping file:// prefix."""
    from pathlib import Path

    raw = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    # Spark reports a file:// URI; strip scheme prefix if present.
    if raw.startswith("file:"):
        raw = raw[len("file:"):]
    # Remove any extra leading slashes that would produce //<path> on macOS.
    return Path(raw) / f"{db}.db" / table


def test_b4_load_from_hive_fails_loud_on_missing_partition(spark):
    import shutil

    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        load_eval_predictions_from_hive,
    )
    # Empty test catalog — no ml_recsys.eval_predictions
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_recsys")
    spark.sql("DROP TABLE IF EXISTS ml_recsys.eval_predictions")
    # Also remove the warehouse directory if it exists from a previous run so
    # that CREATE TABLE does not see a stale location and raise AnalysisException.
    table_dir = _warehouse_table_dir(spark, "ml_recsys", "eval_predictions")
    if table_dir.exists():
        shutil.rmtree(table_dir)
    spark.sql(
        "CREATE TABLE ml_recsys.eval_predictions "
        "(cust_id STRING, snap_date STRING, prod_name STRING, score DOUBLE, "
        "rank INT, label INT, model_version STRING) "
        "USING parquet PARTITIONED BY (snap_date, model_version)"
    )
    params = {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2099-01-01"},  # nonexistent
        "model_version": "ghost_mv",
    }
    with pytest.raises(DataConsistencyError, match="B4"):
        load_eval_predictions_from_hive(params)


def test_b4_load_from_hive_returns_partition_when_present(spark):
    import shutil

    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        load_eval_predictions_from_hive,
        persist_eval_predictions,
    )
    # Drop any pre-existing table + warehouse dir so saveAsTable starts clean.
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_recsys")
    spark.sql("DROP TABLE IF EXISTS ml_recsys.eval_predictions")
    table_dir = _warehouse_table_dir(spark, "ml_recsys", "eval_predictions")
    if table_dir.exists():
        shutil.rmtree(table_dir)

    eval_pred = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    params = {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2026-01-31"},
        "model_version": "MV_X",
    }
    persist_result = persist_eval_predictions(eval_pred, params)
    out = load_eval_predictions_from_hive(params)
    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "p1", 0.9)]


def test_persist_eval_predictions_returns_input_df(spark):
    """persist_eval_predictions is an identity pass-through: catalog auto-save
    handles the actual Hive write. Function returns the same DataFrame object
    passed in (referential identity, not just equality).
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        persist_eval_predictions,
    )

    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = persist_eval_predictions(df)
    assert out is df


def _base_params_for_validator():
    """Minimal params dict the validator needs."""
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2026-01-31"},
        "model_version": "MV_X",
        "hive": {"db": "ml_recsys"},
    }


def test_b4_validator_raises_when_partition_empty(spark):
    """Empty DataFrame in (simulates catalog filter returned nothing).
    Validator must raise DataConsistencyError tagged (B4).
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    empty = spark.createDataFrame(
        [],
        "cust_id STRING, snap_date STRING, prod_name STRING, "
        "score DOUBLE, rank INT, label INT",
    )
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(
            empty, _base_params_for_validator()
        )


def test_b4_validator_raises_when_snap_date_filter_yields_empty(spark):
    """DataFrame has rows but no rows match the configured evaluation.snap_date.
    Validator filters then raises B4.
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    params = _base_params_for_validator()
    params["evaluation"]["snap_date"] = "2099-01-01"  # mismatch
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(df, params)


def test_b4_validator_passes_when_partition_present(spark):
    """DataFrame has matching snap_date row → validator returns the filtered DF."""
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c2", "2025-12-31", "p1", 0.5, 1, 0),  # different snap_date, filtered out
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    out = validate_enriched_eval_predictions_present(
        df, _base_params_for_validator()
    )
    rows = [(r["cust_id"], r["snap_date"]) for r in out.collect()]
    assert rows == [("c1", "2026-01-31")]
