"""Pipeline-aware shims for compare-mode nodes.

Thin wrappers over `evaluation/comparison/` pure modules + `nodes_spark.py`
helpers. Each function is one Pipeline ``Node`` body — accepts framework-
materialized inputs (DataFrames + parameters dict + spark session) and
returns the next handle.
"""

from __future__ import annotations

import logging
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.alignment import common_universe as _common_universe
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common as _restrict
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions as _load_compare
from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
from recsys_tfb.utils.spark import get_or_create_spark_session

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict) -> SparkDataFrame:
    """Pipeline shim: resolve a SparkSession and dispatch to source loader."""
    spark = get_or_create_spark_session()
    return _load_compare(parameters, spark)


def restrict_to_common(
    eval_predictions: SparkDataFrame,
    compare_predictions_raw: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
    """Pipeline shim: call the pure restrict function + capture coverage dict.

    Returns ``(a_common, b_common, coverage_partial)`` — coverage_partial
    carries full-universe sizes + dropped product lists so the report can
    show what was filtered. Computed here because ``_restrict`` itself loses
    access to the originals after returning.
    """
    schema = get_schema(parameters)
    cust_col = schema["entity"][0]
    item_col = schema["item"]

    a_prods_full = {r[0] for r in eval_predictions.select(item_col).distinct().collect()}
    b_prods_full = {r[0] for r in compare_predictions_raw.select(item_col).distinct().collect()}
    a_cust_full = eval_predictions.select(cust_col).distinct().count()
    b_cust_full = compare_predictions_raw.select(cust_col).distinct().count()

    # Compute the conceptual intersection sets for coverage — these are
    # the "common universe" sizes (cust intersection × prod intersection).
    # Post-restrict counts may be smaller when A/B don't fully cover the
    # cross product.
    common_cust, common_prod = _common_universe(
        eval_predictions, compare_predictions_raw, cust_col, item_col
    )

    a_common, b_common = _restrict(
        eval_predictions, compare_predictions_raw, label_table, parameters
    )

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    coverage_partial = {
        "kind_a": "model_version",
        "model_version_a": parameters.get("model_version", "(this run)"),
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_cust_A_full": a_cust_full,
        "n_cust_B_full": b_cust_full,
        "n_prod_A_full": len(a_prods_full),
        "n_prod_B_full": len(b_prods_full),
        "n_cust_common": len(common_cust),
        "n_prod_common": len(common_prod),
        "dropped_prods_A": sorted(a_prods_full - common_prod),
        "dropped_prods_B": sorted(b_prods_full - common_prod),
    }
    return a_common, b_common, coverage_partial


def generate_comparison_report(
    eval_predictions_common: SparkDataFrame,
    compare_predictions_common: SparkDataFrame,
    coverage_partial: dict,
    parameters: dict,
) -> str:
    """Run compute_all_metrics on both sides + assemble HTML."""
    metrics_a = compute_all_metrics(eval_predictions_common, parameters)
    metrics_b = compute_all_metrics(compare_predictions_common, parameters)

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    label_a = "Model"
    label_b = src.get("label", "Compare")
    comparison = build_comparison_result(metrics_a, metrics_b, label_a, label_b)

    return assemble_comparison_report(
        metrics_a, metrics_b, comparison, coverage_partial, parameters
    )


def persist_eval_predictions(eval_predictions: SparkDataFrame) -> SparkDataFrame:
    """Pass-through node that routes the in-memory eval_predictions to the
    framework-auto-save edge for catalog entry ``enriched_eval_predictions``
    (HiveTableDataset). All write-side machinery — dynamic-partition
    overwrite, ``model_version`` partition column injection, CREATE TABLE
    IF NOT EXISTS, ``${hive.db}`` qualification — lives in the catalog
    layer. This function exists solely as the named DAG edge.
    """
    return eval_predictions


def load_eval_predictions_from_hive(parameters: dict) -> SparkDataFrame:
    """For --compare-only mode: read previously-persisted eval_predictions.

    Raises (B4) when the matching (snap_date, model_version) partition is
    absent — message tells the user to run evaluation first.
    """
    schema = get_schema(parameters)
    eval_params = parameters.get("evaluation", {}) or {}
    snap_date = str(eval_params.get("snap_date") or "").strip()
    mv = parameters.get("model_version", "unknown")
    spark = get_or_create_spark_session()

    df = (
        spark.table("ml_recsys.eval_predictions")
        .filter(F.col(schema["time"]).cast("string") == snap_date)
        .filter(F.col("model_version") == mv)
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"(B4) ml_recsys.eval_predictions has no partition for "
            f"snap_date={snap_date!r} model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without --compare) "
            "first to populate the partition."
        )
    return df.drop("model_version")


def validate_enriched_eval_predictions_present(
    enriched_eval_predictions: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """B4 invariant — fail loud if no partition exists for the current
    (snap_date, model_version) in ``enriched_eval_predictions``.

    Pattern: small validator node, pass-through (echoes
    ``validate_predictions`` in inference pipeline). Catalog auto-loads the
    table filtered by ``model_version`` via ``partition_filter`` (which
    drops the column on the way out). This node filters by snap_date and
    asserts at least one row remains; otherwise raises
    ``DataConsistencyError`` with an actionable message.

    Used only in ``--compare-only`` mode. In default / ``--compare`` modes
    the same partition is freshly written by ``persist_eval_predictions``
    earlier in the same pipeline, so B4 cannot fire.
    """
    schema = get_schema(parameters)
    eval_params = parameters.get("evaluation", {}) or {}
    snap_date = str(eval_params.get("snap_date") or "").strip()
    mv = parameters.get("model_version", "unknown")
    hive_db = (parameters.get("hive") or {}).get("db", "ml_recsys")

    df = enriched_eval_predictions.filter(
        F.col(schema["time"]).cast("string") == snap_date
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"(B4) {hive_db}.enriched_eval_predictions has no partition "
            f"for snap_date={snap_date!r} model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without "
            "--compare) first to populate the partition."
        )
    return df
