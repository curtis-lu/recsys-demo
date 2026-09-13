"""Pipeline-aware shims for compare-mode nodes.

Thin wrappers over `evaluation/comparison/` pure modules + `nodes_spark.py`
helpers. Each function is one Pipeline ``Node`` body — accepts framework-
materialized inputs (DataFrames + parameters dict + spark session) and
returns the next handle.
"""

from __future__ import annotations

import logging
from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common as _restrict
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions as _load_compare
from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_date,
    restrict_to_eval_snap_date,
)
from recsys_tfb.utils.spark import get_or_create_spark_session

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict) -> SparkDataFrame:
    """Pipeline shim: resolve a SparkSession and dispatch to source loader."""
    spark = get_or_create_spark_session()
    return _load_compare(parameters, spark)


def restrict_to_common(
    eval_predictions: SparkDataFrame,
    compare_predictions_raw: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
    """Pipeline shim: call the pure restrict function + capture coverage dict.

    Returns ``(a_common, b_common, coverage_partial)`` — coverage_partial
    carries full-universe sizes, the common sizes and dropped item lists so
    the report can show what was filtered.

    Population size is counted in **query groups** — distinct ``[time] +
    entity`` combinations, every column of ``schema.entity`` — because that is
    the unit mAP divides by. Reporting it in any other unit puts two different
    scales side by side in one table with nothing telling the reader they
    differ. See ``docs/adr/0015-compare-population-counted-in-query-groups.md``.

    The common count is taken from what the restriction kept, not re-derived
    from the raw frames (ADR-0020 bug 14). It used to ``intersect`` the raw
    query groups, which matches ``NULL == NULL``, while the restriction's
    equi-join drops null keys — so it counted groups no metric ever saw.

    ``eval_predictions`` is ``enriched_eval_predictions`` read back from Hive,
    every month this ``model_version`` was evaluated on; the node keeps the
    evaluated month before anything else (ADR-0018 decision 1).
    """
    # Decision — compare the evaluated month only: the table holds every month
    # this model_version was evaluated on.
    eval_predictions = restrict_to_eval_snap_date(eval_predictions, parameters)

    schema = get_schema(parameters)
    time_col = schema["time"]
    query_group_cols = [time_col, *schema["entity"]]

    # Decision — B is scored against A's labels: not the label B landed with,
    # and not a fresh label_table join, since A's own label is not the current
    # label_table under --post-training / --compare-only. One answer for both
    # sides in every mode (ADR-0020 bug 7; the full why is in
    # evaluation/comparison/restrict.py).
    a_common, b_common, universe = _restrict(
        eval_predictions, compare_predictions_raw, parameters
    )

    a_groups_full = eval_predictions.select(*query_group_cols).distinct().count()
    b_groups_full = compare_predictions_raw.select(*query_group_cols).distinct().count()
    # Groups both restricted frames still hold. A left-semi equi-join, so null
    # keys never match — the same null rule as the restriction's own joins. On
    # symmetric candidate sets this equals either side's kept group count; when
    # one side scored a common entity only on items the other lacks, that group
    # is in one side's metrics but not in "common".
    groups_common = (
        a_common.select(*query_group_cols).distinct()
        .join(
            b_common.select(*query_group_cols).distinct(),
            on=query_group_cols, how="left_semi",
        )
        .count()
    )
    common_items = universe.common_items

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    coverage_partial = {
        "kind_a": "model_version",
        "model_version_a": parameters.get("model_version", "(this run)"),
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_query_group_A_full": a_groups_full,
        "n_query_group_B_full": b_groups_full,
        "n_query_group_common": groups_common,
        "n_item_A_full": len(universe.a_items),
        "n_item_B_full": len(universe.b_items),
        "n_item_common": len(common_items),
        "dropped_items_A": sorted(universe.a_items - common_items),
        "dropped_items_B": sorted(universe.b_items - common_items),
    }
    return a_common, b_common, coverage_partial


def generate_comparison_report(
    eval_predictions_common: SparkDataFrame,
    compare_predictions_common: SparkDataFrame,
    coverage_partial: dict,
    segment_columns: dict,
    parameters: dict,
) -> str:
    """Run compute_all_metrics on both sides + assemble HTML.

    Only this run's side segments, by what ``prepare_eval_data`` joined
    (``segment_columns``; under ``--compare-only`` the copy landed next to the
    enriched partition). Its frame can hold a column the other run mode joined,
    all NULL, so the frame's columns are not asked (ADR-0020 bug 6). The
    compared side is another prediction table with no segment columns.
    """
    metrics_a = compute_all_metrics(
        eval_predictions_common, parameters,
        segment_columns=segment_columns["joined"],
    )
    metrics_b = compute_all_metrics(
        compare_predictions_common, parameters, segment_columns=[]
    )

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    label_a = "Model"
    label_b = src.get("label", "Compare")
    comparison = build_comparison_result(metrics_a, metrics_b, label_a, label_b)

    return assemble_comparison_report(
        metrics_a, metrics_b, comparison, coverage_partial, parameters
    )


def validate_enriched_eval_predictions_present(
    enriched_eval_predictions: SparkDataFrame,
    parameters: dict,
) -> None:
    """B4 invariant — fail loud if ``enriched_eval_predictions`` holds no rows
    for the evaluated month under this ``model_version``.

    A zero-output gate: it passes nothing on. ``restrict_to_common`` reads the
    table and keeps the evaluated month itself, like every other reader
    (ADR-0018 decision 1). The catalog has already pruned the table to this
    ``model_version`` via ``partition_filter``; this node keeps the evaluated
    month and asserts a row remains, otherwise raises
    ``DataConsistencyError`` saying what to run first.

    Slicing never pulls a zero-output node back in (R3 in
    ``docs/agents/architecture-constraints.md``). Accepted here: the mode
    lists it explicitly and ``--compare-only`` has no resume point, the same
    shape ADR-0013 registered for ``validate_data_consistency``.

    Used only in ``--compare-only`` mode. In the other modes
    ``prepare_eval_data`` writes the partition earlier in the same run, so B4
    cannot fire.
    """
    mv = parameters.get("model_version", "unknown")
    hive_db = (parameters.get("hive") or {}).get("db", "ml_recsys")

    if restrict_to_eval_snap_date(enriched_eval_predictions, parameters).isEmpty():
        raise DataConsistencyError(
            f"(B4) {hive_db}.enriched_eval_predictions has no partition "
            f"for evaluation.snap_date={eval_snap_date(parameters)!r} "
            f"model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without "
            "--compare) first to populate the partition."
        )
