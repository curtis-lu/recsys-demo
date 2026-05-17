"""Evaluation pipeline nodes — Spark backend."""

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.calibration import plot_calibration_curves
from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_positive_rate_rank_heatmap,
    plot_rank_heatmap,
    plot_score_distributions,
    plot_score_distributions_by_label,
)
from recsys_tfb.evaluation.report_builder import assemble_report

logger = logging.getLogger(__name__)


def prepare_eval_data(
    ranked_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Join ranked predictions with labels using Spark.

    For external segment sources, loads Parquet files and joins to labels
    before merging with predictions.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    eval_params = parameters.get("evaluation", {})
    segment_sources = eval_params.get("segment_sources", {})

    labels = label_table

    # Join external segment sources (single Spark impl; source seam inside).
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        labels = join_segment_sources(labels, segment_sources)

    # Filter predictions to the resolved model_version (resolved upstream by
    # __main__.py via core.versioning.resolve_model_version).
    model_version = parameters.get("model_version")
    if model_version is None:
        raise RuntimeError(
            "parameters['model_version'] missing. CLI should resolve via "
            "core.versioning.resolve_model_version before pipeline run."
        )
    logger.info("Filtering predictions to model_version=%s", model_version)
    ranked_predictions = ranked_predictions.filter(F.col("model_version") == model_version)

    # Filter labels to snap_dates in predictions
    pred_snap_dates = ranked_predictions.select(time_col).distinct()
    labels = labels.join(pred_snap_dates, on=time_col, how="inner")

    # Merge predictions with labels
    eval_predictions = ranked_predictions.join(labels, on=identity_cols, how="inner")

    # Downstream report rendering selects schema["rank"] from eval_predictions.
    # When the predictions source is
    # training_eval_predictions (--post-training mode), `rank` is absent because
    # the table no longer stores it (Spark mAP recomputes rank internally via
    # rank_within_query). Add it here when missing so downstream stays uniform;
    # when present (ranked_predictions source), trust the upstream value.
    rank_col = schema["rank"]
    if rank_col not in eval_predictions.columns:
        from recsys_tfb.evaluation.metrics_spark import rank_within_query
        score_col = schema["score"]
        entity_cols = schema["entity"]
        query_cols = [time_col] + entity_cols
        # rank_within_query adds a "pos" 1-based rank within each
        # (snap_date, cust_id), ordered by score desc.
        eval_predictions = rank_within_query(eval_predictions, query_cols, score_col)
        eval_predictions = eval_predictions.withColumnRenamed("pos", rank_col)
        logger.info(
            "prepare_eval_data: injected '%s' column via rank_within_query "
            "(predictions source did not provide it)",
            rank_col,
        )

    logger.info("Eval data prepared via Spark join")
    return eval_predictions


def compute_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics using the Spark-native pipeline.

    Thin wrapper over `evaluation.metrics_spark.compute_all_metrics`. All
    row-level work stays in Spark; only small aggregated dicts are collected.
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    result = compute_all_metrics(eval_predictions, parameters)
    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        result["n_queries"],
        result["n_excluded_queries"],
    )
    return result


def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
) -> str:
    """Build the HTML report. Metrics dicts drive §0–§8; only the
    diagnostics section (when enabled) needs row-level pandas, collected
    here with minimal columns and an optional sample cap.
    """
    schema = get_schema(parameters)
    id_cols = schema["identity_columns"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    item_col = schema["item"]

    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}

    diagnostics_frames = None
    if sections_cfg.get("diagnostics", True):
        sample_rows = diag_cfg.get("sample_rows")
        sdf = eval_predictions
        if sample_rows:
            sdf = sdf.limit(int(sample_rows))
        pred_cols = list(dict.fromkeys(id_cols + [score_col, rank_col]))
        predictions = sdf.select(*pred_cols).toPandas()
        # NOTE: when sample_rows is set, diagnostics labels are deduped over
        # the sampled rows only (acceptable: diagnostics are display-only).
        labels = (
            sdf.select(*list(dict.fromkeys(id_cols + [label_col])))
            .distinct()
            .toPandas()
        )
        figs = []
        if diag_cfg.get("include_distributions", True):
            figs += plot_score_distributions(
                predictions, item_col=item_col, score_col=score_col
            )
            figs += plot_score_distributions_by_label(
                predictions, labels, id_cols=tuple(id_cols),
                item_col=item_col, score_col=score_col, label_col=label_col
            )
            figs.append(
                plot_rank_heatmap(
                    predictions, item_col=item_col, rank_col=rank_col
                )
            )
            figs.append(
                plot_positive_rank_heatmap(
                    predictions, labels, id_cols=tuple(id_cols),
                    item_col=item_col, rank_col=rank_col, label_col=label_col
                )
            )
            figs.append(
                plot_positive_rate_rank_heatmap(
                    predictions, labels, id_cols=tuple(id_cols),
                    item_col=item_col, rank_col=rank_col, label_col=label_col
                )
            )
        if diag_cfg.get("include_calibration", True):
            figs.append(
                plot_calibration_curves(
                    predictions, labels,
                    n_bins=diag_cfg.get("n_calibration_bins", 10),
                    id_cols=tuple(id_cols), item_col=item_col,
                    score_col=score_col, label_col=label_col,
                )
            )
        diagnostics_frames = {"figures": figs}

    return assemble_report(
        evaluation_metrics, parameters,
        baseline_metrics=baseline_metrics,
        diagnostics_frames=diagnostics_frames,
    )
