"""Evaluation pipeline nodes — Spark backend."""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.calibration import plot_calibration_curves
from recsys_tfb.evaluation.compare import (
    build_comparison_result,
    plot_comparison_metrics,
)
from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_positive_rate_rank_heatmap,
    plot_rank_heatmap,
    plot_score_distributions,
    plot_score_distributions_by_label,
)
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.statistics import (
    compute_product_statistics,
    compute_segment_statistics,
)

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

    # Downstream report rendering (_render_html_report) selects schema["rank"]
    # from eval_predictions. When the predictions source is
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
    """Generate HTML report from Spark evaluation results.

    Collects the eval_predictions to pandas (post-aggregation, manageable size)
    and runs the pandas-based report rendering inline.
    """
    eval_pd = eval_predictions.toPandas()
    return _render_html_report(
        eval_pd, evaluation_metrics, parameters, baseline_metrics
    )


def _render_html_report(
    eval_predictions: pd.DataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
) -> str:
    """Generate HTML evaluation report.

    Reuses existing visualization modules (distributions, calibration,
    statistics, segments, compare) to build a comprehensive report.
    Optionally includes baseline comparison if baseline_metrics is provided.

    Returns:
        HTML string of the complete report.
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    eval_params = parameters.get("evaluation", {})
    report_config = eval_params.get("report", {})
    include_baseline = report_config.get("include_baseline_comparison", True)
    include_calibration = report_config.get("include_calibration", True)
    include_distributions = report_config.get("include_distributions", True)
    n_calibration_bins = report_config.get("n_calibration_bins", 10)

    segment_columns = list(eval_params.get("segment_columns", []))
    segment_sources = eval_params.get("segment_sources", {})
    for source_config in segment_sources.values():
        seg_col = source_config["segment_column"]
        if seg_col not in segment_columns:
            segment_columns.append(seg_col)

    pred_cols = identity_cols + [score_col, rank_col]
    predictions = eval_predictions[pred_cols].copy()

    label_cols_set = set(identity_cols + [label_col] + segment_columns)
    label_cols = [c for c in eval_predictions.columns if c in label_cols_set]
    labels = eval_predictions[label_cols].drop_duplicates()

    sections: list[ReportSection] = []

    summary_tables = []
    overall_df = pd.DataFrame([evaluation_metrics["overall"]]).T
    overall_df.columns = ["Overall"]
    summary_tables.append(overall_df)

    if evaluation_metrics.get("macro_avg"):
        summary_tables.append(pd.DataFrame(evaluation_metrics["macro_avg"]))

    table_titles = ["Overall", "Macro Average"]
    sections.append(
        ReportSection(
            title="Metrics Summary",
            description=(
                "Overall ranking metrics and macro average (unweighted mean)."
            ),
            tables=summary_tables,
            table_titles=table_titles[:len(summary_tables)],
        )
    )

    if evaluation_metrics.get("per_item"):
        item_df = pd.DataFrame(evaluation_metrics["per_item"]).T
        product_stats_df = compute_product_statistics(labels)
        sections.append(
            ReportSection(
                title="Per-Item Metrics",
                description=(
                    "Per-item attribution metrics — hit_rate@K (item-level recall), "
                    "map_attr@K / ndcg_attr@K (per-row contributions averaged across "
                    "queries where the item is positive), and mean_pos."
                ),
                tables=[item_df, product_stats_df],
                table_titles=["Per-Item Metrics", "Dataset Statistics"],
            )
        )

    if evaluation_metrics.get("per_segment"):
        seg_df = pd.DataFrame(evaluation_metrics["per_segment"]).T
        sections.append(
            ReportSection(
                title="Per-Segment Metrics",
                description=(
                    "Per-segment query-level metrics (map@K / ndcg@K / precision@K / "
                    "recall@K averaged across queries within each segment). "
                    "precision@K at K=n_products is base rate; recall@K at K=n_products "
                    "is always 1.0."
                ),
                tables=[seg_df],
                table_titles=["Per-Segment Metrics"],
            )
        )

    if include_distributions:
        dist_figs = plot_score_distributions(predictions)
        label_dist_figs = plot_score_distributions_by_label(predictions, labels)
        sections.append(
            ReportSection(
                title="Score Distributions",
                description="Histogram and boxplot of prediction scores per product.",
                figures=dist_figs + label_dist_figs,
            )
        )

        rank_fig = plot_rank_heatmap(predictions)
        pos_rank_fig = plot_positive_rank_heatmap(predictions, labels)
        pos_rate_fig = plot_positive_rate_rank_heatmap(predictions, labels)
        sections.append(
            ReportSection(
                title="Rank Distribution",
                description="Rank distribution heatmaps.",
                figures=[rank_fig, pos_rank_fig, pos_rate_fig],
            )
        )

    if include_calibration:
        cal_fig = plot_calibration_curves(
            predictions, labels, n_bins=n_calibration_bins
        )
        sections.append(
            ReportSection(
                title="Calibration Curves",
                description="Predicted probability vs actual positive rate.",
                figures=[cal_fig],
            )
        )

    # Segment dataset statistics (the metric numbers themselves are already in
    # the Per-Segment Metrics section above, computed once by metrics_spark).
    for seg_col in segment_columns:
        if seg_col not in labels.columns:
            continue
        seg_stats = compute_segment_statistics(labels, segment_column=seg_col)
        display_name = seg_col.replace("_", " ").title()
        sections.append(
            ReportSection(
                title=f"Segment Dataset Statistics: {display_name}",
                description=f"Customer / positive-rate breakdown by {seg_col}.",
                tables=[seg_stats],
                table_titles=["Dataset Statistics"],
            )
        )

    if include_baseline and baseline_metrics is not None:
        comparison = build_comparison_result(
            evaluation_metrics, baseline_metrics, "Model", "Baseline"
        )
        comp_figs = plot_comparison_metrics(comparison)

        delta_df = pd.DataFrame([comparison["overall_delta"]]).T
        delta_df.columns = ["Delta (Model - Baseline)"]
        overall_model = pd.DataFrame([evaluation_metrics["overall"]]).T
        overall_model.columns = ["Model"]
        overall_base = pd.DataFrame([baseline_metrics.get("overall", {})]).T
        overall_base.columns = ["Baseline"]
        summary = pd.concat([overall_model, overall_base, delta_df], axis=1)

        sections.append(
            ReportSection(
                title="Baseline Comparison",
                description="Model vs baseline performance comparison.",
                figures=comp_figs,
                tables=[summary],
                table_titles=["Overall Comparison"],
            )
        )

    snap_date = eval_params.get("snap_date", "unknown")
    metadata = {
        "Snap Date": snap_date,
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Queries": evaluation_metrics["n_queries"],
        "Excluded Queries": evaluation_metrics["n_excluded_queries"],
    }
    html = generate_html_report(
        sections, title="Model Evaluation Report", metadata=metadata
    )

    logger.info("Report generated: %d sections", len(sections))
    return html
