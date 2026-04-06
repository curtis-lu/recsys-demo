"""Evaluation pipeline nodes — pandas backend."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

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
from recsys_tfb.evaluation.metrics import compute_all_metrics
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.segments import (
    build_segment_metrics_table,
    compute_segment_metrics,
    load_and_join_segment_sources,
)
from recsys_tfb.evaluation.statistics import (
    compute_product_statistics,
    compute_segment_statistics,
)

logger = logging.getLogger(__name__)


def prepare_eval_data(
    ranked_predictions: pd.DataFrame,
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Join ranked predictions with labels and optional segment sources.

    Reads segment_sources from parameters_evaluation and joins external
    segment data to the label_table before merging with predictions.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    eval_params = parameters.get("evaluation", {})
    segment_sources = eval_params.get("segment_sources", {})

    # Join external segment sources to labels
    labels = label_table.copy()
    if segment_sources:
        labels = load_and_join_segment_sources(labels, segment_sources)

    # Filter labels to snap_dates present in predictions
    pred_snap_dates = ranked_predictions[time_col].unique()
    labels = labels[labels[time_col].isin(pred_snap_dates)]

    # Merge predictions with labels
    eval_predictions = ranked_predictions.merge(
        labels,
        on=identity_cols,
        how="inner",
    )

    logger.info(
        "Eval data prepared: %d prediction rows, %d label rows, %d merged rows",
        len(ranked_predictions),
        len(labels),
        len(eval_predictions),
    )
    return eval_predictions


def compute_metrics(
    eval_predictions: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics from eval_predictions (already joined with labels).

    Delegates to compute_all_metrics from evaluation/metrics.py.
    The eval_predictions DataFrame contains both score and label columns.
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    eval_params = parameters.get("evaluation", {})
    k_values = eval_params.get("k_values", [5, "all"])

    # Split back into predictions and labels for compute_all_metrics
    pred_cols = identity_cols + [score_col, rank_col]
    predictions = eval_predictions[pred_cols].copy()

    # Labels: identity_cols + label + any segment columns
    label_cols = identity_cols + [label_col]
    segment_columns = eval_params.get("segment_columns", [])
    for seg_col in segment_columns:
        if seg_col in eval_predictions.columns and seg_col not in label_cols:
            label_cols.append(seg_col)
    # Also include external segment source columns
    segment_sources = eval_params.get("segment_sources", {})
    for source_config in segment_sources.values():
        seg_col = source_config["segment_column"]
        if seg_col in eval_predictions.columns and seg_col not in label_cols:
            label_cols.append(seg_col)
    labels = eval_predictions[label_cols].copy()

    metrics = compute_all_metrics(
        predictions=predictions,
        labels=labels,
        k_values=k_values,
        parameters=parameters,
    )

    logger.info(
        "Metrics computed: n_queries=%d, n_excluded=%d",
        metrics["n_queries"],
        metrics["n_excluded_queries"],
    )
    return metrics


def generate_report(
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

    # Split eval_predictions for visualization functions that expect
    # separate predictions and labels DataFrames
    pred_cols = identity_cols + [score_col, rank_col]
    predictions = eval_predictions[pred_cols].copy()

    label_cols_set = set(identity_cols + [label_col] + segment_columns)
    label_cols = [c for c in eval_predictions.columns if c in label_cols_set]
    labels = eval_predictions[label_cols].drop_duplicates()

    sections: list[ReportSection] = []

    # --- Metrics Summary ---
    summary_tables = []
    overall_df = pd.DataFrame([evaluation_metrics["overall"]]).T
    overall_df.columns = ["Overall"]
    summary_tables.append(overall_df)

    if evaluation_metrics.get("macro_avg"):
        summary_tables.append(pd.DataFrame(evaluation_metrics["macro_avg"]))
    if evaluation_metrics.get("micro_avg"):
        summary_tables.append(pd.DataFrame(evaluation_metrics["micro_avg"]))

    table_titles = ["Overall", "Macro Average", "Micro Average"]
    sections.append(
        ReportSection(
            title="Metrics Summary",
            description=(
                "Overall ranking metrics, macro average (unweighted mean), "
                "and micro average (query-count-weighted mean)."
            ),
            tables=summary_tables,
            table_titles=table_titles[:len(summary_tables)],
        )
    )

    # --- Per-Product Metrics ---
    if evaluation_metrics.get("per_product"):
        prod_df = pd.DataFrame(evaluation_metrics["per_product"]).T
        product_stats_df = compute_product_statistics(labels)
        sections.append(
            ReportSection(
                title="Per-Product Metrics",
                description="Metrics and dataset statistics broken down by product.",
                tables=[prod_df, product_stats_df],
                table_titles=["Ranking Metrics", "Dataset Statistics"],
            )
        )

    # --- Score Distributions ---
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

        # Rank Distribution
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

    # --- Calibration ---
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

    # --- Segment Analysis ---
    k_values = eval_params.get("k_values", [5, "all"])
    for seg_col in segment_columns:
        if seg_col not in labels.columns:
            continue
        seg_metrics = compute_segment_metrics(
            predictions, labels, segment_column=seg_col, k_values=k_values
        )
        seg_table = build_segment_metrics_table(seg_metrics)
        seg_stats = compute_segment_statistics(labels, segment_column=seg_col)
        display_name = seg_col.replace("_", " ").title()
        sections.append(
            ReportSection(
                title=f"Segment Analysis: {display_name}",
                description=f"Metrics and statistics by {seg_col}.",
                tables=[seg_table, seg_stats],
                table_titles=["Ranking Metrics", "Dataset Statistics"],
            )
        )

    # --- Baseline Comparison ---
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

    # --- Generate HTML ---
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
