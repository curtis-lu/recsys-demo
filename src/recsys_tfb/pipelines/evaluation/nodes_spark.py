"""Evaluation pipeline nodes — Spark backend."""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
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
from recsys_tfb.evaluation.segments import (
    build_segment_metrics_table,
    compute_segment_metrics,
)
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

    # Join external segment sources
    if segment_sources:
        spark = label_table.sparkSession
        for seg_name, source_config in segment_sources.items():
            filepath = source_config["filepath"]
            key_columns = source_config["key_columns"]
            segment_column = source_config["segment_column"]
            try:
                seg_df = spark.read.parquet(filepath)
                labels = labels.join(
                    seg_df.select(key_columns + [segment_column]),
                    on=key_columns,
                    how="left",
                )
                logger.info("Joined segment source '%s' (%s)", seg_name, segment_column)
            except Exception:
                logger.warning(
                    "Segment source '%s' not found at %s — skipping",
                    seg_name,
                    filepath,
                )

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

    logger.info("Eval data prepared via Spark join")
    return eval_predictions


def compute_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics using Spark SQL.

    Uses window functions to compute AP, nDCG, Precision@K, Recall@K
    entirely within Spark, then collects the small aggregated result.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    eval_params = parameters.get("evaluation", {})
    k_values_raw = eval_params.get("k_values", [5, "all"])

    group_cols = [time_col] + entity_cols

    # Resolve "all" to actual product count
    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = []
    for k in k_values_raw:
        if isinstance(k, str) and k == "all":
            k_values.append(n_products)
        else:
            k_values.append(int(k))
    k_values = sorted(set(k_values))

    # Step 1: Add position within each query (ordered by score desc)
    query_window = Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())
    df = eval_predictions.withColumn("pos", F.row_number().over(query_window))

    # Step 2: Cumulative relevant count and total relevant per query
    cumsum_window = (
        Window.partitionBy(*group_cols)
        .orderBy(F.col(score_col).desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        "cum_rel", F.sum(F.col(label_col)).over(cumsum_window)
    )
    df = df.withColumn(
        "total_rel",
        F.sum(F.col(label_col)).over(Window.partitionBy(*group_cols)),
    )

    # Step 3: Per-row metric contributions
    df = df.withColumn("precision_at_pos", F.col("cum_rel") / F.col("pos"))
    df = df.withColumn(
        "dcg_contrib",
        F.col(label_col) / F.log2(F.col("pos") + F.lit(1)),
    )

    # Step 4: Per-query aggregation for each K (exclude queries with no positives)
    query_aggs = []
    for k in k_values:
        query_aggs.extend([
            # AP@K: sum(precision_at_pos * label * (pos<=K)) / total_rel
            F.sum(
                F.when(
                    (F.col(label_col) == 1) & (F.col("pos") <= k),
                    F.col("precision_at_pos"),
                ).otherwise(0)
            ).alias(f"_ap_num_{k}"),
            # Precision@K: sum(label where pos<=K) / K
            # (same numerator drives Recall@K with total_rel as denominator)
            F.sum(
                F.when(F.col("pos") <= k, F.col(label_col)).otherwise(0)
            ).alias(f"_prec_num_{k}"),
            # nDCG@K: sum(dcg_contrib where pos<=K) / iDCG@K
            F.sum(
                F.when(F.col("pos") <= k, F.col("dcg_contrib")).otherwise(0)
            ).alias(f"_dcg_{k}"),
        ])

    base_aggs = [F.first("total_rel").alias("total_rel")]

    per_query = (
        df.filter(F.col("total_rel") > 0)
        .groupBy(*group_cols)
        .agg(*(base_aggs + query_aggs))
    )

    # Carry product-level info for per-product breakdown
    # We need to go back to per-row level for per-product metrics
    # Let's compute overall first, then per-product

    # --- Overall metrics ---
    overall = {}
    for k in k_values:
        per_query = (
            per_query.withColumn(
                f"ap_{k}",
                F.col(f"_ap_num_{k}") / F.col("total_rel"),
            )
            .withColumn(
                f"precision_{k}",
                F.col(f"_prec_num_{k}") / F.lit(k),
            )
            .withColumn(
                f"recall_{k}",
                F.col(f"_prec_num_{k}") / F.col("total_rel"),
            )
        )

    # Collect per-query metrics (one row per query, small enough)
    collected = per_query.toPandas()

    if len(collected) == 0:
        logger.warning("No queries with positive labels found")
        return {
            "overall": {},
            "per_product": {},
            "per_segment": {},
            "per_product_segment": {},
            "macro_avg": {},
            "n_queries": 0,
            "n_excluded_queries": 0,
        }

    import numpy as np

    # Build iDCG lookup table
    max_r = int(collected["total_rel"].max())
    max_k = max(k_values)
    max_n = max(max_r, max_k) + 1
    idcg_table = np.zeros(max_n + 1)
    for i in range(1, max_n + 1):
        idcg_table[i] = idcg_table[i - 1] + 1.0 / np.log2(i + 1)

    for k in k_values:
        min_rk_vals = np.minimum(
            collected["total_rel"].values.astype(int), k
        )
        idcg_at_k = idcg_table[min_rk_vals]
        dcg_vals = collected[f"_dcg_{k}"].values
        collected[f"ndcg_{k}"] = np.where(idcg_at_k > 0, dcg_vals / idcg_at_k, 0.0)

    # Aggregate overall
    n_queries_total = eval_predictions.select(*group_cols).distinct().count()
    n_queries_with_pos = len(collected)
    n_excluded = n_queries_total - n_queries_with_pos

    for k in k_values:
        overall[f"map@{k}"] = float(collected[f"ap_{k}"].mean())
        overall[f"ndcg@{k}"] = float(collected[f"ndcg_{k}"].mean())
        overall[f"precision@{k}"] = float(collected[f"precision_{k}"].mean())
        overall[f"recall@{k}"] = float(collected[f"recall_{k}"].mean())

    # --- Per-product metrics ---
    # Go back to row-level data with enrichment, but use Spark
    # For per-product, we use the vectorized approach on collected Spark data
    # Collect the enriched row-level data for per-product decomposition
    enriched_pd = df.filter(F.col("total_rel") > 0).toPandas()

    from recsys_tfb.evaluation.metrics import (
        _aggregate_per_dimension,
        _enrich_with_contributions,
        _macro_average,
    )

    # Re-enrich in pandas for per-product decomposition
    # The data is already filtered to queries with positives
    enriched_contrib = _enrich_with_contributions(
        enriched_pd, k_values, group_cols=group_cols,
        score_col=score_col, label_col=label_col,
    )
    rel = enriched_contrib[enriched_contrib[label_col] == 1]

    per_product = {}
    if len(rel) > 0:
        per_product = _aggregate_per_dimension(rel, [item_col], k_values)

    # Per-segment
    per_segment = {}
    segment_columns = eval_params.get("segment_columns", [])
    has_segment = any(
        col in enriched_pd.columns for col in segment_columns
    )
    if has_segment and len(collected) > 0:
        # Use the first available segment column
        for seg_col in segment_columns:
            if seg_col not in enriched_pd.columns:
                continue
            # Per-query metrics with segment info
            query_seg = enriched_pd.groupby(group_cols).first()[[seg_col]].reset_index()
            query_metrics_df = collected.merge(query_seg, on=group_cols, how="left")

            for seg_val, seg_group in query_metrics_df.groupby(seg_col, sort=True):
                seg_metrics = {}
                for k in k_values:
                    seg_metrics[f"map@{k}"] = float(seg_group[f"ap_{k}"].mean())
                    seg_metrics[f"ndcg@{k}"] = float(seg_group[f"ndcg_{k}"].mean())
                    seg_metrics[f"precision@{k}"] = float(seg_group[f"precision_{k}"].mean())
                    seg_metrics[f"recall@{k}"] = float(seg_group[f"recall_{k}"].mean())
                per_segment[str(seg_val)] = seg_metrics
            break  # Only use first segment column for main per_segment

    # Per-product-segment
    per_product_segment = {}
    if has_segment and len(rel) > 0:
        for seg_col in segment_columns:
            if seg_col in rel.columns:
                per_product_segment = _aggregate_per_dimension(
                    rel, [item_col, seg_col], k_values
                )
                break

    # Macro averages
    macro_avg = {}
    macro_avg["by_product"] = _macro_average(per_product)
    if per_segment:
        macro_avg["by_segment"] = _macro_average(per_segment)
    if per_product_segment:
        macro_avg["by_product_segment"] = _macro_average(per_product_segment)

    result = {
        "overall": overall,
        "per_product": per_product,
        "per_segment": per_segment,
        "per_product_segment": per_product_segment,
        "macro_avg": macro_avg,
        "n_queries": n_queries_total,
        "n_excluded_queries": n_excluded,
    }

    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        n_queries_total,
        n_excluded,
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
