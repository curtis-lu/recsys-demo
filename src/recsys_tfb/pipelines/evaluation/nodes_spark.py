"""Evaluation pipeline nodes — Spark backend."""

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

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

    Uses window functions to compute AP, nDCG, MRR, Precision@K, Recall@K
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

    # First relevant position per query
    df = df.withColumn(
        "first_rel_pos",
        F.min(F.when(F.col(label_col) == 1, F.col("pos"))).over(
            Window.partitionBy(*group_cols)
        ),
    )

    # Step 4: Per-query aggregation for each K
    # Exclude queries with no positives
    query_aggs = []
    for k in k_values:
        # AP@K: mean of precision_at_pos where label=1 AND pos<=K, divided by total_rel
        ap_col = f"ap_{k}"
        ndcg_col = f"ndcg_{k}"
        mrr_col = f"mrr_{k}"
        prec_col = f"precision_{k}"
        recall_col = f"recall_{k}"

        query_aggs.extend([
            # AP@K: sum(precision_at_pos * label * (pos<=K)) / total_rel
            F.sum(
                F.when(
                    (F.col(label_col) == 1) & (F.col("pos") <= k),
                    F.col("precision_at_pos"),
                ).otherwise(0)
            ).alias(f"_ap_num_{k}"),
            # Precision@K: sum(label where pos<=K) / K
            F.sum(
                F.when(F.col("pos") <= k, F.col(label_col)).otherwise(0)
            ).alias(f"_prec_num_{k}"),
            # Recall@K: sum(label where pos<=K) / total_rel
            # (same numerator as precision, different denominator)
            # nDCG@K: sum(dcg_contrib where pos<=K) / iDCG@K
            F.sum(
                F.when(F.col("pos") <= k, F.col("dcg_contrib")).otherwise(0)
            ).alias(f"_dcg_{k}"),
        ])

    query_metrics = (
        df.filter(F.col("total_rel") > 0)
        .groupBy(*group_cols, item_col.join([]) if False else F.lit(0).alias("_dummy"))
        # We group by query (group_cols), with additional columns carried
    )
    # Actually, simpler approach: aggregate per query first
    base_aggs = [
        F.first("total_rel").alias("total_rel"),
        F.first("first_rel_pos").alias("first_rel_pos"),
        F.count("*").alias("n_items"),
    ]

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
        # AP@K
        per_query_with_k = per_query.withColumn(
            f"ap_{k}",
            F.col(f"_ap_num_{k}") / F.col("total_rel"),
        ).withColumn(
            f"precision_{k}",
            F.col(f"_prec_num_{k}") / F.lit(k),
        ).withColumn(
            f"recall_{k}",
            F.col(f"_prec_num_{k}") / F.col("total_rel"),
        ).withColumn(
            f"mrr_{k}",
            F.when(
                F.col("first_rel_pos") <= k,
                F.lit(1.0) / F.col("first_rel_pos"),
            ).otherwise(0.0),
        )

        # iDCG@K: sum_{i=1}^{min(total_rel,K)} 1/log2(i+1)
        # Pre-compute as UDF-free approach: use a lookup
        # For simplicity, compute iDCG in the aggregation
        # iDCG@K = sum of 1/log2(i+1) for i in 1..min(R,K)
        # We'll approximate using the formula with min(total_rel, K)
        min_rk = F.least(F.col("total_rel"), F.lit(k))
        # Build iDCG lookup via SQL expression
        # For efficiency, compute iDCG@K in Python from collected data
        per_query_with_k = per_query_with_k.withColumn(
            f"min_rk_{k}", min_rk
        )

        per_query = per_query_with_k

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
            "micro_avg": {},
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
        overall[f"mrr@{k}"] = float(collected[f"mrr_{k}"].mean())
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
        _micro_average,
    )

    # Re-enrich in pandas for per-product decomposition
    # The data is already filtered to queries with positives
    enriched_contrib = _enrich_with_contributions(
        enriched_pd, k_values, group_cols=group_cols,
        score_col=score_col, label_col=label_col,
    )
    rel = enriched_contrib[enriched_contrib[label_col] == 1]

    per_product = {}
    product_query_counts = {}
    if len(rel) > 0:
        per_product, product_query_counts = _aggregate_per_dimension(
            rel, [item_col], k_values
        )

    # Per-segment
    per_segment = {}
    segment_query_counts = {}
    segment_columns = eval_params.get("segment_columns", [])
    has_segment = any(
        col in enriched_pd.columns for col in segment_columns
    )
    if has_segment and len(collected) > 0:
        # Use the first available segment column
        import pandas as pd
        for seg_col in segment_columns:
            if seg_col not in enriched_pd.columns:
                continue
            # Per-query metrics with segment info
            query_seg = enriched_pd.groupby(group_cols).first()[[seg_col]].reset_index()
            query_metrics_df = collected.merge(query_seg, on=group_cols, how="left")

            metric_keys = [c for c in collected.columns if c.startswith(("ap_", "ndcg_", "mrr_", "precision_", "recall_"))]
            for seg_val, seg_group in query_metrics_df.groupby(seg_col, sort=True):
                seg_metrics = {}
                for k in k_values:
                    seg_metrics[f"map@{k}"] = float(seg_group[f"ap_{k}"].mean())
                    seg_metrics[f"ndcg@{k}"] = float(seg_group[f"ndcg_{k}"].mean())
                    seg_metrics[f"mrr@{k}"] = float(seg_group[f"mrr_{k}"].mean())
                    seg_metrics[f"precision@{k}"] = float(seg_group[f"precision_{k}"].mean())
                    seg_metrics[f"recall@{k}"] = float(seg_group[f"recall_{k}"].mean())
                per_segment[str(seg_val)] = seg_metrics
                segment_query_counts[str(seg_val)] = len(seg_group)
            break  # Only use first segment column for main per_segment

    # Per-product-segment
    per_product_segment = {}
    product_segment_query_counts = {}
    if has_segment and len(rel) > 0:
        for seg_col in segment_columns:
            if seg_col in rel.columns:
                per_product_segment, product_segment_query_counts = (
                    _aggregate_per_dimension(rel, [item_col, seg_col], k_values)
                )
                break

    # Macro/micro averages
    macro_avg = {}
    micro_avg = {}
    macro_avg["by_product"] = _macro_average(per_product)
    micro_avg["by_product"] = _micro_average(per_product, product_query_counts)

    if per_segment:
        macro_avg["by_segment"] = _macro_average(per_segment)
        micro_avg["by_segment"] = _micro_average(per_segment, segment_query_counts)
    if per_product_segment:
        macro_avg["by_product_segment"] = _macro_average(per_product_segment)
        micro_avg["by_product_segment"] = _micro_average(
            per_product_segment, product_segment_query_counts
        )

    result = {
        "overall": overall,
        "per_product": per_product,
        "per_segment": per_segment,
        "per_product_segment": per_product_segment,
        "macro_avg": macro_avg,
        "micro_avg": micro_avg,
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

    Collects the eval_predictions to pandas (post-aggregation, the data
    is at customer x product granularity for a single snap_date, which
    is manageable) and delegates to the pandas report generation logic.
    """
    from recsys_tfb.pipelines.evaluation.nodes_pandas import (
        generate_report as generate_report_pandas,
    )

    eval_pd = eval_predictions.toPandas()
    return generate_report_pandas(
        eval_pd, evaluation_metrics, parameters, baseline_metrics
    )
