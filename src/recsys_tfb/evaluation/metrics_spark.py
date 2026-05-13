"""Spark-native ranking metrics computation.

Pipeline:
    eval_predictions (SparkDataFrame, joined predictions + labels)
      → rank_within_query        (Window: pos)
      → add_query_aggregates     (Window: total_rel; caller filters total_rel > 0)
      → add_row_contributions    (cum_rel, prec_at_pos, dcg_term,
                                  top_k@K, ap_contrib@K, ndcg_contrib@K)
      → aggregate_overall            (collect: small dict)
      → aggregate_by_row_dimension   (collect: per-product / per-product-segment)
      → aggregate_by_query_dimension (collect: per-segment, equal customer weight)
      → macro_average (python dict op, reused from metrics.py)

All row-level work stays in Spark; only small aggregations are collected.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import _macro_average, _resolve_k_values

logger = logging.getLogger(__name__)


def rank_within_query(
    df: SparkDataFrame, group_cols: list[str], score_col: str
) -> SparkDataFrame:
    """Add `pos` column: 1-based rank within each query, ordered by score desc."""
    w = Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())
    return df.withColumn("pos", F.row_number().over(w))


def add_query_aggregates(
    df: SparkDataFrame, group_cols: list[str], label_col: str
) -> SparkDataFrame:
    """Add `total_rel`: sum of label per query. Caller filters total_rel > 0 later."""
    w = Window.partitionBy(*group_cols)
    return df.withColumn("total_rel", F.sum(F.col(label_col)).over(w))


def add_row_contributions(
    df: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> SparkDataFrame:
    """Add per-row contribution columns for ranking metrics.

    Requires upstream columns: pos, total_rel.

    Adds (always):
        cum_rel:      cumulative positive count up to & including this position
        prec_at_pos:  cum_rel / pos
        dcg_term:     label / log2(pos + 1)

    Adds (per K in k_values):
        top_k@{K}:        1.0 if pos <= K else 0.0
        ap_contrib@{K}:   prec_at_pos * label * top_k@{K}
        ndcg_contrib@{K}: dcg_term * top_k@{K} / iDCG@{K}    (added in Task 5)
    """
    w_cum = (
        Window.partitionBy(*group_cols)
        .orderBy(F.col("pos"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn("cum_rel", F.sum(F.col(label_col)).over(w_cum))
    df = df.withColumn("prec_at_pos", F.col("cum_rel") / F.col("pos"))
    df = df.withColumn(
        "dcg_term", F.col(label_col) / F.log2(F.col("pos") + F.lit(1))
    )

    for k in k_values:
        df = df.withColumn(
            f"top_k@{k}", (F.col("pos") <= F.lit(k)).cast("double")
        )
        df = df.withColumn(
            f"ap_contrib@{k}",
            F.col("prec_at_pos") * F.col(label_col) * F.col(f"top_k@{k}"),
        )
        # iDCG@K = sum_{i=1}^{min(total_rel, K)} 1 / log2(i + 1)
        # Computed inline via Spark's aggregate(sequence(...)) higher-order function.
        # No UDF, no collect-and-broadcast.
        idcg_at_k = F.aggregate(
            F.sequence(F.lit(1), F.least(F.col("total_rel"), F.lit(k))),
            F.lit(0.0),
            lambda acc, i: acc + F.lit(1.0) / F.log2(i.cast("double") + F.lit(1.0)),
        )
        df = df.withColumn(
            f"ndcg_contrib@{k}",
            F.when(
                idcg_at_k > 0,
                F.col("dcg_term") * F.col(f"top_k@{k}") / idcg_at_k,
            ).otherwise(F.lit(0.0)),
        )
    return df


def aggregate_overall(
    enriched: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-query metrics → cross-query mean.

    Per-query formulas:
        ap@K        = sum(ap_contrib@K) / total_rel
        ndcg@K      = sum(ndcg_contrib@K)              -- already iDCG-normalized
        precision@K = sum(label * top_k@K) / K
        recall@K    = sum(label * top_k@K) / total_rel

    Overall metric@K = mean across queries.
    Returns a flat dict {"map@K": ..., "ndcg@K": ..., "precision@K": ..., "recall@K": ...}.
    """
    per_query_aggs = [F.first("total_rel").alias("total_rel")]
    for k in k_values:
        per_query_aggs.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(f"ndcg_contrib@{k}").alias(f"_ndcg_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )
    per_query = enriched.groupBy(*group_cols).agg(*per_query_aggs)

    for k in k_values:
        per_query = (
            per_query.withColumn(
                f"ap_{k}", F.col(f"_ap_sum_{k}") / F.col("total_rel")
            )
            .withColumn(f"ndcg_{k}", F.col(f"_ndcg_sum_{k}"))
            .withColumn(f"precision_{k}", F.col(f"_hits_{k}") / F.lit(k))
            .withColumn(f"recall_{k}", F.col(f"_hits_{k}") / F.col("total_rel"))
        )

    final_aggs = []
    for k in k_values:
        final_aggs.extend(
            [
                F.mean(f"ap_{k}").alias(f"map@{k}"),
                F.mean(f"ndcg_{k}").alias(f"ndcg@{k}"),
                F.mean(f"precision_{k}").alias(f"precision@{k}"),
                F.mean(f"recall_{k}").alias(f"recall@{k}"),
            ]
        )
    row = per_query.agg(*final_aggs).collect()[0].asDict()
    return {k: float(v) for k, v in row.items()}


def aggregate_by_query_dimension(
    enriched: SparkDataFrame,
    dim_col: str,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-segment metrics with equal customer weighting.

    Two-stage:
        1. groupBy(group_cols).agg(per-query formulas + first(dim_col))  -- one row per query
        2. groupBy(dim_col).mean(per-query metrics)                       -- equal customer weight

    Matches the pandas per_segment semantic (equal customer weight, not row-level mean).
    """
    per_query_aggs = [
        F.first("total_rel").alias("total_rel"),
        F.first(dim_col).alias(dim_col),
    ]
    for k in k_values:
        per_query_aggs.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(f"ndcg_contrib@{k}").alias(f"_ndcg_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )
    per_query = enriched.groupBy(*group_cols).agg(*per_query_aggs)

    metric_aliases = []
    for k in k_values:
        per_query = (
            per_query.withColumn(
                f"map@{k}", F.col(f"_ap_sum_{k}") / F.col("total_rel")
            )
            .withColumn(f"ndcg@{k}", F.col(f"_ndcg_sum_{k}"))
            .withColumn(f"precision@{k}", F.col(f"_hits_{k}") / F.lit(k))
            .withColumn(f"recall@{k}", F.col(f"_hits_{k}") / F.col("total_rel"))
        )
        metric_aliases.extend(
            [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"]
        )

    final_aggs = [F.mean(m).alias(m) for m in metric_aliases]
    rows = per_query.groupBy(dim_col).agg(*final_aggs).collect()

    result: dict = {}
    for row in rows:
        raw_key = row[dim_col]
        key = raw_key if isinstance(raw_key, str) else str(raw_key)
        result[key] = {m: float(row[m]) for m in metric_aliases}
    return result


def aggregate_by_row_dimension(
    enriched: SparkDataFrame,
    dim_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-product / per-product-segment metrics.

    Filters to label=1 rows, groupBy(dim_cols), takes mean of contribution columns.

    Returns {dim_key: {metric_name: value}}.
    dim_key is the dim column value (stringified) for single-column groupings,
    or '_'.join(values) for multi-column groupings.

    Per-dimension formulas (over label=1 rows in the dim):
        map@K       = mean(ap_contrib@K)
        ndcg@K      = mean(ndcg_contrib@K)
        precision@K = mean(top_k@K)        -- same value as recall@K (matches pandas semantic)
        recall@K    = mean(top_k@K)
    """
    rel = enriched.filter(F.col(label_col) == 1)
    aggs = []
    for k in k_values:
        aggs.extend(
            [
                F.mean(f"ap_contrib@{k}").alias(f"map@{k}"),
                F.mean(f"ndcg_contrib@{k}").alias(f"ndcg@{k}"),
                F.mean(f"top_k@{k}").alias(f"hit_rate@{k}"),
            ]
        )
    rows = rel.groupBy(*dim_cols).agg(*aggs).collect()

    result: dict = {}
    for row in rows:
        if len(dim_cols) == 1:
            raw_key = row[dim_cols[0]]
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
        else:
            key = "_".join(str(row[c]) for c in dim_cols)
        metrics: dict = {}
        for k in k_values:
            hit_rate = float(row[f"hit_rate@{k}"])
            metrics[f"map@{k}"] = float(row[f"map@{k}"])
            metrics[f"ndcg@{k}"] = float(row[f"ndcg@{k}"])
            metrics[f"precision@{k}"] = hit_rate
            metrics[f"recall@{k}"] = hit_rate
        result[key] = metrics
    return result
