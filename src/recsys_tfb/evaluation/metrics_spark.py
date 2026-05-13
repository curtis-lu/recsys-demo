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
