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
