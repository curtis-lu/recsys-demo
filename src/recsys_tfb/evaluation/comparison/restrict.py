"""Restrict A/B compare predictions to the common (cust × prod) universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``);
   restrict keeps the existing label column unchanged.
B side: has no ``label``; restrict does a LEFT JOIN on ``label_table`` and
   fills missing with 0 — mirroring ``prepare_eval_data``'s convention so
   "both sides are scored against the same ground truth".

Re-ranks both sides within ``[snap_date, cust_id]`` because the candidate
set just shrank.
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.alignment import common_universe
from recsys_tfb.evaluation.metrics_spark import rank_within_query


def restrict_to_common(
    a: SparkDataFrame,
    b: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame]:
    schema = get_schema(parameters)
    cust_col = schema["entity"][0]
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    common_cust, common_prod = common_universe(a, b, cust_col, item_col)

    spark = a.sparkSession
    cust_df = spark.createDataFrame([(c,) for c in common_cust], [cust_col])
    prod_df = spark.createDataFrame([(p,) for p in common_prod], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        df = df.join(F.broadcast(cust_df), on=cust_col, how="inner")
        df = df.join(F.broadcast(prod_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(df, [time_col, cust_col], score_col)
        return df.withColumnRenamed("pos", rank_col)

    a_common = _restrict_and_rank(a)
    b_common = _restrict_and_rank(b)

    if label_col not in b_common.columns:
        labels = (
            label_table.select(*identity_cols, label_col)
            .join(F.broadcast(prod_df), on=item_col, how="inner")
        )
        b_common = b_common.join(labels, on=identity_cols, how="left").fillna({label_col: 0})

    return a_common, b_common
