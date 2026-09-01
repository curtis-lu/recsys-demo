"""Restrict A/B compare predictions to the common (entity × item) universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``);
   restrict keeps the existing label column unchanged.
B side: has no ``label``; restrict does a LEFT JOIN on ``label_table`` and
   fills missing with 0 — mirroring ``prepare_eval_data``'s convention so
   "both sides are scored against the same ground truth".

Re-ranks both sides within the query group — ``[time] + entity``, every
column of ``schema.entity`` — because the candidate set just shrank. That is
the same grouping ``compute_test_mAP_spark`` ranks by, so the metrics the
comparison report shows are the metrics the main line computes.
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
    entity_cols = schema["entity"]
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    query_group_cols = [time_col, *entity_cols]

    common_entities, common_items = common_universe(a, b, entity_cols, item_col)

    spark = a.sparkSession
    entity_df = spark.createDataFrame(list(common_entities), entity_cols)
    item_df = spark.createDataFrame([(i,) for i in common_items], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        df = df.join(F.broadcast(entity_df), on=entity_cols, how="inner")
        df = df.join(F.broadcast(item_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(df, query_group_cols, score_col)
        return df.withColumnRenamed("pos", rank_col)

    a_common = _restrict_and_rank(a)
    b_common = _restrict_and_rank(b)

    if label_col not in b_common.columns:
        labels = (
            label_table.select(*identity_cols, label_col)
            .join(F.broadcast(item_df), on=item_col, how="inner")
        )
        b_common = b_common.join(labels, on=identity_cols, how="left").fillna({label_col: 0})

    return a_common, b_common
