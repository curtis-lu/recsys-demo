"""Restrict A/B compare predictions to the common (entity × item) universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``, or
   persisted with ``enriched_eval_predictions`` under ``--compare-only``);
   restrict keeps it unchanged.
B side: takes A's label, joined on the identity columns, so both sides are
   scored against the same ground truth in every run mode (ADR-0020 bug 7).
   A label column B brings with it is dropped first: ``enriched_eval_predictions``
   and ``training_eval_predictions`` both land with one, frozen at whatever
   ``label_table`` said when B was persisted.

   Why A's label and not a fresh ``label_table`` join: A's label is not always
   the current ``label_table`` either. ``--post-training`` keeps the label
   stored with the training predictions on purpose, and ``--compare-only``
   reads the one persisted by the standard run. Re-joining ``label_table`` for
   B alone gives the two sides different answers in both of those modes;
   re-joining it for both makes the comparison disagree with the main report
   of the same run. Copying A's makes "same answer" hold by construction. The
   cost: a B row whose (entity, item) A did not score has no answer to copy
   and counts as 0 — which only happens when the two sides' candidate sets
   are asymmetric.

Re-ranks both sides within the query group — ``[time] + entity``, every
column of ``schema.entity`` — because the candidate set just shrank. That is
the same grouping ``compute_test_mAP_spark`` ranks by, so the metrics the
comparison report shows are the metrics the main line computes.

Also returns the ``CommonUniverse`` it restricted by. Coverage reads its item
sets from there rather than collecting them a second time (ADR-0020 bug 14).
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.alignment import CommonUniverse, common_universe
from recsys_tfb.evaluation.metrics_spark import rank_within_query


def restrict_to_common(
    a: SparkDataFrame,
    b: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, CommonUniverse]:
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    query_group_cols = [time_col, *entity_cols]

    universe = common_universe(a, b, entity_cols, item_col)
    common_entities, common_items = universe.common_entities, universe.common_items

    spark = a.sparkSession
    item_df = spark.createDataFrame([(i,) for i in common_items], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        # No ``F.broadcast`` on the entity side. That hint does not advise
        # Spark, it overrides ``spark.sql.autoBroadcastJoinThreshold`` — so on
        # an entity universe too large to broadcast it still forces the driver
        # to assemble the whole table and ship a copy to every executor. Let
        # Spark pick from the real size. ``left_semi`` restricts without adding
        # columns, so ``df``'s schema is untouched. The item join keeps its
        # hint: that universe is bounded by config. See
        # ``alignment.common_universe`` for the full rule.
        df = df.join(common_entities, on=entity_cols, how="left_semi")
        df = df.join(F.broadcast(item_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(df, query_group_cols, score_col, item_col)
        return df.withColumnRenamed("pos", rank_col)

    a_common = _restrict_and_rank(a)
    b_common = _restrict_and_rank(b)

    if label_col in b_common.columns:
        b_common = b_common.drop(label_col)
    # Raw ``a``, not ``a_common``: b_common is already restricted, so the join
    # only picks shared keys, and A's re-ranking need not run a second time.
    a_labels = a.select(*identity_cols, label_col)
    b_common = b_common.join(a_labels, on=identity_cols, how="left").fillna({label_col: 0})

    return a_common, b_common, universe
