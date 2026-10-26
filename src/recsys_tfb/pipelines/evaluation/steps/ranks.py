"""The ``rank`` column of the evaluated predictions (Spark)."""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.evaluation.metrics_spark import rank_within_query


def rank_within_query_group(frame: SparkDataFrame, schema: dict) -> SparkDataFrame:
    """``frame`` with ``schema["rank"]`` added: 1-based within each query
    group, by score descending, ties by item ascending, the same tie-break
    inference publishes ``rank`` with.

    BIGINT, the type ``ranked_predictions`` declares for ``rank``: the
    schema of ``enriched_eval_predictions`` is fixed by the first write and
    never cast afterwards, so ``row_number()``'s INT would be a type conflict
    on whichever run mode writes second (ADR-0020 bug 15).
    """
    rank_col = schema["rank"]
    ranked = rank_within_query(
        frame, schema["query_group_columns"], schema["score"], schema["item"],
        schema.get("event", []),
    )
    return ranked.withColumn(rank_col, F.col("pos").cast("bigint")).drop("pos")
