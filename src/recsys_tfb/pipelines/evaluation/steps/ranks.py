"""The ``rank`` column of the evaluated predictions (Spark).

Whether a rank is computed or the one the predictions carry is trusted is
decided in ``prepare_eval_data``; this module holds how it is computed.
"""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.evaluation.metrics_spark import rank_within_query


def rank_within_query_group(frame: SparkDataFrame, schema: dict) -> SparkDataFrame:
    """``frame`` with ``schema["rank"]`` added: 1-based within each query
    group, by score descending, ties by item ascending, the same tie-break
    inference publishes ``rank`` with.

    BIGINT, the type ``ranked_predictions`` declares for ``rank``: both run
    modes write the same ``enriched_eval_predictions``, whose schema is fixed
    by the first write and never cast afterwards, so ``row_number()``'s INT
    would be a type conflict on whichever mode writes second (ADR-0020
    bug 15).
    """
    rank_col = schema["rank"]
    ranked = rank_within_query(
        frame, schema["query_group_columns"], schema["score"], schema["item"],
        schema.get("event", []),
    )
    return ranked.withColumn(rank_col, F.col("pos").cast("bigint")).drop("pos")
