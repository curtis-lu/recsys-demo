"""Attaching ``label_table`` to the evaluated predictions (Spark).

Whose label is kept, how a missing label counts and when a duplicated key
stops the run are decided in ``prepare_eval_data``; this module holds how
those are computed.
"""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


def labels_in_months_of(
    labels: SparkDataFrame, predictions: SparkDataFrame, time_col: str,
) -> SparkDataFrame:
    """``labels`` kept to the time values ``predictions`` holds: an inner
    join on the distinct times."""
    return labels.join(
        predictions.select(time_col).distinct(), on=time_col, how="inner")


def count_duplicated_keys(
    labels: SparkDataFrame, identity_cols: list[str],
) -> int:
    """How many keys of ``identity_cols`` hold more than one row of
    ``labels``.

    One Spark action: a groupBy over the rows given (a shuffle) ending in a
    count; only the count reaches the driver, whatever the table size.
    """
    return (
        labels.groupBy(*identity_cols)
        .agg(F.count(F.lit(1)).alias("_n_label_rows"))
        .filter(F.col("_n_label_rows") > 1)
        .count()
    )


def left_join_labels_missing_as_zero(
    predictions: SparkDataFrame,
    labels: SparkDataFrame,
    identity_cols: list[str],
    label_col: str,
) -> SparkDataFrame:
    """Every row of ``predictions``, with ``labels`` left-joined on
    ``identity_cols``; a missing ``label_col`` is 0.

    ``label_col`` is cast to INT, the type ``training_eval_predictions``
    declares for it. The two run modes take the label from different tables
    (that one under ``--post-training``, the user-defined ``label_table`` in
    monitoring) and write the same ``enriched_eval_predictions``, whose schema
    is fixed by the first write and never cast afterwards: left uncast, the
    second mode's write is a type conflict (ADR-0020 bug 15).
    """
    joined = predictions.join(labels, on=identity_cols, how="left")
    if label_col not in joined.columns:
        return joined
    return joined.fillna({label_col: 0}).withColumn(
        label_col, F.col(label_col).cast("int"))
