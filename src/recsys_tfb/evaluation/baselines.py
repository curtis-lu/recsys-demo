"""Popularity baseline for evaluation — Spark.

Replaces each ``eval_predictions`` row's model score with the product's
historical purchase count (sum of positive labels in a pre-snap_date
window), yielding a global-popularity ranking aligned row-for-row with the
model's evaluation set.
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def compute_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(snap_date, prod_name)`` historical purchase count.

    For each ``S`` in ``snap_dates``, count ``sum(label)`` grouped by item
    over ``label_table`` rows whose time falls in
    ``[S - lookback_months, S)``. When a window is empty, fall back to the
    full table (with a warning — the baseline may then have leakage).

    Returns a DataFrame with columns ``(time_col, item_col, score_col)``
    where ``score_col`` holds the count and ``time_col`` is the string ``S``.
    """
    if not snap_dates:
        raise ValueError(
            "compute_purchase_counts requires a non-empty snap_dates list"
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    ts = F.to_date(F.col(time_col))
    per_snap: list[SparkDataFrame] = []
    for s in snap_dates:
        upper = pd.Timestamp(s)
        lower = upper - pd.DateOffset(months=lookback_months)
        window = label_table.filter(
            (ts >= F.lit(str(lower.date())))
            & (ts < F.lit(str(upper.date())))
        )
        if window.limit(1).count() == 0:
            logger.warning(
                "No historical data in [%s, %s) for snap_date=%s; falling "
                "back to full label_table — baseline may have leakage.",
                lower.date(), upper.date(), s,
            )
            window = label_table
        counts = (
            window.groupBy(item_col)
            .agg(F.sum(F.col(label_col)).cast("double").alias(score_col))
            .withColumn(time_col, F.lit(str(upper.date())))
        )
        per_snap.append(counts.select(time_col, item_col, score_col))

    result = per_snap[0]
    for df in per_snap[1:]:
        result = result.unionByName(df)
    return result


def build_baseline_frame(
    eval_predictions: SparkDataFrame,
    purchase_counts: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Replace ``eval_predictions``' model score with the popularity count.

    Drops the model's ``score`` (and ``rank`` / ``model_version`` if present),
    casts ``time_col`` to string for a type-safe join, then left-joins the
    per-``(snap_date, prod_name)`` count as the new ``score``. Products with
    no count get ``score = 0``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    drop_cols = [
        c for c in (score_col, rank_col, "model_version")
        if c in eval_predictions.columns
    ]
    base = eval_predictions.drop(*drop_cols).withColumn(
        time_col, F.to_date(F.col(time_col)).cast("string")
    )
    return base.join(
        F.broadcast(purchase_counts), on=[time_col, item_col], how="left"
    ).fillna(0, subset=[score_col])
