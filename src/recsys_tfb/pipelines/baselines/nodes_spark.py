"""Baselines pipeline nodes — Spark backend."""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def compute_baselines(
    label_table: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Compute baseline predictions using Spark SQL.

    Supports global_popularity and segment_popularity baseline types.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    eval_params = parameters.get("evaluation", {})
    baseline_config = eval_params.get("baseline", {})
    baseline_type = baseline_config.get("type", "global_popularity")
    snap_date = str(eval_params["snap_date"])

    # Historical data: all rows before snap_date
    historical = label_table.filter(F.col(time_col).cast("string") < snap_date)

    # Fallback if no historical data
    hist_count = historical.count()
    if hist_count == 0:
        logger.warning(
            "No historical data before snap_date=%s, using all data. "
            "Baseline may have leakage.",
            snap_date,
        )
        historical = label_table

    # Customer list at snap_date
    snap_labels = label_table.filter(F.col(time_col).cast("string") == snap_date)
    if snap_labels.count() == 0:
        logger.warning(
            "No labels at snap_date=%s, using all customers", snap_date
        )
        snap_labels = label_table
    customers = snap_labels.select(entity_cols[0]).distinct()

    # Product list
    products = label_table.select(item_col).distinct()

    if baseline_type == "segment_popularity":
        segment_column = baseline_config.get("segment_column", "cust_segment_typ")

        # Per-(segment, product) positive rate
        rates = (
            historical.groupBy(segment_column, item_col)
            .agg(F.mean(label_col).alias(score_col))
        )

        # Customer -> segment mapping
        seg_map = (
            label_table.select(entity_cols[0], segment_column)
            .distinct()
        )

        # Cross join customers x products, then join segment rates
        baseline = (
            customers.join(seg_map, on=entity_cols[0], how="left")
            .crossJoin(products)
            .join(
                rates,
                on=[segment_column, item_col],
                how="left",
            )
            .fillna(0.0, subset=[score_col])
            .drop(segment_column)
        )
    else:
        # Global popularity: per-product positive rate
        rates = (
            historical.groupBy(item_col)
            .agg(F.mean(label_col).alias(score_col))
        )

        # Cross join customers x products, then join rates
        baseline = (
            customers.crossJoin(products)
            .join(rates, on=item_col, how="left")
            .fillna(0.0, subset=[score_col])
        )

    # Add snap_date and rank
    baseline = baseline.withColumn(time_col, F.lit(snap_date))

    group_cols = [time_col] + entity_cols
    window = Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())
    baseline = baseline.withColumn(rank_col, F.row_number().over(window))

    n_rows = baseline.count()
    logger.info(
        "Baseline type=%s, rows=%d",
        baseline_type,
        n_rows,
    )
    return baseline


def compute_baseline_metrics(
    baseline_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics on baseline predictions using Spark SQL.

    Collects the baseline predictions (small table: customers x products)
    to pandas and delegates to the standard metrics computation.
    """
    from recsys_tfb.evaluation.metrics import compute_all_metrics

    eval_params = parameters.get("evaluation", {})
    k_values = eval_params.get("k_values", [5, "all"])

    # Baseline predictions and label_table at snap_date are small enough to collect
    baseline_pd = baseline_predictions.toPandas()
    label_pd = label_table.toPandas()

    metrics = compute_all_metrics(
        predictions=baseline_pd,
        labels=label_pd,
        k_values=k_values,
        parameters=parameters,
    )

    logger.info(
        "Baseline metrics computed: n_queries=%d",
        metrics["n_queries"],
    )
    return metrics
