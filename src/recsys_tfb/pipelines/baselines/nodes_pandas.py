"""Baselines pipeline nodes — pandas backend."""

import logging

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.baselines import (
    generate_global_popularity_baseline,
    generate_segment_popularity_baseline,
)
from recsys_tfb.evaluation.metrics import compute_all_metrics

logger = logging.getLogger(__name__)


def compute_baselines(
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Compute baseline predictions using popularity-based methods.

    Reads baseline config from parameters_evaluation and delegates to the
    existing baseline generators in evaluation/baselines.py.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]

    eval_params = parameters.get("evaluation", {})
    baseline_config = eval_params.get("baseline", {})
    baseline_type = baseline_config.get("type", "global_popularity")
    snap_date = eval_params["snap_date"]

    # Get unique customer IDs and products from label_table at snap_date
    snap_labels = label_table[label_table[time_col].astype(str) == str(snap_date)]
    if len(snap_labels) == 0:
        # Fallback: use all customers from label_table
        logger.warning(
            "No labels at snap_date=%s, using all customers from label_table",
            snap_date,
        )
        snap_labels = label_table

    customer_ids = sorted(snap_labels[entity_cols[0]].unique().tolist())
    products = sorted(label_table[item_col].unique().tolist())

    if baseline_type == "segment_popularity":
        segment_column = baseline_config.get("segment_column", "cust_segment_typ")
        baseline = generate_segment_popularity_baseline(
            label_table=label_table,
            snap_date=snap_date,
            customer_ids=customer_ids,
            segment_column=segment_column,
            products=products,
            parameters=parameters,
        )
    else:
        baseline = generate_global_popularity_baseline(
            label_table=label_table,
            snap_date=snap_date,
            customer_ids=customer_ids,
            products=products,
            parameters=parameters,
        )

    logger.info(
        "Baseline type=%s, customers=%d, products=%d, rows=%d",
        baseline_type,
        len(customer_ids),
        len(products),
        len(baseline),
    )
    return baseline


def compute_baseline_metrics(
    baseline_predictions: pd.DataFrame,
    label_table: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics on baseline predictions."""
    eval_params = parameters.get("evaluation", {})
    k_values = eval_params.get("k_values", [5, "all"])

    metrics = compute_all_metrics(
        predictions=baseline_predictions,
        labels=label_table,
        k_values=k_values,
        parameters=parameters,
    )

    logger.info(
        "Baseline metrics computed: n_queries=%d, overall=%s",
        metrics["n_queries"],
        {k: f"{v:.4f}" for k, v in metrics.get("overall", {}).items()},
    )
    return metrics
