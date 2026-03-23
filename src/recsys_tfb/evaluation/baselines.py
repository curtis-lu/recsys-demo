"""Baseline generators for model comparison.

Provides global and segment popularity baselines that output DataFrames
matching the ranked_predictions schema (snap_date, cust_id, prod_name, score, rank).
"""

import logging

import pandas as pd

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def generate_global_popularity_baseline(
    label_table: pd.DataFrame,
    snap_date: str,
    customer_ids: list[str],
    products: list[str] | None = None,
    parameters: dict | None = None,
) -> pd.DataFrame:
    """Generate a global popularity baseline.

    Computes overall positive rate per product from historical data (before snap_date)
    and assigns these rates as scores for every customer.

    Args:
        label_table: DataFrame with columns [snap_date, cust_id, prod_name, label].
        snap_date: Target snap_date (YYYYMMDD format). Only data before this date is used.
        customer_ids: List of customer IDs to generate baseline for.
        products: List of product codes. If None, derived from label_table.

    Returns:
        DataFrame with columns [snap_date, cust_id, prod_name, score, rank].
    """
    schema = get_schema(parameters or {})
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    snap_ts = pd.Timestamp(snap_date)
    label_snap = pd.to_datetime(label_table[time_col])
    historical = label_table[label_snap < snap_ts]

    if len(historical) == 0:
        logger.warning(
            "No historical data before snap_date=%s. Using all available data. "
            "Baseline may have leakage.",
            snap_date,
        )
        historical = label_table

    # Compute positive rate per product
    rates = historical.groupby(item_col)[label_col].mean()

    if products is None:
        products = sorted(rates.index.tolist())

    # Match snap_date dtype to label_table for downstream merge compatibility
    snap_value = snap_ts if pd.api.types.is_datetime64_any_dtype(label_table[time_col]) else snap_date

    # Build baseline: same score for all customers
    rows = []
    for cust_id in customer_ids:
        for prod in products:
            score = float(rates.get(prod, 0.0))
            row = {
                time_col: snap_value,
                item_col: prod,
                score_col: score,
            }
            # For single entity, use the first entity column
            row[entity_cols[0]] = cust_id
            rows.append(row)

    baseline = pd.DataFrame(rows)

    # Rank by descending score within each customer
    baseline[rank_col] = baseline.groupby(group_cols)[score_col].rank(
        method="first", ascending=False
    ).astype(int)

    return baseline


def generate_segment_popularity_baseline(
    label_table: pd.DataFrame,
    snap_date: str,
    customer_ids: list[str],
    segment_column: str = "cust_segment_typ",
    customer_segments: pd.Series | None = None,
    products: list[str] | None = None,
    parameters: dict | None = None,
) -> pd.DataFrame:
    """Generate a segment-level popularity baseline.

    Computes positive rate per (segment, product) from historical data and
    assigns segment-specific scores to each customer.

    Args:
        label_table: DataFrame with columns [snap_date, cust_id, prod_name, label, cust_segment_typ].
        snap_date: Target snap_date (YYYYMMDD format).
        customer_ids: List of customer IDs.
        segment_column: Column name for customer segment.
        customer_segments: Series mapping cust_id → segment. If None, derived from label_table.
        products: List of product codes. If None, derived from label_table.

    Returns:
        DataFrame with columns [snap_date, cust_id, prod_name, score, rank].
    """
    schema = get_schema(parameters or {})
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    snap_ts = pd.Timestamp(snap_date)
    label_snap = pd.to_datetime(label_table[time_col])
    historical = label_table[label_snap < snap_ts]

    if len(historical) == 0:
        logger.warning(
            "No historical data before snap_date=%s. Using all available data. "
            "Baseline may have leakage.",
            snap_date,
        )
        historical = label_table

    # Compute positive rate per (segment, product)
    rates = historical.groupby([segment_column, item_col])[label_col].mean()

    if products is None:
        products = sorted(historical[item_col].unique().tolist())

    # Match snap_date dtype to label_table for downstream merge compatibility
    snap_value = snap_ts if pd.api.types.is_datetime64_any_dtype(label_table[time_col]) else snap_date

    # Build customer → segment mapping
    if customer_segments is None:
        seg_map = label_table.drop_duplicates(entity_cols[0]).set_index(entity_cols[0])[segment_column]
    else:
        seg_map = customer_segments

    rows = []
    for cust_id in customer_ids:
        segment = seg_map.get(cust_id, None)
        for prod in products:
            if segment is not None and (segment, prod) in rates.index:
                score = float(rates.loc[(segment, prod)])
            else:
                score = 0.0
            row = {
                time_col: snap_value,
                item_col: prod,
                score_col: score,
            }
            row[entity_cols[0]] = cust_id
            rows.append(row)

    baseline = pd.DataFrame(rows)
    baseline[rank_col] = baseline.groupby(group_cols)[score_col].rank(
        method="first", ascending=False
    ).astype(int)

    return baseline
