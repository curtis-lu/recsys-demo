"""Baseline generators for model comparison.

Provides global and segment popularity baselines that output DataFrames
matching the ranked_predictions schema (snap_date, cust_id, prod_name, score, rank).
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def generate_global_popularity_baseline(
    label_table: pd.DataFrame,
    snap_date: str,
    customer_ids: list[str],
    products: list[str] | None = None,
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
    snap_ts = pd.Timestamp(snap_date)
    label_snap = pd.to_datetime(label_table["snap_date"])
    historical = label_table[label_snap < snap_ts]

    if len(historical) == 0:
        logger.warning(
            "No historical data before snap_date=%s. Using all available data. "
            "Baseline may have leakage.",
            snap_date,
        )
        historical = label_table

    # Compute positive rate per product
    rates = historical.groupby("prod_name")["label"].mean()

    if products is None:
        products = sorted(rates.index.tolist())

    # Build baseline: same score for all customers
    rows = []
    for cust_id in customer_ids:
        for prod in products:
            score = float(rates.get(prod, 0.0))
            rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": score,
            })

    baseline = pd.DataFrame(rows)

    # Rank by descending score within each customer
    baseline["rank"] = baseline.groupby(["snap_date", "cust_id"])["score"].rank(
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
    snap_ts = pd.Timestamp(snap_date)
    label_snap = pd.to_datetime(label_table["snap_date"])
    historical = label_table[label_snap < snap_ts]

    if len(historical) == 0:
        logger.warning(
            "No historical data before snap_date=%s. Using all available data. "
            "Baseline may have leakage.",
            snap_date,
        )
        historical = label_table

    # Compute positive rate per (segment, product)
    rates = historical.groupby([segment_column, "prod_name"])["label"].mean()

    if products is None:
        products = sorted(historical["prod_name"].unique().tolist())

    # Build customer → segment mapping
    if customer_segments is None:
        seg_map = label_table.drop_duplicates("cust_id").set_index("cust_id")[segment_column]
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
            rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": score,
            })

    baseline = pd.DataFrame(rows)
    baseline["rank"] = baseline.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)

    return baseline
