"""Ranking metrics for model evaluation.

Provides single-query metric functions and an aggregate compute_all_metrics entry point.
"""

import logging
from typing import Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-query metric functions
# ---------------------------------------------------------------------------


def compute_ap(y_true: np.ndarray, y_score: np.ndarray) -> Optional[float]:
    """Compute Average Precision for a single query.

    Returns None if there are no positive labels (AP is undefined).
    """
    if np.sum(y_true) == 0:
        return None

    order = np.argsort(-y_score)
    y_sorted = y_true[order]

    cumsum = np.cumsum(y_sorted)
    positions = np.arange(1, len(y_sorted) + 1)
    precisions = cumsum / positions

    ap = np.sum(precisions * y_sorted) / np.sum(y_true)
    return float(ap)


def compute_ndcg(
    y_true: np.ndarray, y_score: np.ndarray, k: Optional[int] = None
) -> float:
    """Compute Normalized Discounted Cumulative Gain for a single query.

    Args:
        y_true: Relevance labels (binary or graded).
        y_score: Predicted scores.
        k: If provided, only consider the top k items.

    Returns 0.0 if ideal DCG is 0 (no positives).
    """
    order = np.argsort(-y_score)
    y_sorted = y_true[order]

    if k is not None:
        y_sorted = y_sorted[:k]

    # DCG
    positions = np.arange(1, len(y_sorted) + 1)
    discounts = np.log2(positions + 1)
    dcg = np.sum(y_sorted / discounts)

    # Ideal DCG
    ideal_sorted = np.sort(y_true)[::-1]
    if k is not None:
        ideal_sorted = ideal_sorted[:k]
    ideal_positions = np.arange(1, len(ideal_sorted) + 1)
    ideal_discounts = np.log2(ideal_positions + 1)
    idcg = np.sum(ideal_sorted / ideal_discounts)

    if idcg == 0:
        return 0.0

    return float(dcg / idcg)


def compute_precision_at_k(
    y_true: np.ndarray, y_score: np.ndarray, k: int
) -> float:
    """Compute Precision@K for a single query."""
    order = np.argsort(-y_score)
    y_sorted = y_true[order]
    top_k = y_sorted[:k]
    return float(np.sum(top_k) / k)


def compute_recall_at_k(
    y_true: np.ndarray, y_score: np.ndarray, k: int
) -> float:
    """Compute Recall@K for a single query.

    Returns 0.0 if there are no positive labels.
    """
    total_positives = np.sum(y_true)
    if total_positives == 0:
        return 0.0

    order = np.argsort(-y_score)
    y_sorted = y_true[order]
    top_k = y_sorted[:k]
    return float(np.sum(top_k) / total_positives)


def compute_mrr(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Compute Reciprocal Rank for a single query.

    Returns 0.0 if there are no positive labels.
    """
    order = np.argsort(-y_score)
    y_sorted = y_true[order]

    positive_positions = np.where(y_sorted > 0)[0]
    if len(positive_positions) == 0:
        return 0.0

    first_pos = positive_positions[0]
    return float(1.0 / (first_pos + 1))


# ---------------------------------------------------------------------------
# Helpers for per-query aggregation
# ---------------------------------------------------------------------------


def _compute_query_metrics(
    y_true: np.ndarray, y_score: np.ndarray, k_values: list[int]
) -> Optional[dict]:
    """Compute all metrics for a single query. Returns None if no positives."""
    ap = compute_ap(y_true, y_score)
    if ap is None:
        return None

    metrics: dict = {
        "map": ap,
        "ndcg": compute_ndcg(y_true, y_score),
        "mrr": compute_mrr(y_true, y_score),
    }
    for k in k_values:
        metrics[f"ndcg@{k}"] = compute_ndcg(y_true, y_score, k=k)
        metrics[f"precision@{k}"] = compute_precision_at_k(y_true, y_score, k)
        metrics[f"recall@{k}"] = compute_recall_at_k(y_true, y_score, k)

    return metrics


def _aggregate_metric_lists(
    metric_lists: list[dict],
) -> dict:
    """Average a list of per-query metric dicts (ignoring internal _ keys)."""
    if not metric_lists:
        return {}
    keys = [k for k in metric_lists[0].keys() if not k.startswith("_")]
    return {k: float(np.mean([m[k] for m in metric_lists])) for k in keys}


# ---------------------------------------------------------------------------
# compute_all_metrics — main entry point
# ---------------------------------------------------------------------------


def _resolve_k_values(
    k_values: list[Union[int, str]], n_products: int
) -> list[int]:
    """Resolve k_values containing "all" to actual integer values.

    Args:
        k_values: List of integers or "all" strings.
        n_products: Total number of unique products (used to resolve "all").

    Returns:
        List of unique, sorted integer K values.
    """
    resolved = []
    for k in k_values:
        if isinstance(k, str) and k == "all":
            resolved.append(n_products)
        else:
            resolved.append(int(k))
    return sorted(set(resolved))


def compute_all_metrics(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    k_values: list[Union[int, str]] | None = None,
) -> dict:
    """Compute ranking metrics across multiple dimensions.

    Args:
        predictions: DataFrame with columns [snap_date, cust_id, prod_code, score, rank].
        labels: DataFrame with columns [snap_date, cust_id, prod_name, label].
            Optionally includes cust_segment_typ for segment-level metrics.
        k_values: K values for precision@K, recall@K, nDCG@K.
            Supports "all" which resolves to total product count N.
            Defaults to [5, "all"].

    Returns:
        Dict with keys: overall, per_product, per_segment, per_product_segment,
        macro_avg, micro_avg, n_queries, n_excluded_queries.
    """
    if k_values is None:
        k_values = [5, "all"]

    # Rename prod_name → prod_code in labels for join
    labels_renamed = labels.rename(columns={"prod_name": "prod_code"})

    # Join predictions with labels
    merged = predictions.merge(
        labels_renamed[["snap_date", "cust_id", "prod_code", "label"]],
        on=["snap_date", "cust_id", "prod_code"],
        how="inner",
    )

    # Resolve "all" in k_values to actual product count
    n_products = merged["prod_code"].nunique()
    k_values = _resolve_k_values(k_values, n_products)

    # Carry segment column if present
    has_segment = "cust_segment_typ" in labels.columns
    if has_segment:
        seg_map = labels_renamed[["snap_date", "cust_id", "cust_segment_typ"]].drop_duplicates()
        merged = merged.merge(seg_map, on=["snap_date", "cust_id"], how="left")

    # Group by query
    query_groups = merged.groupby(["snap_date", "cust_id"])

    # Collect per-query metrics, also tagged with product/segment info
    all_query_metrics: list[dict] = []
    n_excluded = 0

    for (snap_date, cust_id), group in query_groups:
        y_true = group["label"].values.astype(float)
        y_score = group["score"].values.astype(float)

        qm = _compute_query_metrics(y_true, y_score, k_values)
        if qm is None:
            n_excluded += 1
            continue

        qm["_snap_date"] = snap_date
        qm["_cust_id"] = cust_id
        qm["_products"] = list(group["prod_code"].values)

        if has_segment:
            qm["_segment"] = group["cust_segment_typ"].iloc[0]

        all_query_metrics.append(qm)

    n_queries = len(query_groups)

    # Overall
    overall = _aggregate_metric_lists(all_query_metrics)

    # Per-product: filter queries that include each product, recompute
    products = sorted(merged["prod_code"].unique())
    per_product, product_query_counts = _compute_per_dimension(
        merged, "prod_code", products, k_values
    )

    # Per-segment
    per_segment: dict = {}
    segment_query_counts: dict = {}
    if has_segment:
        segments = sorted(merged["cust_segment_typ"].dropna().unique())
        per_segment, segment_query_counts = _compute_per_dimension_by_query_filter(
            merged, "cust_segment_typ", segments, k_values
        )

    # Per-product-segment
    per_product_segment: dict = {}
    product_segment_query_counts: dict = {}
    if has_segment:
        merged["_prod_seg"] = merged["prod_code"] + "_" + merged["cust_segment_typ"]
        prod_seg_keys = sorted(merged["_prod_seg"].dropna().unique())
        per_product_segment, product_segment_query_counts = _compute_per_dimension(
            merged, "_prod_seg", prod_seg_keys, k_values
        )

    # Macro and micro averages
    macro_avg: dict = {}
    micro_avg: dict = {}

    macro_avg["by_product"] = _macro_average(per_product)
    micro_avg["by_product"] = _micro_average(per_product, product_query_counts)

    if has_segment:
        macro_avg["by_segment"] = _macro_average(per_segment)
        micro_avg["by_segment"] = _micro_average(per_segment, segment_query_counts)
        macro_avg["by_product_segment"] = _macro_average(per_product_segment)
        micro_avg["by_product_segment"] = _micro_average(
            per_product_segment, product_segment_query_counts
        )

    return {
        "overall": overall,
        "per_product": per_product,
        "per_segment": per_segment,
        "per_product_segment": per_product_segment,
        "macro_avg": macro_avg,
        "micro_avg": micro_avg,
        "n_queries": n_queries,
        "n_excluded_queries": n_excluded,
    }


def _compute_per_dimension(
    merged: pd.DataFrame,
    dim_col: str,
    dim_values: list,
    k_values: list[int],
) -> tuple[dict, dict]:
    """Compute metrics separately for each value of dim_col.

    For per-product analysis: groups the merged data by dim_col, then within each
    group computes per-query metrics using (snap_date, cust_id) as query groups.
    """
    per_dim: dict = {}
    query_counts: dict = {}

    for val in dim_values:
        subset = merged[merged[dim_col] == val]
        query_groups = subset.groupby(["snap_date", "cust_id"])

        metrics_list = []
        for _, group in query_groups:
            y_true = group["label"].values.astype(float)
            y_score = group["score"].values.astype(float)
            qm = _compute_query_metrics(y_true, y_score, k_values)
            if qm is not None:
                metrics_list.append(qm)

        per_dim[val] = _aggregate_metric_lists(metrics_list)
        query_counts[val] = len(metrics_list)

    return per_dim, query_counts


def _compute_per_dimension_by_query_filter(
    merged: pd.DataFrame,
    dim_col: str,
    dim_values: list,
    k_values: list[int],
) -> tuple[dict, dict]:
    """Compute metrics by filtering queries based on a customer-level attribute.

    For segment analysis: first identify which queries belong to each segment,
    then compute metrics over all products for those queries.
    """
    per_dim: dict = {}
    query_counts: dict = {}

    for val in dim_values:
        subset = merged[merged[dim_col] == val]
        query_groups = subset.groupby(["snap_date", "cust_id"])

        metrics_list = []
        for _, group in query_groups:
            y_true = group["label"].values.astype(float)
            y_score = group["score"].values.astype(float)
            qm = _compute_query_metrics(y_true, y_score, k_values)
            if qm is not None:
                metrics_list.append(qm)

        per_dim[val] = _aggregate_metric_lists(metrics_list)
        query_counts[val] = len(metrics_list)

    return per_dim, query_counts


def _macro_average(per_dim: dict) -> dict:
    """Unweighted mean of metrics across dimension values."""
    non_empty = [v for v in per_dim.values() if v]
    if not non_empty:
        return {}
    keys = non_empty[0].keys()
    return {k: float(np.mean([m[k] for m in non_empty])) for k in keys}


def _micro_average(per_dim: dict, query_counts: dict) -> dict:
    """Query-count-weighted average of metrics across dimension values."""
    non_empty_keys = [k for k, v in per_dim.items() if v]
    if not non_empty_keys:
        return {}

    total_queries = sum(query_counts[k] for k in non_empty_keys)
    if total_queries == 0:
        return {}

    metric_keys = per_dim[non_empty_keys[0]].keys()
    result = {}
    for mk in metric_keys:
        weighted_sum = sum(
            per_dim[k][mk] * query_counts[k] for k in non_empty_keys
        )
        result[mk] = float(weighted_sum / total_queries)
    return result
