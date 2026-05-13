"""Ranking metrics for model evaluation.

Provides single-query metric functions and an aggregate compute_all_metrics entry point.
"""

import logging
from typing import Optional, Union

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema

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


def compute_ap_at_k(
    y_true: np.ndarray, y_score: np.ndarray, k: int
) -> Optional[float]:
    """Compute Average Precision at K for a single query.

    Only considers the top-K ranked items. Returns None if no positives exist.
    """
    if np.sum(y_true) == 0:
        return None

    order = np.argsort(-y_score)
    y_sorted = y_true[order][:k]

    cumsum = np.cumsum(y_sorted)
    positions = np.arange(1, len(y_sorted) + 1)
    precisions = cumsum / positions

    hits_in_k = np.sum(y_sorted)
    if hits_in_k == 0:
        return 0.0

    ap = np.sum(precisions * y_sorted) / np.sum(y_true)
    return float(ap)


# ---------------------------------------------------------------------------
# Helpers for per-query aggregation
# ---------------------------------------------------------------------------


def _compute_query_metrics(
    y_true: np.ndarray, y_score: np.ndarray, k_values: list[int]
) -> Optional[dict]:
    """Compute all metrics for a single query. Returns None if no positives."""
    if np.sum(y_true) == 0:
        return None

    metrics: dict = {}
    for k in k_values:
        metrics[f"map@{k}"] = compute_ap_at_k(y_true, y_score, k)
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


def _enrich_with_contributions(
    merged: pd.DataFrame,
    k_values: list[int],
    group_cols: list[str] | None = None,
    score_col: str = "score",
    label_col: str = "label",
) -> pd.DataFrame:
    """Add per-row metric contribution columns to the merged DataFrame.

    Sorts by (group_cols, score desc), then computes positional columns
    within each query group. Only rows with label=1 contribute to metrics.

    Returns a copy with added columns: pos, cum_rel, precision,
    and per-K columns: hit@K, map_contrib@K, ndcg_k_contrib@K.
    """
    if group_cols is None:
        group_cols = ["snap_date", "cust_id"]

    df = merged.copy()
    sort_cols = group_cols + [score_col]
    ascending = [True] * len(group_cols) + [False]
    df = df.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)

    grp = df.groupby(group_cols, sort=False)

    # Position within query (1-based)
    df["pos"] = grp.cumcount() + 1
    # Cumulative relevant count
    df["cum_rel"] = grp[label_col].cumsum()

    # --- iDCG lookup per query ---
    # R = total relevant items per query
    r_per_query = grp[label_col].transform("sum")
    # Build iDCG: sum_{i=1}^{R} 1/log2(i+1)
    max_r = int(r_per_query.max()) if len(r_per_query) > 0 else 0
    idcg_table = np.zeros(max_r + 1)
    for i in range(1, max_r + 1):
        idcg_table[i] = idcg_table[i - 1] + 1.0 / np.log2(i + 1)
    df["idcg"] = idcg_table[r_per_query.astype(int).values]

    # Precision at position (for AP contribution): cum_rel / pos
    df["precision"] = df["cum_rel"] / df["pos"]

    # nDCG discount factor (reused per K)
    discount = 1.0 / np.log2(df["pos"].values + 1)

    # Per-K columns
    r_vals = r_per_query.astype(int).values
    for k in k_values:
        in_top_k = (df["pos"] <= k).astype(float)
        df[f"hit@{k}"] = in_top_k

        # map_contrib@K: precision * label * (pos <= K) / total_positives
        # (averaged over relevant rows per dimension gives per-product AP@K)
        df[f"map_contrib@{k}"] = df["precision"] * in_top_k

        # nDCG@K: need iDCG@K per query
        k_cap = np.minimum(r_vals, k)
        max_k_cap = int(k_cap.max()) if len(k_cap) > 0 else 0
        idcg_k_table = np.zeros(max_k_cap + 1)
        for i in range(1, max_k_cap + 1):
            idcg_k_table[i] = idcg_k_table[i - 1] + 1.0 / np.log2(i + 1)
        idcg_at_k = idcg_k_table[k_cap]
        df[f"ndcg_k_contrib@{k}"] = np.where(
            (idcg_at_k > 0) & (df["pos"] <= k), discount / idcg_at_k, 0.0
        )

    return df


def _aggregate_per_dimension(
    enriched_rel: pd.DataFrame,
    groupby_cols: list[str],
    k_values: list[int],
) -> dict:
    """Aggregate per-row metric contributions by groupby_cols.

    Args:
        enriched_rel: Rows with label=1 from the enriched DataFrame.
        groupby_cols: Columns to group by (e.g. ["prod_name"]).
        k_values: K values used to locate hit@K and ndcg_k_contrib@K columns.

    Returns:
        Dict keyed by dimension value, each holding per-K metric dict.
    """
    metric_cols = []
    for k in k_values:
        metric_cols.extend([
            f"map_contrib@{k}",
            f"ndcg_k_contrib@{k}",
            f"hit@{k}",
        ])

    grouped = enriched_rel.groupby(groupby_cols, sort=True)[metric_cols].mean()

    per_dim: dict = {}
    for idx in grouped.index:
        key = idx if isinstance(idx, str) else "_".join(str(x) for x in idx)
        row = grouped.loc[idx]
        metrics: dict = {}
        for k in k_values:
            metrics[f"map@{k}"] = float(row[f"map_contrib@{k}"])
            metrics[f"ndcg@{k}"] = float(row[f"ndcg_k_contrib@{k}"])
            metrics[f"precision@{k}"] = float(row[f"hit@{k}"])
            metrics[f"recall@{k}"] = float(row[f"hit@{k}"])
        per_dim[key] = metrics

    return per_dim


def compute_all_metrics(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    k_values: list[Union[int, str]] | None = None,
    parameters: dict | None = None,
) -> dict:
    """Compute ranking metrics across multiple dimensions.

    Args:
        predictions: DataFrame with columns [time, entity..., item, score, rank].
        labels: DataFrame with columns [time, entity..., item, label].
            Optionally includes cust_segment_typ for segment-level metrics.
        k_values: K values for precision@K, recall@K, nDCG@K.
            Supports "all" which resolves to total product count N.
            Defaults to [5, "all"].
        parameters: Optional parameters dict for schema resolution.
            If None, uses default schema.

    Returns:
        Dict with keys: overall, per_product, per_segment, per_product_segment,
        macro_avg, n_queries, n_excluded_queries.
    """
    schema = get_schema(parameters or {})
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]
    group_cols = [time_col] + entity_cols

    if k_values is None:
        k_values = [5, "all"]

    # Join predictions with labels
    merged = predictions.merge(
        labels[identity_cols + [label_col]],
        on=identity_cols,
        how="inner",
    )

    # Resolve "all" in k_values to actual product count
    n_products = merged[item_col].nunique()
    k_values = _resolve_k_values(k_values, n_products)

    # Carry segment column if present
    has_segment = "cust_segment_typ" in labels.columns
    if has_segment:
        seg_map = labels[group_cols + ["cust_segment_typ"]].drop_duplicates()
        merged = merged.merge(seg_map, on=group_cols, how="left")

    # --- Overall: per-customer loop (standard mAP definition) ---
    query_groups = merged.groupby(group_cols)

    all_query_metrics: list[dict] = []
    n_excluded = 0

    for group_key, group in query_groups:
        y_true = group[label_col].values.astype(float)
        y_score = group[score_col].values.astype(float)

        qm = _compute_query_metrics(y_true, y_score, k_values)
        if qm is None:
            n_excluded += 1
            continue

        # Store group key columns for downstream use
        if isinstance(group_key, tuple):
            for col_name, val in zip(group_cols, group_key):
                qm[f"_{col_name}"] = val
        else:
            qm[f"_{group_cols[0]}"] = group_key

        if has_segment:
            qm["_segment"] = group["cust_segment_typ"].iloc[0]

        all_query_metrics.append(qm)

    n_queries = len(query_groups)
    overall = _aggregate_metric_lists(all_query_metrics)

    # --- Enrich with per-row contributions for vectorized per-product metrics ---
    enriched = _enrich_with_contributions(merged, k_values, group_cols=group_cols, score_col=score_col, label_col=label_col)
    rel = enriched[enriched[label_col] == 1]

    # Per-product: vectorized decomposition
    per_product: dict = {}
    if len(rel) > 0:
        per_product = _aggregate_per_dimension(rel, [item_col], k_values)

    # Per-segment: per-customer metrics → groupby segment → mean (equal customer weight)
    per_segment: dict = {}
    if has_segment and all_query_metrics:
        seg_records = [
            {k: v for k, v in qm.items() if not k.startswith("_") or k == "_segment"}
            for qm in all_query_metrics
        ]
        seg_df = pd.DataFrame(seg_records)
        metric_keys = [k for k in all_query_metrics[0].keys() if not k.startswith("_")]

        for seg_val, seg_group in seg_df.groupby("_segment", sort=True):
            per_segment[seg_val] = {
                k: float(seg_group[k].mean()) for k in metric_keys
            }

    # Per-product-segment: vectorized decomposition with both dimensions
    per_product_segment: dict = {}
    if has_segment and len(rel) > 0:
        per_product_segment = _aggregate_per_dimension(
            rel, [item_col, "cust_segment_typ"], k_values
        )

    # Macro averages
    macro_avg: dict = {}
    macro_avg["by_product"] = _macro_average(per_product)

    if has_segment:
        macro_avg["by_segment"] = _macro_average(per_segment)
        macro_avg["by_product_segment"] = _macro_average(per_product_segment)

    return {
        "overall": overall,
        "per_product": per_product,
        "per_segment": per_segment,
        "per_product_segment": per_product_segment,
        "macro_avg": macro_avg,
        "n_queries": n_queries,
        "n_excluded_queries": n_excluded,
    }


def _macro_average(per_dim: dict) -> dict:
    """Unweighted mean of metrics across dimension values."""
    non_empty = [v for v in per_dim.values() if v]
    if not non_empty:
        return {}
    keys = non_empty[0].keys()
    return {k: float(np.mean([m[k] for m in non_empty])) for k in keys}
