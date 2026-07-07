"""Single-query ranking metrics on numpy arrays.

Scope is intentionally narrow: only the primitives needed by
``tune_hyperparameters`` (HPO loop in training pipeline) live here. The
full evaluation pipeline (dict-shaped per-segment / per-item / overall
metrics) runs on Spark — see ``recsys_tfb.evaluation.metrics_spark``.

HPO scores each trial by running prediction on the val set inside a single
driver, producing numpy arrays. Going through Spark for one scalar per
trial would be massive overhead, so we keep these numpy primitives.
"""

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


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


def compute_mean_ap(
    groups: np.ndarray, y_true: np.ndarray, y_score: np.ndarray
) -> float:
    """Mean of per-group Average Precision.

    A "group" represents one query (e.g. one ``(cust_id, snap_date)`` pair).
    Groups with no positive labels are skipped; if every group is skipped or
    the arrays are empty, returns 0.0.

    Used by ``tune_hyperparameters`` to score val predictions as a true mAP
    (per-customer AP averaged over customers) rather than treating the whole
    val set as a single ranking problem.

    Implementation: ``O(N log N)`` via a single ``np.lexsort`` on
    ``(groups, -y_score)`` followed by a slice-per-group walk. The naive
    ``for g in np.unique(groups): mask = groups == g`` is ``O(N × G)`` and
    becomes unusable at production scale (5M rows × 200k groups ~ 10 min).

    Tied y_score within a group resolves by stable input order (``np.lexsort``
    is mergesort-based). This is a stronger guarantee than the previous
    ``np.argsort`` default, whose tie-break was implementation-defined.
    """
    if len(groups) == 0:
        return 0.0

    # np.lexsort takes keys in REVERSE priority order: the LAST key is primary.
    # Primary = groups (so each group's rows become contiguous); secondary =
    # -y_score (so within each group rows are in descending score order).
    sort_idx = np.lexsort((-y_score, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y_true[sort_idx].astype(np.float64, copy=False)

    # Group boundary indices: where the group id changes, plus the two
    # sentinels. boundaries[i] / boundaries[i+1] bracket group i's rows.
    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])

    aps: list[float] = []
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        y = y_sorted[s:e]  # zero-copy slice, no per-group O(N) mask
        n_pos = y.sum()
        if n_pos == 0:
            continue
        # y is already in score-descending order (from the lexsort), so
        # cumsum gives top-k precision directly — same formula as compute_ap.
        positions = np.arange(1, len(y) + 1, dtype=np.float64)
        precisions = np.cumsum(y) / positions
        aps.append(float(np.dot(precisions, y) / n_pos))

    if not aps:
        return 0.0
    return float(np.mean(aps))


def positive_row_contributions(
    groups: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Per-positive-row AP contribution + original-order row indices.

    contrib[i] is the within-query cumulative precision of positive row
    row_idx[i] (zeroed when its rank exceeds ``k``). Queries with no
    positive rows contribute nothing. Shared by
    :func:`compute_macro_per_item_map` and the diagnosis bootstrap
    (``diagnosis.metric.uncertainty``) — cluster resampling never changes
    within-query ranking, so contributions are computed exactly once.
    """
    if len(groups) == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.int64)

    sort_idx = np.lexsort((-y_score, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y_true[sort_idx].astype(np.float64, copy=False)

    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])

    contribs: list[np.ndarray] = []
    row_idx: list[np.ndarray] = []
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        y = y_sorted[s:e]
        if y.sum() == 0:
            continue
        positions = np.arange(1, len(y) + 1, dtype=np.float64)
        prec = np.cumsum(y) / positions
        if k is not None:
            prec = prec * (positions <= k)
        pos_mask = y == 1
        contribs.append(prec[pos_mask])
        row_idx.append(sort_idx[s:e][pos_mask])

    if not contribs:
        return np.array([], dtype=np.float64), np.array([], dtype=np.int64)
    return np.concatenate(contribs), np.concatenate(row_idx)


def macro_from_per_item(
    values: np.ndarray,
    n_pos: np.ndarray,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> Optional[float]:
    """Parameterized macro combine over per-item values.

    Order of operations: (1) drop items with ``n_pos < min_positives``;
    (2) shrink each surviving value toward the pooled (n_pos-weighted)
    mean of the survivors with factor ``n/(n+k)``; (3) weight items
    ``∝ n_pos**weight_alpha`` (alpha=0 → equal weight). Defaults reproduce
    the plain equal-weight mean. Returns None when every item is excluded
    (caller picks the fallback).
    """
    keep = n_pos >= min_positives
    if not keep.any():
        return None
    v = values[keep].astype(np.float64, copy=True)
    n = n_pos[keep].astype(np.float64)
    if shrinkage_k > 0:
        pooled = float(np.dot(v, n) / n.sum())
        v = (n * v + shrinkage_k * pooled) / (n + shrinkage_k)
    w = n ** weight_alpha
    w = w / w.sum()
    return float(np.dot(w, v))


def compute_macro_per_item_map(
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> float:
    """Macro average over items of per-item attributed mAP@k.

    Reproduces ``evaluation.metrics_spark`` ``macro_avg["by_item"]["map_attr@K"]``
    on numpy arrays so the HPO loop can score a trial without a Spark job.

    Ranking is *within each query* (``groups``, e.g. ``(snap_date, cust_id)``),
    exactly as in :func:`compute_mean_ap`. Each positive row contributes its
    within-query cumulative precision ``prec_at_pos`` (zeroed when its rank is
    beyond ``k``). Per item we average that contribution over the item's
    positive rows (row-equal-weight); items are then combined via
    :func:`macro_from_per_item`. ``k=None`` means no truncation — full mAP,
    equivalent to ``k = n_products``.

    ``weight_alpha`` / ``min_positives`` / ``shrinkage_k`` default to
    ``0``/``0``/``0``, which reproduces the original plain equal-weight
    macro over items — required for backward compatibility with
    ``tune_hyperparameters`` (HPO loop), which calls this positionally.
    Order of operations when non-default: (1) ``min_positives`` drops
    cold items; (2) ``shrinkage_k`` shrinks surviving per-item values
    toward the pooled (n_pos-weighted) mean of the survivors; (3)
    ``weight_alpha`` weights items ``∝ n_pos**weight_alpha``. See
    :func:`macro_from_per_item` for the exact formulas.

    Empty input, or no positive rows anywhere, returns ``0.0``. If every
    item is excluded by ``min_positives``, also returns ``0.0``.

    Implementation mirrors :func:`compute_mean_ap`: one ``np.lexsort`` on
    ``(groups, -y_score)`` (``O(N log N)``) via
    :func:`positive_row_contributions`, then a vectorized per-item
    aggregation via ``np.unique`` + ``np.bincount``.
    """
    contrib_all, row_idx = positive_row_contributions(groups, y_true, y_score, k)
    if len(contrib_all) == 0:
        return 0.0

    items_all = items[row_idx]
    _, inv = np.unique(items_all, return_inverse=True)
    sums = np.bincount(inv, weights=contrib_all)
    counts = np.bincount(inv)
    per_item = sums / counts
    macro = macro_from_per_item(
        per_item, counts, weight_alpha, min_positives, shrinkage_k
    )
    return 0.0 if macro is None else macro
