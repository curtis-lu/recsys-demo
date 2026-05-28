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
