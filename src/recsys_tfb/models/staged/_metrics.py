"""Binary metrics on numpy arrays for stage-1 per-group HPO scoring.

No sklearn dependency (production: no additional packages). AUC is the
rank-based Mann-Whitney estimator with average ranks for ties — exact for
the pairwise definition, O(n log n).
"""

import numpy as np

_EPS = 1e-15


def _average_ranks(x: np.ndarray) -> np.ndarray:
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=np.float64)
    ranks[order] = np.arange(1, len(x) + 1, dtype=np.float64)
    _, inverse, counts = np.unique(x, return_inverse=True, return_counts=True)
    sums = np.bincount(inverse, weights=ranks)
    return (sums / counts)[inverse]


def binary_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """AUC via average ranks; NaN when only one class is present."""
    y = np.asarray(y_true).astype(bool)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    ranks = _average_ranks(np.asarray(y_score, dtype=np.float64))
    return float(
        (ranks[y].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    )


def binary_logloss(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Mean binary cross-entropy; scores clipped to (eps, 1-eps)."""
    y = np.asarray(y_true, dtype=np.float64)
    p = np.clip(np.asarray(y_score, dtype=np.float64), _EPS, 1.0 - _EPS)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))
