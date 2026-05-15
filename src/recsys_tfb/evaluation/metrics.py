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
    Groups with no positive labels are skipped (``compute_ap`` returns None);
    if every group is skipped or the arrays are empty, returns 0.0.

    Used by ``tune_hyperparameters`` to score val predictions as a true mAP
    (per-customer AP averaged over customers) rather than treating the whole
    val set as a single ranking problem.
    """
    if len(groups) == 0:
        return 0.0
    aps: list[float] = []
    for g in np.unique(groups):
        mask = groups == g
        ap = compute_ap(y_true[mask], y_score[mask])
        if ap is not None:
            aps.append(ap)
    if not aps:
        return 0.0
    return float(np.mean(aps))
