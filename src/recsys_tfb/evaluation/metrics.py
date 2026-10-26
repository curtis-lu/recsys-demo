"""Metric primitives on numpy arrays, and the rule behind the all-positive accounts.

A leaf module: besides ``logging`` / ``typing`` / ``numpy`` it imports only
``utils.ranking`` (the tie rule the Spark ranking shares) and
``evaluation.result_readers``, and neither imports the project. Add no other
project import: this module is imported widely (``evaluation.metrics_spark``,
``scripts/``), so one risks a cycle. scikit-learn is imported inside the two
functions that need it, so importers do not pay its load time.

The HPO loop scores each trial with these on driver-side arrays: a Spark job
per trial for one scalar would cost far more than the trial. The two
binary-prediction average precisions here are exact; the report's ``pr_auc``
(``evaluation.prediction_quality``) is binned, so the two are never reconciled.
"""

import logging
from collections.abc import Sequence
from typing import Optional

import numpy as np

from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARN_SHARE,
    ALL_POSITIVE_WARNS_KEY,
    N_WITH_POSITIVE_KEY,
    drop_all_positive_groups,
)
from recsys_tfb.utils.ranking import order_by_score_then_item

logger = logging.getLogger(__name__)


def n_groups_with_positive(bundle: dict) -> Optional[int]:
    """Query groups holding a positive in one grain of the row facts:
    ``n_queries - n_excluded_queries``.

    The one place that subtraction is written; everything else reads it or its
    landed copy (:data:`N_WITH_POSITIVE_KEY`). ``n_excluded_queries`` counts
    only the groups holding no positive, switch on or off, so this holds either
    way. ``None`` when either count is missing or ``None``, rather than a guess
    at the missing one.
    """
    n_queries = bundle.get("n_queries")
    n_zero = bundle.get("n_excluded_queries")
    if n_queries is None or n_zero is None:
        return None
    return n_queries - n_zero


def all_positive_share(bundle: dict) -> Optional[float]:
    """All-positive groups ÷ groups holding a positive, for one grain of the
    row facts.

    Over the groups holding a positive, not every group, so the dataset's
    ``test_zero_positive_group_ratio`` cannot move it. ``None`` when the bundle
    has no :data:`ALL_POSITIVE_KEY`, lacks either group count, or no group
    holds a positive.
    """
    n_all_positive = bundle.get(ALL_POSITIVE_KEY)
    if n_all_positive is None:
        return None
    n_with_positive = n_groups_with_positive(bundle)
    if n_with_positive is None or n_with_positive <= 0:
        return None
    return n_all_positive / n_with_positive


def all_positive_share_warns(bundle: dict, parameters: dict) -> bool:
    """Whether to warn about a bundle's all-positive share: only with the
    switch off (on, those groups are already dropped).
    """
    if drop_all_positive_groups(parameters):
        return False
    share = all_positive_share(bundle)
    return share is not None and share > ALL_POSITIVE_WARN_SHARE


def all_positive_accounts(facts: dict, parameters: dict) -> dict:
    """One grain's all-positive accounts, landed by the row facts beside its
    counts. The report and the log read the landed values rather than
    computing them again (ADR-0031 principle 1).
    """
    return {
        N_WITH_POSITIVE_KEY: n_groups_with_positive(facts),
        ALL_POSITIVE_SHARE_KEY: all_positive_share(facts),
        ALL_POSITIVE_WARNS_KEY: all_positive_share_warns(facts, parameters),
    }


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
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    event_keys: Sequence[np.ndarray] = (),
) -> float:
    """Mean over query groups of each group's Average Precision.

    Groups with no positive labels are skipped; if every group is skipped or
    the arrays are empty, returns 0.0.

    One sort, then a slice per group. The naive per-group mask
    (``groups == g``) is ``O(N × G)``, unusable at production scale.

    Tied scores within a group resolve by ``items`` ascending, then by each
    array of ``event_keys``: the rule the Spark metrics rank with
    (:mod:`recsys_tfb.utils.ranking`), so the same rows score the same whatever
    order they arrive in. Both only break ties. ``event_keys`` is empty unless
    the deployment declares ``event``, when one group can hold the same item
    more than once.
    """
    if len(groups) == 0:
        return 0.0

    # Each group's rows contiguous, score descending, ties by item then event.
    sort_idx = order_by_score_then_item(groups, y_score, items, event_keys)
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
        # y is already in score-descending order (from the sort), so
        # cumsum gives top-k precision directly — same formula as compute_ap.
        positions = np.arange(1, len(y) + 1, dtype=np.float64)
        precisions = np.cumsum(y) / positions
        aps.append(float(np.dot(precisions, y) / n_pos))

    if not aps:
        return 0.0
    return float(np.mean(aps))


def positive_row_contributions(
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
    event_keys: Sequence[np.ndarray] = (),
) -> tuple[np.ndarray, np.ndarray]:
    """Per-positive-row AP contribution + original-order row indices.

    Returns ``(contrib, row_idx)``, always this 2-tuple: ``contrib[i]`` is the
    within-query cumulative precision of positive row ``row_idx[i]`` (zeroed
    when its rank exceeds ``k``). Queries with no positive rows contribute
    nothing. Ranking and ties as in :func:`compute_mean_ap`.

    Sampling weights deliberately stay out: a query counted twice ranks the
    same, so weights cannot change what this computes, only be broadcast onto
    the returned rows (:func:`align_positive_row_weights`). Taking them here
    would make the return arity vary with the arguments.
    """
    if len(groups) == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.int64)

    sort_idx = order_by_score_then_item(groups, y_score, items, event_keys)
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


def align_positive_row_weights(
    weights: np.ndarray,
    n_rows: int,
    row_idx: np.ndarray,
) -> np.ndarray:
    """Validate a query-level weight vector and select the positive rows.

    ``weights`` is **row-aligned**: one entry per input row (``n_rows`` of
    them), every row of a query carrying that query's weight. Returns
    ``weights[row_idx]`` as float64, aligned element-for-element with the
    ``contrib`` of the :func:`positive_row_contributions` call that returned
    ``row_idx``.
    """
    w = np.asarray(weights, dtype=np.float64)
    if w.shape != (n_rows,):
        raise ValueError(
            f"weights must be row-aligned with groups: expected shape "
            f"({n_rows},), got {w.shape}"
        )
    return w[row_idx]


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
    ``∝ n_pos**weight_alpha`` (alpha=0 → equal weight). The defaults return
    ``v.mean()`` directly: ``np.dot`` with uniform weights can differ from
    ``mean`` in the last ulp (as in ``metrics_spark.macro_average``).
    Returns None when every item is excluded (caller picks the fallback).
    """
    keep = n_pos >= min_positives
    if not keep.any():
        return None
    v = values[keep].astype(np.float64, copy=True)
    n = n_pos[keep].astype(np.float64)
    if weight_alpha == 0.0 and shrinkage_k == 0.0:
        return float(v.mean())
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
    weights: Optional[np.ndarray] = None,
    event_keys: Sequence[np.ndarray] = (),
) -> float:
    """Macro average over items of per-item attributed mAP@k.

    Must equal ``evaluation.metrics_spark``'s
    ``macro_avg["by_item"]["map_attr@K"]``: HPO scores with this instead of a
    Spark job.

    Each positive row contributes its within-query cumulative precision
    (:func:`positive_row_contributions`). Per item that contribution is
    averaged over the item's positive rows (row-equal-weight), and items are
    combined by :func:`macro_from_per_item`. ``k=None`` means no truncation.

    ``weights`` is **row-aligned, query-level**: giving a query weight ``w``
    means exactly that query appearing ``w`` times. So each item's AP becomes
    the weighted mean over its positive rows, and the ``n_pos`` handed to
    :func:`macro_from_per_item` is the sum of those weights, which
    ``min_positives`` / ``shrinkage_k`` / ``weight_alpha`` then see. The
    combine across items stays equal-weight (a replayed query adds no items),
    so ``weights`` is not passed to :func:`macro_from_per_item` as well: that
    would count it twice. ``weights=None`` runs the unweighted path, not an
    ``np.ones`` fill-in, so the unweighted value stays bit-for-bit.

    Empty input, no positive rows anywhere, or every item excluded by
    ``min_positives`` returns ``0.0``.

    ``items`` is both the tie-break and the per-item key, so HPO may pass the
    order-preserving codes of :func:`recsys_tfb.utils.ranking.item_sort_codes`
    in place of the values. ``event_keys`` is only the tie-break, never a
    per-item key: one item's several rows in a query group all attribute to
    that item, which keeps the macro comparable with and without ``event``.
    """
    contrib_all, row_idx = positive_row_contributions(
        groups, items, y_true, y_score, k, event_keys
    )
    # Validate/broadcast before the empty-input return so a malformed weight
    # vector raises regardless of whether the input happened to be empty.
    w_pos = (
        None if weights is None
        else align_positive_row_weights(weights, len(groups), row_idx)
    )
    if len(contrib_all) == 0:
        return 0.0

    items_all = items[row_idx]
    _, inv = np.unique(items_all, return_inverse=True)
    if w_pos is None:
        sums = np.bincount(inv, weights=contrib_all)
        counts = np.bincount(inv)
    else:
        sums = np.bincount(inv, weights=contrib_all * w_pos)
        # The effective number of positive rows behind each item, not the row
        # count: dividing weighted mass by the raw count would silently
        # rescale every per-item AP.
        counts = np.bincount(inv, weights=w_pos)

    per_item = sums / counts
    macro = macro_from_per_item(
        per_item, counts, weight_alpha, min_positives, shrinkage_k
    )
    return 0.0 if macro is None else macro


def compute_pooled_average_precision(
    y_true: np.ndarray,
    y_score: np.ndarray,
    weights: Optional[np.ndarray] = None,
) -> float:
    """Average precision with every row one binary prediction, all in one pool.

    The HPO objective ``pooled_average_precision``. No query group and no item
    enter it: rows are ranked by score across the whole of val. That also
    rewards telling apart the entities that tend to have positives at all, no
    help to the ranking inside a group, which is why it is opt-in.

    Computed by ``sklearn.metrics.average_precision_score``, so it reconciles
    with it exactly, tie rule included: tied scores form one threshold, unlike
    the ranking metrics here, which break ties by item. ``weights`` is the val
    table's ``zero_positive_group_weight``, a design weight (ADR-0025
    decision 3). Leave it out and each kept zero-positive group counts once
    instead of ``1 / r`` times: a number for a different population, with
    nothing to say so.

    No positive row raises ``ValueError`` (pre-check): scikit-learn would
    return ``-0.0`` with a ``UserWarning``, a constant that scores every HPO
    trial alike.

    Test scores the same definition on Spark
    (``metrics_spark.compute_pooled_average_precision``, ADR-0028 decision 4).
    Nothing but ``tests/test_pipelines/test_training/test_compute_test_metrics.py``
    holds the two together: change one, change both.
    """
    from sklearn.metrics import average_precision_score

    _require_a_positive_row(y_true, "pooled_average_precision")
    return float(average_precision_score(y_true, y_score, sample_weight=weights))


def compute_macro_per_item_average_precision(
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    weights: Optional[np.ndarray] = None,
) -> float:
    """Mean over items of each item's :func:`compute_pooled_average_precision`.

    The HPO objective ``macro_per_item_average_precision``: one average
    precision per item over that item's rows, then the plain mean, so an item
    shown a lot does not drown out one shown rarely. Tie rule and ``weights``
    as in :func:`compute_pooled_average_precision`.

    **It compares a row only with rows of the same item**, so it does not check
    that scores are comparable across items: adding a constant to every score
    of one item leaves it unchanged. For scores compared across items, use the
    pooled one (``docs/pipelines/training.md`` §3.2; ADR-0025 decision 4's
    addendum).

    **An item with no positive row in val is left out of the mean**, never
    handed to scikit-learn: its ``-0.0`` would pull the mean towards 0. This
    matches ``compute_macro_per_item_map``, which counts items from their
    positive rows. ``evaluation.metric``'s ``min_positives`` / ``weight_alpha``
    / ``shrinkage_k`` are not read, matching what the HPO loop passes
    ``compute_macro_per_item_map``.

    One stable sort by item, then one slice per item: masking ``items == item``
    instead costs a full pass over val per item.

    No item with a positive raises ``ValueError``, for the reason given in
    :func:`compute_pooled_average_precision`. Its Spark twin on test is
    ``metrics_spark.compute_macro_per_item_average_precision``, held to this
    one the same way.
    """
    from sklearn.metrics import average_precision_score

    _require_a_positive_row(y_true, "macro_per_item_average_precision")
    order = np.argsort(items, kind="stable")
    items_s = np.asarray(items)[order]
    y_s = np.asarray(y_true)[order]
    score_s = np.asarray(y_score)[order]
    w_s = None if weights is None else np.asarray(weights)[order]
    starts = np.flatnonzero(np.r_[True, items_s[1:] != items_s[:-1]])
    ends = np.r_[starts[1:], len(items_s)]

    per_item = [
        average_precision_score(
            y_s[a:b], score_s[a:b],
            sample_weight=None if w_s is None else w_s[a:b],
        )
        for a, b in zip(starts, ends)
        if np.any(y_s[a:b] > 0)
    ]
    return float(np.mean(per_item))


def _require_a_positive_row(y_true: np.ndarray, objective: str) -> None:
    if not np.any(np.asarray(y_true) > 0):
        raise ValueError(
            f"{objective}: val holds no positive row, so average precision "
            f"is undefined (scikit-learn would return -0.0 with a warning)."
        )
