"""Metric primitives on numpy arrays, plus the shared reader of ``evaluation.metric``.

This is a leaf module: its module-level imports are ``logging`` / ``typing``
/ ``numpy``, plus ``utils.ranking`` for the within-query order. Do not add any
other project import — ``diagnosis.metric.*``, ``evaluation.metrics_spark``,
``evaluation.report_builder`` and several ``scripts/`` import from here, so a
project import risks a cycle. ``utils.ranking`` is the exception because it
imports nothing from the project, so it cannot close one; it is where the tie
rule lives, shared with the Spark ranking (#355). scikit-learn is imported
only inside the two functions that need it (the binary-prediction average
precision below), so importers of this module do not pay its load time.

What lives here:

* Per-query AP primitives for ``tune_hyperparameters`` (HPO loop in the
  training pipeline). HPO scores each trial on driver-side numpy arrays;
  going through Spark for one scalar per trial would be massive overhead.
* The per-item macro primitives (``positive_row_contributions`` /
  ``macro_from_per_item`` / ``compute_macro_per_item_map``), shared by HPO,
  ``metrics_spark.macro_average`` and the diagnosis bootstrap.
* :func:`metric_params` — the only reader of ``evaluation.metric`` that the
  metrics, diagnosis and report code uses (ADR-0020 design H), so the
  fallback semantics cannot drift between copies. A15 in
  ``core/consistency.py`` reads the same block separately, only to validate
  value domains.
* :func:`resolved_all_k` — the only reader of the K a metrics bundle's
  ``"all"`` values are stored at, for the reports and for training (#434).
* :func:`drop_all_positive_groups` / :func:`all_positive_share` /
  :func:`all_positive_share_warns` — the only reader of
  ``evaluation.query_filter.drop_all_positive_groups`` and the one rule for
  when the all-positive share warns, shared by the evaluation nodes (log) and
  the report (#376).
* :func:`compute_pooled_average_precision` /
  :func:`compute_macro_per_item_average_precision` — the HPO objectives that
  score every val row as one binary prediction (#430). Not the report's
  ``pr_auc`` (``evaluation.prediction_quality``): exact where that one is
  binned, and never to be reconciled with it.

The dict-shaped per-segment / per-item / overall metrics of the evaluation
pipeline run on Spark — see ``recsys_tfb.evaluation.metrics_spark``.
"""

import logging
from collections.abc import Sequence
from typing import Optional

import numpy as np

from recsys_tfb.utils.ranking import order_by_score_then_item

logger = logging.getLogger(__name__)


def metric_params(parameters: dict) -> dict:
    """Read ``evaluation.metric`` → ``{"k", "weight_alpha", "min_positives", "shrinkage_k"}``.

    The one reader of that block that the metrics / diagnosis / report code
    uses (ADR-0020 design H): the Spark main metrics, the metric CI, the
    diagnosis family, the report and the ``scripts/`` diagnoses all call this
    instead of keeping their own copy. A15
    (``core.consistency.diagnosis_metric_param_errors``) reads the same block
    separately, only to validate value domains; it resolves no fallbacks.

    Fallback: a missing ``evaluation`` / ``metric`` block, a missing key, or
    an explicit ``None`` value all resolve to ``k=None`` (no truncation),
    ``weight_alpha=0.0``, ``min_positives=0``, ``shrinkage_k=0.0`` — the plain
    equal-weight, untruncated macro. Value domains are validated by A15
    (``core.consistency.diagnosis_metric_param_errors``), not here.

    ``k`` and ``evaluation.k_values`` are independent axes. ``k_values`` is
    the K grid of the ``@K`` families. ``k`` is the truncation depth of the
    headline per-item macro family — point estimate and CI alike.
    ``macro_from_per_item`` and ``metrics_spark.macro_average`` do not accept
    ``k``; callers split it off.
    """
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    return {
        "k": None if m.get("k") is None else int(m["k"]),
        "weight_alpha": float(m.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(m.get("min_positives", 0) or 0),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0) or 0.0),
    }


#: Top-level key of a ``metrics_spark.compute_all_metrics`` bundle and of its
#: ``category`` sub-bundle (#376): how many of the query groups holding a
#: positive are *all-positive* — every row's label positive, so every ranking
#: metric is full marks whatever the order. Always written, whether or not
#: ``evaluation.query_filter.drop_all_positive_groups`` drops them, because the
#: report reads only the written bundle. ``n_excluded_queries`` keeps counting
#: only the zero-positive groups.
ALL_POSITIVE_KEY = "n_all_positive_queries"

#: With the switch off, the log and the report warn when the all-positive
#: groups are more than this share of the groups holding a positive (#376).
#: A constant, not a setting.
ALL_POSITIVE_WARN_SHARE = 0.10


def drop_all_positive_groups(parameters: dict) -> bool:
    """Read ``evaluation.query_filter.drop_all_positive_groups``; off unless ``True``.

    The one reader of the switch (#376). A missing block, a missing key or an
    explicit ``None`` all mean off. Not under ``evaluation.metric``:
    :func:`metric_params` returns four fixed keys and drops any other, so a
    switch written there would silently do nothing. The value domain (a bool)
    is validated by A49 in ``core/consistency.py``, not here.
    """
    qf = ((parameters.get("evaluation", {}) or {}).get("query_filter", {}) or {})
    return bool(qf.get("drop_all_positive_groups", False) or False)


def all_positive_share(bundle: dict) -> Optional[float]:
    """All-positive groups ÷ groups holding a positive, for one metrics bundle.

    The denominator is the mAP's (``n_queries - n_excluded_queries``), so the
    share reads as "this much of the mAP is full marks the order never
    earned", and the dataset's ``test_zero_positive_group_ratio`` cannot move
    it. ``None`` when the bundle predates #376 (no :data:`ALL_POSITIVE_KEY`) or
    no group holds a positive.
    """
    n_all_positive = bundle.get(ALL_POSITIVE_KEY)
    if n_all_positive is None:
        return None
    n_with_positive = (bundle.get("n_queries") or 0) - (
        bundle.get("n_excluded_queries") or 0)
    if n_with_positive <= 0:
        return None
    return n_all_positive / n_with_positive


def all_positive_share_warns(bundle: dict, parameters: dict) -> bool:
    """Whether to warn about a bundle's all-positive share (#376).

    Only with the switch off — on, those groups are already dropped — and only
    strictly above :data:`ALL_POSITIVE_WARN_SHARE`. The log and the report
    each call this, per grain, so they cannot disagree on when to warn.
    """
    if drop_all_positive_groups(parameters):
        return False
    share = all_positive_share(bundle)
    return share is not None and share > ALL_POSITIVE_WARN_SHARE


#: Top-level key of a ``metrics_spark.compute_all_metrics`` bundle: the K
#: ``evaluation.k_values: "all"`` resolved to. Written only when the ranked
#: frame holds one row per event — the one case in which it can differ from
#: the item count (``resolved_all_k``).
ALL_K_KEY = "all_k"


def resolved_all_k(bundle: dict) -> int:
    """The K a metrics bundle's ``"all"`` values are stored at (``map@{K}``).

    The one reader of that number (#434). ``"all"`` has one producer
    (``metrics_spark._resolve_all_k``) and several readers — the main report,
    the comparison report, training's test mAP. Each used to count the items
    itself; with ``event`` declared the producer's K is the widest query
    group, not the item count, so every lookup missed and nothing raised (a
    blank ``map@all`` cell, a test mAP logged as 0.0).

    * ``bundle[ALL_K_KEY]`` when present. The producer writes it only when the
      frame it ranked held one row per event, the one case in which the widest
      group can be longer than the item list.
    * Otherwise ``dataset_overview.totals.n_items`` — the producer's K in every
      other case, counted over the same frame. Nothing is written then, so the
      artifact of a deployment without ``event`` stays value-for-value what it
      was.
    * ``0`` when the bundle carries neither: a slim baseline bundle, which has
      no overview. ``0`` means unknown, as ``report_builder.count_items``
      already reads it; the report reads a baseline at the model's K instead.

    Reads the overview directly rather than through
    ``report_builder.count_items`` because this module imports nothing from the
    project. The pre-#327 refusal lives there, and every report section still
    reads the overview through it.
    """
    recorded = bundle.get(ALL_K_KEY)
    if recorded is not None:
        return int(recorded)
    totals = (bundle.get("dataset_overview") or {}).get("totals") or {}
    return int(totals.get("n_items", 0))


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
    """Mean of per-group Average Precision.

    A "group" represents one query (e.g. one ``(cust_id, snap_date)`` pair).
    Groups with no positive labels are skipped; if every group is skipped or
    the arrays are empty, returns 0.0.

    Used by ``tune_hyperparameters`` to score val predictions as a true mAP
    (per-customer AP averaged over customers) rather than treating the whole
    val set as a single ranking problem.

    Implementation: ``O(N log N)`` via one sort on ``(groups, -y_score,
    items)`` followed by a slice-per-group walk. The naive
    ``for g in np.unique(groups): mask = groups == g`` is ``O(N × G)`` and
    becomes unusable at production scale (5M rows × 200k groups ~ 10 min).

    Tied y_score within a group resolves by ``items`` ascending, then by each
    array of ``event_keys`` — the rule the Spark metrics rank with,
    :mod:`recsys_tfb.utils.ranking` — so the same rows score the same whatever
    order they arrive in. Neither only breaks ties.

    ``event_keys`` is empty unless the deployment declares ``event``, in which
    case one query group can hold the same item more than once and ``items``
    alone no longer separates every pair of rows. Empty reproduces the
    pre-#378 order exactly.
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

    contrib[i] is the within-query cumulative precision of positive row
    row_idx[i] (zeroed when its rank exceeds ``k``). Queries with no
    positive rows contribute nothing. Within a query rows rank by score
    descending, ties by ``items`` ascending (:mod:`recsys_tfb.utils.ranking`);
    ``items`` takes the raw item values and only breaks ties. Shared by
    :func:`compute_macro_per_item_map` and the diagnosis bootstrap
    (``diagnosis.metric.uncertainty``) — cluster resampling never changes
    within-query ranking, so contributions are computed exactly once.

    Returns a **fixed 2-tuple** ``(contrib, row_idx)`` — always, for every
    argument combination. Sampling weights deliberately do NOT belong here:
    a query counted twice has the same internal ranking, hence the same
    per-row precisions, so weights cannot influence what this function
    computes; they only need broadcasting onto the returned positive rows.
    That broadcast lives in :func:`align_positive_row_weights`, which every
    weighted caller shares, so this function's return arity never varies
    with its arguments.
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

    ``weights`` is **row-aligned**: one entry per input row, every row of a
    query carrying that query's weight (e.g. the inverse inclusion
    probability of a stratified diagnosis sample). ``n_rows`` is the input
    row count the vector must match (``len(groups)``); ``row_idx`` is the
    original-order index vector returned by
    :func:`positive_row_contributions`. Returns ``weights[row_idx]`` as
    float64 — the weight of each positive row, aligned element-for-element
    with that call's ``contrib``.

    Why this is a separate function rather than an optional argument of
    :func:`positive_row_contributions`: a return arity that changes with an
    argument is a footgun for callers that unpack it, and the two things
    have genuinely different jobs — one ranks, one broadcasts. Keeping the
    validation and the broadcast here (rather than inline at each call site)
    is what stops the logic from being duplicated between
    :func:`compute_macro_per_item_map` and the diagnosis bootstrap.
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
    ``∝ n_pos**weight_alpha`` (alpha=0 → equal weight). Defaults reproduce
    the plain equal-weight mean bit-for-bit (``np.dot`` with uniform weights
    can differ from ``mean`` in the last ulp, so the inactive path returns
    ``v.mean()`` directly — same principle as metrics_spark.macro_average).
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

    Reproduces ``evaluation.metrics_spark`` ``macro_avg["by_item"]["map_attr@K"]``
    on numpy arrays so the HPO loop can score a trial without a Spark job.

    Ranking is *within each query* (``groups``, e.g. ``(snap_date, cust_id)``),
    exactly as in :func:`compute_mean_ap`. Each positive row contributes its
    within-query cumulative precision ``prec_at_pos`` (zeroed when its rank is
    beyond ``k``). Per item we average that contribution over the item's
    positive rows (row-equal-weight); items are then combined via
    :func:`macro_from_per_item`. ``k=None`` means no truncation — full mAP,
    equivalent to ``k = n_items``.

    ``weight_alpha`` / ``min_positives`` / ``shrinkage_k`` default to
    ``0``/``0``/``0``, which reproduces the original plain equal-weight
    macro over items — required for backward compatibility with
    ``tune_hyperparameters`` (HPO loop), which calls this positionally.
    Order of operations when non-default: (1) ``min_positives`` drops
    cold items; (2) ``shrinkage_k`` shrinks surviving per-item values
    toward the pooled (n_pos-weighted) mean of the survivors; (3)
    ``weight_alpha`` weights items ``∝ n_pos**weight_alpha``. See
    :func:`macro_from_per_item` for the exact formulas.

    ``weights`` (optional) is a **row-aligned, query-level** weight vector —
    typically the inverse inclusion probability of a stratified diagnosis
    sample, where unweighted estimates are biased whenever the sampling ratio
    is below 1. Semantics: giving a query weight ``w`` is defined to be exactly
    equivalent to that query appearing ``w`` times in the input. Concretely,
    each item's AP becomes the *weighted* mean over its positive rows, and the
    per-item ``n_pos`` handed to :func:`macro_from_per_item` becomes the sum of
    those weights — so ``min_positives`` / ``shrinkage_k`` / ``weight_alpha``
    all see the effective (weighted) count rather than the raw row count.

    The macro combine across items stays equal-weight: weighting corrects each
    item's *within-item* estimate, it does not re-weight the items themselves.
    That is precisely the duplication semantics — replaying a query does not
    add items to the catalogue. This is also why ``weights`` is not threaded
    into :func:`macro_from_per_item`: its ``n_pos`` argument already carries
    the weight, so a second weight channel there would double-count.

    ``weights=None`` (the default) runs the original unweighted code path
    verbatim — no ``np.ones`` fill-in — so the main metric path, which shares
    these primitives with the diagnosis layer, stays bit-for-bit unchanged.

    Empty input, or no positive rows anywhere, returns ``0.0``. If every
    item is excluded by ``min_positives``, also returns ``0.0``.

    Implementation mirrors :func:`compute_mean_ap`: one sort on
    ``(groups, -y_score, items)`` (``O(N log N)``) via
    :func:`positive_row_contributions`, then a vectorized per-item
    aggregation via ``np.unique`` + ``np.bincount``. ``items`` is both the
    tie-break and the per-item key, so HPO may pass the order-preserving codes
    of :func:`recsys_tfb.utils.ranking.item_sort_codes` in place of the values.

    ``event_keys`` is only the tie-break, never a per-item key: with ``event``
    declared, one item's several rows in a query group still all attribute to
    that item, which is what makes the per-item macro comparable across the
    two shapes.
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
        # Weighted denominator: the effective number of positive rows behind
        # each item, NOT len(rows). Using the raw count here would divide
        # weighted mass by unweighted support and silently rescale every
        # per-item AP.
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

    The HPO objective ``pooled_average_precision`` (#430). No query group and
    no item enter it: the rows are ranked by score across the whole of val,
    which is the reading a deployment trained on impression logs looks at
    (ADR-0024's background). Ranking across groups also rewards telling apart
    the entities that tend to have positives at all — no help to the ranking
    inside a group, and the reason this is an objective one opts into.

    Computed by ``sklearn.metrics.average_precision_score``, so the value can
    be reconciled with it exactly. That is also its tie rule: tied scores form
    one threshold, whatever order the rows arrive in — unlike the ranking
    metrics in this module, which break ties by item. ``weights`` is the val
    table's ``zero_positive_group_weight`` (1, or ``1 / r`` on a kept group
    holding no positive): a design weight, not an unbiased estimate (ADR-0025
    decision 3). Leave it out and each kept zero-positive group counts once
    instead of ``1 / r`` times — a valid number for a different population,
    with nothing to say so.

    scikit-learn is imported here rather than at the top: importing
    ``sklearn.metrics`` takes about 0.6 s, and every importer of this module
    (the reports, diagnosis, ``scripts/``) would pay it for two functions only
    the HPO loop calls.

    No positive row raises ``ValueError``: a **pre-check** on the input.
    scikit-learn would answer ``-0.0`` with a ``UserWarning``, and a constant
    scores every HPO trial alike. ``tune_hyperparameters`` stops a val set
    like this before its first trial; this is the backstop for other callers.
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

    The HPO objective ``macro_per_item_average_precision`` (#430): one average
    precision per item over that item's rows, then the plain mean, so an item
    shown a lot does not drown out one shown rarely. Tie rule and ``weights``
    as in :func:`compute_pooled_average_precision`.

    **It compares a row only with rows of the same item.** Average precision
    reads the order of scores, not how close they are to 0, and here a row's
    score counts as high or low only against the same item's other rows. So it
    checks that an item's positives score above that item's negatives; it does
    not check that scores are comparable across items. Adding one constant to
    every score of an item leaves it unchanged, and a negative of item A
    scored above a positive of item B costs nothing here, while it costs
    points in :func:`compute_pooled_average_precision`. Fit for scores each
    item uses on its own (a per-item threshold); for scores compared across
    items — one item picked per query group, one shared threshold — use the
    pooled one. Worked example: ``docs/pipelines/training.md`` §3.2; the
    decision: ADR-0025 decision 4's addendum.

    **An item with no positive row in val is left out of the mean**, never
    handed to scikit-learn — it would come back as ``-0.0`` with a warning and
    pull the mean towards 0. That is also what ``compute_macro_per_item_map``
    does: it counts items from their positive rows only. The consequence is
    the caller's to show: with many items and few positives the mean covers a
    handful of items, and an item with one or two positives weighs as much as
    a large one. ``evaluation.metric``'s ``min_positives`` / ``weight_alpha``
    / ``shrinkage_k`` are not read, matching what the HPO loop passes
    ``compute_macro_per_item_map`` (#430).

    One stable sort by item, then one contiguous slice per item. Masking
    ``items == item`` per item instead costs a full pass over val for every
    item — four times slower at 1000 items and 5 million rows (#430).

    No item with a positive raises ``ValueError``, for the reason given in
    :func:`compute_pooled_average_precision`.
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
