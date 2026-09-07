"""Algorithm-agnostic ranking / query-group helpers.

Single source for: which objectives are learning-to-rank, the default eval
metric for them, and converting the per-row group-id array produced by
``recsys_tfb.io.extract.extract_Xy_with_groups`` into the run-length
``group`` count vector LightGBM / XGBoost ranking require.

Pure module: imports only numpy. Reused by both LightGBM (Phase 1) and a
future XGBoost adapter (Phase 4), so it must stay framework-free.
"""

from __future__ import annotations

import numpy as np

# LightGBM learning-to-rank objectives this project supports. XGBoost's
# rank:* objectives map onto the same group plumbing in Phase 4.
RANKING_OBJECTIVES: frozenset[str] = frozenset({"lambdarank", "rank_xendcg"})


def is_ranking_objective(objective: str | None) -> bool:
    """True iff ``objective`` is a supported learning-to-rank objective."""
    return objective in RANKING_OBJECTIVES


def objective_cache_key(objective: str | None) -> str:
    """Sub-path segment isolating one objective's on-disk lgb binary.

    Every ranking objective gets its **own** segment: ``lgb/lambdarank/``,
    ``lgb/rank_xendcg/``, ``lgb/binary/``.

    The two ranking objectives no longer build the same rows: lambdarank
    drops zero-positive query groups and rank_xendcg keeps them (see
    :func:`objective_drops_zero_positive_groups`). The lgb cache is not keyed
    by ``model_version``, so a shared segment would silently serve a .bin
    built for the other objective's rows. The segment split landed one commit
    ahead of the row split (#314 before #315), so that collision was never
    live.

    The segment does **not** separate a .bin built before the row split from
    one built after — same objective, same path, different rows. That one is
    caught in ``LightGBMAdapter.prepare_train_inputs`` instead, by the
    presence of the counts sidecar, because it is a property of when the
    directory was written rather than of the objective naming it.

    Non-ranking objectives all map to ``"binary"``. That collision is
    deliberate: they build a byte-identical .bin (same X/y/weight, no group
    vector) with no divergence planned, and it keeps existing
    ``lgb/binary/`` dirs valid with no migration.

    Returned values are drawn from RANKING_OBJECTIVES plus the literal
    ``"binary"``, never arbitrary config text, so the segment is path-safe.
    """
    if is_ranking_objective(objective):
        return objective
    return "binary"


def objective_drops_zero_positive_groups(objective: str | None) -> bool:
    """True iff ``objective`` trains only on query groups holding a positive.

    ``lambdarank`` and only ``lambdarank``. A query group whose labels are all
    zero yields no pair of differing labels, so its gradient contribution is
    *exactly* zero — measured on LightGBM 4.6.0 with an all-zero-label group
    over 50 rounds: 1 tree, 0 splits, zero variance in the predictions. Those
    rows cost training time and teach nothing.

    ``rank_xendcg`` genuinely learns from them and must keep every row. Its
    target distribution ``q_i = (2^y_i - g_i) / sum_j (2^y_j - g_j)`` draws a
    fresh random ``g`` each round, so an all-zero-label group becomes a
    *random* ranking to fit rather than a flat one — the same data grew 50
    trees, all with splits, prediction std 0.045.

    Derived from ``objective``, deliberately **not** a config key: a key would
    add a state ("filter on, objective rank_xendcg") that has no correct
    meaning. What the filter did is answered by the log and the manifest, not
    by config.

    Named for its argument because the sibling that does the work is one
    letter away: this one answers *whether* an objective filters,
    :func:`drop_zero_positive_groups` performs it.
    """
    return objective == "lambdarank"


def _groups_with_positives_mask(
    y: np.ndarray, group_ids: np.ndarray
) -> np.ndarray:
    """Row mask: True where the row's query group holds at least one positive.

    Private because every caller wants the rows, not the mask —
    :func:`drop_zero_positive_groups` is the one place that applies it, which
    is what keeps the aligned arrays from being sliced by two different masks.
    """
    y = np.asarray(y)
    group_ids = np.asarray(group_ids)
    if y.ndim != 1 or group_ids.ndim != 1:
        raise ValueError(
            f"y and group_ids must be 1-D (one per row), got shapes "
            f"{y.shape} and {group_ids.shape}"
        )
    if y.shape[0] != group_ids.shape[0]:
        raise ValueError(
            f"y and group_ids must have the same length (one per row), got "
            f"{y.shape[0]} and {group_ids.shape[0]}"
        )
    if y.shape[0] == 0:
        return np.empty(0, dtype=bool)
    # inverse gives each row the dense index of its group, so one bincount
    # counts positives per group without sorting or grouping the rows.
    _, inverse = np.unique(group_ids, return_inverse=True)
    positives_per_group = np.bincount(inverse, weights=(y > 0).astype(np.float64))
    return positives_per_group[inverse] > 0


def drop_zero_positive_groups(
    y: np.ndarray, group_ids: np.ndarray, *aligned: np.ndarray
) -> tuple[tuple[np.ndarray, ...], dict]:
    """Rows of query groups holding a positive, plus what was dropped.

    ``y`` and ``group_ids`` are the per-row label and query-group id arrays
    from ``recsys_tfb.io.extract.extract_Xy_with_groups``; ``aligned`` is
    every other per-row array that has to travel with them (the feature
    matrix, the sample weights). Group ids need not be sorted or contiguous,
    and groups may differ in size.

    Returns ``((y, group_ids, *aligned), counts)`` — the arrays in the order
    they were given, and a dict of ``groups_total`` / ``groups_kept`` /
    ``groups_dropped`` / ``rows_total`` / ``rows_kept`` / ``rows_dropped``.

    One function rather than an exported mask, and varargs rather than a fixed
    signature, for the same reason: every array is sliced by the *same* mask
    in one place. A weight vector sliced by a second mask — or left unsliced —
    re-assigns every weight to another row, and nothing downstream raises.

    The counts come back rather than being logged here so this stays a pure
    numpy function (this module is shared with a future XGBoost adapter);
    both call sites log them in their own words, and the .bin cache persists
    them.

    Any label > 0 counts as a positive, so a graded-relevance ``label_gain``
    setup keeps working; only an all-zero group is dropped. Empty input comes
    back empty with all counts zero.
    """
    mask = _groups_with_positives_mask(y, group_ids)
    group_ids = np.asarray(group_ids)
    counts = {
        "groups_total": int(np.unique(group_ids).size),
        "groups_kept": int(np.unique(group_ids[mask]).size),
        "rows_total": int(mask.size),
        "rows_kept": int(mask.sum()),
    }
    counts["groups_dropped"] = counts["groups_total"] - counts["groups_kept"]
    counts["rows_dropped"] = counts["rows_total"] - counts["rows_kept"]
    kept = tuple(np.asarray(a)[mask] for a in (y, group_ids, *aligned))
    return kept, counts


def default_metric_for_objective(
    objective: str | None, metric: str | None
) -> str | None:
    """Default the eval metric to ``"ndcg"`` for a ranking objective with no
    metric set.

    LightGBM binary metrics are invalid under lambdarank/rank_xendcg and make
    early stopping silently meaningless. An explicitly-set *contradictory*
    metric is rejected upstream by the A7 consistency check
    (``ranking_objective_conflicts``); this helper only fills the
    *unset* case so behaviour is never silently wrong.
    """
    if is_ranking_objective(objective) and not metric:
        return "ndcg"
    return metric


def to_contiguous_groups(group_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Make every query group contiguous and return ``(sort_perm, counts)``.

    ``group_ids``: per-row int array from ``extract_Xy_with_groups`` — rows in
    the same query share an id; ids are NOT guaranteed contiguous or sorted.
    LightGBM/XGBoost ranking need rows ordered so each group is one
    consecutive block plus a run-length ``group`` count vector.

    Returns:
        sort_perm: int64 index array. Apply as ``X[sort_perm]``,
            ``y[sort_perm]`` before building the Dataset.
        counts: int64 run-length per group, ordered to match the sorted rows;
            ``counts.sum() == len(group_ids)``.

    Empty input returns two empty int64 arrays. The sort is stable so row
    order within a group is preserved and the result is deterministic
    regardless of the integer labels chosen for the groups.
    """
    group_ids = np.asarray(group_ids)
    if group_ids.ndim != 1:
        raise ValueError(
            f"group_ids must be 1-D (one id per row), got shape "
            f"{group_ids.shape}"
        )
    if group_ids.shape[0] == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64)
    sort_perm = np.argsort(group_ids, kind="stable").astype(np.int64)
    sorted_ids = group_ids[sort_perm]
    _, counts = np.unique(sorted_ids, return_counts=True)
    return sort_perm, counts.astype(np.int64)
