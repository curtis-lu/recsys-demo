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


def objective_family(objective: str | None) -> str:
    """Coarse family used to key the on-disk lgb-binary cache sub-path.

    ``"ranking"`` for any RANKING_OBJECTIVES value (so lambdarank and
    rank_xendcg share one group-bearing binary), else ``"binary"``.
    """
    return "ranking" if is_ranking_objective(objective) else "binary"


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
