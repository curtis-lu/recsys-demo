"""Query-group array helpers that hold for any ranking algorithm.

Operate on the per-row group-id array ``recsys_tfb.io.extract.
extract_Xy_with_groups`` produces: make each group one contiguous block with
the run-length counts ranking libraries take, drop the groups holding no
positive, count the groups a pairwise objective can learn nothing from.

*Which* objectives rank, which metrics they early-stop on and which of them
drop zero-positive groups are not here: those are facts about one library,
declared by its adapter (``ModelAdapter.rules``, ADR-0030 decision 3).

Pure module: imports only numpy.
"""

from __future__ import annotations

import numpy as np


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
    matrix; the sample weights, where the caller resolved them up front; or a
    plain ``np.arange`` row index, which is how a caller recovers *which*
    rows survived without deriving the mask a second time). Group ids need
    not be sorted or contiguous, and groups may differ in size.

    Returns ``((y, group_ids, *aligned), counts)`` — the arrays in the order
    they were given, and a dict of ``groups_total`` / ``groups_kept`` /
    ``groups_dropped`` / ``rows_total`` / ``rows_kept`` / ``rows_dropped``.

    One function rather than an exported mask, and varargs rather than a fixed
    signature, for the same reason: every array is sliced by the *same* mask
    in one place. A weight vector sliced by a second mask — or left unsliced —
    re-assigns every weight to another row, and nothing downstream raises.

    The counts come back rather than being logged here so this stays a pure
    numpy function; both call sites log them in their own words, and the
    .bin cache persists them.

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


def single_label_group_counts(
    y: np.ndarray, group_ids: np.ndarray
) -> dict[str, int]:
    """``{"groups_total", "groups_single_label"}`` — how many query groups hold
    rows of one label value only.

    A ranking objective learns from pairs of rows whose labels differ, so such
    a group gives it nothing to compare: all negatives, all positives, or a
    single row. Counted rather than filtered — whether a share is too high is
    the deployment's call, and a row-level draw that thins small groups is the
    usual cause (ADR-0025 decision H). Two different graded labels are
    comparable, so only equality counts. Group ids need not be contiguous.
    """
    y = np.asarray(y)
    group_ids = np.asarray(group_ids)
    if y.shape[0] == 0:
        return {"groups_total": 0, "groups_single_label": 0}
    _, inverse = np.unique(group_ids, return_inverse=True)
    n_groups = int(inverse.max()) + 1
    lo = np.full(n_groups, np.inf)
    hi = np.full(n_groups, -np.inf)
    np.minimum.at(lo, inverse, y.astype(np.float64))
    np.maximum.at(hi, inverse, y.astype(np.float64))
    return {
        "groups_total": n_groups,
        "groups_single_label": int(np.sum(lo == hi)),
    }


def to_contiguous_groups(group_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Make every query group contiguous and return ``(sort_perm, counts)``.

    ``group_ids``: per-row int array from ``extract_Xy_with_groups`` — rows in
    the same query share an id; ids are NOT guaranteed contiguous or sorted.
    Ranking libraries need rows ordered so each group is one consecutive
    block plus a run-length ``group`` count vector.

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
