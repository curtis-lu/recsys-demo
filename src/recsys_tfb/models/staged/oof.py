"""Stage-2 OOF cross-fitting: entity-hash folds + leakage guard (spec D5).

Folds are entity-disjoint via zlib.crc32 (IEEE-802.3, the same polynomial
family as Spark's F.crc32 used by the dataset split), keyed on the row's
entity identity string — callers with a multi-column entity schema pass the
'|'-joined composite string. Fold site "staged_oof" is deliberately distinct
from PR #68's reference ("composite_oof"): assignments are internal to this
design, no cross-compatibility intended.
"""

import zlib

import numpy as np

_FOLD_SITE = "staged_oof"


def assign_folds(entity_keys: np.ndarray, n_folds: int, seed: int) -> np.ndarray:
    """Deterministic, entity-disjoint fold index in [0, n_folds) per row.

    Hash computed once per distinct entity then broadcast —
    len(unique) << len(rows) at our scale (spec D12).
    """
    if int(n_folds) < 2:
        raise ValueError(
            f"n_folds must be >= 2 for OOF cross-fitting, got {n_folds!r}")
    keys = np.asarray(entity_keys)
    uniq, inv = np.unique(keys, return_inverse=True)
    fold_of = np.array(
        [zlib.crc32(f"{_FOLD_SITE}|{seed}|{e}".encode()) % int(n_folds)
         for e in uniq],
        dtype=np.int64,
    )
    return fold_of[inv]


def oof_is_leakage_clean(folds: np.ndarray, producing_fold: np.ndarray) -> bool:
    """True iff every row was scored by its OWN fold's held-out booster
    (which trained on all OTHER folds)."""
    folds = np.asarray(folds)
    producing_fold = np.asarray(producing_fold)
    return bool(len(folds) == len(producing_fold)
                and np.all(producing_fold == folds))
