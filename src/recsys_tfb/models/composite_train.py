"""Composite (two-stage) training orchestration.

Driver-local pandas/numpy, consistent with the existing single-machine LightGBM
training. Folds are customer-disjoint via zlib.crc32 (the IEEE-802.3 polynomial,
matching Spark's F.crc32 used by the dataset split) so the same customer never
appears in two folds.
"""
from __future__ import annotations

import zlib

import numpy as np

_FOLD_SITE = "composite_oof"


def assign_folds(entity_ids: np.ndarray, n_folds: int, seed: int) -> np.ndarray:
    """Deterministic, customer-disjoint fold index in [0, n_folds) per row."""
    out = np.empty(len(entity_ids), dtype=np.int64)
    for i, e in enumerate(entity_ids):
        token = f"{_FOLD_SITE}|{seed}|{e}".encode()
        out[i] = zlib.crc32(token) % n_folds
    return out


def oof_is_leakage_clean(folds: np.ndarray, producing_fold: np.ndarray) -> bool:
    """True iff every row was scored out-of-fold.

    `producing_fold[i]` is the held-out fold whose booster scored row i. For a
    clean OOF, row i must be scored by the booster of its OWN fold (which trained
    on all OTHER folds), so `producing_fold[i] == folds[i]` for every row.
    """
    return bool(np.all(producing_fold == folds))
