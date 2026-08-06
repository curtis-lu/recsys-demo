"""Category vocabularies: how one gets built, and how unknowns get reported.

The dataset pipeline's half of categorical encoding. The *encoding* itself —
"a value outside the vocabulary becomes the sentinel" — is shared with the
inference pipeline and lives in ``recsys_tfb.preprocessing``; what is only ever
a dataset concern is where a vocabulary comes from (the fit) and how many values
fell outside it (the apply-side warning).

Named per ADR-0008 §2 for the concern it implements. Each function is one
mechanism; the decisions they serve — leakage-free fit, vocabulary source —
are named at the call sites in ``nodes.py``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.preprocessing import UNKNOWN_CATEGORY_CODE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def collect_vocabularies_from_data(
    df: DataFrame,
    columns: list[str],
) -> dict[str, list]:
    """Each column's sorted distinct non-NULL values, as observed in ``df``.

    Sorted, because a value's *index* in this list is its encoded value: an
    unstable order would re-encode the same data differently between runs and
    quietly invalidate an already-trained model against a refit preprocessor.

    NULL is excluded rather than given an index — the encoder maps anything
    outside the vocabulary to the unknown sentinel, so a NULL and an
    unseen-in-training category land in the same place by construction.

    Cost: one ``distinct().collect()`` per column. What reaches the driver is
    bounded by category cardinality, not by row count.
    """
    vocabularies: dict[str, list] = {}
    for col in columns:
        distinct_rows = (
            df.select(col)
            .filter(F.col(col).isNotNull())
            .distinct()
            .orderBy(col)
            .collect()
        )
        vocabularies[col] = [row[col] for row in distinct_rows]
    return vocabularies


def read_declared_vocabularies(
    categorical_values: dict,
    columns: list[str],
) -> dict[str, list]:
    """Each column's vocabulary as declared in ``schema.categorical_values``.

    Raises ``DataConsistencyError`` naming every undeclared column at once.
    There is no fallback to "collect it from the data": these columns are not in
    ``feature_table``, so the data cannot answer — an undeclared one would get
    an empty vocabulary and encode every row to the unknown sentinel, losing the
    dimension without raising.
    """
    missing = [c for c in columns if c not in categorical_values]
    if missing:
        raise DataConsistencyError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )
    return {col: list(categorical_values[col]) for col in columns}


def warn_unknown_encodings(df: DataFrame, columns: list[str]) -> None:
    """Warn once per column that encoded any value to the unknown sentinel.

    Single pass: one aggregation returns the sentinel count for every encoded
    column at once. The per-column ``.count()`` this replaced re-scanned the
    full multi-month feature_table once per categorical (N actions).
    """
    if not columns:
        return
    unknown_counts = df.agg(*[
        F.sum(F.when(F.col(c) == UNKNOWN_CATEGORY_CODE, 1).otherwise(0)).alias(c)
        for c in columns
    ]).collect()[0]
    for col in columns:
        n_unknown = unknown_counts[col] or 0
        if n_unknown > 0:
            logger.warning(
                "apply_preprocessor_to_features: %d unknowns in column '%s'",
                n_unknown, col,
            )
