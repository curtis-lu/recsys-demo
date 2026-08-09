"""Category vocabularies: where one comes from.

The dataset pipeline's half of categorical encoding. What is only ever a
dataset concern is where a vocabulary comes from — the leakage-free fit over
the train months, and the schema declaration that supplies a domain the data
cannot. Everything downstream of the vocabulary is shared with the inference
pipeline and lives in ``recsys_tfb.preprocessing``: the encoding itself
("a value outside the vocabulary becomes the sentinel"), which categoricals a
frame may encode, and the count of how many fell outside. The last of those
lived here until #185, when inference acquired the same warning.

Named per ADR-0008 §2 for the concern it implements. Each function is one
mechanism; the decisions they serve — leakage-free fit, vocabulary source —
are named at the call sites in ``nodes.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


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


def require_declared_categoricals(
    categorical_values: dict,
    columns: list[str],
) -> None:
    """Pre-check: every column whose vocabulary must be declared, is.

    Names every undeclared column at once. There is no fallback to "collect it
    from the data": these columns are not in ``feature_table``, so the data
    cannot answer — an undeclared one would get an empty vocabulary and encode
    every row to the unknown sentinel, losing the dimension without raising.
    """
    missing = [c for c in columns if c not in categorical_values]
    if missing:
        raise DataConsistencyError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )


def read_declared_vocabularies(
    categorical_values: dict,
    columns: list[str],
) -> dict[str, list]:
    """Each column's vocabulary as declared in ``schema.categorical_values``.

    Assumes the declarations exist — :func:`require_declared_categoricals` is
    the step that says so, and the caller runs it first.
    """
    return {col: list(categorical_values[col]) for col in columns}
