"""Encoding and dtype mechanics shared by the dataset and inference pipelines.

This module is what is left of the old ``preprocessing/`` package after #168:
the mechanisms that genuinely have callers on both sides of a pipeline
boundary, plus the key contract of the artifact those two sides exchange.
Everything else in that package had exactly one consumer and now lives with it.

Membership follows that "callers on both sides" test, not history:
``encodable_categoricals`` and ``warn_unknown_encodings`` moved here from
``pipelines/dataset/steps/`` in #185, when the inference pipeline stopped
encoding identity categoricals on the Spark side and became their second
caller. Until then they had one consumer, and the same rule put them there.

Everything here is mechanism, not decision: ``encode_categoricals``
implements "unknown category -> sentinel" once the sentinel is chosen,
``encodable_categoricals`` implements "an identity categorical is not this
frame's to encode" once identity is defined, and
``cast_feature_floats_to_float32`` implements "numeric features converge on
float32" once that convergence is decided. The decisions themselves are named
by the callers' steps; see ADR-0008 section 2 for where that line is drawn.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypedDict

from pyspark.sql import functions as F
from pyspark.sql import types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class PreprocessorMetadata(TypedDict):
    """The key contract of ``preprocessor.json`` (catalog entry ``preprocessor``).

    This is the one thing in the codebase that is genuinely shared across
    pipelines: the **writer** lives in the dataset pipeline
    (``fit_preprocessor_metadata``) and the **reader** lives in the inference
    pipeline (``apply_preprocessor``), with training reading it too via
    ``models/feature_selection.py``. Until #168 the two sides agreed only
    because they happened to sit in the same file; this class is what replaces
    that coincidence with a definition point.

    Why it needs one: a key renamed or added on the dataset side is invisible to
    the reader until inference runs, and inference is the pipeline this repo
    cannot exercise end-to-end locally (#63). The failure mode is not a crash at
    the boundary but a ``dict`` lookup that silently applies stale semantics.

    Keys:
        feature_columns: Ordered model feature columns. The order is load-bearing
            — it is the numpy column layout, the ``feature_name`` baked into the
            LightGBM ``.bin``, and the booster's reported names, all of which
            must agree.
        categorical_columns: Subset of columns that are encoded via
            ``category_mappings`` rather than used as raw numerics.
        category_mappings: Per categorical column, the ordered list of category
            values fit on the train months. A value's index in this list is its
            encoded value; anything absent encodes to the ``-1`` sentinel that
            ``encode_categoricals`` writes.
        drop_columns: Columns excluded from ``feature_columns`` by configuration.
            Kept in the artifact because the inference side needs to drop them
            from its scoring frame too.

    Not enforced at runtime: ``TypedDict`` is erased at import time and this repo
    runs no type checker. Two tests are what keep it honest —
    ``tests/test_preprocessing.py::TestPreprocessorMetadataContract`` pins these
    key names, and ``test_dataset/test_nodes.py::TestFitPreprocessorMetadataKeyContract``
    asserts the real fit output's key set against ``__annotations__``. The reader
    side needs no test of its own: renaming a key it uses already turns the
    inference node tests red.
    """

    feature_columns: list[str]
    categorical_columns: list[str]
    category_mappings: dict[str, list]
    drop_columns: list[str]


UNKNOWN_CATEGORY_CODE = -1
"""The encoded value of a category the fit never saw.

Defined once, here, because both halves of the round trip depend on it: the
encoder writes it, and the dataset pipeline counts it to report how much of a
month fell outside its vocabulary. Negative on purpose — every real category
gets its index in ``category_mappings``, so no valid value can collide with it.
"""


def encodable_categoricals(
    categorical_cols: list[str],
    frame_cols: list[str],
    identity_cols: list[str],
) -> list[str]:
    """The categoricals a Spark-side frame is entitled to encode itself.

    Present in the frame, and not an identity column. An identity categorical
    (``schema.item`` and friends) has a second life as an output key — the
    dataset pipeline replaces it from ``keys`` at join time, and the inference
    pipeline writes it to a partition column — so encoding it here either
    encodes a column that is about to be replaced or, worse, ships the integer
    code into a partition directory name (ADR-0010 section 6). Both pipelines
    defer it to the driver instead, where ``io/extract.py::pdf_to_X`` encodes
    a *copy* and leaves the identity value alone.
    """
    return [
        c for c in categorical_cols
        if c in frame_cols and c not in identity_cols
    ]


def encode_categoricals(
    df: DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> DataFrame:
    """Encode categorical columns via Spark SQL map literal. Unknown values -> -1.

    Uses F.create_map (JVM-side) instead of createDataFrame(list) + broadcast join
    to avoid sc.parallelize(), which would pickle data with the driver's protocol
    (5 on Python 3.10) and fail on Python 3.6 workers.
    """
    result = df
    for col in categorical_cols:
        categories = category_mappings[col]
        if not categories:
            result = result.withColumn(
                col, F.lit(UNKNOWN_CATEGORY_CODE).cast("integer"),
            )
            continue
        pairs: list = []
        for idx, cat in enumerate(categories):
            pairs.extend([F.lit(cat), F.lit(idx)])
        map_col = F.create_map(*pairs)
        result = result.withColumn(
            col,
            F.coalesce(
                map_col[F.col(col)], F.lit(UNKNOWN_CATEGORY_CODE),
            ).cast("integer"),
        )
    return result


def warn_unknown_encodings(
    df: DataFrame,
    columns: list[str],
    *,
    context: str,
) -> None:
    """Warn once per column that encoded any value to the unknown sentinel.

    Single pass: one aggregation returns the sentinel count for every encoded
    column at once. The per-column ``.count()`` this replaced re-scanned the
    full multi-month feature_table once per categorical (N actions).

    ``context`` names the calling node, because the two callers report on
    different populations: the dataset side sees a value the *train months*
    never showed, the inference side a value the *scoring population* holds but
    the fit never saw. Both are the same sentinel and the same question — how
    much of this frame fell outside the vocabulary — asked of different data.
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
                "%s: %d unknowns in column '%s'", context, n_unknown, col,
            )


def castable_numeric_feature_columns(
    schema: T.StructType,
    feature_cols: list[str],
) -> list[str]:
    """The feature columns ``cast_feature_floats_to_float32`` would convert.

    Split out from the cast itself because a second caller needs the answer
    *without* the conversion: the dataset pipeline's B8 precision gate
    (``core/consistency.py``) checks exactly the columns the cast will narrow,
    and reading the same selector is what makes that a structural fact instead
    of two lists that have to be kept in step by hand. Widening the cast is
    therefore one edit here, and the gate widens with it.

    Takes a ``StructType`` rather than a DataFrame so the rule is testable
    without a SparkSession. Returns frame order, not ``feature_cols`` order —
    the cast rebuilds columns in the order the frame holds them.
    """
    feature_set = set(feature_cols)
    return [
        f.name for f in schema.fields
        if f.name in feature_set
        and isinstance(f.dataType, (T.DecimalType, T.DoubleType))
    ]


def cast_feature_floats_to_float32(
    df: DataFrame,
    feature_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Cast DecimalType and DoubleType columns within feature_cols to float (float32).

    Invariant: model_input's numeric feature columns are stored as float32.

    LightGBM is histogram-based GBT (max_bin=256, so split decisions resolve
    at log2(256)=8-bit granularity). float32's ~7-digit decimal precision is
    far beyond what binning can use, making float64 / decimal128 pure waste:

    - decimal128 is the disaster case: pandas/pyarrow materializes it as
      Python ``decimal.Decimal`` objects (~70 B/value vs 4 B/float32), so
      extract_Xy peak memory explodes (originally OOM-killed the val read).
    - DoubleType is the silent case: ~2x the memory of float32 (8 vs 4 B)
      and ~2x slower on SIMD-vectorized pandas ops, with zero compensating
      benefit for the model.

    Identity and label columns are intentionally NOT cast — they should not
    be a numeric float type to begin with, and silent coercion of primary
    keys / label dtype would mask a real schema bug.

    Returns:
        (df, casted_cols) where ``casted_cols`` is the subset of
        ``feature_cols`` that were DecimalType or DoubleType.
    """
    casted_feature_cols = castable_numeric_feature_columns(df.schema, feature_cols)
    for col in casted_feature_cols:
        df = df.withColumn(col, F.col(col).cast("float"))
    return df, casted_feature_cols
