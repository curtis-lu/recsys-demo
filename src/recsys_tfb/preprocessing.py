"""Encoding and dtype mechanics shared by the dataset and inference pipelines.

This module is what is left of the old ``preprocessing/`` package after #168:
the two mechanisms that genuinely have callers on both sides of a pipeline
boundary, plus the key contract of the artifact those two sides exchange.
Everything else in that package had exactly one consumer and now lives with it.

Both mechanisms are pure mechanism, not decision: ``_encode_categoricals``
implements "unknown category -> sentinel" once the sentinel is chosen, and
``_cast_feature_floats_to_float32`` implements "numeric features converge on
float32" once that convergence is decided. The decisions themselves are named
by the callers' steps; see ADR-0008 section 2 for where that line is drawn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from pyspark.sql import functions as F
from pyspark.sql import types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


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
            ``_encode_categoricals`` writes.
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


def _encode_categoricals(
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
            result = result.withColumn(col, F.lit(-1).cast("integer"))
            continue
        pairs: list = []
        for idx, cat in enumerate(categories):
            pairs.extend([F.lit(cat), F.lit(idx)])
        map_col = F.create_map(*pairs)
        result = result.withColumn(
            col, F.coalesce(map_col[F.col(col)], F.lit(-1)).cast("integer")
        )
    return result


def _cast_feature_floats_to_float32(
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
    feature_set = set(feature_cols)
    casted_feature_cols = [
        f.name for f in df.schema.fields
        if f.name in feature_set
        and isinstance(f.dataType, (T.DecimalType, T.DoubleType))
    ]
    for col in casted_feature_cols:
        df = df.withColumn(col, F.col(col).cast("float"))
    return df, casted_feature_cols
