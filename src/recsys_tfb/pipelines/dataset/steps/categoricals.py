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

import logging

from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError, categorical_dtype_errors
from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date
from recsys_tfb.utils.item_columns import combine_item_columns

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
    quietly invalidate an already-trained model against a refit preprocessor —
    and, with no model involved, the months ``apply_preprocessor_to_features``
    already wrote, since a full run refits here but encodes only the months it
    has not written yet.

    NULL is excluded rather than given an index — the encoder maps anything
    outside the vocabulary to the unknown sentinel, so a NULL and an
    unseen-in-training category land in the same place by construction.
    ``collect_set`` drops NULL itself.

    One aggregation over every column, not a ``distinct()`` per column. The
    per-column form cost a full scan of ``df`` and two to five Spark jobs per
    column, so it grew with the categorical count; this is one scan and a fixed
    one to three jobs whatever the count (the exact numbers move with AQE and
    with how ``df`` is partitioned — measured on ``local[*]``, 2026-09-19).
    ``sort_array`` rather than ``sorted`` on the driver because it is the
    ordering ``ORDER BY`` uses — the per-column form's — so every value keeps
    the index it had, by construction rather than by a Python/Spark coincidence.

    Not exact on a ``double``/``float`` column: in Spark 3.3.2 ``collect_set``
    keeps every NaN occurrence as its own element, and does not normalise
    ``-0.0`` — depending on how the rows are partitioned it keeps both zeros, or
    one with whichever sign arrived first — where ``distinct()`` folds each to a
    single value. A column holding NaN or ``-0.0`` would get a different
    vocabulary without an error, and a NaN-heavy one would hold every NaN in one
    task's memory. B5 rules such a column out, along with every other type
    outside string / integer / boolean (nested floats such as ``array<double>``
    differ the same way); :func:`require_supported_categorical_dtypes` is the
    step that says so, and the caller runs it first.

    Cost: one scan of ``df``. What reaches the driver is bounded by category
    cardinality, not by row count.
    """
    if not columns:
        # ``agg`` refuses an empty expression list, and there is nothing to scan.
        return {}
    row = df.agg(*[F.sort_array(F.collect_set(col)) for col in columns]).collect()[0]
    return {col: list(row[i]) for i, col in enumerate(columns)}


def require_supported_categorical_dtypes(
    columns: list[str],
    dtypes: dict[str, str],
    table: str = "feature_table",
) -> None:
    """Pre-check, runtime backstop of B5: every column to collect has a type a
    categorical may have — string, an integer type, or boolean.

    The Layer-2 gate already runs B5, but ``fit_preprocessor_metadata`` does not
    consume the gate's output, so a sliced run (``--only-node`` or
    ``--from-node fit_preprocessor_metadata``) skips it. Without this step such
    a run would hand a ``double``/``float`` column to
    :func:`collect_vocabularies_from_data`, which returns a wrong vocabulary
    without raising whenever the column holds a NaN or a ``-0.0``; a
    ``decimal``/``date``/``timestamp``/``binary`` one would crash the
    preprocessor's JSON save after the scan. The rule itself stays in
    ``core/consistency.py`` — this only raises on what it reports.

    ``dtypes`` is ``dict(df.dtypes)``: schema metadata, no Spark job.
    """
    errors = categorical_dtype_errors(columns, dtypes, table=table)
    if errors:
        raise DataConsistencyError(
            "Categorical dtype check failed (B5):\n- " + "\n- ".join(errors)
        )


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


def count_items_in_months(
    sample_pool: DataFrame,
    schema: dict,
    months: list,
) -> list:
    """The sorted distinct non-NULL items ``sample_pool`` offers as candidates
    in ``months`` — the item list when it is counted from the data (#379).

    A multi-column item is combined first (``combine_item_columns``, B16
    included), so the list holds the values every stage sees. The month
    filter is the one every read in the dataset pipeline uses
    (``months_filter_as_date``, ADR-0029 decision 3), and the collection is
    :func:`collect_vocabularies_from_data`'s: sorted, NULL excluded, one scan.
    What reaches the driver is bounded by the item count.
    """
    item = schema["item"]
    pool = combine_item_columns(sample_pool, schema, "sample_pool")
    pool = pool.filter(months_filter_as_date(schema["time"], months))
    return collect_vocabularies_from_data(pool, [item])[item]


def warn_items_outside_the_list(
    keys: DataFrame,
    item_col: str,
    items: list,
    split: str,
) -> list:
    """Log, as a warning, the items the split's ``keys`` hold that ``items``
    lacks, and return them sorted.

    What it scans is the keys table, not the model_input built from it. The
    callers hand in the landed ``val_keys`` / ``test_keys`` (test's already
    scoped to this run's months, a partition filter), so this is one Spark
    job reading one column of a partitioned parquet table, and no join. The
    model_input holds the same items — the build's joins are left joins from
    the keys — but it is unlanded, and a distinct over it would run the whole
    build join a second time. What reaches the driver is bounded by the item
    count.

    The items only — no row counts: counting them per item is one more
    aggregation over the split for a number that decides nothing (whether to
    retrain turns on which items are new, not on how many rows they have).
    """
    present = {
        r[0] for r in keys.select(item_col).distinct().collect()
        if r[0] is not None
    }
    new = sorted(present - set(items), key=str)
    if new:
        logger.warning(
            "%s holds %d new item(s) the item list (counted from the train "
            "months) lacks: %s. Their rows are kept and encode to the unknown "
            "code; the model has not seen them, so it ranks them as it ranks "
            "a missing value.", split, len(new), new,
        )
    return new
