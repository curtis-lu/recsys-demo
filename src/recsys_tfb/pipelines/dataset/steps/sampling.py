"""Sampling mechanics for key selection: the columns a draw needs, the effective
per-row ratio, the draw itself (per row or per whole unit), the columns a
split's keys come out with, how a draw reports itself, and the drop of rows
whose entity is NULL.

Named for the concern it implements, not for its backend — the ``_spark`` suffix
this module used to carry pointed at a pandas/Spark dual track that no longer
exists.

Each function here carries at most one mechanism. The four decisions ADR-0008 §1
counted inside the old ``select_keys`` — month filter, override precedence,
keep/drop by identity key, output columns = identity + carry — are now named
steps in the key-selecting nodes (``nodes.py``); this module holds only how each
step is computed on Spark.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.utils.hashing import HASH_BUCKETS, ratio_to_threshold, spark_bucket

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

logger = logging.getLogger(__name__)

EFFECTIVE_RATIO_COL = "_effective_ratio"


def log_sampled_keys(
    sample_ratio: float,
    group_keys: list[str],
    sample_ratio_overrides: dict | None,
    site: str | None,
) -> None:
    """Report a draw. ``site=None`` means no draw ran — nothing could be dropped.

    Written once and called from each key-selecting node: the nodes duplicate
    their *decisions* on purpose (ADR-0008 §2), but a log line is mechanism, and
    two copies of a format string drift.
    """
    if site is None:
        logger.info("Sampled keys (ratio=1.0, no sampling)")
        return
    logger.info(
        "Sampled keys (ratio=%.2f, group_keys=%s, overrides=%s, site=%s)",
        sample_ratio, group_keys, sample_ratio_overrides, site,
    )


def _any_column_is_null(cols: list[str]) -> Column:
    """Row-wise predicate: at least one of ``cols`` is NULL on this row.

    Handed back as a Column instead of being applied to a frame so one caller
    can use it and its negation. A keep-filter and a drop-filter written as two
    separate expressions can drift into overlapping or into leaving a gap, and
    neither shows up as an error -- the rows just appear twice or vanish.

    ``isNull()`` never evaluates to NULL itself, so ``~`` on this is an exact
    complement, not the three-valued logic that would apply to a comparison.
    """
    predicate = F.lit(False)
    for col in cols:
        predicate = predicate | F.col(col).isNull()
    return predicate


def drop_rows_with_null_entity(
    keys: DataFrame,
    entity_cols: list[str],
    *,
    split: str,
) -> tuple[DataFrame, bool]:
    """``keys`` without the rows whose entity is NULL in any column, and
    whether there were any -- reported by :func:`_warn_dropped_null_entity`
    when there were.

    The flag is handed back because a caller may have to tell "nothing
    arrived" from "everything arrived broken" later on, and those are fixed in
    different places (``split_train_keys``' empty-split message).

    Costs one ``isEmpty`` -- and note which way it short-circuits: it stops at
    the first NULL row, so a *dirty* input answers at once while a clean one
    has to read ``entity_cols`` across the whole frame to prove there is
    nothing there. A narrow scan with no shuffle, over whatever months the
    caller already filtered ``keys`` to -- which is why callers filter first.
    The report's own pass is paid only when there is something to say.
    """
    is_null = _any_column_is_null(entity_cols)
    dropped = keys.filter(is_null)
    dropped_any = not dropped.isEmpty()
    if dropped_any:
        _warn_dropped_null_entity(dropped, entity_cols, split=split)
    return keys.filter(~is_null), dropped_any


def _warn_dropped_null_entity(
    dropped: DataFrame, cols: list[str], *, split: str,
) -> None:
    """Report rows dropped for a NULL entity: whose keys, how many, and in
    which column.

    ``warn_``, not ``require_``: this reports, it does not raise (rule 12 of
    docs/agents/pipeline-node-design.md). Whose job it is to raise is settled
    in ADR-0006 — upstream, in ``source_etl``.

    One Spark action for the whole report: the row total and every per-column
    NULL count come out of a single ``agg``.

    Per-column counts rather than a bare total, because an entity can be
    several columns: a total leaves the reader auditing all of them, and the
    zeros are what say the rest are clean. The split leads the line because
    every split drops these (ADR-0029 decision 5), and "which table is dirty"
    starts with "whose months".
    """
    counts = dropped.agg(
        F.count(F.lit(1)).alias("_dropped"),
        *[
            F.count(F.when(F.col(c).isNull(), F.lit(1))).alias(f"_null_{c}")
            for c in cols
        ],
    ).first()
    per_column = ", ".join(f"{c}={counts[f'_null_{c}']}" for c in cols)
    logger.warning(
        "%s keys: dropped %d row(s) whose entity is NULL (NULL by column: "
        "%s). Such a row belongs to no entity: it joins to neither features "
        "nor labels. Every split drops these before its keys land (train and "
        "train_dev when they are split off sample_keys); this reports them. "
        "The check that owns them is upstream -- source_etl's "
        "primary_key_not_null (ADR-0006).",
        split, counts["_dropped"], per_column,
    )


def sampling_columns(
    group_keys: list[str],
    identity_key: list[str],
    carry_columns: list[str],
) -> list[str]:
    """Columns the draw has to see, deduplicated and in that precedence order.

    Projecting down to these before the draw is what keeps the widest frame in
    the pipeline from being carried through the hash and the override join.
    """
    return list(dict.fromkeys(
        list(group_keys) + list(identity_key) + list(carry_columns)
    ))


def key_output_columns(
    identity_key: list[str],
    carry_columns: list[str],
) -> list[str]:
    """The identity key, plus carried columns that are not already part of it."""
    return list(identity_key) + [c for c in carry_columns if c not in identity_key]


def draw_can_drop_rows(sample_ratio: float, sample_ratio_overrides: dict | None) -> bool:
    """Whether a draw could remove anything at all.

    A full ratio with no overrides cannot: every bucket is under the threshold.
    Callers use this to skip the draw rather than compute a crc32 over the whole
    pool only to satisfy it trivially.
    """
    return sample_ratio < 1.0 or bool(sample_ratio_overrides)


def with_effective_sample_ratio(
    keys: DataFrame,
    group_keys: list[str],
    sample_ratio: float,
    sample_ratio_overrides: dict | None,
) -> DataFrame:
    """Attach ``_effective_ratio``: a matching group's override, else the default.

    The group key is the ``group_keys`` values joined with ``"|"``, every part
    cast to string so it matches the override dict's keys byte-for-byte.

    Mapping is a broadcast hash-join: one O(1) probe per row, versus the linear
    CASE-WHEN chain it replaced (O(n_overrides) string compares per row, which
    dominated CPU at ~10^2 overrides x ~10^8 rows). Row-for-row identical:
    matched key -> override ratio, unmatched -> ``sample_ratio``. The dict has
    unique keys, so the left join is 1:1 and never fans out rows.
    """
    if not sample_ratio_overrides:
        return keys.withColumn(EFFECTIVE_RATIO_COL, F.lit(sample_ratio))

    if len(group_keys) == 1:
        group_key_col = F.col(group_keys[0]).cast("string")
    else:
        group_key_col = F.concat_ws("|", *[F.col(k).cast("string") for k in group_keys])

    ratio_df = _ratio_lookup_df(keys.sparkSession, sample_ratio_overrides)
    return (
        keys.withColumn("_gk", group_key_col)
        .join(F.broadcast(ratio_df), on="_gk", how="left")
        .withColumn(
            EFFECTIVE_RATIO_COL,
            F.coalesce(F.col("_override_ratio"), F.lit(sample_ratio)),
        )
    )


def keep_rows_drawn_under_ratio(
    keys: DataFrame,
    identity_key: list[str],
    seed: int,
    *,
    site: str,
) -> DataFrame:
    """Keep the rows whose identity key's deterministic draw lands under its ratio.

    ``crc32(identity_key | site | seed) % HASH_BUCKETS < _effective_ratio *
    HASH_BUCKETS``, so the same key draws the same way across reruns and
    partition layouts. ``site`` namespaces the draw, so two callers sharing a
    seed do not select the same rows. This helper has one caller today
    (``select_train_keys``, ``site="sample_keys"``) — #414 removed the
    calibration split, which was the second — but the seed is shared with
    ``split_train_keys`` (``site="split_train_dev"``) and ``select_val_keys``
    (``site="val_keys"``, via :func:`keep_entities_drawn_under_ratio`), so the
    namespacing is still doing work.

    Requires the frame to already carry ``_effective_ratio``
    (:func:`with_effective_sample_ratio`), and leaves its working columns on the
    frame for the caller's output-column step to drop.
    """
    keys = keys.withColumn("_bucket", spark_bucket(keys, identity_key, seed, site=site))
    threshold_expr = (F.col(EFFECTIVE_RATIO_COL) * F.lit(HASH_BUCKETS)).cast("int")
    return keys.filter(F.col("_bucket") < threshold_expr)


def unit_drawn_under_ratio(
    df: DataFrame,
    unit_cols: list[str],
    ratio: float,
    seed: int,
    *,
    site: str,
) -> Column:
    """Row-wise predicate: this row's unit -- its ``unit_cols`` values --
    draws a bucket under ``ratio``.

    The one mechanism behind every "a whole unit lands on one side" in this
    pipeline: the val draw, the train / train_dev split, and the zero-positive
    group draw (ADR-0029 decision 5). The bucket reads nothing but the unit's
    own values, so every row of one unit gets the same answer -- the unit
    stays whole by construction, not because a join put it back together. The
    old shape (distinct the units, bucket them, join back) gave the same
    answer through three shuffles, and made "whole" a property of the join
    keys instead.

    Except on NULL, and silently: ``concat_ws`` skips a NULL, so a row with a
    NULL unit column still gets a bucket, where the join dropped it (NULL
    never equals NULL). So callers drop NULL-entity rows first
    (:func:`drop_rows_with_null_entity`).

    ``~`` on this is an exact complement: the bucket is never NULL (``crc32``
    of a ``concat_ws``, which is never NULL), so the comparison never is
    either. That is what lets the split take one side with it and the other
    side with its negation.

    ``unit_cols`` is a list, not a single column, because an entity is a
    *tuple* of columns in this framework. Hashing one of them would draw at a
    coarser unit than the caller asked for and keep or drop whole groups of
    units together — silently, since the result is still a valid sample of
    something. The caller decides which columns those are; this only performs
    the draw.
    """
    bucket = spark_bucket(df, unit_cols, seed, site=site)
    return bucket < F.lit(ratio_to_threshold(ratio))


def keep_entities_drawn_under_ratio(
    keys: DataFrame,
    unit_cols: list[str],
    ratio: float,
    seed: int,
    *,
    site: str,
) -> DataFrame:
    """Keep every row of the entities whose own draw lands under ``ratio``.

    The draw is on the entity, not the row, so an entity is kept whole or not at
    all — which is what :func:`keep_rows_drawn_under_ratio` does *not* promise.
    It is :func:`unit_drawn_under_ratio` applied as a filter, so it neither
    shuffles nor de-duplicates: a key that appears twice in ``keys`` comes out
    twice or not at all.

    ``unit_cols`` is whatever the caller draws on — the whole entity, or the
    coarser unit it declared (``dataset.val_sample_keys``).
    """
    return keys.filter(
        unit_drawn_under_ratio(keys, unit_cols, ratio, seed, site=site)
    )


def _ratio_lookup_df(spark, sample_ratio_overrides: dict) -> DataFrame:
    """Build a tiny ``(_gk -> _override_ratio)`` lookup as a JVM-side
    ``LocalRelation`` via a ``VALUES`` clause.

    Deliberately avoids ``spark.createDataFrame(list)``, which routes through
    ``sc.parallelize()`` and pickles rows with the driver's protocol (5 on
    Python 3.10) -> fails to unpickle on pre-3.8 Python workers. A ``VALUES``
    inline table is materialized entirely on the driver JVM and never touches
    Python workers, so the downstream broadcast hash-join is safe regardless of
    the cluster's worker Python. Mirrors the constraint documented in
    ``preprocessing.encode_categoricals``.
    """
    def _esc(s) -> str:
        # Single-quote string literals: double embedded quotes, escape backslash.
        return str(s).replace("\\", "\\\\").replace("'", "''")

    rows = ", ".join(
        f"('{_esc(gk)}', CAST({float(ratio)} AS DOUBLE))"
        for gk, ratio in sample_ratio_overrides.items()
    )
    return spark.sql(
        f"SELECT * FROM VALUES {rows} AS _ratio_lookup(_gk, _override_ratio)"
    )
