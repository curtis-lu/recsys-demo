"""Sampling mechanics for key selection: the columns a draw needs, the effective
per-row ratio, the draw itself (per row or per entity), the columns a split's
keys come out with, and how a draw reports itself.

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
    from pyspark.sql import DataFrame

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
    partition layouts. ``site`` namespaces the draw: two callers sharing a seed
    (train vs calibration) would otherwise select the same rows.

    Requires the frame to already carry ``_effective_ratio``
    (:func:`with_effective_sample_ratio`), and leaves its working columns on the
    frame for the caller's output-column step to drop.
    """
    keys = keys.withColumn("_bucket", spark_bucket(keys, identity_key, seed, site=site))
    threshold_expr = (F.col(EFFECTIVE_RATIO_COL) * F.lit(HASH_BUCKETS)).cast("int")
    return keys.filter(F.col("_bucket") < threshold_expr)


def keep_entities_drawn_under_ratio(
    keys: DataFrame,
    entity_cols: list[str],
    ratio: float,
    seed: int,
    *,
    site: str,
) -> DataFrame:
    """Keep every row of the entities whose own draw lands under ``ratio``.

    The draw is on the entity, not the row, so an entity is kept whole or not at
    all — which is what :func:`keep_rows_drawn_under_ratio` does *not* promise.

    ``entity_cols`` is a list, not a single column, because an entity is a
    *tuple* of columns in this framework. Hashing one of them would draw at a
    coarser unit than the caller asked for and keep or drop whole groups of
    entities together — silently, since the result is still a valid sample of
    something. The caller decides which columns those are (the whole entity, or
    the coarser unit it declared); this only performs the draw.
    """
    entities = keys.select(*entity_cols).distinct()
    sampled_entities = entities.withColumn(
        "_bucket", spark_bucket(entities, entity_cols, seed, site=site),
    ).filter(
        F.col("_bucket") < F.lit(ratio_to_threshold(ratio))
    ).select(*entity_cols)
    return keys.join(sampled_entities, on=entity_cols, how="inner")


def _ratio_lookup_df(spark, sample_ratio_overrides: dict) -> DataFrame:
    """Build a tiny ``(_gk -> _override_ratio)`` lookup as a JVM-side
    ``LocalRelation`` via a ``VALUES`` clause.

    Deliberately avoids ``spark.createDataFrame(list)``, which routes through
    ``sc.parallelize()`` and pickles rows with the driver's protocol (5 on
    Python 3.10) -> fails to unpickle on pre-3.8 Python workers. A ``VALUES``
    inline table is materialized entirely on the driver JVM and never touches
    Python workers, so the downstream broadcast hash-join is safe regardless of
    the cluster's worker Python. Mirrors the constraint documented in
    ``preprocessing._encode_categoricals``.
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
