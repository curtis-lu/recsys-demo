"""Assembling the scoring population's feature frame: membership, enrichment,
the chunk boundary, and the width of what gets landed.

The through-line of this module is that **membership and features come from
different tables and must not be allowed to merge into one question**.
``inference_population`` says who gets scored; ``feature_table`` says what is
known about them. Every function here is written so that a gap in the second
never removes a row decided by the first — losing an entity would shorten the
published ranking with nothing going red.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from pyspark.sql import functions as F

from recsys_tfb.pipelines.inference.steps.partitions import ENTITY_BUCKET_COL
from recsys_tfb.utils.hashing import spark_bucket

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

#: Salt for the entity bucket hash. Fixed rather than configurable on purpose:
#: the bucket assignment only has to be *stable*, and a knob here would let
#: someone reshuffle every entity between runs, orphaning every partition
#: already written for this ``model_version``.
_BUCKET_SITE = "inference_entity_bucket"
_BUCKET_SEED = 0


def population_members(
    inference_population: DataFrame,
    time_col: str,
    join_key: list[str],
    snap_dates: list,
) -> DataFrame:
    """The run's members, keys only, for the configured months.

    Projected down to the join key before anything else touches it: everything
    downstream needs membership, not the population table's own columns, and a
    wide frame here would be carried through the coverage aggregation as well.

    ``cast("date")`` before the comparison for the same reason
    ``dataset/steps/scoping.months_filter_as_date`` does it: a time column
    reaches a node as DATE from a source table but as a **string** when the
    catalog reloads it from a Hive partition column, and ``isin`` against the
    unnormalised form matches zero rows while raising nothing.
    """
    return (
        inference_population
        .filter(F.col(time_col).cast("date").isin(snap_dates))
        .select(*join_key)
    )


def require_population_covers_snap_dates(
    members: DataFrame, time_col: str, snap_dates: list,
) -> None:
    """Pre-check: every configured month exists in ``inference_population``.

    Fail loud rather than score the months that happen to be there. A month
    missing upstream would otherwise produce a complete-looking run that
    published a subset of what its config claims — and the resume planner would
    then read the absent partitions as work still to do on every later run,
    never as an error.

    Cost: one ``distinct().collect()`` over the time column. What lands on the
    driver is bounded by the month count, not by the population size.
    """
    available_dates = {
        pd.Timestamp(row[time_col]).date()
        for row in members.select(time_col).distinct().collect()
        if row[time_col] is not None
    }
    missing_dates = sorted(set(snap_dates) - available_dates)
    if missing_dates:
        raise ValueError(
            "inference_population missing inference.snap_dates: "
            f"{[value.isoformat() for value in missing_dates]}"
        )


def report_feature_coverage(
    members: DataFrame,
    feature_table: DataFrame,
    join_key: list[str],
    time_col: str,
) -> None:
    """Log how many members have no feature row, per month.

    The durable record of the decision made by
    :func:`join_features_keeping_all_members`: members without features are
    kept, so nothing downstream can tell how many there were. Without this line
    a feature pipeline that silently stopped covering half the population would
    look identical to a healthy run.

    Cost: one shuffle and one ``collect()`` bounded by the month count. Measured
    over a **narrow** projection — the join key plus a presence flag — so the
    aggregation never touches the wide feature columns.
    """
    ft_keys = (
        feature_table.select(*join_key).distinct()
        .withColumn("_ft_present", F.lit(True))
    )
    presence = members.join(ft_keys, on=join_key, how="left")
    coverage = (
        presence.groupBy(time_col)
        .agg(
            F.count(F.lit(1)).alias("members"),
            F.sum(
                F.when(F.col("_ft_present").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("members_missing_features"),
        )
        .collect()
    )
    for row in coverage:
        logger.info(
            "feature coverage %s=%s: members=%d missing_features=%d",
            time_col, row[time_col], row["members"],
            row["members_missing_features"],
        )


def join_features_keeping_all_members(
    members: DataFrame, feature_table: DataFrame, join_key: list[str],
) -> DataFrame:
    """LEFT join, members on the left — never INNER.

    An INNER join would drop members the feature table has no row for, which is
    silent data loss: the entity simply never appears in the published ranking,
    every group that remains is complete, and no check anywhere sees a gap. The
    missing block arrives as NULLs instead, which LightGBM handles natively.

    Direction matters as much as the join type: with ``feature_table`` on the
    left the same LEFT join would *add* non-members.
    """
    return members.join(feature_table, on=join_key, how="left")


def with_entity_bucket(
    df: DataFrame, entity_cols: list[str], n_buckets: int,
) -> DataFrame:
    """Assign each entity to a chunk boundary by hashing the entity columns.

    **The entity columns only, never the time.** The ranking group is
    ``(time, entity)`` and the bucket has to be a subset of it, or one entity's
    items would land in different buckets and the group would be split across
    partitions that are scored independently.

    The value is a **string** because it is a partition value; the plan side
    keeps it an ``int`` so bucket 10 sorts after bucket 2
    (``chunk_plans.ScoringChunk``).

    The salt is fixed rather than configurable — see :data:`_BUCKET_SITE`.
    """
    return df.withColumn(
        ENTITY_BUCKET_COL,
        spark_bucket(
            df, entity_cols, _BUCKET_SEED, _BUCKET_SITE, n_buckets,
        ).cast("string"),
    )


def drop_excluded_columns_keeping_identity(
    df: DataFrame, drop_cols: list[str], identity_cols: list[str],
) -> DataFrame:
    """Drop the preprocessor's ``drop_columns``, except identity columns.

    An identity column named in ``drop_columns`` is a config mistake, not an
    instruction: dropping it here would remove the key the ranking groups by and
    the partition columns the write needs. Names that are not columns of this
    frame are ignored rather than raising — ``drop_columns`` is written against
    the source feature table, which is wider than what reaches this point.
    """
    cols_to_drop = [
        c for c in drop_cols if c in df.columns and c not in identity_cols
    ]
    return df.drop(*cols_to_drop)


def require_feature_columns_present(
    available: list[str], feature_columns: list[str],
) -> None:
    """Post-condition: the join produced every feature the preprocessor names.

    Failing means this node's inputs disagree — a ``feature_table`` that no
    longer carries a column ``preprocessor.json`` was fit with. Raising here is
    what keeps the landed table's promise that it holds the artifact's full
    feature set; a ``select`` of what happens to be present would land a
    narrower table that every later reader would treat as complete.
    """
    missing = sorted(set(feature_columns) - set(available))
    if missing:
        raise ValueError(
            f"Missing feature columns in inference population: {missing}"
        )


def stored_population_columns(
    keep_identity: list[str], feature_columns: list[str],
) -> list[str]:
    """The landed table's width: identity (item excluded), features, bucket.

    The **full** feature set the preprocessor names, never the subset one model
    wants — that is what lets the table be scoped by ``base_dataset_version``
    and reused across ``model_version`` (ADR-0010 §5). Narrowing it to a model's
    view is the "optimisation" that would silently bind the table to that model.

    The order is not load-bearing at the write: ``HiveTableDataset.save``
    re-projects to the table's own column order before ``insertInto``, because
    that projection is positional and follows the TABLE, not the frame. It is
    fixed here so the frame a reader (or a test) inspects has a stable shape.
    """
    return [*keep_identity, *feature_columns, ENTITY_BUCKET_COL]


def with_time_as_partition_string(df: DataFrame, time_col: str) -> DataFrame:
    """Spell the time partition value as ``YYYY-MM-DD``, here rather than later.

    The partition column is STRING. Letting Spark coerce a date on the way into
    ``insertInto`` makes the directory name depend on that coercion, and the
    resume planner compares directory names against
    ``scoping.iso_snap_dates`` — a mismatch in spelling makes every finished
    chunk look unwritten, which re-scores silently rather than failing.
    """
    return df.withColumn(time_col, F.col(time_col).cast("date").cast("string"))
