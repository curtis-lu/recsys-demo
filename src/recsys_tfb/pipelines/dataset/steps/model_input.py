"""The mechanics a split's ``model_input`` is assembled from: the two joins, the
output column rule, the zero-positive group draw, and the column guard behind
them.

Each function here carries at most one mechanism. Which of them a split runs,
and why each is the right answer for this task, is the story
``nodes.build_model_input`` and the ``filter_*`` nodes tell (ADR-0008 §2).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, NamedTuple

from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

logger = logging.getLogger(__name__)

#: The hash site of the zero-positive group draw. One site for every split, on
#: purpose: a query group lives in exactly one split (the months are disjoint,
#: and train / train_dev are cut by entity), so no two splits ever hash the
#: same group, and one site is what makes the keys path and the model_input
#: path provably the same draw.
ZERO_POSITIVE_GROUP_SITE = "zero_positive_groups"

#: Working column of the draw: whether the row's group holds a positive. Not
#: ``drop_groups_without_positives``' ``__grp_pos`` — that one holds a sum.
_HAS_POSITIVE = "__grp_has_pos"


class ZeroPositiveGroupCounts(NamedTuple):
    """What one zero-positive group draw kept, by field rather than position."""

    with_positive: int
    zero_total: int
    zero_kept: int


def require_columns_present(
    columns: list[str],
    required: list[str],
    context: str,
) -> None:
    """Post-condition: ``columns`` carries every name in ``required``.

    Generic on purpose, and ``context`` is the part that carries the meaning:
    ``build_model_input`` calls this twice about different things, and the two
    calls pass different context strings because sharing one would let a test's
    ``pytest.raises(match=...)`` be satisfied by the other rule's message
    (ADR-0005 §1).

    Same shape as ``feature_columns.require_base_key_columns``, different job:
    that one is a *pre*-check with a fixed subject (``feature_table`` must carry
    time + entity) and says so in its message.
    """
    missing = set(required) - set(columns)
    if missing:
        raise ValueError(f"Missing columns in {context}: {sorted(missing)}")


def join_labels_missing_as_negative(
    keys: DataFrame,
    label_table: DataFrame,
    join_key: list[str],
    label_col: str,
) -> DataFrame:
    """LEFT join ``label_table`` onto ``keys``; a miss becomes label 0.

    LEFT + COALESCE, never INNER: an INNER join would silently drop the misses,
    which is most of the frame.
    """
    joined = keys.join(label_table, on=join_key, how="left")
    return joined.withColumn(label_col, F.coalesce(F.col(label_col), F.lit(0)))


def join_features_missing_as_null(
    dataset: DataFrame,
    preprocessed_feature_table: DataFrame,
    base_key: list[str],
) -> DataFrame:
    """LEFT join the encoded features on ``base_key``; a miss leaves them NULL.

    LEFT, never INNER: the row survives with an all-NULL feature block rather
    than disappearing, which is what keeps the output's grain equal to ``keys``'.
    """
    return dataset.join(preprocessed_feature_table, on=base_key, how="left")


def model_input_columns(
    key_columns: list[str],
    available_columns: list[str],
    identity_cols: list[str],
    label_col: str,
    feature_columns: list[str],
) -> list[str]:
    """The output column list: identity, label, features, then carried columns.

    A carried column is one the *keys* brought in that is not already identity,
    a feature, or the label — the join width is otherwise wider than this
    (label_table and feature_table both carry columns nothing downstream reads).
    """
    carry_present = [
        c for c in key_columns
        if c not in identity_cols and c not in feature_columns
        and c != label_col and c in available_columns
    ]
    return list(dict.fromkeys(
        identity_cols + [label_col] + feature_columns + carry_present
    ))


def drop_groups_without_positives(
    df: DataFrame,
    group_cols: list[str],
    label_col: str,
) -> DataFrame:
    """Drop rows whose ``group_cols`` partition has ``sum(label_col) == 0``.

    A window sum rather than a semi-join against an aggregate: one pass, and the
    partitioning is the same one the caller means by "query group".
    """
    w = Window.partitionBy(*group_cols)
    return (
        df.withColumn("__grp_pos", F.sum(F.col(label_col)).over(w))
          .filter(F.col("__grp_pos") > 0)
          .drop("__grp_pos")
    )


def _kept_under_ratio(
    has_positive: Column,
    df: DataFrame,
    group_cols: list[str],
    ratio: float,
    seed: int,
) -> Column:
    """The keep rule as one Column: a group holding a positive, or a group whose
    own draw lands under ``ratio``.

    Private and shared by the draw and its count, so the two can never keep
    different groups. The bucket hashes ``group_cols`` alone, so every row of
    one group gets the same bucket — that, not a join, is what keeps or drops
    a group whole.
    """
    bucket = spark_bucket(df, group_cols, seed, site=ZERO_POSITIVE_GROUP_SITE)
    return has_positive | (bucket < F.lit(ratio_to_threshold(ratio)))


def keep_zero_positive_groups_drawn_under_ratio(
    df: DataFrame,
    group_cols: list[str],
    label_col: str,
    ratio: float,
    seed: int,
    *,
    weight_col: str | None = None,
) -> DataFrame:
    """Every row of a group holding a positive, and every row of each group
    holding none whose own draw lands under ``ratio``.

    With ``weight_col`` the rows carry the inverse of the probability that
    their group survived: 1 where the group holds a positive, ``1 / ratio``
    where it does not. Whether a split carries that column is the caller's
    decision.

    The two ends short-cut to the rows the general path gives anyway, and cost
    less: ``ratio`` 0 is :func:`drop_groups_without_positives` (a threshold of
    0 keeps no zero-positive group), ``ratio`` 1 keeps every row with no
    window at all.

    Refuses a frame that already holds ``weight_col`` rather than overwriting
    it: a feature column by that name would otherwise be replaced by weights
    with nothing raised. This is B12's runtime backstop (``core/consistency.py``
    checks the same thing on metadata at the start of the dataset pipeline).
    """
    if weight_col is not None and weight_col in df.columns:
        raise ValueError(
            f"{weight_col!r} is already a column of this frame, so the "
            f"zero-positive group weight would overwrite it. The name is the "
            f"framework's own; rename the source column."
        )
    if ratio >= 1.0:
        return df if weight_col is None else df.withColumn(weight_col, F.lit(1.0))
    if ratio <= 0.0:
        kept = drop_groups_without_positives(df, group_cols, label_col)
        return kept if weight_col is None else kept.withColumn(weight_col, F.lit(1.0))

    w = Window.partitionBy(*group_cols)
    flagged = df.withColumn(_HAS_POSITIVE, F.sum(F.col(label_col)).over(w) > 0)
    kept = flagged.filter(
        _kept_under_ratio(F.col(_HAS_POSITIVE), flagged, group_cols, ratio, seed)
    )
    if weight_col is not None:
        kept = kept.withColumn(
            weight_col,
            F.when(F.col(_HAS_POSITIVE), F.lit(1.0)).otherwise(F.lit(1.0 / ratio)),
        )
    return kept.drop(_HAS_POSITIVE)


def count_zero_positive_groups_kept(
    df: DataFrame,
    group_cols: list[str],
    label_col: str,
    ratio: float,
    seed: int,
) -> ZeroPositiveGroupCounts:
    """Groups holding a positive, zero-positive groups, and zero-positive groups
    kept, under the same draw :func:`keep_zero_positive_groups_drawn_under_ratio`
    makes.

    **One Spark action** over ``df``'s group and label columns — on a lazy
    frame that re-runs its plan, so the caller decides when it is worth it.
    """
    groups = df.groupBy(*group_cols).agg(
        (F.sum(F.col(label_col)) > 0).alias(_HAS_POSITIVE)
    )
    kept = _kept_under_ratio(F.col(_HAS_POSITIVE), groups, group_cols, ratio, seed)
    tally = {
        (bool(row[0]), bool(row[1])): int(row[2])
        for row in groups.groupBy(F.col(_HAS_POSITIVE), kept.alias("__kept"))
        .count().collect()
    }
    zero_kept = tally.get((False, True), 0)
    return ZeroPositiveGroupCounts(
        with_positive=tally.get((True, True), 0),
        zero_total=zero_kept + tally.get((False, False), 0),
        zero_kept=zero_kept,
    )


def log_zero_positive_group_draw(
    split: str,
    ratio: float,
    counts: ZeroPositiveGroupCounts | None,
) -> None:
    """Report what the zero-positive group draw kept.

    Written once and called by every ``filter_*`` node: the nodes repeat their
    decisions on purpose (ADR-0008 §2), a log line is mechanism. ``counts`` is
    ``None`` at the two ends, where the answer needs no counting.
    """
    if ratio >= 1.0:
        logger.info(
            "%s zero-positive query groups: r=%g, every group kept (not counted)",
            split, ratio,
        )
        return
    if counts is None:
        logger.info(
            "%s zero-positive query groups: r=%g, none kept", split, ratio,
        )
        return
    logger.info(
        "%s zero-positive query groups: r=%g, kept %d of %d (weight 1/r=%.4g "
        "on their rows is a design weight — few kept groups means an unstable "
        "estimate); %d group(s) holding a positive, all kept",
        split, ratio, counts.zero_kept, counts.zero_total, 1.0 / ratio,
        counts.with_positive,
    )
