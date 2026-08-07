"""The mechanics a split's ``model_input`` is assembled from: the two joins, the
output column rule, the zero-positive group filter, and the column guard behind
them.

Each function here carries at most one mechanism. Which of them a split runs,
and why each is the right answer for this task, is the story
``nodes.build_model_input`` tells (ADR-0008 §2).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _validate_columns(
    columns: list[str],
    required: list[str],
    context: str,
) -> None:
    """Check that all required columns exist. Raises ValueError if missing."""
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
