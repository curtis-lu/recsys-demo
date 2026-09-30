"""How the quadrant population is computed on Spark: the quadrant labels, a
deterministic sample of every cell, and every cell's extreme rows.

A cell is one (item × quadrant). Everything here is native Spark — no UDF, and
nothing is collected: ``select_shap_population`` (``nodes.py``) brings the two
small results to the driver itself, and says there which rows it asks for and
why.

A candidate's identity travels as one text column, ``_ck``: the
``identity_columns`` cast to string and joined with ``|``. It orders the
sample (its crc32, then itself) and breaks score ties among the extremes, so
both are deterministic — the same rows on every run over the same table.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

#: The four quadrants, in the order every quadrant artifact lists them.
QUADRANTS = ("TP", "FP", "FN", "TN")


def label_quadrants(ranked, rank_col, label_col, top_k, identity_cols):
    """``ranked`` plus ``quadrant`` — "rank within ``top_k``" crossed with
    ``label == 1``: TP, FP, FN, TN — and ``_ck``, the candidate's identity as
    text."""
    is_top = F.col(rank_col) <= F.lit(top_k)
    is_pos = F.col(label_col) == F.lit(1)
    quadrant = (
        F.when(is_top & is_pos, F.lit("TP"))
        .when(is_top & ~is_pos, F.lit("FP"))
        .when(~is_top & is_pos, F.lit("FN"))
        .otherwise(F.lit("TN"))
    )
    ck = F.concat_ws("|", *[F.col(c).cast("string") for c in identity_cols])
    return ranked.withColumn("quadrant", quadrant).withColumn("_ck", ck)


def sample_each_cell(labeled, item_col, per_cell):
    """At most ``per_cell`` rows of every cell: the ones whose ``_ck`` has the
    lowest crc32, ties by ``_ck`` itself. Which rows is decided by a hash of
    their identity, not by their score, and it is the same rows on every run."""
    w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
        F.crc32(F.col("_ck")), F.col("_ck"))
    return (
        labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
        .where(F.col("_cell_rn") <= F.lit(per_cell))
    )


def extremes_of_each_cell(labeled, item_col, rank_col, score_col, label_col,
                          identity_cols):
    """Every cell's highest- and lowest-scored row, ``role`` ``high`` and
    ``low``, with the identity, ``quadrant``, ``rank``, ``score`` and
    ``label``.

    The tie-break is asymmetric — ``_ck`` ascending for the high, descending
    for the low — so a cell whose rows tie on score still gives two different
    rows; only a one-row cell gives the same row as both. ``score`` is the name
    ``compute_quadrant_cases`` reads, whatever the prediction table calls its
    score column (ADR-0030 decision 5).
    """
    w_high = Window.partitionBy(item_col, "quadrant").orderBy(
        F.col(score_col).desc(), F.col("_ck").asc())
    w_low = Window.partitionBy(item_col, "quadrant").orderBy(
        F.col(score_col).asc(), F.col("_ck").desc())
    highs = (labeled.withColumn("_rn", F.row_number().over(w_high))
             .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("high")))
    lows = (labeled.withColumn("_rn", F.row_number().over(w_low))
            .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("low")))
    return highs.unionByName(lows).select(
        *identity_cols, "quadrant", "role",
        F.col(rank_col).alias("rank"), F.col(score_col).alias("score"),
        F.col(label_col).alias("label"))
