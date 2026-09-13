"""Within-query rank: score descending, ties broken by item ascending.

One rule for the two Spark places that rank candidates: inference's
``rank_predictions`` (the published ``rank``) and evaluation's
``metrics_spark.rank_within_query``, which every metric pass ranks with in both
modes, and which ``prepare_eval_data`` and the comparison mode use to fill in or
redo ``rank``. Each of them used to spell it out as ``row_number()`` by score
descending, which leaves ties undefined: ``row_number`` numbers tied rows in
whatever order they reach the window, so the same rows could be ranked
differently on every run and by each pipeline, and nothing raises (ADR-0020
bug 11).

**What it does not cover.** Training's quadrant assignment
(``diagnosis/model/population_spark.py``) already spells the same order inline;
it ranks for a training diagnosis, not a published or evaluated rank. The
driver-side numpy path (``evaluation/metrics.py``, run on the diagnosis sample)
breaks ties by input order instead. Neither is bound to this function.

**Why the item.** Time and entity are the window's partition, so the item is the
one column left that tells two tied rows of a query apart — ``(time, entity,
item)`` is the row's identity. Ascending is a convention, not a judgement: any
fixed order makes the rank reproducible, none makes a tie meaningful.

**Why here.** A pure Spark expression with no project imports, the same kind of
helper as ``hashing.spark_bucket``. Placing it under ``evaluation/`` would open
the first inference → evaluation import edge for the sake of one window.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, Window
from pyspark.sql import functions as F


def rank_by_score_then_item(
    group_cols: Sequence[str], score_col: str, item_col: str,
) -> Column:
    """1-based position in each ``group_cols`` group: ``score_col`` descending,
    then ``item_col`` ascending.

    ``row_number`` rather than ``rank``, as both callers had it: positions run
    1..N with no repeats even across a tie, which is what the published rank's
    range check (``validate_predictions``) expects.
    """
    w = Window.partitionBy(*group_cols).orderBy(
        F.col(score_col).desc(), F.col(item_col).asc()
    )
    return F.row_number().over(w)
