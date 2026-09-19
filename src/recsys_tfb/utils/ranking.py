"""Within-query rank: score descending, ties broken by item ascending.

One rule, written twice — once per engine — and nowhere else:

* :func:`rank_by_score_then_item` is the Spark window. Inference's
  ``rank_predictions`` (the published ``rank``) and evaluation's
  ``metrics_spark.rank_within_query`` rank with it; the latter serves every
  metric pass in both modes, and ``prepare_eval_data`` and the comparison mode
  use it to fill in or redo ``rank``.
* :func:`order_by_score_then_item` is the numpy twin for the driver: the metric
  primitives in ``evaluation/metrics.py`` (which HPO scores trials with), the
  metric diagnoses and their bootstrap.

Every one of those places used to spell the sort out itself — Spark as
``row_number()`` by score descending, numpy as ``np.lexsort((-score, groups))``
— which leaves ties undefined: tied rows were numbered in whatever order they
reached the window or the array, so the same rows could be ranked differently
on every run and by each place, and nothing raises (ADR-0020 bug 11, #355).

**What it does not cover.** Training's quadrant assignment
(``diagnosis/model/population_spark.py``) already spells the same order inline;
it ranks for a training diagnosis, not a published or evaluated rank.

**Why the item.** Time and entity are the window's partition, so the item is the
one column left that tells two tied rows of a query apart — ``(time, entity,
item)`` is the row's identity. Ascending is a convention, not a judgement: any
fixed order makes the rank reproducible, none makes a tie meaningful.

**Items compare by their own type.** Spark's ``ORDER BY`` on an integer item
column puts 2 before 10; the numpy twin must too, so it orders the values it is
given and never their string form (where ``"10" < "2"``). Callers that keep a
string copy of the item for labels pass the raw column here.

**Why here.** A pure helper with no project imports, the same kind as
``hashing.spark_bucket``; placing it under ``evaluation/`` would open the first
inference → evaluation import edge for the sake of one window. pyspark is
imported inside the Spark function only: the numpy callers include modules kept
pyspark-free on purpose (``diagnosis/metric/_common.py``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import Column


def rank_by_score_then_item(
    group_cols: Sequence[str], score_col: str, item_col: str,
) -> Column:
    """1-based position in each ``group_cols`` group: ``score_col`` descending,
    then ``item_col`` ascending.

    ``row_number`` rather than ``rank``, as both callers had it: positions run
    1..N with no repeats even across a tie, which is what the published rank's
    range check (``validate_predictions``) expects.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*group_cols).orderBy(
        F.col(score_col).desc(), F.col(item_col).asc()
    )
    return F.row_number().over(w)


def order_by_score_then_item(
    groups: np.ndarray, score: np.ndarray, items: np.ndarray,
) -> np.ndarray:
    """Row indices that put each group's rows next to each other, and within a
    group ``score`` descending, then ``items`` ascending.

    Position ``i`` inside a group's run of the returned order is rank ``i + 1``
    of :func:`rank_by_score_then_item` for the same rows. The groups themselves
    come out in ``groups`` id order; that order means nothing here, so callers
    whose output lists groups in sequence give them ids that do not depend on
    row order (``pd.factorize(key, sort=True)``).

    ``items`` are the raw item values, or any array that sorts the same way
    (the codes from :func:`item_sort_codes`).
    """
    # np.lexsort takes keys in REVERSE priority order: the LAST key is primary.
    return np.lexsort((item_sort_codes(items), -np.asarray(score), groups))


def item_sort_codes(items: np.ndarray) -> np.ndarray:
    """Small integers that sort exactly as ``items`` do (0 = smallest).

    Lets a caller that ranks the same items many times — HPO scores every trial
    on one validation set — pay for sorting the item values once, and hand the
    codes to :func:`order_by_score_then_item` in place of the values.
    """
    return pd.factorize(np.asarray(items), sort=True)[0]
