"""Within-query rank: score descending, ties by item ascending, then by each
``event`` column ascending.

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
it ranks for a training diagnosis, not a published or evaluated rank. The
manual spike scripts ``scripts/item_ability_diagnosis.py``,
``scripts/suppression_ledger_diagnosis.py`` and
``scripts/per_item_score_shift_optuna_diagnosis.py`` still sort with their own
``np.lexsort`` and hand string items to the metric primitives; they predate
``diagnosis/metric/`` and were left out of #355.

**Why the item, and why ``event`` after it.** The query group is the window's
partition, so what is left to tell two tied rows apart is the rest of the
row's identity: the item, plus the ``event`` columns when that optional role is
declared. Those two cases are complete between them and need no third branch
(ADR-0025 decision 1): with no ``event`` declared, the duplicate checks
guarantee one row per item inside a query group, so the item already decides;
with ``event`` declared, identity uniqueness guarantees the ``event`` columns
tell the same item's rows apart. Ascending is a convention, not a judgement:
any fixed order makes the rank reproducible, none makes a tie meaningful.

**The known cost of "ascending", stated because it is measured, not
theoretical.** When ``event`` is a timestamp, the earlier impression always
outranks the tied later one. On a config that declares ``event`` without
attaching per-impression features — where every row of one item in one query
necessarily scores the same, so *everything* is a tie — that direction was
worth about 0.02 mAP on this repo's ad example (issue #378). Hashing the
identity instead would be time-independent and was rejected: Spark and numpy
would have to agree on the hash value, which means materialising it as a
dataset column. Evaluation prints the share of tied rows instead, so a
deployment can see how big its own version of this is.

**Items compare by their own type.** Spark's ``ORDER BY`` on an integer item
column puts 2 before 10; the numpy twin must too, so it orders the values it is
given and never their string form (where ``"10" < "2"``). Callers that keep a
string copy of the item for labels pass the raw column here.

**Why here.** A pure helper with no project imports, the same kind as
``hashing.spark_bucket``; placing it under ``evaluation/`` would open the first
inference → evaluation import edge for the sake of one window. pyspark is
imported inside the Spark function only: the numpy callers include modules kept
pyspark-free on purpose (``diagnosis/metric/_common.py``). That is also why the
``recsys_tfb.utils`` package re-exports nothing — importing any submodule loads
the package first.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import Column


def rank_by_score_then_item(
    group_cols: Sequence[str],
    score_col: str,
    item_col: str,
    event_cols: Sequence[str] = (),
) -> Column:
    """1-based position in each ``group_cols`` group: ``score_col`` descending,
    then ``item_col`` ascending, then each of ``event_cols`` ascending.

    ``row_number`` rather than ``rank``, as both callers had it: positions run
    1..N with no repeats even across a tie, which is what the published rank's
    range check (``validate_predictions``) expects.

    ``event_cols`` empty — every caller before #378, and every deployment that
    declares no ``event`` role — produces exactly the window this had before,
    so no existing rank moves.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*group_cols).orderBy(
        F.col(score_col).desc(),
        F.col(item_col).asc(),
        *(F.col(c).asc() for c in event_cols),
    )
    return F.row_number().over(w)


def order_by_score_then_item(
    groups: np.ndarray,
    score: np.ndarray,
    items: np.ndarray,
    event_keys: Sequence[np.ndarray] = (),
) -> np.ndarray:
    """Row indices that put each group's rows next to each other, and within a
    group ``score`` descending, then ``items`` ascending, then each array of
    ``event_keys`` ascending.

    Position ``i`` inside a group's run of the returned order is rank ``i + 1``
    of :func:`rank_by_score_then_item` for the same rows. The groups themselves
    come out in ``groups`` id order; that order means nothing here, so callers
    whose output lists groups in sequence give them ids that do not depend on
    row order (``pd.factorize(key, sort=True)``).

    ``items``, and each array in ``event_keys``, are the raw column values, or
    any array that sorts the same way (the codes from
    :func:`item_sort_codes`). ``event_keys`` is in ``schema.columns.event``'s
    declared order — that order is what decides the rank, so it is the caller's
    job to preserve it, and ``get_schema`` is what makes it stable.

    **Where the two engines are made to agree.** Both put NULL/NaN first:
    Spark's ``asc()`` does by default, and ``pd.factorize(sort=True)`` codes a
    missing value as ``-1``, below every real code. Both compare values by
    their own type rather than their string form, which is why the raw column
    is passed here and never ``astype(str)`` of it (``"10" < "2"``).
    """
    # np.lexsort takes keys in REVERSE priority order: the LAST key is primary.
    # So: groups, then -score, then items, then the event keys in declared
    # order -- which is this tuple read backwards.
    keys = (
        *(item_sort_codes(k) for k in reversed(list(event_keys))),
        item_sort_codes(items),
        -np.asarray(score),
        groups,
    )
    return np.lexsort(keys)


def item_sort_codes(items: np.ndarray) -> np.ndarray:
    """Small integers that sort exactly as ``items`` do (0 = smallest).

    Lets a caller that ranks the same items many times — HPO scores every trial
    on one validation set — pay for sorting the item values once, and hand the
    codes to :func:`order_by_score_then_item` in place of the values. The codes
    go through this function again inside every call, but on small integers
    that is one hash pass, not a sort of strings.

    Value-generic despite the name: :func:`order_by_score_then_item` runs each
    ``event`` array through it too, and a caller may pre-code those the same
    way. A missing value codes to ``-1`` and therefore sorts first, which is
    what matches Spark's default NULL placement under ``asc()``.
    """
    return pd.factorize(np.asarray(items), sort=True)[0]
