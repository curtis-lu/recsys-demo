"""What one inference run is scoped to: which months, how many entity buckets,
and cutting a persisted table down to those months.

The three config readers sit together because they answer the same question at
different grains — the scoring grid is ``snap_dates x entity_buckets x
products`` — and because two of them have a shape decision that must be made in
exactly one place. ``restrict_to_snap_dates`` is the only one that touches
Spark; it lives here rather than in a module of its own because applying the
month scope to a frame is the same decision as deriving it, one step later.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

#: Entity buckets when ``inference.entity_buckets`` is absent. Inside
#: ``chunk_plans.HEALTHY_BUCKET_RANGE``, which is where the bounds are derived
#: from driver memory on one side and partition file size on the other — so the
#: unconfigured case does not warn.
DEFAULT_ENTITY_BUCKETS = 10


def iso_snap_dates(parameters: dict) -> list[str]:
    """``inference.snap_dates`` as ``YYYY-MM-DD`` strings.

    The single place that decides what the config's date values mean — see
    :func:`snap_dates_as_dates` for the other shape, which derives from this
    one. The string form is what gets compared against Hive partition directory
    names, so a ``datetime`` reaching that comparison would never match a
    directory and every chunk would look unwritten.
    """
    return [
        pd.Timestamp(value).date().isoformat()
        for value in (parameters.get("inference", {}) or {}).get("snap_dates", [])
    ]


def snap_dates_as_dates(parameters: dict) -> list:
    """The same months as ``datetime.date``, for comparing against a date column.

    Two consumers want two shapes and both are load-bearing: a Hive partition
    directory name is a string, while ``isin`` against a cast-to-date column
    wants date objects. Derived from :func:`iso_snap_dates` rather than parsed
    again, so there is still exactly one place that decides what the config's
    date values mean.
    """
    return [pd.Timestamp(value).date() for value in iso_snap_dates(parameters)]


def entity_buckets(parameters: dict) -> int:
    """How many entity buckets one month's population is cut into.

    Read in two places that must agree — the builder hashes entities into this
    many buckets, the scoring loop plans one chunk per bucket — so it is one
    function rather than two ``.get`` calls that could drift to different
    defaults. Changing it between runs of the same ``model_version`` reshuffles
    every entity and orphans the partitions already written; ``chunk_plans``
    reports those as surplus rather than deleting them.
    """
    return int(
        (parameters.get("inference", {}) or {}).get(
            "entity_buckets", DEFAULT_ENTITY_BUCKETS
        )
    )


def restrict_to_snap_dates(df: DataFrame, parameters: dict) -> DataFrame:
    """Cut a persisted inference table down to the months this run scores.

    ``unranked_predictions`` and ``ranked_staging`` accumulate across months
    while ``inference.snap_dates`` names one run's worth, so a node that reads
    either one back has to say which months it means. Without this cut the
    second month reads back every historical month, re-ranks it, and republishes
    it — silently, because a re-publish of an unchanged month looks exactly like
    a correct one (ADR-0010 section 5).

    The model-version half of the old ``_filter_current_inference_scope`` is
    **gone, not moved**: ``partition_filter: model_version`` makes the catalog's
    load emit ``WHERE model_version = '…'`` and drop the column, so a comparison
    here would be dead code against a column the frame no longer has.

    Both failure modes raise rather than pass the frame through. An empty scope
    that quietly means "keep everything" is the exact behaviour this exists to
    prevent, and it would show up downstream as a republished month rather than
    as an error.

    Both are **pre-checks** — they fault the inputs, not this function's own
    result — of two different kinds. The empty-``snap_dates`` one is a
    **runtime backstop for A27**: the config it faults is rejected at CLI entry
    by ``core.consistency.inference_grid_errors``, so reaching it means this
    function was called outside the CLI (a test, a notebook) rather than that
    an operator mis-set the key. The missing-column one has no Layer-1 twin —
    it inspects the frame, so only the caller's upstream can be at fault.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]

    snap_dates = snap_dates_as_dates(parameters)
    if not snap_dates:
        raise ValueError(
            "inference.snap_dates is empty; refusing to rank or validate an "
            "unrestricted table (every historical month would be republished)"
        )
    if time_col not in df.columns:
        raise ValueError(
            f"cannot restrict to inference.snap_dates: no {time_col!r} column "
            f"in {sorted(df.columns)}"
        )

    return df.filter(F.col(time_col).cast("date").isin(snap_dates))
