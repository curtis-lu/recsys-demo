"""The metastore boundary: which partitions exist, which buckets hold rows, and
the one-partition-per-save guard.

Everything here is about the seam where a Python-side answer meets a Hive
partition directory, and every function in it exists because that seam has a
silent failure mode:

* a partition value read back as a string that no Python value equals, so a
  finished chunk looks unwritten (:data:`_HIVE_NULL_PARTITION`);
* a bucket with no partition, which is either a legitimately small population
  or silent data loss (:func:`populated_buckets`);
* a frame spanning two partitions handed to a dynamic-overwrite ``insertInto``,
  where the second save deletes the first chunk's rows
  (:func:`require_single_partition`).

None of these announce themselves, which is why the answers are computed here
rather than inferred from row counts downstream.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from pyspark.sql import functions as F

from recsys_tfb.pipelines.inference.steps.chunk_plans import ScoringChunk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

#: Partition column carrying the entity chunk boundary. A mechanism column, not
#: part of any published contract: it exists so one ``save()`` touches exactly
#: one partition (ADR-0010 section 3, constraint C), and ``rank_predictions``
#: drops it on the way to ``ranked_staging``.
ENTITY_BUCKET_COL = "entity_bucket"

#: Hive's literal for a NULL partition value. Same guard as the dataset and
#: training month planners: the parquet side reconstructs it as ``None`` ->
#: ``"None"``, so the two spellings would never match and the chunk would look
#: permanently unwritten. Dropping it means "not written yet", which re-scores
#: rather than skips.
_HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def written_score_partitions(
    predictions_dataset, time_col: str, item_col: str
) -> set[ScoringChunk]:
    """Chunks already scored for this ``model_version``, from the metastore.

    The dataset object reaches the scoring node through ``Node(writes=...)``,
    and it is the only route to the metastore there — that node gets no
    SparkSession of its own. Its ``partition_filter`` scopes the answer to this
    ``model_version``, which is exactly the scope the resume question is asked
    in; without that scoping a previous model's partitions would be counted as
    this run's work already done, and ``completeness`` would pass on the old
    scores (ADR-0010 section 5).

    A dataset type that cannot list partitions makes every chunk look unwritten:
    that re-scores (wasteful) rather than skips (silently stale), which is the
    direction this has to fail in.

    Cost: one metastore listing, no scan — but a real round trip whose duration
    grows with the partition count, which is why its caller times it.
    """
    lister = getattr(predictions_dataset, "existing_partition_values", None)
    if lister is None:
        logger.warning(
            "[chunks] predict: %s cannot list partitions, so no chunk can be "
            "shown complete; every configured chunk will be scored.",
            type(predictions_dataset).__name__,
        )
        return set()

    written: set[ScoringChunk] = set()
    for spec in lister():
        snap_date = spec.get(time_col)
        item = spec.get(item_col)
        bucket = spec.get(ENTITY_BUCKET_COL)
        if snap_date is None or item is None or bucket is None:
            continue
        if _HIVE_NULL_PARTITION in (snap_date, item, bucket):
            logger.warning(
                "[chunks] predict: ignoring score partition with a NULL value "
                "(%s=%r, %s=%r, %s=%r); that chunk will be treated as not yet "
                "written.",
                time_col, snap_date, item_col, item, ENTITY_BUCKET_COL, bucket,
            )
            continue
        try:
            bucket_index = int(bucket)
        except (TypeError, ValueError):
            logger.warning(
                "[chunks] predict: ignoring score partition with a "
                "non-numeric %s=%r; that chunk will be treated as not yet "
                "written.", ENTITY_BUCKET_COL, bucket,
            )
            continue
        written.add(ScoringChunk(str(snap_date), bucket_index, str(item)))
    return written


def populated_buckets(
    features: DataFrame, time_col: str, snap_dates: list[str]
) -> dict[str, set[int]]:
    """Which entity buckets the landed table actually holds rows for, per month.

    Exists to separate two things a zero-row chunk cannot tell apart:

    * a bucket that is **legitimately** empty — a population smaller than the
      bucket count leaves gaps, and no partition is written for it, so it must
      not be counted as a missing partition; and
    * a bucket that **should** have had entities. That one is the silent-data-
      loss failure mode, and it has to raise.

    A partition exists for a bucket exactly when the builder wrote at least one
    row for it, so the table's partition values *are* the answer. One action per
    month over a partition column, so the cost scales with the number of
    partitions rather than the number of rows — and a fully resumed run passes
    an empty month list and pays nothing.

    What this does **not** catch: an ``inference_population_features`` that is
    itself stale (built against an older population, reachable via
    ``--from-node predict_and_write_scores``). Then the missing entities have no
    partition either and both sides agree. That residual is the documented one —
    resume's judgement is "the partition exists", never "the partition is
    fresh"; ``--rebuild-dates`` is the override.
    """
    populated: dict[str, set[int]] = {}
    for snap_date in snap_dates:
        rows = (
            features
            .filter(F.col(time_col) == snap_date)
            .select(ENTITY_BUCKET_COL)
            .distinct()
            .collect()
        )
        populated[snap_date] = {int(row[ENTITY_BUCKET_COL]) for row in rows}
    return populated


def require_single_partition(pdf: pd.DataFrame, partition_cols: list[str]) -> None:
    """One ``save()``, one partition — constraint C in its testable form.

    Post-condition on the frame the scoring loop just built.

    ``HiveTableDataset.save()`` is ``insertInto`` under
    ``partitionOverwriteMode=dynamic``, whose semantics are "touch only the
    partitions present in this frame, but *replace* those wholesale". Hand it a
    frame spanning two chunks' partitions and the second save deletes the first
    chunk's rows — 90% of the data gone with no error message
    (ADR-0010 section 3, constraint C).

    Free to check here and nowhere else: the frame is a pandas frame that was
    just built in the driver, so counting its distinct partition values touches
    memory rather than launching a Spark job. The same assertion inside
    ``save()`` would have to act on a lazy plan, which is the second full
    lineage execution ADR-0009 removed.

    Deliberately **not** a before/after diff of
    ``existing_partition_values()``: re-publishing an existing partition makes
    that diff empty by construction, so it is blind to exactly the successive
    overwrite this guards (``io/hive_table_dataset.py`` records the same trap).
    """
    combos = pdf[partition_cols].drop_duplicates()
    if len(combos) != 1:
        raise ValueError(
            f"a single save must cover exactly one partition, got "
            f"{len(combos)} distinct {partition_cols} combination(s): "
            f"{combos.to_dict('records')}. Dynamic-partition overwrite "
            "replaces whole partitions, so successive saves would delete each "
            "other's rows without an error."
        )
