"""Which scoring chunks this run has to compute — a Spark-free decision.

The scoring loop's unit of work is one ``(time, entity_bucket, item)`` chunk,
which is also exactly one partition of ``unranked_predictions``
(ADR-0010 section 5). So "has this chunk already been computed" is answerable
from the metastore's partition list, and deciding it is arithmetic on three
sets: the configured grid, the partitions already present, and the months the
operator asked to redo.

**No pyspark, not even a deferred import.** The judgement here is where a
resume goes silently wrong — skipping a chunk that was never scored looks
identical to correctly skipping a finished one — so it needs tests that run in
milliseconds rather than behind a 2-4 minute SparkSession. Same reason
``pipelines/dataset/month_plans.py`` is kept pure; that module is the shape
this one follows.

Translating the metastore's answer into :class:`ScoringChunk` tuples is the
caller's job (``steps/partitions.py``), because that is where the schema's
column names live and where a malformed partition value has to be logged.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import NamedTuple

logger = logging.getLogger(__name__)

#: Bucket counts outside this range are honoured but warned about. The lower
#: bound comes from the driver: one chunk should not eat half the driver, so a
#: 128 GB driver scoring a 10M-entity population wants at least ~4 buckets. The
#: upper bound comes from storage: ``unranked_predictions`` holds E/B rows per
#: partition, and by B = 50 that is already down to a few MB per file — far
#: below one HDFS block. ADR-0010 section 4 derives both.
HEALTHY_BUCKET_RANGE = (5, 20)


class ScoringChunk(NamedTuple):
    """One unit of scoring work, and one partition of ``unranked_predictions``.

    ``entity_bucket`` is an ``int`` here while the partition value is a string:
    the boundary conversion belongs at the metastore edge, and keeping it
    numeric in the plan is what makes bucket 10 sort after bucket 2.
    """

    snap_date: str
    entity_bucket: int
    item: str


@dataclass(frozen=True)
class ScoringChunkPlan:
    """What this run will do, will skip, and will redo — plus what is stray.

    ``to_process`` is ordered ``(snap_date, entity_bucket, item)``: the outer
    loop is the bucket so one bucket's features reach the driver once and the
    inner item loop reuses them (ADR-0010 section 4, decision 3). Iterating this
    tuple in order *is* that loop order.

    ``rebuilt`` is a subset of ``to_process``, not a fourth disjoint bucket: a
    forced chunk is still processed. It is reported separately because "redone
    on purpose" and "was never there" are different facts about a run.
    """

    to_process: tuple[ScoringChunk, ...]
    skipped: tuple[ScoringChunk, ...]
    rebuilt: tuple[ScoringChunk, ...]
    surplus: tuple[ScoringChunk, ...]


def plan_scoring_chunks(
    snap_dates: Sequence[str],
    items: Sequence[str],
    n_buckets: int,
    written: Iterable[ScoringChunk] | Iterable[tuple[str, int, str]],
    rebuild: Iterable[str] = (),
) -> ScoringChunkPlan:
    """Decide the scoring work for one run.

    Args:
        snap_dates: the months this run scores (``inference.snap_dates``,
            already normalised to ``YYYY-MM-DD``). The config is the authority
            on which months exist; there is deliberately no "fall back to
            whatever the table holds", which would resurrect a month dropped
            from the config.
        items: the candidate set (``inference.products``).
        n_buckets: how many entity buckets the population is split into.
        written: partitions already present for this ``model_version``, as
            ``(snap_date, entity_bucket, item)`` triples.
        rebuild: months to recompute even though their partitions exist.

    Raises:
        ValueError: on an empty grid (no months, no items, or ``n_buckets < 1``).
            All three would make this function return an empty plan, and the
            run would then report success having scored nothing.

            **Pre-checks, and runtime backstops for A27**: the three faults are
            in this function's arguments, not in what it computes, and the
            config that produces them is rejected at CLI entry by
            ``core.consistency.inference_grid_errors`` — which reports all
            three at once, where these abort on the first. Reaching one of
            these means the function was called outside the CLI (a test, a
            notebook), not that an operator mis-set the key. Kept rather than
            deleted because this is a pure function with callers that do not
            go through the gate, and an empty plan is the one output shape
            nothing downstream can tell from a finished run.
    """
    if n_buckets < 1:
        raise ValueError(
            f"inference.entity_buckets must be at least 1, got {n_buckets}. "
            "Zero buckets means zero chunks, which would look like a "
            "successful run that scored nobody."
        )
    if not snap_dates:
        raise ValueError(
            "inference.snap_dates is empty; there is nothing to score"
        )
    if not items:
        raise ValueError(
            "inference.products is empty; there is nothing to rank"
        )
    low, high = HEALTHY_BUCKET_RANGE
    if not low <= n_buckets <= high:
        logger.warning(
            "[chunks] inference.entity_buckets=%d is outside the healthy "
            "%d-%d window: below it a single chunk may not fit the driver, "
            "above it the per-partition files fall far below one HDFS block "
            "(ADR-0010 section 4).",
            n_buckets, low, high,
        )

    months = list(dict.fromkeys(snap_dates))
    candidates = list(dict.fromkeys(items))
    forced = set(rebuild)
    already = {ScoringChunk(*chunk) for chunk in written}

    to_process: list[ScoringChunk] = []
    skipped: list[ScoringChunk] = []
    rebuilt: list[ScoringChunk] = []
    for snap_date in months:
        for bucket in range(n_buckets):
            for item in candidates:
                chunk = ScoringChunk(snap_date, bucket, item)
                if snap_date in forced:
                    rebuilt.append(chunk)
                    to_process.append(chunk)
                elif chunk in already:
                    skipped.append(chunk)
                else:
                    to_process.append(chunk)

    # Set difference against the expected grid, not a "did we cover everything"
    # count: lowering entity_buckets or renaming an item leaves partitions
    # behind that no future run will touch, and re-scoring cannot delete them.
    # They keep contributing rows to this model_version's ranking, so they have
    # to be named. Scoped to this run's months — other months' partitions are
    # legitimately there.
    expected = {
        ScoringChunk(snap_date, bucket, item)
        for snap_date in months
        for bucket in range(n_buckets)
        for item in candidates
    }
    surplus = sorted(
        chunk for chunk in already
        if chunk.snap_date in set(months) and chunk not in expected
    )
    if surplus:
        logger.warning(
            "[chunks] %d partition(s) exist that this run's grid does not "
            "cover (%s items x %d buckets): %s. Re-scoring cannot remove "
            "them; they will keep contributing rows to this model_version "
            "until they are dropped by hand.",
            len(surplus), len(candidates), n_buckets, surplus,
        )

    return ScoringChunkPlan(
        to_process=tuple(to_process),
        skipped=tuple(skipped),
        rebuilt=tuple(rebuilt),
        surplus=tuple(surplus),
    )


def as_rows(chunks) -> list[list]:
    """Chunks as sorted plain lists, so the manifest stays JSON-shaped.

    :class:`ScoringChunk` is a ``NamedTuple``, which survives a round trip
    through this pipeline's in-memory catalog but not through anything that
    serialises the manifest. Lists are what ``validate_predictions`` compares,
    so the conversion happens once, here, rather than at each reader.

    Sorted for the same reason the plan is: two runs that did the same work must
    produce the same manifest, and a set's iteration order is not that.
    """
    return sorted(list(chunk) for chunk in chunks)


#: The five lists a run produces, keyed as :func:`build_chunk_report` carries
#: them. Ordered as the decision is made: score it, leave it alone, redo it on
#: request, find nobody in it — plus the partitions the grid no longer covers.
#:
#: **These are not five disjoint buckets**, which is why :func:`_tally` does
#: not simply expose them: ``rebuilt`` is a subset of what the run set out to
#: process (see :class:`ScoringChunkPlan`), so it overlaps ``processed`` and
#: ``empty``, and ``surplus`` sits outside the configured grid entirely.
CHUNK_KINDS = ("processed", "skipped", "rebuilt", "empty", "surplus")


def _tally(rows: dict, month=None) -> dict:
    """Count the lists in a shape whose keys say how they add up.

    ``processed``, ``skipped`` and ``empty`` partition the configured grid —
    every chunk lands in exactly one — so ``grid`` is their sum and a reader
    can check it. ``rebuilt`` is spelled ``of_which_rebuilt`` because it is a
    *subset* of the first and third: reading these as five buckets and adding
    them up overshoots the grid by however many chunks ``--rebuild-dates``
    forced (a real run measured processed=80 with of_which_rebuilt=80). The key
    name is the only place that warning can live — JSON carries no comments,
    and this file exists to be opened months later by someone who has not read
    this module.

    ``month`` restricts the count to one ``snap_date``; ``None`` counts all.
    """
    def n(kind):
        return sum(
            1 for row in rows[kind] if month is None or row[0] == month
        )

    return {
        "grid": n("processed") + n("skipped") + n("empty"),
        "processed": n("processed"),
        "skipped": n("skipped"),
        "empty": n("empty"),
        "of_which_rebuilt": n("rebuilt"),
        "surplus": n("surplus"),
    }


def build_chunk_report(manifest: dict, surplus: Iterable, run_id: str) -> dict:
    """The scoring manifest as something that outlives the process.

    ``score_manifest`` already carries every list this returns, but it is a
    memory-only catalog entry on purpose: landing it would let a
    ``--from-node rank_predictions`` slice load a *previous* run's manifest
    instead of re-running the scoring node, and ``validate_predictions`` reads
    values out of it (``docs/pipelines/inference.md`` section 7.4). So the
    record is a separate artifact rather than a persisted twin — same lists,
    different job: this one is read by people after the fact, never by a node.

    Three things are added on the way out. ``surplus`` is one: the plan
    computes it and only ever ``logger.warning``s it, yet it names partitions
    that keep contributing rows to the published ranking until someone drops
    them by hand. ``by_snap_date`` is the second, and it covers **every
    configured month** including the ones no list mentions — without that row,
    "this month was entirely skipped" and "this month was not in the run" read
    identically.

    ``run_id`` is the third, and it is what makes the file safe to read back.
    It lands in ``data/inference/<model_version>/<snap_date>/``, a directory
    every run of that model and month reuses, and a slice that never reaches
    the scoring node leaves the previous run's copy sitting there untouched.
    The stamp is how ``__main__._chunk_report_extra`` refuses to quote it as
    this run's.
    """
    rows = {
        "processed": manifest["chunks_processed"],
        "skipped": manifest["chunks_skipped"],
        "rebuilt": manifest["chunks_rebuilt"],
        "empty": manifest["chunks_empty"],
        "surplus": as_rows(surplus),
    }
    return {
        **manifest,
        "run_id": run_id,
        "chunks_surplus": rows["surplus"],
        "counts": _tally(rows),
        # A chunk row is ``[snap_date, entity_bucket, item]`` (:func:`as_rows`),
        # so element 0 is the month.
        "by_snap_date": {
            month: _tally(rows, month)
            for month in sorted(set(manifest["snap_dates"]))
        },
    }
