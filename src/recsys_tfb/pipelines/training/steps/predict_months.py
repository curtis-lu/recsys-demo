"""Which test months this predict run still has to write — a Spark-free decision.

A ``(model_version, snap_date)`` prediction set is an immutable product:
``model_version`` hashes everything that defines the model, so the same model
over the same month's model_input predicts bit-identically. That is what makes
skipping a finished month free, and it is also what makes the judgement
dangerous — a month skipped because it was never written looks exactly like a
month skipped because it was finished, and both look like a clean run.

**No pyspark, not even a deferred import, and nothing from this project
either.** predict receives no SparkSession at all, so a month-plan bug would
otherwise only be reachable through tests that pay a 2-4 minute SparkSession
cold start; kept pure, the same judgements are tested in milliseconds.
``tests/test_pipelines/test_training/test_predict_months.py`` pins both
properties with an AST scan of this file. Same arrangement as
``pipelines/inference/steps/chunk_plans.py``, which is the shape this module
follows, and as ``pipelines/dataset/month_plans.py``.

**The decisions are not here.** Each one is written out at its call site in
``predict_and_write_test_predictions``, one ``# Decision —`` per call
(``docs/agents/pipeline-node-design.md`` rules 4 and 9). Read the node to find
out what this run decided; read here to find out how the sets are compared.

``month_dir`` lives in this module although ``steps/local_cache.py`` needs
it just as much, and the direction is forced rather than chosen: this module may
not import anything from the project, so the cache module has to be the importer
(ADR-0014, issue #231). One implementation and never a copy, because the two
questions it answers — "what is this month's cache directory called" and "is
this the month the config named" — have to give the same answer, and a drift
between them caches a month under one name while looking for it under another,
with no error on either side.
"""

from __future__ import annotations

import logging
from collections.abc import Container, Iterable, Mapping
from typing import NamedTuple

logger = logging.getLogger(__name__)

#: Hive's stand-in for a NULL partition value.
_HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def month_dir(snap_date) -> str:
    """Directory-name form of a test month (``2026-01-31`` → ``20260131``).

    Doubling as the comparison key is the point: a month is the same month
    however it was spelled — config value, cache directory name, Hive partition
    value — which is the equality every decision below is asked in.

    It normalises spelling, not calendar day, and the difference matters.
    ``"2026-1-31"`` and ``"2026-01-31"`` stay *different* keys here, so the
    second one finds no rows in the cache and gets named by
    :func:`require_months_are_cached` instead of quietly merging with the first.
    ``core/consistency.py`` keeps a deliberate copy of this rule (A26) for the
    collision it has to catch before the pipeline starts; that copy is pinned to
    this one by a test.

    Named without the ``test_`` prefix its predecessor carried (``nodes``'
    ``_test_month_dir``) and it has to stay that way: pytest collects any
    module-level name beginning with ``test_``, so a test file importing it
    would fail at collection with "fixture 'snap_date' not found" rather than
    anything that names the real cause.
    """
    return str(snap_date).strip().replace("-", "")


def configured_months(configured: Iterable) -> dict[str, str]:
    """Configured months as ``month key → the literal that was configured``.

    Keyed rather than listed so that repeats of one month collapse to one entry,
    and the literal is carried alongside so every log line and error message can
    name the month the way the operator wrote it rather than the way this module
    compares it.

    First spelling wins on a collision. Two *different* spellings of one month
    are rejected at CLI entry (A26), so by the time this runs a key can only be
    carrying repeats of a single literal and the choice cannot matter.
    """
    by_key: dict[str, str] = {}
    for raw in configured:
        by_key.setdefault(month_dir(raw), str(raw).strip())
    return by_key


def require_months_are_cached(
    months: Mapping[str, str], cache_items: Mapping[str, set[str]]
) -> None:
    """Pre-check — every configured month has rows in the driver-local cache.

    Stays in the pipeline rather than moving to ``core/consistency.py`` because
    what it compares against only exists once the cache has been read
    (``docs/agents/pipeline-node-design.md`` rule 11).

    Raising is the whole point, and the alternative is worse than it looks: a
    month with no cached rows also has no written partitions, so the
    completeness test below would read ∅ == ∅ and call it done. The run would
    then succeed, skip the month on every future run, and hand evaluation an
    empty report for a month the operator explicitly asked for.
    """
    for key in sorted(months):
        if key not in cache_items:
            raise ValueError(
                f"test month {months[key]!r} is in dataset.test_snap_dates but "
                "has no rows in the test cache. Run the dataset pipeline for "
                "that month first (predict cannot invent it, and treating it "
                "as already-done would silently produce an empty report)."
            )


def months_already_written(
    months: Mapping[str, str],
    cache_items: Mapping[str, set[str]],
    written_items: Mapping[str, set[str]],
) -> set[str]:
    """Month keys whose written item partitions equal the month's cached items.

    Set equality, not "some partition exists": the weaker test calls a run that
    died halfway complete — leaving the missing items absent forever — and never
    notices a month that gained an item after it was first predicted.

    ``cache_items[key]`` on purpose, not ``.get(key, set())``. A month absent
    from the cache would compare ∅ == ∅ and come back "already written", which
    is the one direction this must never fail in; :func:`require_months_are_cached`
    is what turns that state into a message, and a ``KeyError`` here is the
    backstop if it was not called.
    """
    return {
        key for key in months
        if written_items.get(key, set()) == cache_items[key]
    }


def warn_about_surplus_partitions(
    months: Mapping[str, str],
    cache_items: Mapping[str, set[str]],
    written_items: Mapping[str, set[str]],
    exclude: Container[str] = (),
) -> None:
    """Say which months hold prediction partitions the cache no longer explains.

    A set difference, not a superset test: an item renamed between runs leaves
    both a surplus partition and a missing one, and a superset test sees
    neither. The surplus is only reported, never repaired — re-predicting writes
    the items that are in the cache and cannot delete one that is not, so it
    survives every run, and ``compute_test_mAP_spark`` reads the whole
    ``model_version``, which means a stale item keeps contributing rows to the
    metric until someone drops the partition by hand.

    ``exclude`` takes the months already being redone for another reason: the
    message's "will be re-predicted on every run" clause is not true of a month
    named by ``--rebuild-dates``, which is being redone because it was asked
    for.
    """
    for key in sorted(months):
        if key in exclude:
            continue
        surplus = written_items.get(key, set()) - cache_items.get(key, set())
        if not surplus:
            continue
        logger.warning(
            "[months] predict: %s has prediction partitions for items that "
            "are not in the cache (%s). Re-predicting cannot remove them, "
            "so they will keep contributing rows to this model_version's "
            "metrics until they are dropped by hand, and this month will "
            "be re-predicted on every run.",
            months[key], sorted(surplus),
        )


def written_prediction_partitions(
    predictions_dataset, time_col: str, item_col: str
) -> dict[str, set[str]]:
    """Item partitions already written per month, read off the catalog dataset.

    predict never receives a SparkSession — its inputs are the model, the cache
    handles, the preprocessor, parameters and the predictions dataset object —
    so that object is the only route to the metastore. It already scopes itself
    to this ``model_version`` through its ``partition_filter``, which is exactly
    the scope the completeness question is asked in. The dataset arrives
    duck-typed rather than imported, which is what keeps this module free of
    project imports.

    A dataset type that cannot list partitions makes every month look
    incomplete: that re-predicts (wasteful) rather than skips (silently stale),
    which is the direction this has to fail in.
    """
    lister = getattr(predictions_dataset, "existing_partition_values", None)
    if lister is None:
        logger.warning(
            "[months] predict: %s cannot list partitions, so no month can be "
            "shown complete; every configured month will be predicted.",
            type(predictions_dataset).__name__,
        )
        return {}

    written: dict[str, set[str]] = {}
    for spec in lister():
        month, item = spec.get(time_col), spec.get(item_col)
        if month is None or item is None:
            continue
        if _HIVE_NULL_PARTITION in (month, item):
            # Hive writes a NULL partition value as this literal, while the
            # parquet side reconstructs it as None -> "None"; the two spellings
            # would never match, so this month would look permanently
            # incomplete. Drop it and say so: dropping means "not written yet",
            # which re-predicts rather than skips. Mirrors the same guard on the
            # dataset side (pipelines/dataset/month_plans.py).
            logger.warning(
                "[months] predict: ignoring prediction partition with a NULL "
                "value (%s=%r, %s=%r); that month will be treated as not yet "
                "written.", time_col, month, item_col, item,
            )
            continue
        written.setdefault(month_dir(month), set()).add(str(item))
    return written


class PredictMonthPlan(NamedTuple):
    """Which test months this predict run will write, and which it will not.

    ``to_process`` and ``skipped`` partition the configured months (disjoint,
    union == configured). ``rebuilt`` is the subset of ``to_process`` that is
    being redone because ``--rebuild-dates`` named it, whether or not it had
    work left — reported separately so a forced re-run is distinguishable in
    the manifest from one that merely had partitions missing.

    Labels, not keys: everything downstream of the plan is read by a person.
    """

    to_process: list[str]
    skipped: list[str]
    rebuilt: list[str]


def plan_predict_months(
    months: Mapping[str, str],
    done: Container[str],
    rebuild: Container[str],
) -> PredictMonthPlan:
    """Split the configured months three ways, given what is done and forced.

    Pure bookkeeping: every judgement it applies was already made by the caller
    and arrives as a set. What is left here is the order (sorted by month key,
    so the manifest and the logs read chronologically for the ``YYYYMMDD`` form)
    and the precedence — ``rebuild`` is tested before ``done``, which is what
    makes ``--rebuild-dates`` able to override completeness rather than merely
    agree with it.
    """
    to_process: list[str] = []
    skipped: list[str] = []
    rebuilt: list[str] = []

    for key in sorted(months):
        label = months[key]
        if key in rebuild:
            rebuilt.append(label)
            to_process.append(label)
        elif key in done:
            skipped.append(label)
        else:
            to_process.append(label)

    return PredictMonthPlan(
        to_process=to_process, skipped=skipped, rebuilt=rebuilt
    )
