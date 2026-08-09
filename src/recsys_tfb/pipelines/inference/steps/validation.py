"""Inference output validation — and the register of which check runs where.

Validation happens at **two layers**, and this module is where the split is
written down (ADR-0011 §3). The asymmetry that drives it: inside a chunk the
data is already a pandas frame in the driver, so a check costs nothing; over
the whole batch every check is a Spark scan of the published table.

===============  =====================================================
:data:`CHUNK_CHECKS`   run in :func:`validate_scored_chunk`, once per
                       ``(time, bucket, item)`` chunk, before its ``save()``
:data:`BATCH_CHECKS`   run in ``validate_predictions``, once, over the ranked
                       table, on the way to publication
===============  =====================================================

The main reason for the split is **not** cost, it is failing early: a chunk
check fires on the first chunk rather than after every chunk has been scored
and ranked, which on a months-long backfill is the difference between ten
minutes and four hours.

A check belongs to the batch layer when it needs rows a single chunk cannot
hold — one chunk is one item, so anything that compares a query group's items
against each other has to wait for the whole table. Everything else belongs to
the chunk layer.

The two lists are pinned behaviourally, one file per layer, so a check that
drifts into the wrong one turns something red:
``tests/test_pipelines/test_inference/test_chunk_validation.py`` covers the
chunk half, ``…/test_validation.py`` the batch half.

**Both registers resolve to code in this module.** The chunk register's four
checks are the four blocks of :func:`validate_scored_chunk`; the batch
register's four are one ``<name>_failure`` function each, so a reader who finds
a name in :data:`BATCH_CHECKS` finds its implementation by that name without
leaving the file. What stays in ``nodes.py`` is the Spark that produces the
facts — the grouped aggregation and the windowed scan — because that is
mechanism, and because the two-action budget is a property of the node, not of
any single check.

**No pyspark, deliberately.** The batch predicates read an already-collected
summary row by key, so this module imports no Spark and neither layer's tests
have to wait for a SparkSession.
"""

import logging
from collections.abc import Collection

import pandas as pd

logger = logging.getLogger(__name__)

#: Checks that run per chunk, in the driver, with no Spark involvement.
#:
#: ``no_missing`` and ``no_duplicates`` used to run over the ranked table.
#: Neither could still fail there: the entity identity is coerced to ``str``
#: before it is written (a null becomes the *string* ``"None"``), and a
#: duplicate row shows up at the batch layer as a group of the wrong size,
#: which is ``completeness``'s job. ``chunk_row_count`` is the by-chunk half of
#: the row accounting; the whole-table half is ``partition_completeness``.
CHUNK_CHECKS = (
    "chunk_row_count",
    "no_missing",
    "no_duplicates",
    "item_values_are_known",
)

#: Checks that need the whole ranked table, run once before publication.
#:
#: Each name resolves to ``<name>_failure`` in this module, which returns the
#: failure dict or ``None``. The register and the implementations move together
#: because a name here with no function is a check that silently stopped
#: running — and the layering assertions would keep passing.
#:
#: ``score_range`` is deliberately absent: on every path that applies a
#: calibrator the ``[0, 1]`` bound holds by construction and the check cannot
#: go red, and on an uncalibrated ranking objective the raw booster output is
#: unbounded and the check is a false alarm — decoration or wrong, with no
#: third case (ADR-0011 §2).
BATCH_CHECKS = (
    "partition_completeness",
    "completeness",
    "rank_consistency",
    "score_varies_within_group",
)

#: What fraction of query groups may score every item identically before
#: ``score_varies_within_group`` fails the run. Below it, the tied groups are
#: logged and publication continues.
#:
#: **Not a tuning knob — it separates two measured regimes.** A tie is not
#: only produced by the bug this check hunts. ``IsotonicRegression`` fits a
#: monotone function with *plateaus*, so a group whose raw scores all land on
#: one plateau comes out exactly tied even though everything upstream is
#: correct — and ``training.calibration.method: isotonic`` is a supported
#: setting. Measured on a synthetic fit (50k calibration rows at a ~5%
#: positive rate, 200k entities x 8 items): **61 of 200,000 groups tied
#: (0.03%)**, against **zero** on the uncalibrated scores. At production scale
#: a ``> 0`` rule would therefore block every correct isotonic run — the same
#: shape of false alarm that sank the product-form partition count on small
#: populations (ADR-0011 §3).
#:
#: The failure it does have to catch sits four orders of magnitude away: a
#: degenerate item value is written by *code*, so it applies to every chunk and
#: ties **100%** of groups. Anywhere between the two regimes works; a half is
#: the coarsest boundary that names "most groups", which is what the failure
#: looks like and what no benign mechanism produces.
CONSTANT_GROUP_FAILURE_RATIO = 0.5


class ValidationError(Exception):
    """Raised when inference output fails sanity checks."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        msg = (
            f"{len(failures)} sanity check(s) failed: "
            + ", ".join(f["check"] for f in failures)
        )
        super().__init__(msg)


def validate_scored_chunk(
    out_pdf: pd.DataFrame,
    source_pdf: pd.DataFrame,
    *,
    schema: dict,
    known_items: Collection[str],
) -> None:
    """Check one chunk's scores before it is written. Raises :class:`ValidationError`.

    Called with the frame about to be saved and the frame it was scored from,
    so "how many rows went in" and "how many came out" are both in hand. Pure
    pandas over data already in the driver — no Spark action, so the cost is
    invisible next to the ``predict`` that produced ``out_pdf``.

    ``source_pdf`` is not a convenience: the entity identity reaches ``out_pdf``
    through ``astype(str)``, which turns a null into the string ``"None"``. Read
    the null check off the output and it can never go red, which is the exact
    shape of decorative check ADR-0011 exists to remove.

    ``item_values_are_known`` is the only check in either layer that would catch
    the failure ADR-0011 §1 reproduced on a real run — identity ``prod_name``
    written as the integer code, so the published partitions are named ``0``…``7``
    while the scores vary perfectly normally and every shape check stays green.
    **No configuration can make it fire today**: the caller writes the loop's
    item variable, which comes from ``inference.products`` by construction. It
    is a guard on that one assignment, at a seam this repo has already got
    wrong once, and the mutation audit — not a config-level test — is what
    demonstrates it. That is a different thing from the ``score_range`` check
    ADR-0011 §2 deleted, which was unreddable no matter what the *code* did.

    ``chunk_row_count`` is in the same category, and more weakly so. The caller
    builds ``out_pdf`` by handing pandas the entity arrays and the score array
    together, so a length disagreement raises ``All arrays must be of the same
    length`` at construction, before this ever runs — and ``entity`` cannot be
    empty (A7). It is a guard on that construction staying row-preserving, not
    a check on the data. Do not read it as protection against a short chunk;
    nothing in either layer sees one.

    Args:
        out_pdf: the frame about to be handed to ``save()``.
        source_pdf: the bucket's features this chunk was scored from, before any
            string coercion.
        schema: ``core.schema.get_schema(parameters)``. Taken whole rather than
            unpacked into four column arguments that would always travel
            together — every other node in this pipeline reads its column roles
            the same way.
        known_items: the items this run is configured to score.
    """
    identity_cols = schema["identity_columns"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    score_col = schema["score"]

    failures: list[dict] = []

    n_in, n_out = len(source_pdf), len(out_pdf)
    if n_out != n_in:
        failures.append({
            "check": "chunk_row_count",
            "detail": f"scored {n_out} rows from {n_in} entities",
        })

    null_counts: dict[str, int] = {}
    for col in entity_cols:
        n_null = int(source_pdf[col].isna().sum())
        if n_null:
            null_counts[col] = n_null
    for col in [c for c in identity_cols if c not in entity_cols] + [score_col]:
        n_null = int(pd.isna(out_pdf[col]).sum())
        if n_null:
            null_counts[col] = n_null
    if null_counts:
        failures.append({
            "check": "no_missing",
            "detail": f"NaN values found: {null_counts}",
        })

    n_dupes = int(out_pdf.duplicated(subset=identity_cols).sum())
    if n_dupes:
        failures.append({
            "check": "no_duplicates",
            "detail": f"{n_dupes} duplicate rows on {identity_cols}",
        })

    # ``str`` on both sides. The item column comes back from pandas with
    # whatever dtype the frame carries, while ``inference.products`` is
    # whatever YAML parsed — and the schema is configurable, so an integer item
    # is a legal instantiation of this framework. Comparing 3 against "3" would
    # fail every correct run of one.
    unknown = sorted(
        {str(value) for value in out_pdf[item_col].unique()}
        - {str(value) for value in known_items}
    )
    if unknown:
        failures.append({
            "check": "item_values_are_known",
            "detail": (
                f"{len(unknown)} {item_col} value(s) are not configured items: "
                f"{unknown[:10]}; configured: {sorted(known_items)}"
            ),
        })

    if failures:
        raise ValidationError(failures)


def partition_completeness_failure(
    expected_partitions: Collection, written_partitions: Collection,
) -> dict | None:
    """Does every scored chunk have a partition, and every partition a chunk?

    Replaces the old ``row_count_match``, whose two halves are both gone: the
    un-exploded ``inference_population_features`` is ``|items|`` times shorter
    than the ranked output, so that comparison would fail on every correct run,
    and reading the frame cost a full re-run of the population-to-feature join
    every time.

    Comparing the manifest's *row* count instead would fail on every resume — it
    counts only what this run wrote, and a resumed run writes a fraction
    (ADR-0011 §3). So the comparison is over partition sets, and it catches both
    directions: a partition a successive save deleted, and a stale one left
    behind by an ``entity_buckets`` change that re-scoring can never remove.

    **Zero scans.** Both arguments are lists the manifest already carries, which
    is why this one runs before the aggregation rather than reading from it.

    The caller subscripts the manifest rather than using ``.get(..., [])``: two
    absent keys would compare equal and this check would pass vacuously, and a
    vacuous pass here is indistinguishable from a real one.
    """
    expected = {tuple(row) for row in expected_partitions}
    written = {tuple(row) for row in written_partitions}
    if expected == written:
        return None
    missing = sorted(expected - written)
    stale = sorted(written - expected)
    return {
        "check": "partition_completeness",
        "detail": (
            f"{len(missing)} scored chunk(s) have no partition "
            f"({missing[:10]}) and {len(stale)} partition(s) belong to "
            f"no scored chunk ({stale[:10]}); "
            f"expected {len(expected)}, "
            f"found {len(written)}"
        ),
    }


def completeness_failure(summary, n_products: int) -> dict | None:
    """Does every query group hold exactly one row per configured item?

    The batch layer's reason for existing: one chunk is one item, so nothing
    inside a chunk can see that a group is short. It is also where a duplicate
    row surfaces — as a group of the wrong size — which is why ``no_duplicates``
    lives in the chunk layer only rather than in both.
    """
    if summary["n_incomplete"] <= 0:
        return None
    return {
        "check": "completeness",
        "detail": (
            f"{summary['n_incomplete']} groups do not have exactly "
            f"{n_products} products, sizes: min={summary['min_size']}, "
            f"max={summary['max_size']}"
        ),
    }


def score_varies_within_group_failure(summary) -> dict | None:
    """Do too many groups score every item identically?

    The data-layer half of a backstop that ``require_item_is_a_feature`` only
    holds up at the config layer. If the item value fed to the model degenerates
    to a constant, an entity's items all score identically and the ranking is
    whatever order ``row_number`` happened to pick — while every shape check
    stays green: the group is complete, the ranks are 1..N, and the lag check's
    ``>`` is false on a tie (ADR-0011 §1, example two).

    **A proportion, not ``> 0``**, and that is load-bearing — see
    :data:`CONSTANT_GROUP_FAILURE_RATIO` for the two measured regimes it
    separates. Ties below the threshold are what a correct isotonic run looks
    like, so they are warned about and published; silence would make an
    arbitrary internal ranking unfindable, and raising would block a supported
    calibration setting.

    Groups of size one are excluded upstream, in the aggregation: a group of one
    cannot vary, and a single-item configuration would otherwise make every
    group look constant. Group size is :func:`completeness_failure`'s question.
    """
    if summary["n_constant"] <= 0:
        return None
    constant_ratio = summary["n_constant"] / max(summary["n_groups"], 1)
    logger.warning(
        "%d of %d query group(s) (%.3f%%) score every product "
        "identically; their internal ranking is arbitrary. Ties are "
        "expected in small numbers when a calibrator maps a group's "
        "raw scores onto one plateau.",
        summary["n_constant"], summary["n_groups"], 100 * constant_ratio,
    )
    if constant_ratio <= CONSTANT_GROUP_FAILURE_RATIO:
        return None
    return {
        "check": "score_varies_within_group",
        "detail": (
            f"{summary['n_constant']} of {summary['n_groups']} "
            f"groups ({100 * constant_ratio:.1f}%) score every "
            f"product identically, above the "
            f"{100 * CONSTANT_GROUP_FAILURE_RATIO:.0f}% threshold; "
            f"the item value reaching the model is likely constant"
        ),
    }


def rank_consistency_failure(
    summary, n_products: int, count_score_order_violations,
) -> dict | None:
    """Are the ranks 1..N, and do they run in descending score order?

    One check with two questions, because the second only means anything once
    the first holds — and because answering it costs a Spark action. So the
    counter arrives as a **thunk** the caller has already bound to its frame,
    and it is called only on the path that needs it. Handing in the count
    instead would spend that action on every run whose ranks are already
    provably wrong, and report the same check twice.

    The range is a global min-of-mins and max-of-maxes rather than a per-group
    range, so a group ranked ``[1, 2, 3]`` beside one ranked ``[1..22]`` passes.
    :func:`completeness_failure`'s group-size check stands in for it, and
    ADR-0011's "what this ADR did not solve" records the pair as roughly
    sufficient — the weakness predates the two-layer split and was not fixed
    there or here.
    """
    if summary["min_rank"] != 1 or summary["max_rank"] != n_products:
        return {
            "check": "rank_consistency",
            "detail": (
                f"Rank range [{summary['min_rank']}, {summary['max_rank']}] "
                f"expected [1, {n_products}]"
            ),
        }
    violations = count_score_order_violations()
    if violations > 0:
        return {
            "check": "rank_consistency",
            "detail": f"{violations} rows where score increases with rank",
        }
    return None
