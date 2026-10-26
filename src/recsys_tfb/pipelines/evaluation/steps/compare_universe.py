"""The common (query group × item) universe of a two-way comparison, and both
sides restricted to it.

Finding the universe
--------------------

``common_universe``: given two prediction DataFrames, return the common query
groups (as a Spark DataFrame) and the common items (as a Python set).

**Why query groups, not entities.** A query group is
``schema["query_group_columns"]`` — the unit each ranking is made in and mAP
divides by. With one evaluated date "common entities" and "common query
groups" are the same set. With several dates they are not: an entity B
scored in January only would, keyed by entity, keep A's February rows for it,
and the two sides' metrics would be computed over different populations with
nothing raising.

**Why the time is compared as text.** A is ``enriched_eval_predictions``, whose
time partition is STRING; an ``external_hive`` B may carry its time column as
DATE. Both sides' time is cast to string before the intersection and before
each restriction join: the same comparison ``steps/compare_sources.py`` already
selected B's dates with, rather than whichever way Spark's implicit coercion
goes for mixed types. On Spark 3.3.2 the implicit coercion gives the same
answer for ISO DATE against STRING, so the DATE test in
``test_compare_universe.py`` pins the result, not the cast.

**Why the two sides are handled differently.** The asymmetry is structural,
not a tuning choice, and it holds for any instantiation of this framework:

- The **item** universe is bounded by the item list. Consistency invariant
  A3 requires ``schema.item`` to carry a ``schema.categorical_values`` entry
  (``core/consistency.py::require_item_list_declared``), the size of a product
  catalogue, not of a population. Collecting items and broadcasting them is
  correct.
- The **entity** universe has no such bound. It is discovered from the data,
  and grows with the population being scored. So its intersection is computed
  inside Spark and never returned to the driver.

``.collect()`` of an unbounded entity universe lands in the driver's *Python*
heap, which ``spark.driver.memory`` does not cover — that setting sizes the JVM
heap. See ``docs/notes/2026-07-11-training-oom-investigation.md``.

Restricting to it
-----------------

``trim_to_common_universe``: keep each side's rows inside that universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``, or
   persisted with ``enriched_eval_predictions`` under ``--compare-only``);
   restrict keeps it unchanged.
B side: takes A's label, joined on the identity columns, so both sides are
   scored against the same ground truth in every run mode (ADR-0020 bug 7).
   A label column B brings with it is dropped first: it is frozen at whatever
   ``label_table`` said when B was persisted.

   Why A's label and not a fresh ``label_table`` join: A's label is not always
   the current ``label_table`` either. ``--post-training`` keeps the label
   stored with the training predictions on purpose, and ``--compare-only``
   reads the one persisted by the standard run. Re-joining ``label_table`` for
   B alone gives the two sides different answers in both of those modes;
   re-joining it for both makes the comparison disagree with the main report
   of the same run. The cost: a B row whose (entity, item) A did not score has
   no answer to copy and counts as 0 — which only happens when the two sides'
   candidate sets are asymmetric.

Re-ranks both sides within the query group —
``schema["query_group_columns"]``, every column of it — because the candidate
set just shrank. That is the same grouping ``compute_test_metrics`` ranks
by, so the metrics the comparison report shows are the metrics the main line
computes.
"""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics_spark import rank_within_query
from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
    COMPARE_SOURCE_KEY,
    compare_source_identity,
)
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint


#: Temporary column holding the time as text on the side being restricted, so
#: the join matches text with text (see the module docstring). Dropped before
#: anything leaves this module.
_TIME_TEXT = "__compare_time_text"


def query_groups_with_text_time(
    df: SparkDataFrame, time_col: str, query_group_cols: list[str],
) -> SparkDataFrame:
    """``df``'s distinct query groups, the time column cast to string.

    The one place a two-way comparison builds query groups to match across the
    sides, so the intersection and the coverage count cannot disagree on how a
    DATE and a STRING time compare.

    Takes the whole ``query_group_columns`` rather than the time and the
    entity separately, so a query group that grows a column grows here too.
    ``time_col`` stays a parameter because it names the one member that needs
    the cast.

    The membership is checked, not assumed: a ``time_col`` outside
    ``query_group_cols`` would skip the cast *and* drop the time from the
    key — the module docstring's first failure mode, arriving silently.
    """
    if time_col not in query_group_cols:
        raise ValueError(
            f"query_groups_with_text_time: time column {time_col!r} is not in "
            f"the query group {query_group_cols}. Both must come from the same "
            "get_schema() result."
        )
    return df.select(*[
        F.col(c).cast("string").alias(c) if c == time_col else F.col(c)
        for c in query_group_cols
    ]).distinct()


class CommonUniverse(NamedTuple):
    """What ``common_universe`` found.

    ``a_items`` / ``b_items`` are the two collects ``common_items`` was built
    from. They ride along so coverage reports the same sets instead of
    collecting them again (ADR-0020 bug 14).
    """

    common_query_groups: SparkDataFrame
    common_items: set
    a_items: set
    b_items: set


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    time_col: str,
    query_group_cols: list[str],
    item_col: str,
) -> CommonUniverse:
    """``common_query_groups`` is a **lazy** DataFrame with exactly
    ``query_group_cols``, one row per shared query group, the time as text.
    That list is taken whole, in the order ``get_schema`` derived it — every
    column of ``schema.entity`` in declaration order, not just the first.
    Callers join on the whole row; intersecting only the first entity column
    would keep entities that exist on one side alone, and leaving out the time
    would keep an entity's months that only one side scored.

    Lazy means every consumer re-evaluates the intersection's shuffle. That is
    deliberate: caching it here would leak, because the DataFrames the caller
    builds on top are themselves lazy and there is no point inside this module
    where it is safe to ``unpersist``. Lifecycle belongs to whoever forces the
    result. See the module docstring for why bounded driver memory is worth
    that trade.

    ``common_items`` is a Python set — see the module docstring for why this
    side may come back to the driver and the entity side may not.

    Raises ``DataConsistencyError`` when either intersection is empty.
    """
    a_groups = query_groups_with_text_time(a, time_col, query_group_cols)
    b_groups = query_groups_with_text_time(b, time_col, query_group_cols)
    # A left-semi join, not ``intersect``: they disagree on nulls.
    # ``intersect`` treats ``NULL == NULL`` as a match, while the equi-join that
    # ``trim_to_common_universe`` then runs drops null keys. Otherwise a
    # universe whose only shared entity is null-keyed passes the gate and then
    # restricts to zero rows, silently.
    common_query_groups = a_groups.join(
        b_groups, on=query_group_cols, how="left_semi")
    # ``isEmpty()`` stops once one row materialises, though the join underneath
    # still shuffles both sides. It avoids the two ``count()`` calls below, full
    # passes over both populations, which run only when we are already raising.
    if common_query_groups.isEmpty():
        raise DataConsistencyError(
            f"compare common_query_groups is empty — A has {a_groups.count()} "
            f"query groups, B has {b_groups.count()} query groups, "
            f"intersection = 0. Check that both sides cover the same "
            f"{time_col!r} values and that the query group columns "
            f"{query_group_cols} carry matching types — and that B was "
            f"produced under the same schema.columns: a model version run "
            f"before an optional role (e.g. occasion) was declared has that "
            f"column NULL, and NULL keys match nothing."
        )

    a_items = {r[0] for r in a.select(item_col).distinct().collect()}
    b_items = {r[0] for r in b.select(item_col).distinct().collect()}
    common_items = a_items & b_items
    if not common_items:
        raise DataConsistencyError(
            f"compare common_items is empty — A has {len(a_items)} items, "
            f"B has {len(b_items)} items (after mapping), intersection = 0. "
            "Check prod_mapping config."
        )

    return CommonUniverse(common_query_groups, common_items, a_items, b_items)


def trim_to_common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, CommonUniverse]:
    schema = get_schema(parameters)
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    query_group_cols = schema["query_group_columns"]

    universe = common_universe(a, b, time_col, query_group_cols, item_col)
    common_items = universe.common_items
    # The time as text under a temporary name on both sides of the semi join,
    # so each side's own time column (STRING or DATE) is left as it was.
    common_groups = universe.common_query_groups.withColumnRenamed(
        time_col, _TIME_TEXT
    )
    # Derived from ``query_group_cols`` rather than respelled, so a query group
    # that grows a column is joined on that column here too.
    common_group_join_key = [
        _TIME_TEXT if c == time_col else c for c in query_group_cols
    ]

    spark = a.sparkSession
    item_df = spark.createDataFrame([(i,) for i in common_items], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        # No ``F.broadcast`` on the query-group side. That hint does not advise
        # Spark, it overrides ``spark.sql.autoBroadcastJoinThreshold`` — so on
        # an entity universe too large to broadcast it still forces the driver
        # to assemble the whole table and ship a copy to every executor. The
        # item join keeps its hint: that universe is bounded by config (module
        # docstring).
        df = (
            df.withColumn(_TIME_TEXT, F.col(time_col).cast("string"))
            .join(common_groups, on=common_group_join_key, how="left_semi")
            .drop(_TIME_TEXT)
        )
        df = df.join(F.broadcast(item_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(
            df, query_group_cols, score_col, item_col, schema.get("event", []),
        )
        return df.withColumnRenamed("pos", rank_col)

    a_common = _restrict_and_rank(a)
    b_common = _restrict_and_rank(b)

    if label_col in b_common.columns:
        b_common = b_common.drop(label_col)
    # Raw ``a``, not ``a_common``: b_common is already restricted, so the join
    # only picks shared keys, and A's re-ranking need not run a second time.
    a_labels = a.select(*identity_cols, label_col)
    b_common = b_common.join(a_labels, on=identity_cols, how="left").fillna({label_col: 0})

    return a_common, b_common, universe


def trim_to_common_with_coverage(
    eval_predictions: SparkDataFrame,
    compare_predictions_raw: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
    """``trim_to_common_universe``, plus the coverage:
    ``(a_common, b_common, coverage)``.

    ``eval_predictions`` is already kept to the evaluated dates and its
    partitions checked (``make_restrict_to_common_node``), so
    ``segment_columns.json``'s own fingerprint is not compared here.

    Sizes are counted in **query groups** (``query_group_columns``), the unit
    mAP divides by; any other unit would put two scales side by side in one
    table (ADR-0015).

    The common count is taken from what the restriction kept, not from the raw
    frames (ADR-0020 bug 14): an ``intersect`` of the raw query groups matches
    ``NULL == NULL`` where the restriction's equi-join drops null keys, so it
    would count groups no metric saw.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    query_group_cols = schema["query_group_columns"]

    # Decision — B is scored against A's labels: not the label B landed with,
    # and not a fresh label_table join, since A's own label is not the current
    # label_table under --post-training / --compare-only. One answer for both
    # sides in every mode (ADR-0020 bug 7; the full why is in the module
    # docstring).
    a_common, b_common, universe = trim_to_common_universe(
        eval_predictions, compare_predictions_raw, parameters
    )

    a_groups_full = eval_predictions.select(*query_group_cols).distinct().count()
    b_groups_full = compare_predictions_raw.select(*query_group_cols).distinct().count()
    # Groups both restricted frames still hold. A left-semi equi-join, so null
    # keys never match — the same null rule as the restriction's own joins. On
    # symmetric candidate sets this equals either side's kept group count; when
    # one side scored a common entity only on items the other lacks, that group
    # is in one side's metrics but not in "common".
    groups_common = (
        query_groups_with_text_time(a_common, time_col, query_group_cols)
        .join(
            query_groups_with_text_time(b_common, time_col, query_group_cols),
            on=query_group_cols, how="left_semi",
        )
        .count()
    )
    common_items = universe.common_items

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    coverage = {
        "kind_a": "model_version",
        "model_version_a": parameters.get("model_version", "(this run)"),
        # A literal: the table pipeline.py wires as this node's first input in
        # both compare modes, named the way a model_version source names its
        # `source`, since the comparison report prints the two side by side.
        "source_a": "enriched_eval_predictions",
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_query_group_A_full": a_groups_full,
        "n_query_group_B_full": b_groups_full,
        "n_query_group_common": groups_common,
        "n_item_A_full": len(universe.a_items),
        "n_item_B_full": len(universe.b_items),
        "n_item_common": len(common_items),
        "dropped_items_A": sorted(universe.a_items - common_items),
        "dropped_items_B": sorted(universe.b_items - common_items),
        "config_fingerprint": fingerprint(
            parameters, "restrict_to_common", (segment_columns,)),
        COMPARE_SOURCE_KEY: compare_source_identity(parameters),
    }
    return a_common, b_common, coverage
