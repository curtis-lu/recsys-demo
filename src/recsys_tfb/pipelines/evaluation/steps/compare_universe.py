"""The common (entity × item) universe of a two-way comparison, and both sides
restricted to it.

Finding the universe
--------------------

``common_universe``: given two prediction DataFrames, return the common
entities (as a Spark DataFrame) and the common items (as a Python set).

**Why the two sides are handled differently.** The asymmetry is structural,
not a tuning choice, and it holds for any instantiation of this framework:

- The **item** universe has a declared upper bound. Consistency invariant A3
  requires ``schema.item`` to carry a non-empty ``schema.categorical_values``
  entry (see ``core/consistency.py::resolved_item_values``), so its
  cardinality is whatever config says — knowable before the job runs, and
  sized to fit the driver. Collecting items and broadcasting them is correct.
- The **entity** universe has no such bound. It is discovered from the data,
  and grows with the population being scored. So its intersection is computed
  inside Spark and never returned to the driver.

That second point is not theoretical: ``.collect()`` of an unbounded entity
universe lands in the driver's *Python* heap, which ``spark.driver.memory``
does not cover — that setting sizes the JVM heap. This repo has already been
bitten by exactly that distinction; see
``docs/notes/2026-07-11-training-oom-investigation.md``. In the banking
instantiation the numbers are 22 products against a seven-figure customer
population (#275), which is what made the failure visible, but the rule above
is what makes it general.

Restricting to it
-----------------

``restrict_to_common``: keep each side's rows inside that universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``, or
   persisted with ``enriched_eval_predictions`` under ``--compare-only``);
   restrict keeps it unchanged.
B side: takes A's label, joined on the identity columns, so both sides are
   scored against the same ground truth in every run mode (ADR-0020 bug 7).
   A label column B brings with it is dropped first: ``enriched_eval_predictions``
   and ``training_eval_predictions`` both land with one, frozen at whatever
   ``label_table`` said when B was persisted.

   Why A's label and not a fresh ``label_table`` join: A's label is not always
   the current ``label_table`` either. ``--post-training`` keeps the label
   stored with the training predictions on purpose, and ``--compare-only``
   reads the one persisted by the standard run. Re-joining ``label_table`` for
   B alone gives the two sides different answers in both of those modes;
   re-joining it for both makes the comparison disagree with the main report
   of the same run. Copying A's makes "same answer" hold by construction. The
   cost: a B row whose (entity, item) A did not score has no answer to copy
   and counts as 0 — which only happens when the two sides' candidate sets
   are asymmetric.

Re-ranks both sides within the query group — ``[time] + entity``, every
column of ``schema.entity`` — because the candidate set just shrank. That is
the same grouping ``compute_test_mAP_spark`` ranks by, so the metrics the
comparison report shows are the metrics the main line computes.

Also returns the ``CommonUniverse`` it restricted by. Coverage reads its item
sets from there rather than collecting them a second time (ADR-0020 bug 14).
"""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics_spark import rank_within_query


class CommonUniverse(NamedTuple):
    """What ``common_universe`` found.

    ``a_items`` / ``b_items`` are the two collects ``common_items`` was built
    from. They ride along so coverage reports the same sets instead of
    collecting them again (ADR-0020 bug 14).
    """

    common_entities: SparkDataFrame
    common_items: set
    a_items: set
    b_items: set


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    entity_cols: list[str],
    item_col: str,
) -> CommonUniverse:
    """Return ``CommonUniverse(common_entities, common_items, a_items, b_items)``.

    ``common_entities`` is a **lazy** DataFrame with exactly ``entity_cols``,
    one row per shared entity — an entity is the combination of **every**
    column in ``schema.entity``, in declaration order. Callers join on the
    whole row; intersecting only the first column would keep entities that
    exist on one side alone.

    Lazy means every consumer re-evaluates the intersection's shuffle. That is
    deliberate: caching it here would leak, because the DataFrames the caller
    builds on top are themselves lazy and there is no point inside this module
    where it is safe to ``unpersist``. Lifecycle belongs to whoever forces the
    result. See the module docstring for why bounded driver memory is worth
    that trade.

    ``common_items`` is a Python set — see the module docstring for why this
    side may come back to the driver and the entity side may not. ``a_items``
    and ``b_items`` are each side's full item set, the inputs of that
    intersection.

    Raises ``DataConsistencyError`` when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_entities = a.select(*entity_cols).distinct()
    b_entities = b.select(*entity_cols).distinct()
    # A left-semi join, not ``intersect``. Both compute the same set for
    # non-null keys, but they disagree on nulls: ``intersect`` treats
    # ``NULL == NULL`` as a match, while the equi-join that ``restrict_to_common``
    # then runs drops null keys. Using the join here keeps the empty-universe gate and the
    # restriction that follows it agreeing on what "common" means — otherwise a
    # universe whose only shared entity is null-keyed passes the gate and then
    # restricts to zero rows, silently.
    common_entities = a_entities.join(b_entities, on=entity_cols, how="left_semi")
    # ``isEmpty()`` stays in the JVM — no rows cross into Python — and stops
    # once one row materialises. It is not free: the join underneath is a
    # shuffle, so both sides are shuffled before that row exists. What it does
    # avoid is the two ``count()`` calls below, which are full passes over both
    # populations and therefore run only when we are already raising.
    if common_entities.isEmpty():
        raise DataConsistencyError(
            f"compare common_entities is empty — A has {a_entities.count()} "
            f"entities, B has {b_entities.count()} entities, intersection = 0. "
            f"Check that both sides cover the same time column value and that "
            f"the entity columns {entity_cols} carry matching types."
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

    return CommonUniverse(common_entities, common_items, a_items, b_items)


def restrict_to_common(
    a: SparkDataFrame,
    b: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, CommonUniverse]:
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    query_group_cols = [time_col, *entity_cols]

    universe = common_universe(a, b, entity_cols, item_col)
    common_entities, common_items = universe.common_entities, universe.common_items

    spark = a.sparkSession
    item_df = spark.createDataFrame([(i,) for i in common_items], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        # No ``F.broadcast`` on the entity side. That hint does not advise
        # Spark, it overrides ``spark.sql.autoBroadcastJoinThreshold`` — so on
        # an entity universe too large to broadcast it still forces the driver
        # to assemble the whole table and ship a copy to every executor. Let
        # Spark pick from the real size. ``left_semi`` restricts without adding
        # columns, so ``df``'s schema is untouched. The item join keeps its
        # hint: that universe is bounded by config. See ``common_universe``
        # above for the full rule.
        df = df.join(common_entities, on=entity_cols, how="left_semi")
        df = df.join(F.broadcast(item_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(df, query_group_cols, score_col, item_col)
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
