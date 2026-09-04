"""Common-universe alignment for 2-way model comparison.

Pure-function module: given two prediction DataFrames, return the common
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
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    entity_cols: list[str],
    item_col: str,
) -> tuple[SparkDataFrame, set]:
    """Return ``(common_entities, common_items)``.

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
    side may come back to the driver and the entity side may not.

    Raises ``DataConsistencyError`` (B3) when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_entities = a.select(*entity_cols).distinct()
    b_entities = b.select(*entity_cols).distinct()
    # A left-semi join, not ``intersect``. Both compute the same set for
    # non-null keys, but they disagree on nulls: ``intersect`` treats
    # ``NULL == NULL`` as a match, while the equi-join that ``restrict_to_common``
    # then runs drops null keys. Using the join here keeps the B3 gate and the
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
            f"(B3) compare common_entities is empty — A has {a_entities.count()} "
            f"entities, B has {b_entities.count()} entities, intersection = 0. "
            f"Check that both sides cover the same time column value and that "
            f"the entity columns {entity_cols} carry matching types."
        )

    a_items = {r[0] for r in a.select(item_col).distinct().collect()}
    b_items = {r[0] for r in b.select(item_col).distinct().collect()}
    common_items = a_items & b_items
    if not common_items:
        raise DataConsistencyError(
            f"(B3) compare common_items is empty — A has {len(a_items)} items, "
            f"B has {len(b_items)} items (after mapping), intersection = 0. "
            "Check prod_mapping config."
        )

    return common_entities, common_items
