"""Common-universe alignment for 2-way model comparison.

Pure-function module: given two prediction DataFrames, return the common
entities (as a Spark DataFrame) and the common items (as a Python set).

**The two sides are handled differently on purpose, and the asymmetry is
decided by the data, not by taste.** Entities are a production population in
the millions, so their intersection is computed inside Spark and never
returned to the driver: a ``.collect()`` of a million entities lands in the
driver's *Python* heap, which ``spark.driver.memory`` does not cover (that
setting sizes the JVM heap; see
``docs/notes/2026-07-11-training-oom-investigation.md``). Items are 22
products (ADR-0010), so collecting them and broadcasting a 22-row table is
the right thing and stays.
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

    ``common_entities`` is a DataFrame with exactly ``entity_cols``, one row
    per shared entity — an entity is the combination of **every** column in
    ``schema.entity``, in declaration order. Callers join on the whole row;
    intersecting only the first column would keep entities that exist on one
    side alone.

    ``common_items`` is a Python set: the item universe is 22 products, small
    enough to live in the driver.

    Raises ``DataConsistencyError`` (B3) when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_entities = a.select(*entity_cols).distinct()
    b_entities = b.select(*entity_cols).distinct()
    # ``intersect`` is a distinct set intersection evaluated by Spark; it
    # matches columns by position, and both sides were selected in the same
    # order just above.
    common_entities = a_entities.intersect(b_entities)
    # ``isEmpty()`` runs entirely in the JVM (no rows cross into Python) and
    # short-circuits on the first row. The two ``count()`` calls below are
    # full passes over both populations, so they only run when we are already
    # raising — the happy path pays nothing for them.
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
