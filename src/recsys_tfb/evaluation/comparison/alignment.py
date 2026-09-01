"""Common-universe alignment for 2-way model comparison.

Pure-function module: given two prediction DataFrames, return the
intersection of entities and of (mapped) items as Python sets.
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    entity_cols: list[str],
    item_col: str,
) -> tuple[set[tuple], set]:
    """Return ``(common_entities, common_items)`` as Python sets.

    An entity is the combination of **every** column in ``schema.entity``, so
    ``common_entities`` holds one tuple per entity, values in declaration
    order — a one-column schema yields 1-tuples. Callers join on the whole
    tuple; intersecting only the first column would keep entities that exist
    on one side alone.

    Raises ``DataConsistencyError`` (B3) when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_entities = {tuple(r) for r in a.select(*entity_cols).distinct().collect()}
    b_entities = {tuple(r) for r in b.select(*entity_cols).distinct().collect()}
    common_entities = a_entities & b_entities
    if not common_entities:
        raise DataConsistencyError(
            f"(B3) compare common_entities is empty — A has {len(a_entities)} "
            f"entities, B has {len(b_entities)} entities, intersection = 0. "
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
