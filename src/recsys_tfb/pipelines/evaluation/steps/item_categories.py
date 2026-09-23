"""Item categories read off a sample_pool column (#379).

``evaluation.item_categories.column`` names the column; ``prepare_eval_data``
reads it once and lands the table as ``evaluation_item_categories``. Which
months are read, what a NULL means and what an item in two categories does
are decided in that node; this module holds how the triples are read on Spark
and how the driver turns them into the table.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.utils.item_columns import combine_item_columns

#: The category column's value as text, carried through the item combining.
_CATEGORY = "_item_category"


def read_item_category_rows(
    population: SparkDataFrame,
    schema: dict,
    category_column: str,
    population_name: str,
) -> list[tuple]:
    """Distinct ``(time as text, item, category as text or None)`` of
    ``population``, one small collect.

    The category is copied out **before** a multi-column item is combined:
    combining drops the source columns (``utils.item_columns``), and the
    category column may be one of them (``campaign_id`` of
    ``[campaign_id, creative_format]``). Combining through
    ``combine_item_columns`` rather than the bare expression keeps its B16
    check. The category is read as text so an integer column lands as a JSON
    string, the type the collapsed frame's item column takes.

    What reaches the driver is bounded by months × items × categories, not by
    rows; the caller has already restricted ``population`` to the months.
    """
    time_col, item_col = schema["time"], schema["item"]
    frame = population.withColumn(
        _CATEGORY, F.col(category_column).cast("string"))
    frame = combine_item_columns(frame, schema, population_name)
    return [
        (r[0], r[1], r[2])
        for r in frame.select(
            F.col(time_col).cast("string"), F.col(item_col), F.col(_CATEGORY),
        ).distinct().collect()
    ]


def category_of_each_item(rows: Iterable[tuple]) -> dict:
    """``{item: category}`` from ``(time, item, category)`` triples in which
    no item has two non-NULL categories (B18, checked by the caller).

    A NULL category is ignored where another row of the item names one; an
    item whose every row is NULL is its own category — what
    ``unmapped: singleton`` does for an item a hand-written mapping leaves
    out. A NULL item is skipped: it joins nothing.
    """
    named: dict = {}
    seen: set = set()
    for _time, item, category in rows:
        if item is None:
            continue
        seen.add(item)
        if category is not None:
            named[item] = category
    return {item: named.get(item, item) for item in sorted(seen, key=str)}
