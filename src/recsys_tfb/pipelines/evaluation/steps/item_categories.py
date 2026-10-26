"""Item categories read off a sample_pool column.

``evaluation.item_categories.column`` names the column; ``prepare_eval_data``
reads it once and lands the table as ``evaluation_item_categories``. Which
months are read, what a NULL means and what an item in two categories does
are decided in that node; this module holds how the triples are read on Spark,
how the driver turns them into the table, and which table a later reader ranks
by (``landed_category_mapping``).
"""

from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import (
    item_category_column,
    item_categories_enabled,
    item_list_counted_from_data,
)
from recsys_tfb.evaluation.metrics_spark import hand_category_mapping
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
    rows.
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
    item whose every row is NULL is its own category (as ``unmapped:
    singleton`` does), spelled as text so the table holds one value type
    whatever the item column's type. A NULL item is skipped: it joins nothing.
    """
    named: dict = {}
    seen: set = set()
    for _time, item, category in rows:
        if item is None:
            continue
        seen.add(item)
        if category is not None:
            named[item] = category
    return {item: named.get(item, str(item)) for item in sorted(seen, key=str)}


def landed_category_mapping(
    item_categories: Optional[dict], parameters: dict
) -> Optional[dict]:
    """The ``{item: category}`` table the category pass ranks by:

    * column mode: the one ``prepare_eval_data`` read off
      ``evaluation.item_categories.column``;
    * item list counted from the data: the hand-written mapping, resolved
      against the evaluated model's item list landed beside it
      (``known_items``);
    * otherwise ``None``: the hand-written mapping in ``parameters`` applies.

    Pre-checks (inputs):

    * column mode: the table was read off today's column. The fingerprint
      misses a table missing altogether and the ``--compare-only`` path;
      unchecked, a table from another column would be ranked, and with none
      every item would silently be its own category.
    * item list counted from the data, categories on: ``known_items`` is
      there. The conf holds no list in this mode, so without it the mapping
      could not be checked; the run stops rather than skip the check.
    """
    column = item_category_column(parameters)
    if column is None:
        if not item_list_counted_from_data(parameters):
            return None
        known = (item_categories.get("known_items")
                 if isinstance(item_categories, dict) else None)
        if known is None and item_categories_enabled(parameters):
            raise ValueError(
                "The item list is counted from the train months, so a "
                "hand-written evaluation.item_categories.mapping is checked "
                "against the evaluated model's list — which prepare_eval_data "
                "reads off its preprocessor (preprocessor_on_disk) and lands "
                "as evaluation_item_categories.known_items, and this run has "
                "none. Re-run with --from-node prepare_eval_data, with the "
                "model's data/dataset/<base_dataset_version>/preprocessor.json "
                "in place."
            )
        return hand_category_mapping(parameters, known_items=known)
    if isinstance(item_categories, dict) and item_categories.get("column") == column:
        return item_categories["mapping"]
    landed = (
        "none was landed for this run" if not isinstance(item_categories, dict)
        else f"the landed one was read off {item_categories.get('column')!r}"
    )
    raise ValueError(
        f"evaluation.item_categories.column={column!r}, but {landed} "
        f"(evaluation_item_categories). prepare_eval_data reads the table off "
        f"sample_pool: re-run with --from-node prepare_eval_data (for "
        f"--compare-only, the standard --post-training run first)."
    )
