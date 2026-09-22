"""Combine a multi-column ``item`` into one column on read (#394, ADR-0027).

``schema.columns.item`` may list several columns of the user's own tables
(e.g. ``[campaign_id, creative_format]``). Each entry that reads one of those
tables — ``sample_pool``, ``label_table``, the candidate-level feature table,
an external comparison table — calls :func:`combine_item_columns` first. After
it the frame has one column named ``COMBINED_ITEM_COLUMN`` holding the values
joined with ``-`` and no longer has the source columns: exactly what a source
SQL that combined them itself would have produced, which is what every step
downstream already handles.

A single declared column is left alone — same frame, same name — so no
existing deployment sees any change.

Why an entry that forgets to call this cannot go wrong quietly: its frame has
no ``item`` column, and every consumer either joins on ``identity_columns`` or
groups by ``item``, both of which fail on an unresolved name.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError, item_source_column_errors
from recsys_tfb.core.schema import COMBINED_ITEM_COLUMN

#: Fixed, not configurable (ADR-0027 decision 2). Values that themselves hold a
#: ``-`` are normal and fine; only two different combinations producing the
#: same value break anything, and B15 refuses that.
ITEM_SEPARATOR = "-"


def combined_item_value(source_columns: list[str]) -> Column:
    """The combined item value, as a Spark expression over the source columns.

    ``concat`` rather than ``concat_ws``: ``concat_ws`` skips a null part, so
    ``("a-b", null)`` would become ``a-b`` — the same item as ``("a", "b")``,
    and the row would join that item's labels. ``concat`` makes the whole
    value null instead, which is exactly a null item column today. Each part
    is cast to string so an integer id combines as its digits.
    """
    parts: list[Column] = []
    for idx, col in enumerate(source_columns):
        if idx:
            parts.append(F.lit(ITEM_SEPARATOR))
        parts.append(F.col(col).cast("string"))
    return F.concat(*parts)


def combine_item_columns(df: DataFrame, schema: dict, table: str) -> DataFrame:
    """Return ``df`` with a multi-column item combined into one column.

    Args:
        df: One of the user's own tables, as read.
        schema: The resolved schema (``get_schema``).
        table: The table's name, for the error messages.

    Raises:
        DataConsistencyError: B16 — ``df`` lacks a source column, or already
            has a column named ``COMBINED_ITEM_COLUMN`` (the combined value
            would overwrite it).
    """
    sources = schema["item_source_columns"]
    if len(sources) < 2:
        return df
    errors = item_source_column_errors(schema, table, df.columns)
    if errors:
        raise DataConsistencyError("\n".join(errors))
    return df.withColumn(
        COMBINED_ITEM_COLUMN, combined_item_value(sources)
    ).drop(*sources)
