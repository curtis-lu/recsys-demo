"""Segment-column joins for evaluation (Spark).

A segment column comes from one of two places (ADR-0020 bug 6):

* the run mode's population table, a catalog input of ``prepare_eval_data``,
  joined by :func:`join_segment_columns`;
* an ``evaluation.segment_sources.<column>`` override, a Hive table read
  through the ``_read_segment_source`` seam by :func:`join_segment_sources`.
  An override declares ``table`` (Hive-qualified name), ``key_columns`` and
  ``segment_column``; one that cannot be read fails loud, never a silent skip.

Which of the two a column takes, and what happens when the population table
lacks it, is decided in ``prepare_eval_data``, not here.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

#: The group a query lands in when its segment column exists but holds NULL
#: for it: the population table (or an override table) has no value for that
#: key. Reported and counted like any group, never averaged into a macro: one
#: made-up group weighted like a real one would drag the average (ADR-0020
#: bug 6). Parenthesised so no plausible real segment value spells it.
UNMATCHED_SEGMENT = "(unmatched)"


def segment_key(value) -> str:
    """The dict key a segment value is reported under.

    NULL becomes :data:`UNMATCHED_SEGMENT`; anything else is stringified. A
    real value that already spells the sentinel raises: it would merge into
    the unmatched group and silently leave every macro.
    """
    if value is None:
        return UNMATCHED_SEGMENT
    key = value if isinstance(value, str) else str(value)
    if key == UNMATCHED_SEGMENT:
        raise ValueError(
            f"segment value {key!r} is the name evaluation reserves for "
            f"queries with no segment value; it would be merged with them and "
            f"left out of every macro average. Rename that value upstream."
        )
    return key


def _read_segment_source(
    spark: SparkSession, source_config: dict
) -> SparkDataFrame:
    """Read one segment source from its Hive table.

    SEAM: only this function knows the storage backend. ``spark.table``
    failure (e.g. table absent) raises — the caller wraps it with context.
    """
    return spark.table(source_config["table"])


def join_segment_columns(
    df: SparkDataFrame,
    source: SparkDataFrame,
    key_columns: list[str],
    segment_columns: list[str],
    *,
    source_name: str,
) -> SparkDataFrame:
    """Left-join ``segment_columns`` from ``source`` onto ``df`` by ``key_columns``.

    ``source`` is deduplicated to one row per key first: a segment is an
    entity attribute and the source may be finer-grained (``sample_pool`` has
    a row per item), so the join cannot fan ``df`` out. A same-named column
    already on ``df`` (e.g. carried in by ``label_table``) is dropped first:
    the source is authoritative for it. A key with no row in ``source`` gets
    NULL, which the metric layer reports as :data:`UNMATCHED_SEGMENT`.
    """
    missing_keys = [k for k in key_columns if k not in source.columns]
    if missing_keys:
        raise ValueError(
            f"{source_name} has no key column(s) {missing_keys} to join "
            f"segment columns {segment_columns} by; it has {source.columns}."
        )
    for col in segment_columns:
        if col in df.columns:
            logger.info(
                "dropping pre-existing column %r from the input; %s is "
                "authoritative for it", col, source_name,
            )
            df = df.drop(col)
    seg = source.select(*key_columns, *segment_columns).dropDuplicates(
        key_columns
    )
    df = df.join(seg, on=list(key_columns), how="left")
    logger.info("Joined segment column(s) %s from %s",
                list(segment_columns), source_name)
    return df


def join_segment_sources(
    df: SparkDataFrame,
    segment_sources: dict,
) -> SparkDataFrame:
    """Left-join each overridden segment column from its Hive table onto ``df``.

    For each entry: read the Hive ``table``, check it has ``key_columns +
    segment_column``, then :func:`join_segment_columns`. Fails loud on a
    missing table or missing column: an override is explicit configuration,
    unlike a population table that happens to lack a column.
    """
    spark = df.sparkSession
    for seg_name, source_config in segment_sources.items():
        table = source_config["table"]
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        try:
            seg_df = _read_segment_source(spark, source_config)
        except Exception as e:  # noqa: BLE001 — re-raised with context below
            raise ValueError(
                f"segment source {seg_name!r}: cannot read Hive table "
                f"{table!r}. A configured segment source must exist."
            ) from e

        missing = [
            c for c in key_columns + [segment_column] if c not in seg_df.columns
        ]
        if missing:
            raise ValueError(
                f"segment source {seg_name!r}: Hive table {table!r} is "
                f"missing column(s) {missing}. Expected key_columns + "
                f"segment_column = {key_columns + [segment_column]}; table "
                f"has {seg_df.columns}."
            )

        df = join_segment_columns(
            df, seg_df, key_columns, [segment_column],
            source_name=f"segment source {seg_name!r} ({table})",
        )

    return df
