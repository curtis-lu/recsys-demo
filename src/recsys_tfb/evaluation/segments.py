"""External segment-source joining for evaluation (Spark, Hive-table sources).

``_read_segment_source`` is the source seam: it reads a Hive table via
``spark.table(...)``. Each ``segment_sources`` entry declares a ``table``
(Hive-qualified name), ``key_columns`` and ``segment_column``. A configured
source that cannot be read fails loud — never a silent skip.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _read_segment_source(
    spark: SparkSession, source_config: dict
) -> SparkDataFrame:
    """Read one segment source from its Hive table.

    SEAM: only this function knows the storage backend. ``spark.table``
    failure (e.g. table absent) raises — the caller wraps it with context.
    """
    return spark.table(source_config["table"])


def join_segment_sources(
    df: SparkDataFrame,
    segment_sources: dict,
) -> SparkDataFrame:
    """Left-join each segment column from its Hive table onto ``df``.

    For each entry: read the Hive ``table``, select ``key_columns +
    segment_column``, dedupe to one row per ``key_columns`` (the segment is a
    customer-grained attribute; the source table may be finer-grained), and
    left-join onto ``df``. Fails loud on a missing table or missing column. A
    ``segment_column`` already present on ``df`` is dropped first —
    ``segment_sources`` is the authoritative source for that column.
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

        # segment_sources is authoritative: drop any pre-existing same-named
        # column on df so the join does not produce an ambiguous reference.
        if segment_column in df.columns:
            logger.info(
                "join_segment_sources: dropping pre-existing column %r from "
                "the input; segment source %r is authoritative",
                segment_column, seg_name,
            )
            df = df.drop(segment_column)

        # dropDuplicates(key_columns) guarantees at most one row per key ->
        # the left join cannot fan out df.
        seg = seg_df.select(key_columns + [segment_column]).dropDuplicates(
            key_columns
        )
        df = df.join(seg, on=key_columns, how="left")
        logger.info(
            "Joined segment source %r (%s) from %s",
            seg_name, segment_column, table,
        )

    return df
