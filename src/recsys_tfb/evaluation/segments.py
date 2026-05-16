"""External segment-source joining for evaluation (Spark, single impl).

``_read_segment_source`` is the source seam: today it reads a Parquet
file; a future change swaps only this function to read a Hive table
(``spark.table("ml_recsys.<segment_table>")``) without touching the join
logic. See spec "Out of scope / 後續工作".
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _read_segment_source(
    spark: SparkSession, source_config: dict
) -> SparkDataFrame | None:
    """Read one external segment source. None when the source is absent.

    SEAM: only this function knows the storage backend. Uses Spark's reader
    (filesystem/scheme-agnostic — local, HDFS, etc.) and treats ANY read
    failure as "absent" (non-fatal), preserving the pre-refactor behaviour.
    A future Hive swap replaces only this function body
    (``spark.table("ml_recsys.<segment_table>")``).
    """
    try:
        return spark.read.parquet(source_config["filepath"])
    except Exception:
        return None


def join_segment_sources(
    labels: SparkDataFrame,
    segment_sources: dict,
) -> SparkDataFrame:
    """Left-join each external segment column onto ``labels``.

    Missing sources are warned and skipped (non-fatal), preserving the
    pre-refactor behaviour.
    """
    spark = labels.sparkSession
    for seg_name, source_config in segment_sources.items():
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        seg_df = _read_segment_source(spark, source_config)
        if seg_df is None:
            logger.warning(
                "Segment source '%s' not found at %s — skipping",
                seg_name,
                source_config["filepath"],
            )
            continue

        labels = labels.join(
            seg_df.select(key_columns + [segment_column]),
            on=key_columns,
            how="left",
        )
        logger.info("Joined segment source '%s' (%s)", seg_name, segment_column)

    return labels
