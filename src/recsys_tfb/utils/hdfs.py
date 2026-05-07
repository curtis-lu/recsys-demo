"""HDFS↔driver-local file-copy utilities.

Pure mechanics, agnostic to caller. No knowledge of Hive business semantics
or cache protocol — those live in the calling module (e.g. training/nodes.py).
"""

from __future__ import annotations

import os


def get_hive_table_location(spark, database: str, table: str) -> str:
    """Return the HDFS Location URI of a Hive table via DESCRIBE FORMATTED.

    Args:
        spark: active SparkSession.
        database: Hive database name.
        table: Hive table name.

    Returns:
        Raw URI from the Location row (e.g. 'hdfs://nn:9000/warehouse/db.tbl').

    Raises:
        RuntimeError: if no row with col_name == 'Location' is present.
    """
    rows = spark.sql(f"DESCRIBE FORMATTED {database}.{table}").collect()
    for row in rows:
        col_name = row.col_name.strip() if row.col_name else ""
        if col_name == "Location":
            location = (row.data_type or "").strip()
            if location:
                return location
            # fall through: treat empty/None data_type as missing
    raise RuntimeError(
        f"Location not found in DESCRIBE FORMATTED for {database}.{table}"
    )


def copy_hdfs_to_local(
    spark, src: str, dst: str, *, glob: bool = False
) -> None:
    """Copy an HDFS path (file or directory) to a driver-local path.

    Uses Spark's Hadoop FileSystem via JVM bridge — does not depend on a
    `hadoop` CLI on PATH.

    Args:
        spark: active SparkSession (used for JVM bridge + Hadoop config).
        src: HDFS source URI (or glob pattern when glob=True).
        dst: driver-local destination directory.
        glob: if True, treat src as a glob pattern and copy every match
            into dst/, preserving each match's basename.

    Raises:
        FileNotFoundError: glob=True and no paths matched.
    """
    os.makedirs(dst, exist_ok=True)

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    src_path = jvm.org.apache.hadoop.fs.Path(src)

    if glob:
        # Implemented in Task 3
        raise NotImplementedError("glob mode added in Task 3")
    else:
        dst_path = jvm.org.apache.hadoop.fs.Path(dst)
        # copyToLocalFile(deleteSource, src, dst, useRawLocalFileSystem)
        fs.copyToLocalFile(False, src_path, dst_path, False)
