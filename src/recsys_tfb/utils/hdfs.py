"""HDFS↔driver-local file-copy utilities.

Pure mechanics, agnostic to caller. No knowledge of Hive business semantics
or cache protocol — those live in the calling module (e.g. training/nodes.py).
"""

from __future__ import annotations


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
