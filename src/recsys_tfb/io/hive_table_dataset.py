"""Hive table dataset with INSERT OVERWRITE PARTITION semantics.

Supports both external and managed tables, partitioned and non-partitioned,
read-only mode, and append/overwrite write modes. Designed to be reusable
across pipelines (source_etl, dataset, inference).
"""

from __future__ import annotations

import logging

from recsys_tfb.io.base import AbstractDataset

logger = logging.getLogger(__name__)


_VALID_WRITE_MODES = ("overwrite", "append")


class HiveTableDataset(AbstractDataset):
    """Read/write a Hive table via Spark, with dynamic-partition insert-overwrite.

    Writes use ``INSERT [OVERWRITE|INTO] TABLE db.table`` via
    ``DataFrame.write.insertInto``, with
    ``spark.sql.sources.partitionOverwriteMode=dynamic`` when partitioned so
    that only the partitions present in the DataFrame are overwritten.

    The table is created on first write via ``CREATE [EXTERNAL] TABLE IF NOT
    EXISTS``; subsequent writes reuse the existing table.
    """

    def __init__(
        self,
        database: str,
        table: str,
        columns: list[dict] | str | None = None,
        partition_cols: list[dict] | None = None,
        partition_filter: dict | None = None,
        external: bool = True,
        location: str | None = None,
        stored_as: str = "PARQUET",
        write_mode: str = "overwrite",
        table_properties: dict | None = None,
        read_only: bool = False,
    ):
        self._database = database
        self._table = table
        self._infer_columns = columns == "auto"
        self._columns: list[dict] = [] if self._infer_columns else (columns or [])
        self._partition_cols = partition_cols or []
        self._partition_filter = dict(partition_filter or {})
        self._external = external
        self._location = location
        self._stored_as = stored_as
        self._write_mode = write_mode
        self._table_properties = table_properties or {}
        self._read_only = read_only

        self._validate()

    # ---------- validation ----------

    def _validate(self) -> None:
        if self._write_mode not in _VALID_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {_VALID_WRITE_MODES}, "
                f"got '{self._write_mode}'"
            )

        col_names = {c["name"] for c in self._columns}
        part_names = {c["name"] for c in self._partition_cols}

        if self._partition_filter:
            for k, v in self._partition_filter.items():
                if not isinstance(v, str) or not v:
                    raise ValueError(
                        f"partition_filter value for '{k}' must be a non-empty "
                        f"string for Hive table '{self._database}.{self._table}', "
                        f"got {v!r}"
                    )
            filter_names = set(self._partition_filter.keys())
            overlap_filter = filter_names & (col_names | part_names)
            if overlap_filter:
                raise ValueError(
                    f"partition_filter keys overlap with columns/partition_cols "
                    f"on {sorted(overlap_filter)} for Hive table "
                    f"'{self._database}.{self._table}'"
                )

        if self._read_only:
            return

        if not self._columns and not self._infer_columns:
            raise ValueError(
                f"columns is required for writable Hive table "
                f"'{self._database}.{self._table}' (use 'auto' to infer from DataFrame)"
            )

        if self._external and not self._location:
            raise ValueError(
                f"external=True requires 'location' for Hive table "
                f"'{self._database}.{self._table}'"
            )

        if not self._external and self._location:
            logger.warning(
                "Managed Hive table '%s.%s' has explicit location '%s'; "
                "managed tables normally use the Hive warehouse directory.",
                self._database,
                self._table,
                self._location,
            )

        overlap = col_names & part_names
        if overlap:
            raise ValueError(
                f"columns and partition_cols overlap on {sorted(overlap)} "
                f"for Hive table '{self._database}.{self._table}'"
            )

    # ---------- AbstractDataset contract ----------

    def load(self):
        spark = self._get_spark()
        if not self._partition_filter:
            return spark.table(self._qualified_name)
        where = " AND ".join(
            f"{k} = '{self._escape_sql_value(v)}'"
            for k, v in self._partition_filter.items()
        )
        return spark.sql(
            f"SELECT * FROM {self._qualified_name} WHERE {where}"
        )

    def save(self, data) -> None:
        if self._read_only:
            raise RuntimeError(
                f"Cannot save to read-only Hive table '{self._qualified_name}'"
            )

        spark = self._get_spark()
        df = self._to_spark(spark, data)

        if self._partition_filter:
            df = self._apply_partition_filter_cols(df)

        if self._infer_columns and not self._columns:
            self._columns = _infer_columns_from_spark(
                df,
                exclude={c["name"] for c in self._partition_cols}
                | set(self._partition_filter.keys()),
            )

        self._ensure_table_exists(spark)

        if self._partition_cols or self._partition_filter:
            spark.conf.set(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

        df = df.select(*self._insert_column_order())
        df.write.mode(self._write_mode).insertInto(self._qualified_name)

        if (
            (self._partition_cols or self._partition_filter)
            and self._write_mode == "overwrite"
        ):
            part_cols = list(self._partition_filter.keys()) + [
                c["name"] for c in self._partition_cols
            ]
            written = (
                df.select(*part_cols).distinct().collect()
            )
            logger.info(
                "Wrote %d partitions to %s: %s",
                len(written),
                self._qualified_name,
                [{c: row[c] for c in part_cols} for row in written],
            )

    def exists(self) -> bool:
        spark = self._get_spark()
        return spark.catalog.tableExists(self._qualified_name)

    # ---------- helpers ----------

    @property
    def _qualified_name(self) -> str:
        return f"{self._database}.{self._table}"

    def _get_spark(self):
        from recsys_tfb.utils.spark import get_or_create_spark_session

        return get_or_create_spark_session()

    @staticmethod
    def _to_spark(spark, data):
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return spark.createDataFrame(data)
        return data

    def _insert_column_order(self) -> list[str]:
        return (
            [c["name"] for c in self._columns]
            + list(self._partition_filter.keys())
            + [c["name"] for c in self._partition_cols]
        )

    def _apply_partition_filter_cols(self, df):
        """Ensure DataFrame has static partition columns with the filter values.

        - Missing column: add via withColumn(lit(value)).
        - Present with matching value: keep as-is.
        - Present with non-matching or multiple distinct values: raise.
        """
        from pyspark.sql.functions import lit

        for k, v in self._partition_filter.items():
            if k not in df.columns:
                df = df.withColumn(k, lit(v))
                continue
            distinct = df.select(k).distinct().limit(2).collect()
            distinct_vals = {row[k] for row in distinct}
            if distinct_vals != {v}:
                raise ValueError(
                    f"partition_filter mismatch for column '{k}' on "
                    f"'{self._qualified_name}': expected {{'{v}'}}, "
                    f"DataFrame has {distinct_vals}"
                )
        return df

    def _ensure_table_exists(self, spark) -> None:
        sql = self._build_create_ddl()
        spark.sql(sql)
        logger.debug("Ensured Hive table %s exists", self._qualified_name)

    def _build_create_ddl(self) -> str:
        external_kw = "EXTERNAL " if self._external else ""
        col_defs = ",\n    ".join(_format_col(c) for c in self._columns)

        parts = [
            f"CREATE {external_kw}TABLE IF NOT EXISTS {self._qualified_name} (",
            f"    {col_defs}",
            ")",
        ]
        all_part_cols = [
            {"name": k, "type": "STRING"} for k in self._partition_filter.keys()
        ] + list(self._partition_cols)
        if all_part_cols:
            part_defs = ", ".join(_format_col(c) for c in all_part_cols)
            parts.append(f"PARTITIONED BY ({part_defs})")
        parts.append(f"STORED AS {self._stored_as}")
        if self._location:
            parts.append(f"LOCATION '{self._location}'")
        if self._table_properties:
            tblprops = ", ".join(
                f"'{k}'='{v}'" for k, v in self._table_properties.items()
            )
            parts.append(f"TBLPROPERTIES ({tblprops})")
        return "\n".join(parts)


    @staticmethod
    def _escape_sql_value(v: str) -> str:
        return v.replace("'", "''")


def _format_col(col: dict) -> str:
    name = col["name"]
    type_ = col["type"]
    comment = col.get("comment")
    if comment:
        safe = comment.replace("'", "\\'")
        return f"{name} {type_} COMMENT '{safe}'"
    return f"{name} {type_}"


def _infer_columns_from_spark(df, exclude: set[str]) -> list[dict]:
    return [
        {"name": f.name, "type": f.dataType.simpleString().upper()}
        for f in df.schema.fields
        if f.name not in exclude
    ]
