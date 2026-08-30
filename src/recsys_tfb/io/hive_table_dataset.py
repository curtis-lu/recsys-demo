"""Hive table dataset with INSERT OVERWRITE PARTITION semantics.

Supports both external and managed tables, partitioned and non-partitioned,
read-only mode, and append/overwrite write modes. Designed to be reusable
across pipelines (source_etl, dataset, inference).
"""

from __future__ import annotations

import logging
import time

from recsys_tfb.io.base import AbstractDataset

logger = logging.getLogger(__name__)


def _partition_key(spec: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """A hashable identity for a partition spec, order-independent."""
    return tuple(sorted(spec.items()))


_VALID_WRITE_MODES = ("overwrite", "append")


class HiveTableDataset(AbstractDataset):
    """Read/write a Hive table via Spark, with dynamic-partition insert-overwrite.

    Writes use ``INSERT [OVERWRITE|INTO] TABLE db.table`` via
    ``DataFrame.write.insertInto``, with
    ``spark.sql.sources.partitionOverwriteMode=dynamic`` when partitioned so
    that only the partitions present in the DataFrame are overwritten.

    The table is created on first write via ``CREATE [EXTERNAL] TABLE IF NOT
    EXISTS``; subsequent writes reuse the existing table.

    For ``columns="auto"`` tables that already exist, the schema evolves
    append-only on save: new DataFrame columns are added via ALTER TABLE,
    columns the DataFrame lacks are written as typed NULLs, and same-name
    type conflicts raise. Explicitly declared ``columns`` are a contract
    and never evolve.
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
        df = spark.sql(
            f"SELECT * FROM {self._qualified_name} WHERE {where}"
        )
        # partition_filter columns are constant per load (the WHERE pins each
        # to a single value), so they carry no information as data columns.
        # Drop them so downstream joins between two versioned tables don't hit
        # "Reference '<col>' is ambiguous". The on-disk partitioning and
        # partition pruning are unaffected; save() re-injects these columns
        # from partition_filter via _apply_partition_filter_cols.
        return df.drop(*self._partition_filter.keys())

    def save(self, data) -> None:
        if self._read_only:
            raise RuntimeError(
                f"Cannot save to read-only Hive table '{self._qualified_name}'"
            )

        spark = self._get_spark()
        df = self._to_spark(spark, data)

        if self._partition_filter:
            df = self._apply_partition_filter_cols(df)

        if self._infer_columns and self._table_exists(spark):
            df = self._evolve_schema(spark, df)
        else:
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

        # What this write touched is answerable from the metastore, and it has
        # to be: `df` is a plan, not a result. `insertInto` does not
        # materialise it, so asking the frame itself —
        # `df.select(part_cols).distinct().collect()` — builds a second query
        # execution with every source scan, join and shuffle still in it, and
        # runs the whole lineage again to print one log line. `SHOW PARTITIONS`
        # answers from metadata alone, at a cost independent of table size.
        # What that costs in exactness is written down in ADR-0009.
        report_partitions = bool(
            (self._partition_cols or self._partition_filter)
            and self._write_mode == "overwrite"
        )
        # ...but only when a `partition_filter` isolates this run's output.
        # Without one the table accumulates across every run, so
        # `existing_partition_values()` answers about the whole table and a
        # before/after diff cannot tell an overwritten partition from an
        # untouched one — a re-publish would report "0 new" and a partition
        # count unrelated to this write. Those tables keep asking the frame,
        # at the cost above. See ADR-0009 and issue #179.
        #
        # As of #187 no entry in `conf/base/catalog.yaml` takes that branch:
        # the last three (the inference outputs) moved `model_version` from
        # `partition_cols` to `partition_filter`. The branch stays because
        # nothing in this class requires a filter — a future entry declared
        # without one gets the slow-but-correct report rather than none — and
        # `tests/test_core/test_catalog_inference_entries.py` is what keeps the
        # catalog from quietly growing one back.
        scoped_to_this_run = bool(self._partition_filter)
        before = (
            {_partition_key(p) for p in self.existing_partition_values()}
            if report_partitions and scoped_to_this_run
            else set()
        )

        insert_start = time.monotonic()
        df.write.mode(self._write_mode).insertInto(self._qualified_name)
        insert_seconds = time.monotonic() - insert_start

        if not report_partitions:
            logger.info(
                "Wrote %s in %.2fs", self._qualified_name, insert_seconds,
                extra={
                    "event": "table_written",
                    "dataset_name": self._qualified_name,
                    "insert_seconds": round(insert_seconds, 3),
                },
            )
            return

        if not scoped_to_this_run:
            self._log_partitions_from_frame(df)
            return

        after = self.existing_partition_values()
        new_partitions = [p for p in after if _partition_key(p) not in before]
        # Both numbers, always. `new_partitions` alone would read as "nothing
        # was written" for every node that rebuilds its months in full — it
        # overwrites partitions that already existed, so the diff is empty by
        # construction. `after` is what carries the information for those.
        logger.info(
            "Wrote %s in %.2fs: %d partition(s) now present under %s, %d new: %s",
            self._qualified_name,
            insert_seconds,
            len(after),
            self._partition_filter,
            len(new_partitions),
            new_partitions,
            extra={
                "event": "partitions_written",
                "dataset_name": self._qualified_name,
                "insert_seconds": round(insert_seconds, 3),
                "partition_count": len(after),
                "new_partitions": new_partitions,
            },
        )

    def _log_partitions_from_frame(self, df) -> None:
        """Name the written partitions by re-querying the frame.

        The pre-ADR-0009 path, byte-for-byte, kept only for tables with no
        `partition_filter`. It re-executes the frame's whole lineage — that is
        the cost ADR-0009 measures and removes everywhere else.

        Since #187 no catalog entry reaches it: the three inference tables that
        did were the last ones, and rescoping them was exactly what that issue
        did. This is now the fallback for a hypothetical future entry, not a
        live path — which is why the only thing exercising it is
        `TestSaveWithoutPartitionFilterKeepsTheFrameQuery`.
        """
        part_cols = list(self._partition_filter.keys()) + [
            c["name"] for c in self._partition_cols
        ]
        written = df.select(*part_cols).distinct().collect()
        logger.info(
            "Wrote %d partitions to %s: %s",
            len(written),
            self._qualified_name,
            [{c: row[c] for c in part_cols} for row in written],
        )

    def exists(self) -> bool:
        spark = self._get_spark()
        return self._table_exists(spark)

    def existing_partition_values(self) -> list[dict[str, str]]:
        """Partition specs already present, restricted by ``partition_filter``.

        Returns one dict per partition, holding the ``partition_cols`` values
        (the ``partition_filter`` keys are dropped: they are constant by
        construction, exactly as ``load()`` drops them from the data columns).
        ``[]`` when the table or database does not exist yet.

        Values are URL-unescaped, because ``SHOW PARTITIONS`` reports the
        directory name verbatim while every other reader hands back the decoded
        value. Measured on PySpark 3.3.2 + local metastore: an item value
        ``a/b`` lists as ``a%2Fb`` here and reads back as ``a/b`` through
        pyarrow, so a caller comparing the two sets would never see a match for
        any value containing ``/ % =`` and friends. Unescaping here is what
        makes this method's output comparable with the data. (Escaping is also
        why splitting on ``/`` and ``=`` is safe: both are escaped inside
        values.)

        Metadata-only — ``SHOW PARTITIONS`` never touches the data files, so
        the cost is independent of table size. Exists so a caller with no
        SparkSession of its own (the training predict node) can ask "what have
        I already written for this ``model_version``"; the partition_filter is
        what makes the answer scoped to that version rather than the whole
        table. Deliberately unit-test-free: it is a thin query wrapper, and
        every judgement built on it is covered at the predict seam.
        """
        from urllib.parse import unquote

        from pyspark.sql.utils import AnalysisException

        spark = self._get_spark()
        try:
            rows = spark.sql(f"SHOW PARTITIONS {self._qualified_name}").collect()
        except AnalysisException as exc:
            logger.debug(
                "No partitions listed for %s (%s)", self._qualified_name, exc
            )
            return []

        keep = [c["name"] for c in self._partition_cols]
        out: list[dict[str, str]] = []
        for row in rows:
            # Lower-cased keys: the declared case is what this table was created
            # with, but nothing in the contract promises the metastore echoes it
            # back unchanged, and a case mismatch here would silently drop every
            # partition (an empty answer reads as "nothing written yet").
            spec = {
                k.lower(): unquote(v)
                for k, v in (
                    part.split("=", 1)
                    for part in str(row[0]).split("/")
                    if "=" in part
                )
            }
            if any(
                spec.get(k.lower()) != v
                for k, v in self._partition_filter.items()
            ):
                continue
            out.append(
                {k: spec[k.lower()] for k in keep if k.lower() in spec}
            )
        return out

    @property
    def declared_columns(self) -> list[str] | None:
        """Column names a ``save()`` will keep, or ``None`` when it keeps all.

        ``save()`` ends with ``df.select(*self._insert_column_order())``, so a
        column this table never declared is dropped there without an error, a
        warning or a log line. That makes "does this table declare column X?"
        a question a caller has to be able to ask *before* writing — and ask
        of the dataset object, not of ``self._columns``, which is private
        precisely so that callers do not reach into it.

        ``None`` means ``columns: "auto"``: the schema is inferred from the
        DataFrame, so nothing is dropped and there is no declaration to fall
        short of. Returning ``[]`` instead would read as "declares nothing"
        and fail every coverage check — the exact opposite of the truth.

        Order matches the insert order (data columns, then ``partition_filter``
        keys, then ``partition_cols``); callers that only test membership can
        ignore it.

        Metadata-only, and not even that: this answers from the catalog entry
        alone, so it needs no SparkSession and works before the table exists.
        """
        if self._infer_columns:
            return None
        return self._insert_column_order()

    # ---------- helpers ----------

    @property
    def _qualified_name(self) -> str:
        return f"{self._database}.{self._table}"

    def _get_spark(self):
        from recsys_tfb.utils.spark import get_or_create_spark_session

        return get_or_create_spark_session()

    def _table_exists(self, spark) -> bool:
        """Reliable table existence check that works with qualified names.

        ``spark.catalog.tableExists("db.table")`` returns False for qualified
        names in Spark 3.3.2 local-Hive mode (known PySpark quirk); SHOW
        TABLES is the portable alternative.  The two-arg form
        ``tableExists(table, db)`` is available in 3.3.2 but deprecated in
        Spark 3.4+, so we intentionally use SHOW TABLES for portability.

        Hive metastore stores table names in lowercase; using
        ``self._table.lower()`` in both the LIKE pattern and the equality
        check prevents a silent miss when the config uses mixed case.

        If the database itself does not exist, ``SHOW TABLES IN <db>`` raises
        ``AnalysisException``; we treat that the same as table-not-found and
        return False, mirroring the original ``catalog.tableExists`` contract.
        """
        from pyspark.sql.utils import AnalysisException

        try:
            rows = spark.sql(
                f"SHOW TABLES IN {self._database} LIKE '{self._table.lower()}'"
            ).collect()
        except AnalysisException:
            # Database absent == table absent; mirror catalog.tableExists.
            return False
        return any(
            r.tableName.lower() == self._table.lower() and not r.isTemporary
            for r in rows
        )

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

    def _evolve_schema(self, spark, df):
        """Align an auto-schema DataFrame with the existing table (append-only).

        Policy mirrors source_etl's schema evolution, with one deliberate
        difference: a column the table has but the df lacks is NOT an error
        here — these tables are partition-versioned, so a newer version that
        dropped a feature legitimately writes NULL while older partitions
        keep their values. Same-name type conflicts fail loud: ANSI store
        assignment would silently narrow (e.g. double -> int).

        Side effects: may ALTER TABLE ADD COLUMNS; resets ``self._columns``
        to the table's (post-ALTER) non-partition column order so the
        positional insertInto projection follows the TABLE, not the df.
        """
        from pyspark.sql import functions as F

        part_lower = {c["name"].lower() for c in self._partition_cols} | {
            k.lower() for k in self._partition_filter
        }
        table_fields = [
            f
            for f in spark.table(self._qualified_name).schema.fields
            if f.name.lower() not in part_lower
        ]
        df_fields = [
            f for f in df.schema.fields if f.name.lower() not in part_lower
        ]
        df_types = {f.name.lower(): f.dataType.simpleString() for f in df_fields}

        conflicts = [
            (f.name, df_types[f.name.lower()], f.dataType.simpleString())
            for f in table_fields
            if f.name.lower() in df_types
            and df_types[f.name.lower()] != f.dataType.simpleString()
        ]
        if conflicts:
            detail = "; ".join(
                f"{name}: DataFrame={d} vs table={t}" for name, d, t in conflicts
            )
            raise ValueError(
                f"Type conflict writing to Hive table "
                f"'{self._qualified_name}' ({detail}). Schema evolution never "
                f"casts; fix the upstream dtype or rebuild the table."
            )

        table_lower = {f.name.lower() for f in table_fields}
        new_fields = [f for f in df_fields if f.name.lower() not in table_lower]
        if new_fields:
            cols_sql = ", ".join(
                f"{f.name} {f.dataType.simpleString().upper()}"
                for f in new_fields
            )
            logger.info(
                "Schema evolution on %s: ADD COLUMNS %s",
                self._qualified_name,
                [(f.name, f.dataType.simpleString()) for f in new_fields],
            )
            spark.sql(
                f"ALTER TABLE {self._qualified_name} ADD COLUMNS ({cols_sql})"
            )
            # refreshTable 清除 relation cache，使緊接的 insertInto 看到已演化的 schema
            spark.catalog.refreshTable(self._qualified_name)

        for f in table_fields:
            if f.name.lower() not in df_types:
                df = df.withColumn(
                    f.name, F.lit(None).cast(f.dataType.simpleString())
                )

        self._columns = [
            {"name": f.name, "type": f.dataType.simpleString().upper()}
            for f in table_fields + new_fields
        ]
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
