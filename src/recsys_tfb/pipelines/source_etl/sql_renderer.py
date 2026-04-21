"""SQL file reading, template variable substitution, and INSERT OVERWRITE assembly."""

from __future__ import annotations

import re
from pathlib import Path

from recsys_tfb.pipelines.source_etl.models import TableConfig


class SQLRenderer:
    """Read SQL files and render template variables."""

    def __init__(self, sql_dir: Path) -> None:
        self._sql_dir = sql_dir

    def render(self, sql_file: str, variables: dict[str, str]) -> str:
        """Read a SQL file and substitute ``${var}`` placeholders."""
        path = self._sql_dir / sql_file
        template = path.read_text(encoding="utf-8")
        rendered = template
        for key, value in variables.items():
            rendered = rendered.replace(f"${{{key}}}", value)
        # Warn about unresolved variables
        unresolved = re.findall(r"\$\{(\w+)}", rendered)
        if unresolved:
            raise ValueError(
                f"Unresolved template variables in {sql_file}: {unresolved}"
            )
        return rendered

    @staticmethod
    def strip_header_comments(sql: str) -> str:
        """Remove leading ``--`` comment lines (e.g. ``--partition by:``)."""
        lines = sql.splitlines()
        start = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith("--"):
                start = i
                break
        return "\n".join(lines[start:])

    @staticmethod
    def build_insert_overwrite(
        table_config: TableConfig,
        select_sql: str,
        target_db: str,
    ) -> str:
        """Assemble a full INSERT OVERWRITE ... PARTITION ... SELECT statement."""
        select_body = SQLRenderer.strip_header_comments(select_sql)
        partition_spec = ", ".join(table_config.partition_by)
        return (
            f"INSERT OVERWRITE TABLE {target_db}.{table_config.name} "
            f"PARTITION ({partition_spec})\n"
            f"{select_body}"
        )

    @staticmethod
    def build_create_table_ddl(
        table_config: TableConfig,
        schema,
        target_db: str,
    ) -> str:
        """Assemble a Hive-style CREATE TABLE DDL from an inferred Spark schema.

        Uses STORED AS PARQUET (Hive SerDe) instead of USING PARQUET (DataSource)
        so that type resolution is consistent with INSERT OVERWRITE.
        """
        part_names = set(table_config.partition_by)
        data_cols = [
            f"{f.name} {f.dataType.simpleString().upper()}"
            for f in schema.fields
            if f.name not in part_names
        ]
        part_cols = [
            f"{f.name} {f.dataType.simpleString().upper()}"
            for f in schema.fields
            if f.name in part_names
        ]
        col_defs = ",\n    ".join(data_cols)
        part_defs = ", ".join(part_cols)
        return (
            f"CREATE TABLE IF NOT EXISTS {target_db}.{table_config.name} (\n"
            f"    {col_defs}\n"
            f")\n"
            f"PARTITIONED BY ({part_defs})\n"
            f"STORED AS PARQUET"
        )
