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
    def build_aligned_select(
        select_sql: str,
        select_columns: list[str],
        partition_by: dict[str, str],
    ) -> str:
        """Wrap SELECT with explicit CAST on partition columns; partition cols last.

        Non-partition columns are projected unchanged. Partition columns (in config
        order) are appended with ``CAST(col AS <type>) AS col``.

        Raises if a declared partition column is missing from ``select_columns``.
        Matching is case-insensitive (Hive lowercases identifiers).
        """
        body = SQLRenderer.strip_header_comments(select_sql)
        part_lower = {k.lower(): (k, v) for k, v in partition_by.items()}
        select_lower = {c.lower(): c for c in select_columns}

        missing = [k for k in part_lower if k not in select_lower]
        if missing:
            raise ValueError(
                f"Partition columns missing from SELECT output: {missing}. "
                f"SELECT has: {select_columns}"
            )

        non_partition = [
            select_lower[c] for c in (cc.lower() for cc in select_columns)
            if c not in part_lower
        ]
        partition_casts = [
            f"CAST({select_lower[k]} AS {dtype}) AS {name}"
            for k, (name, dtype) in (
                (k, part_lower[k]) for k in (pk.lower() for pk in partition_by)
            )
        ]
        projection = ",\n    ".join(non_partition + partition_casts)
        return f"SELECT\n    {projection}\nFROM (\n{body}\n) _aligned"

    @staticmethod
    def build_hive_ctas(
        table_config: TableConfig,
        aligned_select: str,
        target_db: str,
    ) -> str:
        """Hive-serde CTAS for first-time creation.

        Uses STORED AS PARQUET (Hive writer) so subsequent INSERT OVERWRITE shares
        the same type resolver (fixes the bug from commit 12824e7).
        """
        part_defs = ", ".join(
            f"{name} {dtype}" for name, dtype in table_config.partition_by.items()
        )
        return (
            f"CREATE TABLE {target_db}.{table_config.name}\n"
            f"PARTITIONED BY ({part_defs})\n"
            f"STORED AS PARQUET\n"
            f"AS\n{aligned_select}"
        )

    @staticmethod
    def build_insert_overwrite(
        table_config: TableConfig,
        aligned_select: str,
        target_db: str,
    ) -> str:
        """Assemble INSERT OVERWRITE ... PARTITION (<names>) <aligned_select>.

        Caller MUST pass a SELECT already wrapped by build_aligned_select so
        partition columns are cast to the declared types.
        """
        partition_spec = ", ".join(table_config.partition_by.keys())
        return (
            f"INSERT OVERWRITE TABLE {target_db}.{table_config.name} "
            f"PARTITION ({partition_spec})\n"
            f"{aligned_select}"
        )
