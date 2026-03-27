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
