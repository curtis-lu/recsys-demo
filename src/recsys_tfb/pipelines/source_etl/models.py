"""Data models for the source ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TableConfig:
    """Configuration for a single ETL output table."""

    name: str
    sql_file: str
    partition_by: dict[str, str]  # ordered: {col_name: data_type}, e.g. {"snap_date": "DATE"}
    primary_key: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    quality_checks: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> TableConfig:
        raw_pb = data["partition_by"]
        if isinstance(raw_pb, list):
            raise ValueError(
                f"Table '{data['name']}': partition_by must be a mapping "
                f"{{col_name: data_type}}, not a list. "
                f"Migrate '[col]' to '{{col: TYPE}}' (e.g. snap_date: DATE)."
            )
        if not isinstance(raw_pb, dict) or not raw_pb:
            raise ValueError(
                f"Table '{data['name']}': partition_by must be a non-empty mapping."
            )
        return cls(
            name=data["name"],
            sql_file=data["sql_file"],
            partition_by=dict(raw_pb),
            primary_key=data.get("primary_key", []),
            depends_on=data.get("depends_on", []),
            quality_checks=data.get("quality_checks", {}),
        )


@dataclass
class SourceCheckConfig:
    """Configuration for a source table freshness and schema check."""

    table_name: str
    partition_key: str
    min_row_count: int = 0
    expected_columns: dict[str, str] = field(default_factory=dict)
    allow_new_columns: bool = True

    @classmethod
    def from_dict(cls, table_name: str, data: dict) -> SourceCheckConfig:
        return cls(
            table_name=table_name,
            partition_key=data["partition_key"],
            min_row_count=data.get("min_row_count", 0),
            expected_columns=data.get("expected_columns", {}),
            allow_new_columns=data.get("allow_new_columns", True),
        )


@dataclass
class AuditRecord:
    """A single audit record for one table execution."""

    run_id: str
    snap_date: str
    table_name: str
    status: str  # "success" | "failed" | "skipped"
    row_count: int = 0
    duration_seconds: float = 0.0
    error_message: str = ""
