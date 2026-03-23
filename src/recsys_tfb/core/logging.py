"""Structured logging framework for pipeline execution.

Provides RunContext for execution metadata, JsonFormatter for machine-readable
JSON lines output, ConsoleFormatter for human-readable console output, and
setup_logging() to initialise handlers from config.
"""

import json
import logging
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class RunContext:
    """Execution metadata for a pipeline run."""

    run_id: str = ""
    pipeline: str = ""
    env: str = ""
    dataset_version: str = ""
    model_version: str = ""
    backend: str = ""

    def __post_init__(self) -> None:
        if not self.run_id:
            self.run_id = generate_run_id()


def generate_run_id() -> str:
    """Generate a run ID in the format ``YYYYMMDD_HHMMSS_{6 hex chars}``."""
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M%S")
    suffix = secrets.token_hex(3)  # 6 hex chars
    return f"{ts}_{suffix}"


# Module-level context — set once per pipeline execution via setup_logging().
_current_context: Optional[RunContext] = None


def get_current_context() -> Optional[RunContext]:
    """Return the current RunContext, or None if not set."""
    return _current_context


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record (JSON lines format)."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject RunContext fields
        ctx = _current_context
        if ctx is not None:
            log_entry["run_id"] = ctx.run_id
            log_entry["pipeline"] = ctx.pipeline

        # Merge extra fields attached by callers (e.g. event, node, duration)
        for key in ("event", "node", "duration_seconds", "input_names",
                     "output_names", "status", "error_message",
                     "exception_type", "node_count", "dataset_name"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable log format: ``[YYYY-MM-DD HH:MM:SS] LEVEL [pipeline:node] message``."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname

        # Build context label
        ctx = _current_context
        pipeline = ctx.pipeline if ctx else ""
        node = getattr(record, "node", None)
        if pipeline and node:
            label = f"{pipeline}:{node}"
        elif pipeline:
            label = pipeline
        else:
            label = record.name

        return f"[{ts}] {level} [{label}] {record.getMessage()}"


def setup_logging(
    config: dict,
    context: RunContext,
) -> None:
    """Configure the root logger from config and bind the RunContext.

    Args:
        config: The full parameters dict.  Reads ``config["logging"]`` if
            present; uses sensible defaults otherwise.
        context: The RunContext for this pipeline execution.
    """
    global _current_context
    _current_context = context

    log_config = config.get("logging", {})
    level_name = log_config.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    console_enabled = log_config.get("console", True)
    file_config = log_config.get("file", {})
    file_enabled = file_config.get("enabled", True)
    file_path = file_config.get("path", "logs/")

    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplication on repeated calls
    root.handlers.clear()

    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        console_handler.setLevel(level)
        root.addHandler(console_handler)

    if file_enabled:
        log_dir = Path(file_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{context.pipeline}_{context.run_id}.jsonl"
        file_handler = logging.FileHandler(
            log_dir / filename, encoding="utf-8"
        )
        file_handler.setFormatter(JsonFormatter())
        file_handler.setLevel(level)
        root.addHandler(file_handler)
