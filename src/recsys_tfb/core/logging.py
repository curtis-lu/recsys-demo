"""Structured logging framework for pipeline execution.

Provides RunContext for execution metadata, JsonFormatter for machine-readable
JSON lines output, ConsoleFormatter for human-readable console output, and
setup_logging() to initialise handlers from config.
"""

import json
import logging
import os
import secrets
import time
from contextlib import contextmanager
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
    current_node: str = ""

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

        # Merge extra fields attached by callers (e.g. event, node, step, duration)
        for key in ("event", "node", "step", "duration_seconds", "input_names",
                     "output_names", "status", "error_message",
                     "exception_type", "node_count", "dataset_name"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        if record.exc_info:
            log_entry["traceback"] = self.formatException(record.exc_info)
        elif record.exc_text:
            log_entry["traceback"] = record.exc_text

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable log format: ``[YYYY-MM-DD HH:MM:SS] LEVEL [pipeline:node] message``."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname

        # Build context label
        ctx = _current_context
        pipeline = ctx.pipeline if ctx else ""
        node = getattr(record, "node", None) or (ctx.current_node if ctx else "")
        if pipeline and node:
            label = f"{pipeline}:{node}"
        elif pipeline:
            label = pipeline
        else:
            label = record.name

        msg = f"[{ts}] {level} [{label}] {record.getMessage()}"
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return msg


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


@contextmanager
def log_step(step_logger: logging.Logger, step_name: str):
    """Emit step_started / step_completed events with wall-clock timing.

    Usage::

        with log_step(logger, "merge_features"):
            result = df.merge(other, on=key)
    """
    ctx = _current_context
    node_name = ctx.current_node if ctx else ""
    step_logger.info(
        "Step started: %s", step_name,
        extra={"event": "step_started", "step": step_name, "node": node_name},
    )
    t0 = time.monotonic()
    try:
        yield
        duration = time.monotonic() - t0
        step_logger.info(
            "Step completed: %s (%.2fs)", step_name, duration,
            extra={
                "event": "step_completed",
                "step": step_name,
                "node": node_name,
                "duration_seconds": round(duration, 3),
            },
        )
    except Exception:
        duration = time.monotonic() - t0
        step_logger.info(
            "Step failed: %s after %.2fs", step_name, duration,
            extra={
                "event": "step_failed",
                "step": step_name,
                "node": node_name,
                "duration_seconds": round(duration, 3),
            },
        )
        raise


def _human_bytes(n: "int | None") -> str:
    if n is None:
        return "?"
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(v) < 1024.0:
            return f"{v:.1f}{unit}"
        v /= 1024.0
    return f"{v:.1f}PB"


def log_data_volume(logger, name, obj, *, deep: bool = True, **fields) -> None:
    """Emit a uniform data-volume record for a memory-heavy object.

    Duck-typed dispatch keeps core/logging.py free of pandas/numpy/pyarrow/
    lightgbm imports. Observation must never break the real computation: any
    failure downgrades to a WARNING and returns.
    """
    if obj is None:
        logger.warning(
            "log_data_volume skipped: obj is None name=%s", name,
            extra={"event": "data_volume_skipped"},
        )
        return

    try:
        if hasattr(obj, "num_data"):  # lightgbm.Dataset
            kind, rows, cols = "lgb_dataset", obj.num_data(), obj.num_feature()
            n_bytes, dtype = None, None
        elif hasattr(obj, "memory_usage"):  # pandas.DataFrame
            kind = "pandas"
            rows = len(obj)
            cols = obj.shape[1] if obj.ndim > 1 else 1
            n_bytes = int(obj.memory_usage(deep=deep).sum())
            dts = {str(t) for t in getattr(obj, "dtypes", [])}
            dtype = next(iter(dts)) if len(dts) == 1 else "mixed"
        elif hasattr(obj, "num_rows") and hasattr(obj, "column_names"):  # pyarrow.Table
            kind = "arrow"
            rows, cols, n_bytes, dtype = (
                obj.num_rows, obj.num_columns, obj.nbytes, None
            )
        elif hasattr(obj, "nbytes"):  # numpy.ndarray
            kind = "numpy"
            shape = obj.shape
            rows = shape[0] if shape else 0
            cols = shape[1] if len(shape) > 1 else 1
            n_bytes, dtype = obj.nbytes, str(obj.dtype)
        elif isinstance(obj, (str, Path)):  # file path
            p = Path(obj)
            if not p.exists():
                logger.warning(
                    "log_data_volume skipped: path missing name=%s path=%s",
                    name, p, extra={"event": "data_volume_skipped"},
                )
                return
            kind, rows, cols = "file", None, None
            n_bytes, dtype = p.stat().st_size, None
        else:
            logger.warning(
                "log_data_volume unsupported kind name=%s type=%s",
                name, type(obj).__name__,
                extra={"event": "data_volume_skipped"},
            )
            return
    except Exception as e:  # noqa: BLE001 — observation must not raise
        logger.warning(
            "log_data_volume failed name=%s exc=%s: %s",
            name, type(e).__name__, repr(e)[:200],
            extra={
                "event": "data_volume_skipped",
                "exception_type": type(e).__name__,
            },
        )
        return

    volume = {
        "name": name, "kind": kind, "rows": rows, "cols": cols,
        "bytes": n_bytes, "dtype": dtype, "deep": deep, **fields,
    }
    logger.info(
        "data_volume name=%s kind=%s rows=%s cols=%s bytes=%s dtype=%s",
        name, kind,
        f"{rows:,}" if isinstance(rows, int) else rows,
        cols, _human_bytes(n_bytes), dtype,
        extra={"event": "data_volume", "volume": volume},
    )
