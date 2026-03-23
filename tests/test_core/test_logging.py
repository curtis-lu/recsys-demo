"""Tests for core.logging — structured logging framework."""

import json
import logging
import re

import pytest

from recsys_tfb.core.logging import (
    ConsoleFormatter,
    JsonFormatter,
    RunContext,
    generate_run_id,
    setup_logging,
)


class TestRunId:
    def test_format(self):
        rid = generate_run_id()
        # YYYYMMDD_HHMMSS_{6 hex chars}
        assert re.match(r"^\d{8}_\d{6}_[0-9a-f]{6}$", rid)

    def test_uniqueness(self):
        ids = {generate_run_id() for _ in range(100)}
        assert len(ids) == 100


class TestRunContext:
    def test_auto_generates_run_id(self):
        ctx = RunContext(pipeline="dataset")
        assert re.match(r"^\d{8}_\d{6}_[0-9a-f]{6}$", ctx.run_id)

    def test_explicit_run_id(self):
        ctx = RunContext(run_id="20260322_120000_a1b2c3", pipeline="dataset")
        assert ctx.run_id == "20260322_120000_a1b2c3"


class TestJsonFormatter:
    def test_output_is_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "hello"
        assert parsed["level"] == "INFO"
        assert "timestamp" in parsed

    def test_extra_fields_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="node done", args=(), exc_info=None,
        )
        record.event = "node_completed"
        record.node = "select_sample_keys"
        record.duration_seconds = 1.23
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["event"] == "node_completed"
        assert parsed["node"] == "select_sample_keys"
        assert parsed["duration_seconds"] == 1.23


class TestConsoleFormatter:
    def test_format(self):
        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="completed in 12.5s", args=(), exc_info=None,
        )
        output = formatter.format(record)
        # [YYYY-MM-DD HH:MM:SS] INFO [label] message
        assert re.match(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] INFO", output)
        assert "completed in 12.5s" in output


class TestSetupLogging:
    def test_creates_handlers(self, tmp_path):
        ctx = RunContext(pipeline="dataset", run_id="20260322_120000_aabbcc")
        config = {
            "logging": {
                "level": "DEBUG",
                "console": True,
                "file": {
                    "enabled": True,
                    "path": str(tmp_path / "logs"),
                },
            }
        }
        setup_logging(config, ctx)
        root = logging.getLogger()
        handler_types = [type(h).__name__ for h in root.handlers]
        assert "StreamHandler" in handler_types
        assert "FileHandler" in handler_types
        # Verify file was created
        log_file = tmp_path / "logs" / "dataset_20260322_120000_aabbcc.jsonl"
        assert log_file.exists()

    def test_file_disabled(self, tmp_path):
        ctx = RunContext(pipeline="dataset")
        config = {
            "logging": {
                "console": True,
                "file": {"enabled": False},
            }
        }
        setup_logging(config, ctx)
        root = logging.getLogger()
        handler_types = [type(h).__name__ for h in root.handlers]
        assert "FileHandler" not in handler_types

    def test_default_config_when_omitted(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        ctx = RunContext(pipeline="dataset")
        setup_logging({}, ctx)
        root = logging.getLogger()
        assert len(root.handlers) >= 1  # at least console
