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

    def test_volume_field_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="data_volume", args=(), exc_info=None,
        )
        record.event = "data_volume"
        record.volume = {
            "name": "extract_Xy.pdf", "kind": "pandas",
            "rows": 100, "cols": 12, "bytes": 4096,
            "dtype": "mixed", "deep": True,
        }
        parsed = json.loads(formatter.format(record))
        assert parsed["event"] == "data_volume"
        assert parsed["volume"]["name"] == "extract_Xy.pdf"
        assert parsed["volume"]["rows"] == 100
        assert parsed["volume"]["bytes"] == 4096


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
        # Verify file was created under <pipeline>/<YYYY-MM>/
        log_file = (
            tmp_path / "logs" / "dataset" / "2026-03"
            / "dataset_20260322_120000_aabbcc.jsonl"
        )
        assert log_file.exists()

    def test_creates_handlers_nonstandard_run_id_falls_back_to_current_month(
        self, tmp_path
    ):
        from datetime import datetime, timezone

        ctx = RunContext(pipeline="dataset", run_id="custom-run-id")
        config = {
            "logging": {
                "file": {"enabled": True, "path": str(tmp_path / "logs")},
            }
        }
        setup_logging(config, ctx)
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        log_file = (
            tmp_path / "logs" / "dataset" / month / "dataset_custom-run-id.jsonl"
        )
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


class TestLogDataVolume:
    def _vol_records(self, caplog):
        return [
            r for r in caplog.records
            if getattr(r, "event", None) == "data_volume"
        ]

    def test_pandas_dataframe(self, caplog):
        import pandas as pd

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "my_df", df)

        recs = self._vol_records(caplog)
        assert len(recs) == 1
        vol = recs[0].volume
        assert vol["name"] == "my_df"
        assert vol["kind"] == "pandas"
        assert vol["rows"] == 3
        assert vol["cols"] == 2
        assert vol["bytes"] > 0
        assert vol["deep"] is True

    def test_numpy_2d_array(self, caplog):
        import numpy as np

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        arr = np.zeros((5, 4), dtype=np.float64)
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "X", arr)

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "numpy"
        assert vol["rows"] == 5
        assert vol["cols"] == 4
        assert vol["bytes"] == 5 * 4 * 8
        assert vol["dtype"] == "float64"

    def test_numpy_1d_array(self, caplog):
        import numpy as np

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "y", np.arange(7))

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "numpy"
        assert vol["rows"] == 7
        assert vol["cols"] == 1

    def test_pyarrow_table(self, caplog):
        import pyarrow as pa

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        tbl = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "labels_table", tbl)

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "arrow"
        assert vol["rows"] == 3
        assert vol["cols"] == 2
        assert vol["bytes"] == tbl.nbytes

    def test_lgb_dataset_duck_typed_stub(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        class FakeLgbDataset:
            def num_data(self):
                return 1000

            def num_feature(self):
                return 42

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "ds_train", FakeLgbDataset())

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "lgb_dataset"
        assert vol["rows"] == 1000
        assert vol["cols"] == 42

    def test_file_path(self, caplog, tmp_path):
        from recsys_tfb.core.logging import log_data_volume

        f = tmp_path / "train.bin"
        f.write_bytes(b"\x00" * 2048)
        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "train.bin", str(f))

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "file"
        assert vol["bytes"] == 2048

    def test_dispatch_order_arrow_not_numpy(self, caplog):
        # pyarrow.Table has BOTH .nbytes and .num_rows; must dispatch as arrow.
        import pyarrow as pa

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        tbl = pa.table({"x": [1, 2]})
        assert hasattr(tbl, "nbytes")  # would match numpy branch if mis-ordered
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "t", tbl)

        assert self._vol_records(caplog)[0].volume["kind"] == "arrow"

    def test_none_obj_warns_does_not_raise(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "nothing", None)

        assert not self._vol_records(caplog)
        assert any(
            r.levelno == logging.WARNING and "obj is None" in r.getMessage()
            for r in caplog.records
        )

    def test_unsupported_type_warns(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "weird", object())

        assert not self._vol_records(caplog)
        assert any("unsupported kind" in r.getMessage() for r in caplog.records)

    def test_sizing_exception_is_swallowed(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        class Exploding:
            def memory_usage(self, deep=True):
                raise RuntimeError("boom")

            ndim = 2
            shape = (1, 1)

            def __len__(self):
                return 1

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "bad", Exploding())  # must NOT raise

        assert not self._vol_records(caplog)
        assert any(
            "log_data_volume failed" in r.getMessage()
            and getattr(r, "exception_type", None) == "RuntimeError"
            for r in caplog.records
        )

    def test_missing_file_path_warns(self, caplog, tmp_path):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "ghost", str(tmp_path / "nope.bin"))

        assert not self._vol_records(caplog)
        assert any("path missing" in r.getMessage() for r in caplog.records)
