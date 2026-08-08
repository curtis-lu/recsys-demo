import json
import logging
import time

import pytest

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.core.runner import Runner


def identity(x):
    return x


def double(x):
    return x * 2


def add(a, b):
    return a + b


def failing_func(x):
    raise RuntimeError("intentional failure")


# Long enough to dominate the timer's resolution, short enough to keep the
# suite fast. Phase assertions compare against this, never against a literal.
SLOW = 0.05


def _slow_double(x):
    time.sleep(SLOW)
    return x * 2


class _SlowSaveDataset(MemoryDataset):
    """Stands in for a Hive table: cheap to build a plan for, slow to write."""

    def __init__(self, delay: float, data=None):
        super().__init__(data=data)
        self._delay = delay

    def save(self, data) -> None:
        time.sleep(self._delay)
        super().save(data)


class _SlowLoadDataset(MemoryDataset):
    def __init__(self, delay: float, data=None):
        super().__init__(data=data)
        self._delay = delay

    def load(self):
        time.sleep(self._delay)
        return super().load()


class _FailingSaveDataset(MemoryDataset):
    """A write that dies partway — the shape of an OOM during insertInto."""

    def __init__(self, delay: float):
        super().__init__()
        self._delay = delay

    def save(self, data) -> None:
        time.sleep(self._delay)
        raise RuntimeError("write blew up")


class TestRunner:
    def test_successful_run(self):
        catalog = DataCatalog()
        catalog.add("input_data", MemoryDataset(data=5))

        node = Node(func=double, inputs=["input_data"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        runner.run(pipe, catalog)

        assert catalog.load("result") == 10

    def test_two_node_pipeline(self):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=3))
        catalog.add("y", MemoryDataset(data=7))

        node_a = Node(func=add, inputs=["x", "y"], outputs=["sum"], name="add")
        node_b = Node(func=double, inputs=["sum"], outputs=["doubled"], name="double")
        pipe = Pipeline([node_b, node_a])

        runner = Runner()
        runner.run(pipe, catalog)

        assert catalog.load("doubled") == 20

    def test_missing_input(self):
        catalog = DataCatalog()
        node = Node(func=double, inputs=["missing"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        with pytest.raises(ValueError, match="missing"):
            runner.run(pipe, catalog)

    def test_node_failure(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=failing_func, inputs=["x"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        with pytest.raises(RuntimeError, match="intentional failure"):
            with caplog.at_level(logging.INFO):
                runner.run(pipe, catalog)
        assert "failed" in caplog.text

    def test_timing_logs(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=double, inputs=["x"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(pipe, catalog)

        assert "completed in" in caplog.text

    def test_pipeline_started_log(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=double, inputs=["x"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(pipe, catalog)

        assert "Pipeline started" in caplog.text
        assert "Pipeline completed" in caplog.text

    def test_node_completed_log(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=double, inputs=["x"], outputs=["result"], name="double_node")
        pipe = Pipeline([node])

        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(pipe, catalog)

        assert "double_node completed" in caplog.text

    def test_node_failed_log(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=failing_func, inputs=["x"], outputs=["result"], name="fail_node")
        pipe = Pipeline([node])

        runner = Runner()
        with pytest.raises(RuntimeError):
            with caplog.at_level(logging.ERROR):
                runner.run(pipe, catalog)

        assert "fail_node" in caplog.text
        assert "Pipeline failed" in caplog.text

    def test_memory_dataset_released_after_last_consumer(self):
        """Pipeline-produced MemoryDataset 'mid' should be released after its last consumer."""
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=5))

        node_a = Node(func=double, inputs=["x"], outputs=["mid"], name="A")
        node_b = Node(func=double, inputs=["mid"], outputs=["out"], name="B")
        pipe = Pipeline([node_a, node_b])

        runner = Runner()
        runner.run(pipe, catalog)

        # "x" is an external input (not produced by the pipeline), so NOT released
        ds_x = catalog.get_dataset("x")
        assert ds_x._data is not None

        # "mid" is a pipeline output (produced by node A), released after node B
        ds_mid = catalog.get_dataset("mid")
        assert ds_mid._data is None

        # "out" has no consumer, so it stays
        assert catalog.load("out") == 20

    def test_external_input_not_released(self):
        """External inputs (not produced by the pipeline) should never be released."""
        catalog = DataCatalog()
        catalog.add("ext", MemoryDataset(data=99))

        node = Node(func=double, inputs=["ext"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        runner.run(pipe, catalog)

        # "ext" is external, so not released
        assert catalog.load("ext") == 99

    def test_shared_intermediate_not_released_early(self):
        """A pipeline-produced dataset consumed by multiple nodes is not released until the last consumer."""
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=3))

        # node_a produces "mid", node_b and node_c both consume "mid"
        node_a = Node(func=double, inputs=["x"], outputs=["mid"], name="A")
        node_b = Node(func=identity, inputs=["mid"], outputs=["out_b"], name="B")
        node_c = Node(func=identity, inputs=["mid"], outputs=["out_c"], name="C")
        pipe = Pipeline([node_a, node_b, node_c])

        runner = Runner()
        runner.run(pipe, catalog)

        # "mid" is pipeline-produced, released after last consumer (node C)
        ds_mid = catalog.get_dataset("mid")
        assert ds_mid._data is None

        # Both outputs should still exist
        assert catalog.load("out_b") == 6
        assert catalog.load("out_c") == 6

    def test_non_memory_dataset_not_released(self, tmp_path):
        """ParquetDataset and other non-MemoryDataset types should not be released."""
        from recsys_tfb.io.pickle_dataset import PickleDataset

        filepath = str(tmp_path / "input.pkl")
        ds = PickleDataset(filepath=filepath)
        ds.save(10)

        catalog = DataCatalog()
        catalog.add("x", ds)

        node = Node(func=double, inputs=["x"], outputs=["result"])
        pipe = Pipeline([node])

        runner = Runner()
        runner.run(pipe, catalog)

        # PickleDataset should still be loadable (not released)
        assert catalog.load("x") == 10

    def test_dataset_released_log_event(self, caplog):
        """Verify dataset_released log event is emitted for pipeline-produced datasets."""
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=5))

        node_a = Node(func=double, inputs=["x"], outputs=["mid"], name="A")
        node_b = Node(func=double, inputs=["mid"], outputs=["out"], name="B")
        pipe = Pipeline([node_a, node_b])

        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(pipe, catalog)

        # "x" is external, NOT released; "mid" is pipeline-produced, released
        assert "Released dataset: x" not in caplog.text
        assert "Released dataset: mid" in caplog.text


class TestNodePhaseTiming:
    """A node's wall clock is charged to the phase that actually spent it.

    Spark is lazy: a node function over DataFrames builds a plan in
    milliseconds and the entire computation runs later, inside
    ``catalog.save()``. One ``duration_seconds`` covering both cannot tell
    "this node is slow" from "this node's write is slow" — and that is the
    only distinction that locates a bottleneck.
    """

    @staticmethod
    def _node_completed(caplog):
        records = [
            r for r in caplog.records
            if getattr(r, "event", None) == "node_completed"
        ]
        assert len(records) == 1, f"expected one node_completed, got {len(records)}"
        return records[0]

    def test_slow_save_is_charged_to_save_not_func(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))
        catalog.add("result", _SlowSaveDataset(delay=SLOW))

        node = Node(func=double, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(Pipeline([node]), catalog)

        rec = self._node_completed(caplog)
        assert rec.save_seconds >= SLOW
        assert rec.func_seconds < SLOW

    def test_slow_func_is_charged_to_func_not_save(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=_slow_double, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(Pipeline([node]), catalog)

        rec = self._node_completed(caplog)
        assert rec.func_seconds >= SLOW
        assert rec.save_seconds < SLOW

    def test_slow_load_is_charged_to_load_not_func(self, caplog):
        catalog = DataCatalog()
        catalog.add("x", _SlowLoadDataset(delay=SLOW, data=1))

        node = Node(func=double, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(Pipeline([node]), catalog)

        rec = self._node_completed(caplog)
        assert rec.load_seconds >= SLOW
        assert rec.func_seconds < SLOW

    @staticmethod
    def _node_failed(caplog):
        records = [
            r for r in caplog.records
            if getattr(r, "event", None) == "node_failed"
        ]
        assert len(records) == 1, f"expected one node_failed, got {len(records)}"
        return records[0]

    def test_failure_in_save_is_charged_to_save(self, caplog):
        """The case the split exists for: a write that dies partway."""
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))
        catalog.add("result", _FailingSaveDataset(delay=SLOW))

        node = Node(func=double, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with pytest.raises(RuntimeError, match="write blew up"):
            with caplog.at_level(logging.INFO):
                runner.run(Pipeline([node]), catalog)

        rec = self._node_failed(caplog)
        assert rec.save_seconds >= SLOW
        assert rec.func_seconds < SLOW

    def test_failure_in_func_reports_no_save_phase_at_all(self, caplog):
        """An unreached phase is absent, not zero.

        ``save_seconds: 0.0`` on a node that died in its function would read
        as "the save was instant" rather than "the save never ran".
        """
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))

        node = Node(func=failing_func, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with pytest.raises(RuntimeError, match="intentional failure"):
            with caplog.at_level(logging.INFO):
                runner.run(Pipeline([node]), catalog)

        rec = self._node_failed(caplog)
        assert hasattr(rec, "load_seconds")
        assert hasattr(rec, "func_seconds")
        assert not hasattr(rec, "save_seconds")

    def test_phase_timings_reach_the_jsonl_file(self, tmp_path):
        """The phases must survive the trip to the file, not just to caplog.

        ``JsonFormatter`` copies ``extra`` fields through a fixed whitelist, so
        a field the Runner sets but the formatter does not know about shows up
        on the console and is silently missing from the file. Production reads
        the file.
        """
        from recsys_tfb.core.logging import RunContext, setup_logging

        root = logging.getLogger()
        saved_handlers, saved_level = root.handlers[:], root.level
        try:
            setup_logging(
                {"logging": {
                    "level": "INFO",
                    "console": False,
                    "file": {"enabled": True, "path": str(tmp_path)},
                }},
                RunContext(run_id="20260808_120000_abcdef", pipeline="dataset"),
            )

            catalog = DataCatalog()
            catalog.add("x", MemoryDataset(data=1))
            node = Node(func=double, inputs=["x"], outputs=["result"], name="n")
            Runner().run(Pipeline([node]), catalog)

            for handler in root.handlers:
                handler.flush()
            written = sorted((tmp_path / "dataset" / "2026-08").glob("*.jsonl"))
            assert written, "setup_logging wrote no JSONL file"
            events = [
                json.loads(line)
                for line in written[0].read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        finally:
            for handler in root.handlers:
                handler.close()
            root.handlers[:] = saved_handlers
            root.setLevel(saved_level)

        completed = [e for e in events if e.get("event") == "node_completed"]
        assert len(completed) == 1
        for field in ("load_seconds", "func_seconds", "save_seconds"):
            assert field in completed[0], f"{field} never reached the file"

    def test_phases_account_for_the_whole_node(self, caplog):
        """The three phases partition the node duration — nothing unattributed."""
        catalog = DataCatalog()
        catalog.add("x", MemoryDataset(data=1))
        catalog.add("result", _SlowSaveDataset(delay=SLOW))

        node = Node(func=_slow_double, inputs=["x"], outputs=["result"], name="n")
        runner = Runner()
        with caplog.at_level(logging.INFO):
            runner.run(Pipeline([node]), catalog)

        rec = self._node_completed(caplog)
        phases = rec.load_seconds + rec.func_seconds + rec.save_seconds
        assert phases <= rec.duration_seconds + 0.01
        assert phases >= rec.duration_seconds - 0.05


def test_runner_resolves_at_prefix_input_to_dataset_handle():
    """An input name starting with '@' should be resolved to the catalog
    dataset INSTANCE (not the loaded data), so write-target nodes can call
    `.save()` per-batch.
    """
    from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
    from recsys_tfb.core.node import Node
    from recsys_tfb.core.pipeline import Pipeline
    from recsys_tfb.core.runner import Runner

    captured: dict = {}

    def node_fn(payload, write_ds):
        captured["payload"] = payload
        captured["write_ds"] = write_ds
        return {"ok": True}

    catalog = DataCatalog()
    catalog.add("payload", MemoryDataset(data={"hello": "world"}))
    sentinel_ds = MemoryDataset(data="sentinel-data")
    catalog.add("sink", sentinel_ds)

    pipeline = Pipeline([
        Node(node_fn, inputs=["payload", "@sink"], outputs="manifest"),
    ])
    Runner().run(pipeline, catalog)

    assert captured["payload"] == {"hello": "world"}
    # @sink resolves to the dataset HANDLE, not its data
    assert captured["write_ds"] is sentinel_ds
