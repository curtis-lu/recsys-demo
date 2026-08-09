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


class TestNodeWrites:
    """``Node(writes=[...])`` — declared write targets (A1 / R1).

    The node is handed the catalog dataset OBJECT rather than loaded data, so
    it can drive its own partition write lifecycle: ``.save()`` per partition
    and ``.existing_partition_values()`` to decide what to skip on a resume.
    """

    @staticmethod
    def _catalog(**datasets):
        catalog = DataCatalog()
        for name, ds in datasets.items():
            catalog.add(name, ds)
        return catalog

    def test_write_target_is_handed_over_as_the_catalog_dataset_object(self):
        captured: dict = {}

        def node_fn(payload, sink):
            captured["payload"] = payload
            captured["sink"] = sink
            return {"ok": True}

        sentinel_ds = MemoryDataset(data="sentinel-data")
        catalog = self._catalog(
            payload=MemoryDataset(data={"hello": "world"}), sink=sentinel_ds,
        )

        Runner().run(
            Pipeline([
                Node(node_fn, inputs=["payload"], writes=["sink"], outputs="manifest"),
            ]),
            catalog,
        )

        assert captured["payload"] == {"hello": "world"}
        # Identity, not equality: a write-only proxy wrapping the dataset would
        # satisfy "can call .save()" but fail here -- and a resume needs to ask
        # the real dataset what is already there.
        assert captured["sink"] is sentinel_ds
        assert captured["sink"].load() == "sentinel-data"

    def test_write_targets_bind_by_keyword_not_position(self):
        """The parameter name must equal the dataset name.

        Positional binding would pin write targets to the tail of the
        signature, colliding with this repo's "append a new optional input
        last" convention.
        """
        captured: dict = {}

        # Write parameters in the REVERSE of their `writes` order: positional
        # binding would swap them (both are dataset objects, so nothing would
        # raise); only keyword binding gets this right.
        def node_fn(a, b, second_sink, first_sink):
            captured.update(a=a, b=b,
                            first_sink=first_sink, second_sink=second_sink)
            return "done"

        sink1, sink2 = MemoryDataset(data="one"), MemoryDataset(data="two")
        catalog = self._catalog(
            a=MemoryDataset(data="A"), b=MemoryDataset(data="B"),
            first_sink=sink1, second_sink=sink2,
        )

        Runner().run(
            Pipeline([
                Node(
                    node_fn,
                    inputs=["a", "b"],
                    writes=["first_sink", "second_sink"],
                    outputs="manifest",
                ),
            ]),
            catalog,
        )

        assert captured == {
            "a": "A", "b": "B", "first_sink": sink1, "second_sink": sink2,
        }

    def test_displacing_a_write_target_fails_loudly(self):
        """The regression this repo's own convention would otherwise cause.

        `log_experiment` documents "new optional inputs go last, because the
        Runner binds inputs positionally". Follow that on a writing node
        without moving the write parameter and the extra input lands in the
        write slot. Under POSITIONAL write binding that is silent -- the
        trailing `=None` absorbs the arity error and the node gets a dict
        where it expected a dataset. Keyword binding makes the same mistake
        raise before the node body runs.
        """
        ran = []

        def node_fn(model, sink, gate=None):
            ran.append(True)
            return "done"

        catalog = self._catalog(
            model=MemoryDataset(data="MODEL"),
            gate_manifest=MemoryDataset(data={"gate": "GATE"}),
            sink=MemoryDataset(data="hive"),
        )
        pipeline = Pipeline([
            Node(
                node_fn,
                inputs=["model", "gate_manifest"],  # appended, write not moved
                writes=["sink"],
                outputs="manifest",
            ),
        ])

        with pytest.raises(TypeError, match="multiple values for argument 'sink'"):
            Runner().run(pipeline, catalog)
        assert ran == []

    def test_write_target_must_be_a_registered_catalog_entry(self):
        ran = []

        def node_fn(payload, nowhere):
            ran.append(True)

        catalog = self._catalog(payload=MemoryDataset(data=1))
        pipeline = Pipeline([
            Node(node_fn, inputs=["payload"], writes=["nowhere"], outputs="m"),
        ])

        with pytest.raises(ValueError, match="declares a write to 'nowhere'"):
            Runner().run(pipeline, catalog)
        assert ran == []

    def test_a_node_output_is_not_enough_to_satisfy_a_write_target(self):
        """Order-blindness made concrete.

        `writes` carries no topological edge, so a producer declared later
        would still run later -- and the writer would be handed the `None`
        that `get_dataset` returns for an unregistered name. Rejecting this
        up front is what keeps that silent failure unreachable.
        """
        got: dict = {}

        catalog = self._catalog(seed=MemoryDataset(data="s"))
        pipeline = Pipeline([
            Node(lambda seed, shared: got.update(shared=shared) or "w",
                 inputs=["seed"], writes=["shared"], outputs="wout", name="W"),
            Node(lambda seed: "REAL", inputs=["seed"], outputs="shared",
                 name="P"),
        ])

        with pytest.raises(ValueError, match="not a registered catalog entry"):
            Runner().run(pipeline, catalog)
        assert got == {}

    def test_a_writer_can_read_back_and_its_save_persists(self):
        """The round trip a resume depends on: read what is there, then save.

        Note what this does NOT pin: a write target is structurally ineligible
        for eviction (that needs ``_auto_created``, which only holds names
        absent from the catalog, and a write target must be registered), so
        adding `writes` to the eviction loop changes nothing and no assertion
        here can catch it. The rule that makes it unreachable is the
        registration check, which `test_a_node_output_is_not_enough...` pins.
        """
        seen: dict = {}
        sink = MemoryDataset(data="before")

        catalog = self._catalog(seed=MemoryDataset(data="payload"), sink=sink)
        pipeline = Pipeline([
            Node(lambda seed: seed, inputs=["seed"], outputs="mid", name="produce"),
            Node(lambda mid, sink: seen.update(read_back=sink.load())
                 or sink.save("after") or "w",
                 inputs=["mid"], writes=["sink"], outputs="wout", name="write"),
        ])

        Runner().run(pipeline, catalog)

        assert seen == {"read_back": "before"}      # readable when it ran
        assert catalog.load("sink") == "after"      # and the save survived

    def test_at_prefix_is_no_longer_a_handle_sigil(self):
        """``writes`` replaces ``@``; a leftover ``@x`` must fail loudly.

        Two spellings for one thing is the shape this ticket removes, so the
        old one has to stop working -- visibly, and with a message that names
        the replacement. Without the hint, the two most intuitive "fixes"
        (register a catalog entry literally named ``@sink``, or drop the
        ``@``) both silently load the whole table into the driver instead.
        """
        catalog = self._catalog(
            payload=MemoryDataset(data=1), sink=MemoryDataset(data=2),
        )
        pipeline = Pipeline([
            Node(lambda *a: None, inputs=["payload", "@sink"], outputs="m", name="n"),
        ])

        with pytest.raises(ValueError, match=r"requires input '@sink'") as exc:
            Runner().run(pipeline, catalog)
        assert "Node(writes=" in str(exc.value)
        assert "#186" in str(exc.value)

    def test_write_names_are_logged_on_every_node_event(self, caplog):
        """F2: the Runner's structured log is where a node's I/O surfaces.

        node_failed included: the phase split exists to locate a node killed
        during its write, so the failure path has to say what it was writing.
        """
        catalog = self._catalog(
            payload=MemoryDataset(data=1), sink=MemoryDataset(data=2),
        )

        def boom(payload, sink):
            raise RuntimeError("kaboom")

        ok = Pipeline([
            Node(lambda payload, sink: "m", inputs=["payload"], writes=["sink"],
                 outputs="manifest", name="ok"),
        ])
        bad = Pipeline([
            Node(boom, inputs=["payload"], writes=["sink"], outputs="manifest",
                 name="bad"),
        ])

        with caplog.at_level(logging.INFO):
            Runner().run(ok, catalog)
            with pytest.raises(RuntimeError):
                Runner().run(bad, catalog)

        events = {}
        for r in caplog.records:
            if getattr(r, "event", None):
                events.setdefault(r.event, r)
        assert events["node_started"].write_names == ["sink"]
        assert events["node_completed"].write_names == ["sink"]
        assert events["node_failed"].write_names == ["sink"]
