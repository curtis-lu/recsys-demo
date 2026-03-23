import logging

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
