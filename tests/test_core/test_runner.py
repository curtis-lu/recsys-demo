import logging

import pytest

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.core.runner import Runner


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
