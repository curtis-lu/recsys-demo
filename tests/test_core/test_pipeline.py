import pytest

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def identity(x):
    return x


def add_one(x):
    return x + 1


def source():
    return 1


def take_value(value):
    return value


class TestPipeline:
    def test_linear_chain(self):
        node_a = Node(func=source, outputs=["x"], name="A")
        node_b = Node(func=identity, inputs=["x"], outputs=["y"], name="B")
        pipe = Pipeline([node_b, node_a])  # intentionally reversed
        names = [n.name for n in pipe.nodes]
        assert names == ["A", "B"]

    def test_dict_inputs_order_by_the_dataset_name(self):
        """The edge runs through the dataset name ``x``, not the parameter
        name ``value``; declared consumer-first, so only the edge sorts it."""
        node_a = Node(func=source, outputs=["x"], name="A")
        node_b = Node(func=take_value, inputs={"value": "x"}, outputs=["y"], name="B")
        pipe = Pipeline([node_b, node_a])
        assert [n.name for n in pipe.nodes] == ["A", "B"]

    def test_independent_nodes(self):
        node_a = Node(func=source, outputs=["x"], name="A")
        node_b = Node(func=source, outputs=["y"], name="B")
        pipe = Pipeline([node_a, node_b])
        assert len(pipe.nodes) == 2

    def test_circular_dependency(self):
        node_a = Node(func=identity, inputs=["y"], outputs=["x"], name="A")
        node_b = Node(func=identity, inputs=["x"], outputs=["y"], name="B")
        with pytest.raises(ValueError, match="circular"):
            Pipeline([node_a, node_b])

    def test_only_nodes_with_outputs(self):
        node_a = Node(func=source, outputs=["x"], name="A")
        node_b = Node(func=identity, inputs=["x"], outputs=["y"], name="B")
        node_c = Node(func=source, outputs=["z"], name="C")
        pipe = Pipeline([node_a, node_b, node_c])
        filtered = pipe.only_nodes_with_outputs(["y"])
        names = [n.name for n in filtered.nodes]
        assert "A" in names
        assert "B" in names
        assert "C" not in names

    def test_only_nodes_with_outputs_follows_write_targets(self):
        """Counterpart of _slice_with_expansion, so it must agree with it.

        B writes `x` and nothing else reads it, so counting only `inputs`
        would drop A -- and the two "find my upstream" APIs would answer
        differently for the same pipeline.
        """
        node_a = Node(func=source, outputs=["x"], name="A")
        node_b = Node(func=source, writes=["x"], outputs=["y"], name="B")
        filtered = Pipeline([node_a, node_b]).only_nodes_with_outputs(["y"])
        assert sorted(n.name for n in filtered.nodes) == ["A", "B"]

    def test_inputs_property(self):
        node = Node(func=identity, inputs=["external"], outputs=["result"])
        pipe = Pipeline([node])
        assert pipe.inputs == {"external"}

    def test_inputs_property_excludes_write_targets(self):
        """A write target is not an input; the Runner validates it separately.

        `sink` has no reader here, so if `writes` leaked into this property
        it would show up -- which is what makes this assertion able to fail.
        """
        node = Node(func=identity, inputs=["external"], writes=["sink"],
                    outputs=["result"])
        assert Pipeline([node]).inputs == {"external"}

    def test_outputs_property(self):
        node = Node(func=source, outputs=["x", "y"])
        pipe = Pipeline([node])
        assert pipe.outputs == {"x", "y"}
