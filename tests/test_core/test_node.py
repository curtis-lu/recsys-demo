from recsys_tfb.core.node import Node


def dummy_func(x):
    return x


class TestNode:
    def test_basic_creation(self):
        node = Node(func=dummy_func, inputs=["a", "b"], outputs=["c"])
        assert node.func is dummy_func
        assert node.inputs == ["a", "b"]
        assert node.outputs == ["c"]
        assert node.name == "dummy_func"

    def test_no_inputs(self):
        node = Node(func=dummy_func, inputs=None, outputs=["c"])
        assert node.inputs == []

    def test_string_input_output(self):
        node = Node(func=dummy_func, inputs="a", outputs="b")
        assert node.inputs == ["a"]
        assert node.outputs == ["b"]

    def test_custom_name(self):
        node = Node(func=dummy_func, inputs=["a"], outputs=["b"], name="my_node")
        assert node.name == "my_node"

    def test_repr(self):
        node = Node(func=dummy_func, inputs=["a"], outputs=["b"])
        r = repr(node)
        assert "dummy_func" in r
        assert "a" in r
        assert "b" in r


class TestNodeWrites:
    """``writes`` declares the datasets a node saves to itself (A1 / R1)."""

    def test_writes_defaults_to_empty(self):
        node = Node(func=dummy_func, inputs=["a"], outputs=["b"])
        assert node.writes == []

    def test_writes_normalizes_like_inputs(self):
        assert Node(func=dummy_func, outputs="b", writes="sink").writes == ["sink"]
        assert Node(
            func=dummy_func, outputs="b", writes=["s1", "s2"],
        ).writes == ["s1", "s2"]

    def test_repr_names_the_write_targets(self):
        """The whole point of the parameter: side effects visible on the node.

        Asserting the rendered ``writes=[...]`` fragment, not just the bare
        name -- the name alone would also match a node that merely reads it.
        """
        node = Node(func=dummy_func, inputs=["a"], outputs=["b"], writes=["sink"])
        assert "writes=['sink']" in repr(node)

    def test_repr_stays_quiet_when_a_node_writes_nothing(self):
        assert "writes" not in repr(Node(func=dummy_func, inputs=["a"], outputs=["b"]))
