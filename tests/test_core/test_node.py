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
