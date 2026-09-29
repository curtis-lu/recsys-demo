import pytest

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


def two_params(x, y):
    return x, y


def writer(x, sink):
    return x


class TestNodeDictInputs:
    """``inputs={parameter: dataset}`` (ADR-0030 decision 8)."""

    def test_inputs_stays_the_dataset_names_in_dict_order(self):
        """Sort, slicing and the catalog check read ``inputs``; they need the
        dataset names, not the parameter names."""
        node = Node(func=two_params, inputs={"x": "ds_b", "y": "ds_a"}, outputs="c")
        assert node.inputs == ["ds_b", "ds_a"]
        assert node.keyword_inputs == {"x": "ds_b", "y": "ds_a"}

    def test_any_mapping_binds_by_name(self):
        """Not only ``dict``: a read-only mapping read as a list would be the
        keys again, bound by position."""
        from types import MappingProxyType

        node = Node(func=two_params,
                    inputs=MappingProxyType({"x": "dx", "y": "ds"}), outputs="c")
        assert node.inputs == ["dx", "ds"]
        assert node.keyword_inputs == {"x": "dx", "y": "ds"}

    @pytest.mark.parametrize("inputs", [
        {"x": "a", "z": "b"},  # a misspelt parameter
        {"x": "a"},            # a parameter left unwired
    ])
    def test_a_dict_that_does_not_fit_the_signature_fails_at_construction(self, inputs):
        """Kedro checks this when the node is built. Bound by name, a slip
        would otherwise surface only when the Runner calls the node — for the
        last node of a training run, hours later."""
        with pytest.raises(TypeError, match="do not fit the signature of two_params"):
            Node(func=two_params, inputs=inputs, outputs="c")

    def test_a_key_that_is_also_a_write_target_fails_at_construction(self):
        """The Runner passes write targets by keyword under their own names,
        so a key naming the same parameter would be passed twice."""
        with pytest.raises(TypeError, match=r"\['sink'\] .* write target"):
            Node(func=writer, inputs={"x": "a", "sink": "b"}, writes=["sink"],
                 outputs="c")

    def test_a_list_is_not_held_to_the_signature(self):
        """Unchanged for lists: arity is still the Runner's call to find."""
        assert Node(func=dummy_func, inputs=["a", "b"], outputs="c").inputs == ["a", "b"]

    def test_a_list_keeps_binding_by_position(self):
        assert Node(func=dummy_func, inputs=["a"], outputs="c").keyword_inputs is None

    def test_a6_sees_the_dataset_name_not_the_parameter_name(self):
        with pytest.raises(ValueError, match=r"\['ds'\]"):
            Node(func=dummy_func, inputs={"x": "ds"}, outputs="ds")

    @pytest.mark.parametrize("argument", ["outputs", "writes"])
    def test_a_dict_anywhere_else_is_refused(self, argument):
        """Before dict inputs meant anything, a dict was read as its keys,
        silently. Only ``inputs`` gives a dict a meaning; elsewhere that old
        reading would still be silent, so it raises instead."""
        with pytest.raises(TypeError, match=f"`{argument}` takes a name"):
            Node(func=dummy_func, inputs="a", **{argument: {"k": "v"}})


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


class TestNodeConstructionGuards:
    """A5 / A6 enforced at construction time, not only by the audit test.

    Both constraints were previously checked only by the AST scan in
    ``test_architecture_constraints.py`` -- i.e. discovered when the suite
    runs, not when the node is written. The audit test stays as a second
    line (it also scans the ``pipeline.py``-level registries, which no
    constructor can see), but a malformed node now fails where it is built.
    """

    def test_a5_rejects_a_node_with_no_input_output_or_write(self):
        with pytest.raises(ValueError, match="at least one"):
            Node(func=dummy_func)

    def test_a5_rejects_empty_containers_not_just_none(self):
        """``[]`` is as empty as ``None``; the check is on the normalized form."""
        with pytest.raises(ValueError, match="at least one"):
            Node(func=dummy_func, inputs=[], outputs=[], writes=[])

    def test_a5_accepts_a_zero_output_node_that_has_inputs(self):
        """``outputs=None`` is how this repo spells a side-effect node (A7).

        The audit test's first cut treated this form as unreadable and
        skipped it, which made A5 a no-op. Pinning it here so the
        constructor cannot repeat that: having inputs is enough.
        """
        node = Node(func=dummy_func, inputs=["a"], outputs=None)
        assert node.outputs == []

    def test_a5_accepts_a_node_that_only_declares_writes(self):
        """A node that only writes still has an effect (see A5 in the doc)."""
        assert Node(func=dummy_func, writes="sink").writes == ["sink"]

    def test_a5_error_names_the_node(self):
        with pytest.raises(ValueError, match="my_node"):
            Node(func=dummy_func, name="my_node")

    def test_a6_rejects_an_input_name_reused_as_output(self):
        with pytest.raises(ValueError, match="'a'"):
            Node(func=dummy_func, inputs=["a", "b"], outputs=["a"])

    def test_a6_rejects_a_write_target_reused_as_output(self):
        """``writes`` counts too -- A6 compares ``inputs | writes`` to outputs."""
        with pytest.raises(ValueError, match="'sink'"):
            Node(func=dummy_func, inputs=["a"], outputs=["sink"], writes=["sink"])

    def test_a6_error_names_the_node_and_the_colliding_name(self):
        with pytest.raises(ValueError, match="my_node"):
            Node(func=dummy_func, inputs="a", outputs="a", name="my_node")
