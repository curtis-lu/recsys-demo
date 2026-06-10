"""Tests for Pipeline.slice_from / slice_only — resume-oriented forward slicing.

can_load is a plain callable; no catalog/Spark involved. Node funcs are inert
lambdas — slicing is pure DAG analysis and never calls them.
"""

import pytest

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def _n(name, inputs=None, outputs=None):
    return Node(func=lambda *a: None, inputs=inputs, outputs=outputs, name=name)


def _chain():
    """A -> a -> B -> b -> C -> c -> D (linear)."""
    return Pipeline([
        _n("A", outputs="a"),
        _n("B", inputs=["a"], outputs="b"),
        _n("C", inputs=["b"], outputs="c"),
        _n("D", inputs=["c"], outputs="d"),
    ])


ALL_LOADABLE = lambda name: True
NONE_LOADABLE = lambda name: False


class TestSliceFrom:
    def test_basic_skip_upstream_when_loadable(self):
        pipe, plan = _chain().slice_from("C", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["C", "D"]
        assert plan.mode == "from"
        assert plan.requested == ("C", "D")
        assert plan.auto_included == {}
        assert plan.skipped == ("A", "B")
        assert plan.skipped_side_effect == ()

    def test_expansion_one_level(self):
        # b not loadable -> pull B back; a loadable -> stop, A stays skipped.
        can_load = lambda name: name != "b"
        pipe, plan = _chain().slice_from("C", can_load)
        assert [n.name for n in pipe.nodes] == ["B", "C", "D"]
        assert plan.auto_included == {"B": ("b",)}
        assert plan.skipped == ("A",)

    def test_expansion_recursive(self):
        # b and a both missing -> pull B then A.
        can_load = lambda name: name not in {"a", "b"}
        pipe, plan = _chain().slice_from("C", can_load)
        assert [n.name for n in pipe.nodes] == ["A", "B", "C", "D"]
        assert plan.auto_included == {"B": ("b",), "A": ("a",)}
        assert plan.skipped == ()

    def test_worst_case_degrades_to_full_pipeline(self):
        pipe, plan = _chain().slice_from("D", NONE_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["A", "B", "C", "D"]

    def test_per_dataset_not_per_node(self):
        # M outputs m1 (loadable) and m2 (not). X needs only m1 -> M skipped.
        nodes = [
            _n("M", outputs=["m1", "m2"]),
            _n("X", inputs=["m1"], outputs="x"),
        ]
        can_load = lambda name: name == "m1"
        pipe, plan = Pipeline(nodes).slice_from("X", can_load)
        assert [n.name for n in pipe.nodes] == ["X"]
        assert plan.skipped == ("M",)

    def test_per_dataset_pulls_when_memory_output_needed(self):
        nodes = [
            _n("M", outputs=["m1", "m2"]),
            _n("Y", inputs=["m2"], outputs="y"),
        ]
        can_load = lambda name: name == "m1"
        pipe, plan = Pipeline(nodes).slice_from("Y", can_load)
        assert [n.name for n in pipe.nodes] == ["M", "Y"]
        assert plan.auto_included == {"M": ("m2",)}

    def test_side_effect_node_never_pulled_and_reported(self):
        nodes = [
            _n("guard", inputs=["src"], outputs=None),   # zero-output gate
            _n("A", inputs=["src"], outputs="a"),
            _n("B", inputs=["a"], outputs="b"),
        ]
        pipe, plan = Pipeline(nodes).slice_from("B", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["B"]
        assert plan.skipped == ("A",)
        assert plan.skipped_side_effect == ("guard",)

    def test_topological_position_includes_parallel_branch(self):
        # B independent of A; slice_from("B") keeps C (which needs a, loadable).
        nodes = [
            _n("A", outputs="a"),
            _n("B", outputs="b"),
            _n("C", inputs=["a", "b"], outputs="c"),
        ]
        pipe, plan = Pipeline(nodes).slice_from("B", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["B", "C"]
        assert plan.skipped == ("A",)

    def test_at_handle_input_stripped(self):
        # "@x" resolves dataset name x for can_load / producer lookup.
        nodes = [
            _n("P", outputs="x"),
            _n("Q", inputs=["@x"], outputs="q"),
        ]
        can_load = lambda name: name != "x"
        pipe, plan = Pipeline(nodes).slice_from("Q", can_load)
        assert [n.name for n in pipe.nodes] == ["P", "Q"]
        assert plan.auto_included == {"P": ("x",)}

    def test_unknown_node_raises_with_available_names(self):
        with pytest.raises(ValueError, match="Unknown node 'nope'"):
            _chain().slice_from("nope", ALL_LOADABLE)
        with pytest.raises(ValueError, match="A, B, C, D"):
            _chain().slice_from("nope", ALL_LOADABLE)

    def test_external_pipeline_input_never_expands(self):
        # "src" has no producer in the pipeline -> ignored even if not loadable.
        nodes = [_n("A", inputs=["src"], outputs="a")]
        pipe, plan = Pipeline(nodes).slice_from("A", NONE_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["A"]
        assert plan.auto_included == {}


class TestSliceOnly:
    def test_single_node_when_inputs_loadable(self):
        pipe, plan = _chain().slice_only("C", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["C"]
        assert plan.mode == "only"
        assert plan.requested == ("C",)
        assert plan.skipped == ("A", "B", "D")

    def test_single_node_with_expansion(self):
        can_load = lambda name: name != "b"
        pipe, plan = _chain().slice_only("C", can_load)
        assert [n.name for n in pipe.nodes] == ["B", "C"]
        assert plan.auto_included == {"B": ("b",)}

    def test_unknown_node_raises(self):
        with pytest.raises(ValueError, match="Unknown node"):
            _chain().slice_only("nope", ALL_LOADABLE)
