"""Tests for backend-agnostic preprocessing helpers (preprocessing/_common.py)."""

import pytest

from recsys_tfb.preprocessing._common import _validate_columns, apply_feature_selection


def _meta():
    """Minimal preprocessor_metadata mirroring fit_preprocessor_metadata output."""
    return {
        "feature_columns": ["prod_name", "feat_a", "feat_b", "feat_c"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": {"a": 0, "b": 1}},
        "drop_columns": ["snap_date"],
    }


class TestValidateColumns:
    """D1 — the required-column guard every caller leans on for its error text.

    ``build_model_input`` calls this twice with two different contexts, and
    ADR-0005 turned the missing-item case from a silent fallback into a raise —
    so the message is now the whole user-facing product of that decision, not a
    debugging aid. It is tested here rather than through a caller because the
    caller tests can only reach it with the column sets *they* happen to build.
    """

    def test_missing_columns_raise_naming_all_of_them(self):
        with pytest.raises(ValueError) as ei:
            _validate_columns(["a", "b"], ["a", "b", "c", "d"], "some_node")
        msg = str(ei.value)
        # Sorted, so the text is stable across set-iteration order rather than
        # matching whatever order this particular run produced.
        assert "['c', 'd']" in msg
        assert "some_node" in msg

    def test_context_appears_so_two_call_sites_are_distinguishable(self):
        """The same function guards two column sets inside build_model_input.

        A shared message would let a ``pytest.raises(match=...)`` aimed at one
        be satisfied by the other — the false-green form ADR-0005 calls out by
        name.
        """
        with pytest.raises(ValueError, match=r"build_model_input keys: \['prod_name'\]"):
            _validate_columns(
                ["snap_date", "cust_id"],
                ["snap_date", "cust_id", "prod_name"],
                "build_model_input keys",
            )

    def test_extra_columns_are_not_an_error(self):
        """Only *missing* required columns fail; the frame may carry more.

        Every caller passes a frame wider than ``required`` (keys carry carry
        columns, ``dataset`` carries the whole joined width), so a subset check
        in the other direction would reject every real call.
        """
        assert _validate_columns(["a", "b", "c"], ["a"], "ctx") is None

    def test_exact_match_passes(self):
        assert _validate_columns(["a", "b"], ["a", "b"], "ctx") is None


class TestApplyFeatureSelection:
    def test_exclude_subsets_feature_columns_preserving_order(self):
        params = {"training": {"feature_selection": {"exclude": ["feat_b"]}}}
        view = apply_feature_selection(_meta(), params)
        assert view["feature_columns"] == ["prod_name", "feat_a", "feat_c"]

    def test_exclude_also_drops_from_categorical_columns(self):
        meta = _meta()
        meta["categorical_columns"] = ["prod_name", "feat_a"]
        params = {"training": {"feature_selection": {"exclude": ["feat_a"]}}}
        view = apply_feature_selection(meta, params)
        assert view["feature_columns"] == ["prod_name", "feat_b", "feat_c"]
        assert view["categorical_columns"] == ["prod_name"]

    def test_category_mappings_pass_through_untouched(self):
        params = {"training": {"feature_selection": {"exclude": ["feat_b"]}}}
        view = apply_feature_selection(_meta(), params)
        assert view["category_mappings"] == {"prod_name": {"a": 0, "b": 1}}
        assert view["drop_columns"] == ["snap_date"]

    def test_empty_selection_returns_input_unchanged(self):
        meta = _meta()
        # absent feature_selection
        assert apply_feature_selection(meta, {"training": {}}) is meta
        # present but empty exclude
        assert (
            apply_feature_selection(
                meta, {"training": {"feature_selection": {"exclude": []}}}
            )
            is meta
        )

    def test_original_metadata_not_mutated(self):
        meta = _meta()
        params = {"training": {"feature_selection": {"exclude": ["feat_b"]}}}
        apply_feature_selection(meta, params)
        assert meta["feature_columns"] == ["prod_name", "feat_a", "feat_b", "feat_c"]
        assert meta["categorical_columns"] == ["prod_name"]
