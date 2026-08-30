"""Which feature columns the model wants — the shared authority helper.

Lives under ``tests/test_models`` because the module under test moved out of
``pipelines/inference/steps/`` once training diagnostics became a second
consumer (ADR-0014 decision 7).
"""

import pytest

from recsys_tfb.models.feature_view import (
    model_feature_columns,
    model_feature_view,
    require_ordered_subsequence,
)


class DeclaringModel:
    """Declares its feature names, the way every fitted adapter in this repo does."""

    def __init__(self, names):
        self._names = names

    def feature_names(self):
        return self._names


class SilentModel:
    """Declares nothing — the doubles-only branch (see the module docstring)."""

    def feature_names(self):
        return None


def _preprocessor():
    return {
        "feature_columns": ["a", "b", "c"],
        "categorical_columns": ["c"],
        "category_mappings": {"c": ["x", "y"]},
    }


class TestRequireOrderedSubsequence:
    def test_a_strict_subsequence_passes(self):
        require_ordered_subsequence(["a", "c"], ["a", "b", "c"])

    def test_permuted_same_columns_raise(self):
        with pytest.raises(ValueError, match="order-preserving subsequence"):
            require_ordered_subsequence(["c", "a"], ["a", "b", "c"])

    def test_model_column_absent_from_the_artifact_raises(self):
        with pytest.raises(ValueError, match="stuck at 'd'"):
            require_ordered_subsequence(["a", "d"], ["a", "b", "c"])


class TestModelFeatureColumns:
    def test_the_model_decides_which_columns_and_in_what_order(self):
        cols = model_feature_columns(DeclaringModel(["a", "c"]), _preprocessor())
        assert cols == ["a", "c"]

    def test_no_declaration_falls_back_to_the_artifact(self):
        cols = model_feature_columns(SilentModel(), _preprocessor())
        assert cols == ["a", "b", "c"]


class TestModelFeatureView:
    def test_feature_columns_come_from_the_model_not_the_artifact(self):
        """The point of the seam: a narrower model narrows the view.

        Deriving the view from config instead (``apply_feature_selection`` with an
        empty ``training.feature_selection.exclude``) would return all three
        columns here — that is the mutation this test is here to catch.
        """
        view = model_feature_view(DeclaringModel(["a", "c"]), _preprocessor())
        assert view["feature_columns"] == ["a", "c"]

    def test_encoding_keys_pass_through_from_the_artifact(self):
        """The artifact stays the authority on *how* each column is encoded."""
        preprocessor = _preprocessor()
        view = model_feature_view(DeclaringModel(["a", "c"]), preprocessor)
        assert view["category_mappings"] == preprocessor["category_mappings"]
        assert view["categorical_columns"] == preprocessor["categorical_columns"]

    def test_the_artifact_is_not_mutated(self):
        preprocessor = _preprocessor()
        model_feature_view(DeclaringModel(["a", "c"]), preprocessor)
        assert preprocessor["feature_columns"] == ["a", "b", "c"]

    def test_a_stale_artifact_raises_rather_than_realigning(self):
        with pytest.raises(ValueError, match="order-preserving subsequence"):
            model_feature_view(DeclaringModel(["a", "d"]), _preprocessor())
