"""Tests for backend-agnostic preprocessing helpers (preprocessing/_common.py)."""

from recsys_tfb.preprocessing._common import apply_feature_selection


def _meta():
    """Minimal preprocessor_metadata mirroring fit_preprocessor_metadata output."""
    return {
        "feature_columns": ["prod_name", "feat_a", "feat_b", "feat_c"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": {"a": 0, "b": 1}},
        "drop_columns": ["snap_date"],
    }


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
