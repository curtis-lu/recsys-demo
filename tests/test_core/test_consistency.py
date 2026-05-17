"""Tests for recsys_tfb.core.consistency."""

import pytest

from recsys_tfb.core.consistency import (
    ConsistencyError,
    ConfigConsistencyError,
    DataConsistencyError,
)


class TestExceptionHierarchy:
    def test_consistency_error_is_valueerror(self):
        assert issubclass(ConsistencyError, ValueError)

    def test_config_error_is_consistency_error(self):
        assert issubclass(ConfigConsistencyError, ConsistencyError)

    def test_data_error_is_consistency_error(self):
        assert issubclass(DataConsistencyError, ConsistencyError)


from recsys_tfb.core.consistency import resolved_item_values


class TestResolvedItemValues:
    def _params(self, **over):
        p = {
            "schema": {
                "columns": {"item": "prod_name"},
                "categorical_values": {"prod_name": ["b", "a", "c"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        p.update(over)
        return p

    def test_returns_sorted_declared_values(self):
        assert resolved_item_values(self._params()) == ["a", "b", "c"]

    def test_respects_custom_item_name(self):
        p = {
            "schema": {
                "columns": {"item": "channel_name"},
                "categorical_values": {"channel_name": ["sms", "app"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["channel_name"]}},
        }
        assert resolved_item_values(p) == ["app", "sms"]

    def test_item_declared_categorical_but_no_values_raises(self):
        p = self._params()
        del p["schema"]["categorical_values"]["prod_name"]
        with pytest.raises(ConfigConsistencyError, match=r"schema\.categorical_values\.prod_name"):
            resolved_item_values(p)


from recsys_tfb.core.consistency import config_role_conflicts


class TestConfigRoleConflicts:
    def _params(self, drop, cat):
        return {"dataset": {"prepare_model_input": {
            "drop_columns": drop, "categorical_columns": cat}}}

    def test_no_overlap_returns_empty(self):
        assert config_role_conflicts(
            self._params(["snap_date", "label"], ["prod_name"])) == []

    def test_overlap_returns_offending_columns_sorted(self):
        assert config_role_conflicts(
            self._params(["cust_segment_typ", "label"],
                         ["prod_name", "cust_segment_typ"])) == ["cust_segment_typ"]

    def test_missing_keys_returns_empty(self):
        assert config_role_conflicts({}) == []


from recsys_tfb.core.consistency import (
    inference_products_mismatch,
    override_unknown_items,
    item_missing_from_categorical,
)


def _base(over=None):
    p = {
        "schema": {
            "columns": {"item": "prod_name"},
            "categorical_values": {"prod_name": ["a", "b"]},
        },
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
    }
    if over:
        p.update(over)
    return p


class TestInferenceProductsMismatch:
    def test_equal_sets_returns_empty(self):
        p = _base({"inference": {"products": ["b", "a"]}})
        assert inference_products_mismatch(p) == {"only_in_inference": [],
                                                  "only_in_categorical": []}

    def test_reports_both_directions(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        assert inference_products_mismatch(p) == {
            "only_in_inference": ["c"], "only_in_categorical": ["b"]}

    def test_no_inference_section_returns_empty(self):
        assert inference_products_mismatch(_base()) == {
            "only_in_inference": [], "only_in_categorical": []}


class TestOverrideUnknownItems:
    def test_unknown_item_component_detected(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio_overrides": {"mass|a|0": 0.5, "mass|zzz|0": 0.9}}})
        assert override_unknown_items(p) == ["zzz"]

    def test_item_not_in_group_keys_skipped(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ"],
            "sample_ratio_overrides": {"mass": 0.5}}})
        assert override_unknown_items(p) == []


class TestItemMissingFromCategorical:
    def test_item_present_ok(self):
        assert item_missing_from_categorical(_base()) is False

    def test_item_absent_detected(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["categorical_columns"] = ["gender"]
        assert item_missing_from_categorical(p) is True

    def test_key_absent_uses_default_includes_item(self):
        p = _base()
        del p["dataset"]["prepare_model_input"]["categorical_columns"]
        assert item_missing_from_categorical(p) is False


from recsys_tfb.core.consistency import validate_config_consistency


class TestValidateConfigConsistency:
    def test_clean_config_passes(self):
        validate_config_consistency(_base({"inference": {"products": ["a", "b"]}}))

    def test_a1_conflict_message_names_both_resolutions(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["cust_segment_typ"]
        p["dataset"]["prepare_model_input"]["categorical_columns"] = [
            "prod_name", "cust_segment_typ"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "cust_segment_typ" in msg
        assert "drop_columns" in msg and "categorical_columns" in msg

    def test_collects_multiple_errors_in_one_raise(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["prod_name"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "prod_name" in msg          # A1 (prod_name in drop ∩ categorical)
        assert "c" in msg                  # A4 only_in_inference
