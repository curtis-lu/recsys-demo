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
