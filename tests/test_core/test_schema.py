"""Tests for core.schema — centralized column schema."""

import copy

import pytest

from recsys_tfb.core.schema import (
    ENTITY_GROUPING_KEYS,
    get_entity_grouping,
    get_schema,
)


class TestGetSchemaDefaults:
    def test_defaults_when_no_schema_section(self):
        result = get_schema({})
        assert result["time"] == "snap_date"
        assert result["entity"] == ["cust_id"]
        assert result["item"] == "prod_name"
        assert result["label"] == "label"
        assert result["score"] == "score"
        assert result["rank"] == "rank"

    def test_defaults_identity_columns(self):
        result = get_schema({})
        assert result["identity_columns"] == ["snap_date", "cust_id", "prod_name"]


class TestGetSchemaPartialOverride:
    def test_override_time_only(self):
        params = {"schema": {"columns": {"time": "month_end"}}}
        result = get_schema(params)
        assert result["time"] == "month_end"
        assert result["entity"] == ["cust_id"]
        assert result["item"] == "prod_name"

    def test_override_item_only(self):
        params = {"schema": {"columns": {"item": "product_code"}}}
        result = get_schema(params)
        assert result["item"] == "product_code"
        assert result["time"] == "snap_date"


class TestGetSchemaFullOverride:
    def test_all_keys_overridden(self):
        params = {
            "schema": {
                "columns": {
                    "time": "dt",
                    "entity": ["branch_id", "cust_id"],
                    "item": "product_code",
                    "label": "target",
                    "score": "prob",
                    "rank": "position",
                }
            }
        }
        result = get_schema(params)
        assert result["time"] == "dt"
        assert result["entity"] == ["branch_id", "cust_id"]
        assert result["item"] == "product_code"
        assert result["label"] == "target"
        assert result["score"] == "prob"
        assert result["rank"] == "position"


class TestEntityNormalization:
    def test_entity_string_to_list(self):
        params = {"schema": {"columns": {"entity": "cust_id"}}}
        result = get_schema(params)
        assert result["entity"] == ["cust_id"]

    def test_entity_list_unchanged(self):
        params = {"schema": {"columns": {"entity": ["branch_id", "cust_id"]}}}
        result = get_schema(params)
        assert result["entity"] == ["branch_id", "cust_id"]


class TestIdentityColumnsDerivation:
    def test_default_identity(self):
        result = get_schema({})
        assert result["identity_columns"] == ["snap_date", "cust_id", "prod_name"]

    def test_multi_entity_identity(self):
        params = {
            "schema": {
                "columns": {
                    "entity": ["branch_id", "cust_id"],
                }
            }
        }
        result = get_schema(params)
        assert result["identity_columns"] == [
            "snap_date", "branch_id", "cust_id", "prod_name"
        ]


class TestPureFunction:
    def test_input_not_mutated(self):
        params = {"schema": {"columns": {"time": "month_end"}}}
        original = copy.deepcopy(params)
        get_schema(params)
        assert params == original

    def test_repeated_calls_same_result(self):
        params = {"schema": {"columns": {"entity": "cust_id"}}}
        r1 = get_schema(params)
        r2 = get_schema(params)
        assert r1 == r2


class TestCategoricalValues:
    def test_default_empty_when_absent(self):
        result = get_schema({})
        assert result["categorical_values"] == {}

    def test_returned_from_schema_section(self):
        params = {
            "schema": {
                "categorical_values": {"prod_name": ["a", "b", "c"]},
            }
        }
        result = get_schema(params)
        assert result["categorical_values"] == {"prod_name": ["a", "b", "c"]}

    def test_deep_copied(self):
        values = ["a", "b"]
        params = {"schema": {"categorical_values": {"prod_name": values}}}
        result = get_schema(params)
        result["categorical_values"]["prod_name"].append("c")
        assert values == ["a", "b"]


class TestTwoColumnEntityFixture:
    """Meta test for the shared ``two_column_entity_params`` fixture.

    Multi-column-entity tests are only meaningful if the parameters they run
    on actually resolve to more than one entity column. This class is what
    makes that a checked fact instead of an assumption.
    """

    def test_get_schema_sees_both_entity_columns(self, two_column_entity_params):
        schema = get_schema(two_column_entity_params)
        assert schema["entity"] == ["branch_id", "cust_id"]

    def test_identity_columns_carry_both_entity_columns(
        self, two_column_entity_params
    ):
        schema = get_schema(two_column_entity_params)
        assert schema["identity_columns"] == [
            "snap_date", "branch_id", "cust_id", "prod_name",
        ]

    def test_mis_nested_shape_is_silently_ignored(self, two_column_entity_params):
        """Pins the trap the fixture exists to avoid.

        Moving the column names one level up — straight under ``schema``,
        skipping ``columns`` — makes ``get_schema`` ignore the whole block and
        return the one-column default. No error, no warning. Copying such a
        dict into a multi-entity test yields a green test that never exercised
        a second entity column.
        """
        columns = two_column_entity_params["schema"]["columns"]
        mis_nested = {"schema": dict(columns)}
        assert get_schema(mis_nested)["entity"] == ["cust_id"]


class TestGetEntityGrouping:
    _PARAMS = {"schema": {"columns": {"entity": ["branch_id", "cust_id"]}}}

    def test_undeclared_falls_back_to_the_whole_entity(self):
        for key in ENTITY_GROUPING_KEYS:
            assert get_entity_grouping(self._PARAMS, key) == ["branch_id", "cust_id"]

    def test_declared_value_wins(self):
        params = {**self._PARAMS, "dataset": {"train_split_keys": ["branch_id"]}}
        assert get_entity_grouping(params, "train_split_keys") == ["branch_id"]
        # The other key is unaffected — that separation is the whole point.
        assert get_entity_grouping(params, "val_sample_keys") == ["branch_id", "cust_id"]

    def test_duplicates_are_collapsed(self):
        params = {**self._PARAMS, "dataset": {"val_sample_keys": ["cust_id", "cust_id"]}}
        assert get_entity_grouping(params, "val_sample_keys") == ["cust_id"]

    def test_an_unknown_key_name_raises_instead_of_defaulting(self):
        # A typo at the call site would otherwise resolve to "nothing declared"
        # and return the full entity: a plausible answer that ignores the user's
        # config, from code A29 never inspects.
        with pytest.raises(ValueError, match="not an entity-grouping key"):
            get_entity_grouping(self._PARAMS, "train_split_key")
