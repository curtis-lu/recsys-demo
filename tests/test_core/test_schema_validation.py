"""Tests for schema config validation and source_etl consistency checks."""

import pytest

from recsys_tfb.core.schema import (
    get_schema_for_hash,
    validate_schema_config,
)


class TestValidateSchemaConfig:
    def test_empty_params_ok(self):
        validate_schema_config({})

    def test_defaults_only_ok(self):
        validate_schema_config({"schema": {"columns": {}}})

    def test_full_valid_ok(self):
        validate_schema_config(
            {
                "schema": {
                    "columns": {
                        "time": "snap_date",
                        "entity": ["cust_id"],
                        "item": "prod_name",
                        "label": "label",
                        "score": "score",
                        "rank": "rank",
                    }
                }
            }
        )

    def test_entity_string_ok(self):
        validate_schema_config({"schema": {"columns": {"entity": "cust_id"}}})

    def test_entity_empty_list_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": {"entity": []}}})

    def test_entity_empty_string_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": {"entity": ""}}})

    def test_entity_list_with_empty_element_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": {"entity": ["cust_id", ""]}}})

    def test_entity_list_with_non_string_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": {"entity": ["cust_id", 123]}}})

    def test_entity_wrong_type_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": {"entity": 42}}})

    def test_time_non_string_raises(self):
        with pytest.raises(ValueError, match="time"):
            validate_schema_config({"schema": {"columns": {"time": 123}}})

    def test_item_empty_string_raises(self):
        with pytest.raises(ValueError, match="item"):
            validate_schema_config({"schema": {"columns": {"item": ""}}})

    def test_identity_columns_duplicate_raises(self):
        # entity equals item → identity columns contain duplicate
        with pytest.raises(ValueError, match="duplicates"):
            validate_schema_config(
                {
                    "schema": {
                        "columns": {
                            "entity": ["cust_id"],
                            "item": "cust_id",
                        }
                    }
                }
            )

    def test_columns_not_a_mapping_raises(self):
        with pytest.raises(ValueError, match="mapping"):
            validate_schema_config({"schema": {"columns": ["a", "b"]}})

    def test_categorical_values_wrong_type_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {"categorical_values": ["prod_name"]}}
            )

    def test_categorical_values_empty_list_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {"categorical_values": {"prod_name": []}}}
            )

    def test_categorical_values_non_list_value_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {"categorical_values": {"prod_name": "a,b,c"}}}
            )

    def test_identity_categorical_missing_declaration_raises(self):
        params = {
            "schema": {"categorical_values": {}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["prod_name"],
                }
            },
        }
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(params)

    def test_identity_categorical_with_declaration_ok(self):
        params = {
            "schema": {"categorical_values": {"prod_name": ["a", "b"]}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["prod_name"],
                }
            },
        }
        validate_schema_config(params)

    def test_non_identity_categorical_does_not_require_declaration(self):
        # cust_segment_typ is not in identity_columns; no declaration required.
        params = {
            "schema": {"categorical_values": {}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["cust_segment_typ"],
                }
            },
        }
        validate_schema_config(params)


class TestSchemaValidationDelegatesA3:
    def test_identity_cat_missing_values_still_raises_valueerror(self):
        # behaviour preserved after delegation to consistency.resolved_item_values
        p = {
            "schema": {"columns": {"item": "prod_name"}},
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(p)


class TestGetSchemaForHash:
    def test_returns_canonical_keys_with_categorical_values(self):
        schema = get_schema_for_hash({})
        assert set(schema.keys()) == {
            "time", "entity", "item", "label", "score", "rank",
            "categorical_values",
        }
        assert "identity_columns" not in schema

    def test_reflects_overrides(self):
        schema = get_schema_for_hash(
            {"schema": {"columns": {"time": "month_end", "entity": "customer_id"}}}
        )
        assert schema["time"] == "month_end"
        assert schema["entity"] == ["customer_id"]

    def test_includes_categorical_values(self):
        schema = get_schema_for_hash(
            {"schema": {"categorical_values": {"prod_name": ["a", "b"]}}}
        )
        assert schema["categorical_values"] == {"prod_name": ["a", "b"]}
