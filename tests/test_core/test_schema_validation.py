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


class TestGetSchemaForHash:
    def test_returns_only_canonical_keys(self):
        schema = get_schema_for_hash({})
        assert set(schema.keys()) == {"time", "entity", "item", "label", "score", "rank"}
        assert "identity_columns" not in schema

    def test_reflects_overrides(self):
        schema = get_schema_for_hash(
            {"schema": {"columns": {"time": "month_end", "entity": "customer_id"}}}
        )
        assert schema["time"] == "month_end"
        assert schema["entity"] == ["customer_id"]
