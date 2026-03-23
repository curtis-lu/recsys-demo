"""Tests for core.schema — centralized column schema."""

import copy

import pytest

from recsys_tfb.core.schema import get_schema


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
