"""Tests for compare-source consistency predicates (A11/A12/A13)."""

import pytest
from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    compare_source_well_formed_errors,
    compare_source_key_exists,
    compare_mutual_exclusive_errors,
)


def _base_params() -> dict:
    return {"evaluation": {"compare_sources": {}}}


class TestA11_WellFormed:
    def test_empty_sources_ok(self):
        assert compare_source_well_formed_errors(_base_params()) == []

    def test_model_version_minimal_ok(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version",
            "model_version": "2026-01-31_abc_def",
            "label": "v_prev",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_external_hive_minimal_ok(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["ext"] = {
            "kind": "external_hive",
            "table": "other.preds",
            "label": "Ext",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"},
            "unmapped_policy": "fail",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_missing_kind(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {"label": "X"}
        errs = compare_source_well_formed_errors(p)
        assert any("kind" in e for e in errs)

    def test_unknown_kind(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {"kind": "parquet", "label": "X"}
        errs = compare_source_well_formed_errors(p)
        assert any("kind" in e for e in errs)

    def test_model_version_leaks_columns(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "model_version",
            "model_version": "v1",
            "label": "X",
            "columns": {"cust_id": "c"},
        }
        errs = compare_source_well_formed_errors(p)
        assert any("columns" in e for e in errs)

    def test_model_version_leaks_prod_mapping(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "model_version",
            "model_version": "v1",
            "label": "X",
            "prod_mapping": {"a": "b"},
        }
        errs = compare_source_well_formed_errors(p)
        assert any("prod_mapping" in e for e in errs)

    def test_external_hive_missing_table(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "fail",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("table" in e for e in errs)

    def test_external_hive_missing_required_column(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "table": "t", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s"},  # missing prod_name, score
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "fail",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("prod_name" in e or "score" in e for e in errs)

    def test_unmapped_policy_invalid(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "table": "t", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "skip",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("unmapped_policy" in e for e in errs)

    def test_source_not_a_dict(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = "bad-string-not-dict"
        errs = compare_source_well_formed_errors(p)
        assert any("must be a dict" in e for e in errs)

    def test_model_version_source_default_ok(self):
        """Omitted `source` field defaults to ranked_predictions — no error."""
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version", "model_version": "v1", "label": "X",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_model_version_source_training_eval_ok(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version", "model_version": "v1", "label": "X",
            "source": "training_eval_predictions",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_model_version_source_ranked_predictions_ok(self):
        """Explicit `source: ranked_predictions` (matches the default) is accepted."""
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version", "model_version": "v1", "label": "X",
            "source": "ranked_predictions",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_model_version_source_unknown_raises(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version", "model_version": "v1", "label": "X",
            "source": "score_table",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("source" in e and "score_table" in e for e in errs)


class TestA12_KeyExists:
    def test_none_returns_none(self):
        assert compare_source_key_exists(_base_params(), None) is None

    def test_existing_key_returns_dict(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {"kind": "model_version", "label": "X"}
        assert compare_source_key_exists(p, "v_prev")["label"] == "X"

    def test_missing_key_raises(self):
        with pytest.raises(ConfigConsistencyError, match="v_prev"):
            compare_source_key_exists(_base_params(), "v_prev")


class TestA13_MutualExclusive:
    def test_neither(self):
        assert compare_mutual_exclusive_errors(None, None) == []

    def test_only_compare(self):
        assert compare_mutual_exclusive_errors("x", None) == []

    def test_only_compare_only(self):
        assert compare_mutual_exclusive_errors(None, "x") == []

    def test_both_raises(self):
        errs = compare_mutual_exclusive_errors("x", "y")
        assert any("mutually exclusive" in e.lower() for e in errs)
