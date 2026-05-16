"""Tests for category-level extension of metrics_spark."""

import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(enabled=True, unmapped="singleton"):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]},
        }},
        "evaluation": {"product_categories": {
            "enabled": enabled, "unmapped": unmapped,
            "mapping": {"fund": ["fund_stock", "fund_bond", "fund_mix"]},
        }},
    }


def test_disabled_returns_none():
    p = _params(enabled=False)
    assert ms._build_category_mapping(p) is None


def test_mapping_with_singleton_unmapped():
    m = ms._build_category_mapping(_params())
    assert m["fund_stock"] == "fund"
    assert m["fund_bond"] == "fund"
    assert m["exchange_fx"] == "exchange_fx"   # unmapped -> singleton
    assert m["lonely"] == "lonely"


def test_unknown_product_in_mapping_fails_loud():
    p = _params()
    p["evaluation"]["product_categories"]["mapping"]["x"] = ["not_a_product"]
    with pytest.raises(ValueError, match="not_a_product"):
        ms._build_category_mapping(p)
