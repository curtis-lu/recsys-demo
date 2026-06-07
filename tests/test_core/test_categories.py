"""Tests for core.categories — shared product grouping resolver."""
import pytest

from recsys_tfb.core.categories import resolve_category_mapping, resolve_groups


def _params(**pc):
    return {
        "schema": {
            "columns": {"item": "prod_name"},
            "categorical_values": {
                "prod_name": ["fund_a", "fund_b", "ccard_x", "loner"]
            },
        },
        "product_categories": pc or {
            "mapping": {"fund": ["fund_a", "fund_b"], "ccard": ["ccard_x"]},
            "unmapped": "singleton",
        },
    }


def test_resolve_category_mapping_singletons_unmapped():
    m = resolve_category_mapping(_params())
    assert m == {
        "fund_a": "fund", "fund_b": "fund",
        "ccard_x": "ccard", "loner": "loner",  # singleton
    }


def test_unknown_product_in_mapping_fails_loud():
    p = _params(mapping={"fund": ["nope"]}, unmapped="singleton")
    with pytest.raises(ValueError, match="unknown product"):
        resolve_category_mapping(p)


def test_unsupported_unmapped_policy_fails_loud():
    p = _params(mapping={"fund": ["fund_a"]}, unmapped="merge")
    with pytest.raises(ValueError, match="only 'singleton'"):
        resolve_category_mapping(p)


def test_resolve_groups_item_is_identity():
    g = resolve_groups(_params(), "item")
    assert g == {"fund_a": "fund_a", "fund_b": "fund_b",
                 "ccard_x": "ccard_x", "loner": "loner"}


def test_resolve_groups_category_uses_mapping():
    g = resolve_groups(_params(), "category")
    assert g["fund_a"] == "fund" and g["loner"] == "loner"


def test_resolve_groups_rejects_unknown_grouping():
    with pytest.raises(ValueError, match="grouping"):
        resolve_groups(_params(), "bogus")


def test_evaluation_build_mapping_delegates(monkeypatch):
    from recsys_tfb.evaluation import metrics_spark
    params = {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["fund_a", "loner"]}},
        "product_categories": {"mapping": {"fund": ["fund_a"]}, "unmapped": "singleton"},
        "evaluation": {"product_categories": {"enabled": True}},
    }
    assert metrics_spark._build_category_mapping(params) == {"fund_a": "fund", "loner": "loner"}
    params["evaluation"]["product_categories"]["enabled"] = False
    assert metrics_spark._build_category_mapping(params) is None
