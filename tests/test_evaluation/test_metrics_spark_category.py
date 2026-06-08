"""Tests for category-level extension of metrics_spark."""

import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(enabled=True, unmapped="singleton"):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]},
        },
        "product_categories": {
            "unmapped": unmapped,
            "mapping": {"fund": ["fund_stock", "fund_bond", "fund_mix"]},
        },
        "evaluation": {
            "product_categories": {
                "enabled": enabled,
            },
            "segment_columns": ["cust_segment_typ"],
        },
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
    p["product_categories"]["mapping"]["x"] = ["not_a_product"]
    with pytest.raises(ValueError, match="not_a_product"):
        ms._build_category_mapping(p)


def _raw(spark):
    # c1 wants fund (via fund_bond) ; c1 fund_stock is top score
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_stock", 0.9, 0, "mass"),
            ("20240331", "c1", "fund_bond",  0.4, 1, "mass"),
            ("20240331", "c1", "exchange_fx", 0.7, 0, "mass"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label",
                "cust_segment_typ"],
    )


def test_collapse_to_categories_grain(spark):
    p = _params()
    p["schema"]["categorical_values"]["prod_name"] = [
        "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]
    collapsed = ms.collapse_to_categories(_raw(spark), p)
    rows = {r["prod_name"]: r for r in collapsed.collect()}
    # category column reuses item_col name so downstream stays uniform
    assert set(rows) == {"fund", "exchange_fx"}
    # fund score = max(child score) = max(0.9, 0.4) = 0.9
    assert rows["fund"]["score"] == pytest.approx(0.9)
    # fund label = max(child label) = max(0, 1) = 1
    assert rows["fund"]["label"] == 1
    # segment carried
    assert rows["fund"]["cust_segment_typ"] == "mass"
