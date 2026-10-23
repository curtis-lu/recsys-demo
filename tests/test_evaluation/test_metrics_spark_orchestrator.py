import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(categories=True):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "exchange_fx"]},
        },
        "evaluation": {
            "k_values": [1, "all"],
            "item_categories": {
                "enabled": categories, "unmapped": "singleton",
                "mapping": {"fund": ["fund_stock", "fund_bond"]}},
        },
    }


def _df(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_stock", 0.9, 1),
            ("20240331", "c1", "fund_bond", 0.4, 0),
            ("20240331", "c1", "exchange_fx", 0.7, 0),
            ("20240331", "c2", "fund_stock", 0.2, 0),
            ("20240331", "c2", "fund_bond", 0.3, 0),
            ("20240331", "c2", "exchange_fx", 0.8, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def _both(frame, params, **kw):
    """Row facts and metrics of the same frame, as the pipeline computes them."""
    facts = ms.compute_row_facts(frame, params, **kw)
    return facts, ms.compute_all_metrics(frame, params, facts, **kw)


def test_backward_compatible_keys_present(spark):
    facts, r = _both(_df(spark), _params())
    for k in ("overall", "per_segment", "per_item", "per_item_segment",
              "macro_avg"):
        assert k in r
    for k in ("n_queries", "n_excluded_queries"):
        assert k in facts


def test_dataset_overview_and_category_added(spark):
    facts, r = _both(_df(spark), _params())
    assert "dataset_overview" in facts
    assert facts["dataset_overview"]["totals"]["n_rows"] == 6
    assert "category" in r
    assert set(r["category"]["per_item"]) == {"fund", "exchange_fx"}
    assert "category" in facts
    assert "dataset_overview" in facts["category"]
    assert "category" not in r["category"]  # no infinite nesting


def test_category_absent_when_disabled(spark):
    facts, r = _both(_df(spark), _params(categories=False))
    assert "category" not in r
    assert "category" not in facts
    assert "dataset_overview" in facts
