"""Tests for comparison.sources — load_compare_predictions."""

import pytest
from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions


def _params_for_mv(mv: str, snap: str = "2026-01-31") -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1", "p2", "p3"]},
        },
        "evaluation": {
            "snap_date": snap,
            "compare": {"kind": "model_version", "model_version": mv, "label": "L"},
        },
    }


@pytest.fixture
def ranked_predictions_view(spark):
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, "MV_A"),
            ("c1", "2026-01-31", "p2", 0.7, "MV_A"),
            ("c1", "2026-01-31", "p1", 0.8, "MV_B"),
            ("c1", "2025-12-31", "p1", 0.5, "MV_A"),  # different snap_date
        ],
        ["cust_id", "snap_date", "prod_name", "score", "model_version"],
    )
    df.createOrReplaceTempView("ranked_predictions")
    yield
    spark.catalog.dropTempView("ranked_predictions")


def test_model_version_filters_correctly(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    out = load_compare_predictions(p, spark)
    rows = sorted((r["cust_id"], r["prod_name"], r["score"]) for r in out.collect())
    assert rows == [("c1", "p1", 0.9), ("c1", "p2", 0.7)]


def test_model_version_unknown_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_GHOST")
    with pytest.raises(DataConsistencyError, match="MV_GHOST"):
        load_compare_predictions(p, spark)


def test_unknown_kind_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    p["evaluation"]["compare"]["kind"] = "parquet"
    with pytest.raises(RuntimeError, match="parquet"):
        load_compare_predictions(p, spark)


def test_missing_compare_key_raises(spark):
    p = _params_for_mv("MV_A")
    del p["evaluation"]["compare"]
    with pytest.raises(RuntimeError, match="compare"):
        load_compare_predictions(p, spark)


def _params_for_ext(snap: str = "2026-01-31", policy: str = "fail") -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["fund_stock", "fund_bond", "exchange_usd"]},
        },
        "evaluation": {
            "snap_date": snap,
            "compare": {
                "kind": "external_hive",
                "table": "ext_proj.preds",
                "label": "ExtX",
                "columns": {
                    "cust_id": "customer_id",
                    "snap_date": "as_of_date",
                    "prod_name": "item_code",
                    "score": "pred_score",
                },
                "prod_mapping": {
                    "ext_fund_a": "fund_stock",
                    "ext_fund_b": "fund_bond",
                    "ext_usd": "exchange_usd",
                },
                "unmapped_policy": policy,
            },
        },
    }


@pytest.fixture
def ext_predictions_view(spark):
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "ext_fund_a", 0.9),
            ("c1", "2026-01-31", "ext_fund_b", 0.8),
            ("c2", "2026-01-31", "ext_fund_a", 0.7),
            ("c2", "2026-01-31", "ext_usd", 0.6),
            ("c3", "2025-12-31", "ext_fund_a", 0.5),  # different snap_date
        ],
        ["customer_id", "as_of_date", "item_code", "pred_score"],
    )
    df.createOrReplaceTempView("ext_proj__preds")
    yield df
    spark.catalog.dropTempView("ext_proj__preds")


def test_external_hive_column_rename_and_snap_filter(spark, monkeypatch, ext_predictions_view):
    p = _params_for_ext()
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view if t == "ext_proj.preds" else spark.table(t))
    out = load_compare_predictions(p, spark)
    cols = set(out.columns)
    assert {"cust_id", "snap_date", "prod_name", "score"}.issubset(cols)
    snaps = {r["snap_date"] for r in out.collect()}
    assert snaps == {"2026-01-31"}  # filtered to eval snap


def test_external_hive_prod_mapping_n_to_1_collapse(spark, monkeypatch):
    # Two external prods both map to "fund_stock" — should collapse with max(score)
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "ext_fund_a", 0.7),
            ("c1", "2026-01-31", "ext_fund_b2", 0.9),  # also maps to fund_stock
        ],
        ["customer_id", "as_of_date", "item_code", "pred_score"],
    )
    p = _params_for_ext()
    p["evaluation"]["compare"]["prod_mapping"] = {
        "ext_fund_a": "fund_stock", "ext_fund_b2": "fund_stock",
    }
    monkeypatch.setattr(spark, "table", lambda t: df)
    out = load_compare_predictions(p, spark)
    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "fund_stock", 0.9)]


def test_external_hive_unmapped_fail_raises(spark, monkeypatch, ext_predictions_view):
    p = _params_for_ext(policy="fail")
    p["evaluation"]["compare"]["prod_mapping"] = {"ext_fund_a": "fund_stock"}  # missing fund_b, usd
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view)
    with pytest.raises(DataConsistencyError, match=r"B2.*ext_fund_b|ext_usd"):
        load_compare_predictions(p, spark)


def test_external_hive_unmapped_drop_filters_and_warns(spark, monkeypatch, ext_predictions_view, caplog):
    p = _params_for_ext(policy="drop")
    p["evaluation"]["compare"]["prod_mapping"] = {"ext_fund_a": "fund_stock"}
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view)
    out = load_compare_predictions(p, spark)
    prods = {r["prod_name"] for r in out.collect()}
    assert prods == {"fund_stock"}
    assert any("ext_fund_b" in rec.message or "ext_usd" in rec.message for rec in caplog.records)
