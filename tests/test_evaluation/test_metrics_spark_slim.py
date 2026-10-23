"""Behaviors the popularity baseline's former slim path (compute_overall_per_item,
deleted in #535) guarded, now asserted on compute_all_metrics, which the
baseline runs: segment list followed as handed, category pass skipped without a
mapping, category pass working when an event column is declared."""

import pandas as pd


def _parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {"k_values": [1, 2, 3]},
    }


def _eval_predictions(spark):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 6,
        "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
        "prod_name": ["A", "B", "C", "A", "B", "C"],
        "label": [1, 0, 1, 0, 1, 0],
        "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
    })
    return spark.createDataFrame(pdf)


def _both(frame, params, **kw):
    """Row facts and metrics of the same frame, as the pipeline computes them."""
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_row_facts,
    )
    facts = compute_row_facts(frame, params, **kw)
    return facts, compute_all_metrics(frame, params, facts, **kw)


def test_no_segment_columns_means_no_per_segment(spark):
    """Configured but not joined this run: the baseline follows the list it
    is handed, like the model's metrics."""
    p = _parameters()
    p["evaluation"]["segment_columns"] = ["cust_segment_typ"]
    _, result = _both(_eval_predictions(spark), p)
    assert result["per_segment"] == {}


def test_with_category_silently_skips_when_no_mapping(spark):
    _, result = _both(_eval_predictions(spark), _parameters(), with_category=True)
    assert "category" not in result


# ---------------------------------------------------------------------------
# 宣告 event 時的大類面（#378 回歸測試，與 test_metrics_spark_category.py 同一個 bug）
# ---------------------------------------------------------------------------


def test_baseline_category_pass_works_when_event_is_declared(spark):
    """The baseline runs compute_all_metrics (the former slim path called itself
    recursively for the category pass, fed the output of collapse_to_categories
    — event column already aggregated away). Keeping the row-grain tie-break
    column on that pass makes Spark say Column 'imp_id' does not exist, midway
    through evaluation while the popularity baseline is computed.

    Same bug as test_metrics_spark_category.py's event test, reached through the
    baseline's call; fixing only one of the two call sites would still blow up.
    """
    params = {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
                "event": "imp_id",
            },
            "categorical_values": {"prod_name": ["fund_stock", "fund_bond", "exchange_fx"]},
        },
        "evaluation": {
            "item_categories": {
                "enabled": True, "unmapped": "singleton",
                "mapping": {"fund": ["fund_stock", "fund_bond"]},
            },
        },
    }
    df = spark.createDataFrame(
        [
            ("20240331", "c1", "fund_bond", "i1", 0.9, 1),
            ("20240331", "c1", "fund_bond", "i2", 0.7, 0),
            ("20240331", "c1", "exchange_fx", "i3", 0.2, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )
    _, result = _both(df, params, with_category=True)
    assert "category" in result
    # per_item 只收「有正例的 item」（aggregate_per_item 在該 item 為正例的列
    # 上彙整），所以兩面都只有 fund_bond／fund；exchange_fx 全是負例。
    assert set(result["per_item"]) == {"fund_bond"}
    assert set(result["category"]["per_item"]) == {"fund"}
    # 大類面確實跑完、而且回的是聚合後那張表的數字。
    assert result["category"]["overall"]
