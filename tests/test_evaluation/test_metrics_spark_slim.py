"""Tests for the slim metrics path: compute_overall_per_item."""

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


def test_returns_only_overall_and_per_item(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    result = compute_overall_per_item(_eval_predictions(spark), _parameters())

    assert set(result.keys()) == {"overall", "per_item"}


def test_matches_compute_all_metrics_subset(spark):
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_overall_per_item,
    )

    params = _parameters()
    df = _eval_predictions(spark)

    slim = compute_overall_per_item(df, params)
    full = compute_all_metrics(_eval_predictions(spark), params)

    assert slim["overall"] == full["overall"]
    assert slim["per_item"] == full["per_item"]


def test_empty_when_no_positive_queries(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [0, 0],
        "score": [0.9, 0.1],
    })
    result = compute_overall_per_item(spark.createDataFrame(pdf), _parameters())

    assert result == {"overall": {}, "per_item": {}}


# --- optional per_segment / category slices (baseline by-seg / 大類 compare) ---

def _params_seg_cat():
    p = _parameters()
    p["schema"]["categorical_values"] = {
        "prod_name": ["fund_stock", "fund_bond", "exchange_fx"]}
    p["evaluation"]["segment_columns"] = ["cust_segment_typ"]
    p["evaluation"]["item_categories"] = {
        "enabled": True, "unmapped": "singleton",
        "mapping": {"fund": ["fund_stock", "fund_bond"]}}
    return p


def _eval_seg_cat(spark):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 6,
        "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
        "prod_name": ["fund_stock", "fund_bond", "exchange_fx"] * 2,
        "label": [1, 0, 1, 0, 1, 0],
        "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
        "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
    })
    return spark.createDataFrame(pdf)


def test_with_segment_adds_per_segment_matching_full(spark):
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_overall_per_item,
    )
    p = _params_seg_cat()
    segs = ["cust_segment_typ"]
    slim = compute_overall_per_item(_eval_seg_cat(spark), p, segment_columns=segs)
    assert "per_segment" in slim
    assert set(slim["per_segment"]) == {"mass", "hnw"}
    # Same building blocks as the model path → values identical to full metrics.
    full = compute_all_metrics(_eval_seg_cat(spark), p, segment_columns=segs)
    assert slim["per_segment"] == full["per_segment"]


def test_with_category_adds_category_overall_matching_full(spark):
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_overall_per_item,
    )
    p = _params_seg_cat()
    slim = compute_overall_per_item(_eval_seg_cat(spark), p, with_category=True)
    assert "category" in slim
    assert set(slim["category"]) == {"overall", "per_item"}
    full = compute_all_metrics(_eval_seg_cat(spark), p)
    assert slim["category"]["overall"] == full["category"]["overall"]


def test_no_segment_columns_means_no_per_segment(spark):
    """Configured but not joined this run: the baseline follows the list it
    is handed, like the model's metrics."""
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item
    p = _parameters()
    p["evaluation"]["segment_columns"] = ["cust_segment_typ"]
    result = compute_overall_per_item(_eval_predictions(spark), p)
    assert "per_segment" not in result


def test_with_category_silently_skips_when_no_mapping(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item
    result = compute_overall_per_item(
        _eval_predictions(spark), _parameters(), with_category=True
    )
    assert "category" not in result


# ---------------------------------------------------------------------------
# 宣告 event 時的大類面（#378 回歸測試，與 test_metrics_spark_category.py 同一個 bug）
# ---------------------------------------------------------------------------


def test_slim_category_pass_works_when_event_is_declared(spark):
    """compute_overall_per_item 為了大類面遞迴呼叫自己，而遞迴那一次吃的是
    collapse_to_categories 的輸出——那張表以 (query group, 大類) 聚合過，event
    欄已經不在了。遞迴時沿用逐列那一面的決勝欄，Spark 會說 Column 'imp_id'
    does not exist，而且是在 evaluation 跑到一半、算熱門度基準線的時候。

    與 test_metrics_spark_category.py 的那一條是同一個 bug 的兩條路徑：主指標
    走 _compute_core，基準線走這裡。修好其中一條另一條照樣會炸。
    """
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

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
    result = compute_overall_per_item(df, params, with_category=True)
    assert "category" in result
    # per_item 只收「有正例的 item」（aggregate_per_item 在該 item 為正例的列
    # 上彙整），所以兩面都只有 fund_bond／fund；exchange_fx 全是負例。
    assert set(result["per_item"]) == {"fund_bond"}
    assert set(result["category"]["per_item"]) == {"fund"}
    # 大類面確實跑完、而且回的是聚合後那張表的數字。
    assert result["category"]["overall"]
