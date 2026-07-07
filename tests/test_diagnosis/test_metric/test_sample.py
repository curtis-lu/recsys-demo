"""draw_diagnosis_sample：兩趟設計（小 item 全取＋hash-ratio 補滿）、正例 query only、決定性。"""

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample


def _params(max_queries=3, floor=2, seed=42):
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
        "evaluation": {
            "diagnosis": {
                "sample": {
                    "max_queries": max_queries,
                    "min_pos_queries_per_item": floor,
                    "seed": seed,
                },
            },
        },
    }


def _fixture(spark):
    """hot：4 個正例 query（H1..H4）；cold：1 個（C1）；N1 無正例（必須被排除）。
    每個 query 兩個候選列（hot、cold 各一）。"""
    rows = []
    for cust in ["H1", "H2", "H3", "H4"]:
        rows.append(("20240331", cust, "hot", 0.9, 1))
        rows.append(("20240331", cust, "cold", 0.1, 0))
    rows.append(("20240331", "C1", "hot", 0.9, 0))
    rows.append(("20240331", "C1", "cold", 0.1, 1))
    rows.append(("20240331", "N1", "hot", 0.9, 0))
    rows.append(("20240331", "N1", "cold", 0.1, 0))
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


def test_cold_item_queries_taken_in_full_and_no_positive_free_queries(spark):
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params())
    custs = set(pdf["cust_id"])
    assert "C1" in custs            # cold 正例 query 數 1 < 保底 2 → 全取
    assert "N1" not in custs        # 無正例 query 不進樣本
    assert meta["take_all_items"] == ["cold"]
    assert meta["n_pos_queries_total"] == 5
    assert 1 <= meta["n_queries_sampled"] <= 5


def test_sampled_queries_keep_all_candidate_rows(spark):
    pdf, _ = draw_diagnosis_sample(_fixture(spark), _params())
    sizes = pdf.groupby(["snap_date", "cust_id"]).size()
    assert (sizes == 2).all()       # 被抽中的 query 帶完整候選列（含負例）


def test_deterministic_given_seed(spark):
    df = _fixture(spark)
    pdf1, meta1 = draw_diagnosis_sample(df, _params())
    pdf2, meta2 = draw_diagnosis_sample(df, _params())
    key = ["snap_date", "cust_id", "prod_name"]
    pd.testing.assert_frame_equal(
        pdf1.sort_values(key).reset_index(drop=True),
        pdf2.sort_values(key).reset_index(drop=True),
    )
    assert meta1 == meta2


def test_metadata_shape(spark):
    _, meta = draw_diagnosis_sample(_fixture(spark), _params())
    for k in ["n_pos_queries_total", "n_queries_sampled", "take_all_items",
              "per_item_pos_queries_sampled", "max_queries",
              "min_pos_queries_per_item", "seed", "sample_ratio"]:
        assert k in meta
    assert meta["per_item_pos_queries_sampled"]["cold"] == 1
    assert meta["seed"] == 42


def test_take_all_when_everything_is_small(spark):
    # 保底拉到 10 > 所有 item 的正例 query 數 → 全部 take-all、全量進樣本
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(floor=10))
    assert sorted(meta["take_all_items"]) == ["cold", "hot"]
    assert meta["n_queries_sampled"] == 5
    assert set(pdf["cust_id"]) == {"H1", "H2", "H3", "H4", "C1"}
