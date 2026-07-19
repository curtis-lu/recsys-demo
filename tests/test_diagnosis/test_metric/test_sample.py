"""draw_diagnosis_sample：兩趟設計（小 item 全取＋hash-ratio 補滿）、正例 query only、決定性。"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample


def _params(max_queries=3, floor=2, seed=42, segment_columns=None):
    evaluation = {
        "diagnosis": {
            "sample": {
                "max_queries": max_queries,
                "min_pos_queries_per_item": floor,
                "seed": seed,
            },
        },
    }
    if segment_columns is not None:
        evaluation["segment_columns"] = segment_columns
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
        "evaluation": evaluation,
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


def test_segment_columns_kept_when_configured_and_present(spark):
    sdf = _fixture(spark).withColumn("seg_a", F.lit("x"))
    params = _params(segment_columns=["seg_a", "seg_missing"])
    pdf, _meta = draw_diagnosis_sample(sdf, params)
    assert "seg_a" in pdf.columns          # 配置且存在 → 帶回
    assert "seg_missing" not in pdf.columns  # 配置但不存在 → 靜默略過（沿 score_uncalibrated 慣例）


@pytest.mark.parametrize("reserved", ["stratum", "inclusion_weight"])
def test_reserved_column_in_segment_columns_fails_loud(spark, reserved):
    """撞名必須在抽樣前就炸，且訊息要指得到根因。

    沒有這個守衛時，撞名的欄會被 keep_cols 選進 df，再被 query 級 join 複製成
    兩個同名欄，最後炸在 pandas 的 groupby("stratum")：
    ``ValueError: Grouper for 'stratum' not 1-dimensional``——訊息完全沒提到
    segment 撞名，等於把人送去追一條錯的線。
    """
    sdf = _fixture(spark).withColumn(reserved, F.lit("x"))
    params = _params(segment_columns=[reserved])
    with pytest.raises(ValueError) as exc:
        draw_diagnosis_sample(sdf, params)
    msg = str(exc.value)
    assert "segment_columns" in msg      # 點名是哪個配置鍵
    assert reserved in msg               # 點名是哪個欄
    # 兩者要綁在同一句，不能只是各自出現在訊息某處
    assert f"{reserved!r} (configured via evaluation.segment_columns)" in msg


def test_non_reserved_segment_column_still_works(spark):
    """守衛不得誤傷正常 segment 欄——只有兩個保留名才擋。"""
    sdf = _fixture(spark).withColumn("seg_a", F.lit("x"))
    pdf, _meta = draw_diagnosis_sample(sdf, _params(segment_columns=["seg_a"]))
    assert "seg_a" in pdf.columns


def test_reserved_column_present_but_not_configured_is_fine(spark):
    """來源表剛好有同名欄、但沒配進 segment_columns → 不會被 keep_cols 選中，
    不構成撞名，不該擋。"""
    sdf = _fixture(spark).withColumn("stratum", F.lit("x"))
    pdf, _meta = draw_diagnosis_sample(sdf, _params())
    assert set(pdf["stratum"]) <= {"take_all", "hash_ratio"}


def test_take_all_when_everything_is_small(spark):
    # 保底拉到 10 > 所有 item 的正例 query 數 → 全部 take-all、全量進樣本
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(floor=10))
    assert sorted(meta["take_all_items"]) == ["cold", "hot"]
    assert meta["n_queries_sampled"] == 5
    assert set(pdf["cust_id"]) == {"H1", "H2", "H3", "H4", "C1"}


# ---- 納入機率權重（分層抽樣的 π 顯性化）----


def _stratified_fixture(spark, n_hot=40):
    """造出「兩層都非空」的情境：hot 有 n_hot 個正例 query（會走 hash-ratio），
    cold 只有 1 個（低於保底 → take-all）。搭配 max_queries 遠小於 n_hot 使
    ratio < 1。每個 query 兩個候選列（hot、cold 各一）。"""
    rows = []
    for i in range(n_hot):
        cust = f"HOT{i:03d}"
        rows.append(("20240331", cust, "hot", 0.9, 1))
        rows.append(("20240331", cust, "cold", 0.1, 0))
    rows.append(("20240331", "COLD0", "hot", 0.9, 0))
    rows.append(("20240331", "COLD0", "cold", 0.1, 1))
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


def test_sample_carries_inclusion_weight_and_stratum_columns(spark):
    pdf, _meta = draw_diagnosis_sample(_fixture(spark), _params())
    assert "inclusion_weight" in pdf.columns
    assert "stratum" in pdf.columns
    assert pdf["stratum"].notna().all()
    assert set(pdf["stratum"]) <= {"take_all", "hash_ratio"}


def test_inclusion_weight_degenerates_to_one_without_subsampling(spark):
    # max_queries 遠大於正例 query 數 → 沒有次抽樣，加權退化成全 1
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(max_queries=1000))
    assert meta["sample_ratio"] == 1.0
    assert (pdf["inclusion_weight"] == 1.0).all()


def test_inclusion_weight_is_inverse_probability_per_stratum(spark):
    # 自造 ratio < 1：40 個 hot query + 1 個 cold（take-all），max_queries=11
    pdf, meta = draw_diagnosis_sample(
        _stratified_fixture(spark), _params(max_queries=11, floor=2)
    )
    assert 0.0 < meta["sample_ratio"] < 1.0
    assert meta["take_all_items"], "測試情境沒產生 take-all 層，這條沒測到東西"

    take_all = pdf[pdf["stratum"] == "take_all"]
    hash_ratio = pdf[pdf["stratum"] == "hash_ratio"]
    assert not take_all.empty, "take-all 層是空的，分層沒真的發生"
    assert not hash_ratio.empty, "hash-ratio 層是空的，分層沒真的發生"

    assert (take_all["inclusion_weight"] == 1.0).all()
    expected = 1.0 / meta["sample_ratio"]
    assert (hash_ratio["inclusion_weight"] - expected).abs().max() < 1e-9


def test_inclusion_weight_is_constant_within_a_query(spark):
    # 抽樣單位是 query → 同一 query 的所有候選列必須同權重
    pdf, _meta = draw_diagnosis_sample(
        _stratified_fixture(spark), _params(max_queries=11, floor=2)
    )
    per_query = pdf.groupby(["snap_date", "cust_id"])["inclusion_weight"].nunique()
    assert (per_query == 1).all()


def test_meta_reports_strata_query_counts_and_weights(spark):
    _pdf, meta = draw_diagnosis_sample(
        _stratified_fixture(spark), _params(max_queries=11, floor=2)
    )
    strata = meta["strata"]
    assert set(strata) == {"take_all", "hash_ratio"}
    assert strata["take_all"]["weight"] == 1.0
    assert strata["take_all"]["n_queries"] >= 1
    assert abs(strata["hash_ratio"]["weight"] - 1.0 / meta["sample_ratio"]) < 1e-9
    assert strata["hash_ratio"]["n_queries"] >= 1
    assert (
        strata["take_all"]["n_queries"] + strata["hash_ratio"]["n_queries"]
        == meta["n_queries_sampled"]
    )


def test_strata_lists_only_existing_layers_when_all_take_all(spark):
    # floor 拉高 → 全部 take-all，不存在 hash_ratio 層
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(floor=10))
    assert set(meta["strata"]) == {"take_all"}
    assert (pdf["inclusion_weight"] == 1.0).all()
    # 全 take-all 時 sample_ratio 是 0.0（「沒有 hash 層」的哨兵值），不是 1.0。
    # 這條釘住模組 docstring 的宣告——下游若照「權重＝1/sample_ratio」理解就是
    # ZeroDivisionError，所以權重一律讀 strata。
    assert meta["sample_ratio"] == 0.0
    assert meta["strata"]["take_all"]["weight"] == 1.0


# ---- sampling_description（人看得懂的一句話，動態組字）----


def test_sampling_description_present(spark):
    _pdf, meta = draw_diagnosis_sample(_fixture(spark), _params())
    assert "sampling_description" in meta
    assert isinstance(meta["sampling_description"], str)
    assert meta["sampling_description"]


def test_sampling_description_says_not_sampled_when_ratio_is_one(spark):
    # max_queries 遠大於正例 query 數 → sample_ratio == 1.0（有 hash 層但沒吃到）
    _pdf, meta = draw_diagnosis_sample(
        _fixture(spark), _params(max_queries=1000)
    )
    assert meta["sample_ratio"] == 1.0
    desc = meta["sampling_description"]
    assert "未抽樣" in desc
    assert f"{meta['n_pos_queries_total']:,}" in desc


def test_sampling_description_says_no_hash_layer_when_ratio_is_zero_sentinel(spark):
    # floor 拉高 → 全 take-all，sample_ratio 的 0.0 是「無 hash 層」哨兵值
    _pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(floor=10))
    assert meta["sample_ratio"] == 0.0
    desc = meta["sampling_description"]
    assert "無 hash-ratio 層" in desc
    assert "抽了 0%" not in desc
    assert "0%" not in desc


def test_sampling_description_says_stratified_with_actual_query_counts(spark):
    # 自造 ratio < 1 的情境：沿用既有的 _stratified_fixture(max_queries=11, floor=2)
    _pdf, meta = draw_diagnosis_sample(
        _stratified_fixture(spark), _params(max_queries=11, floor=2)
    )
    assert 0.0 < meta["sample_ratio"] < 1.0
    desc = meta["sampling_description"]
    assert "分層" in desc
    strata = meta["strata"]
    assert f"{strata['take_all']['n_queries']:,}" in desc
    assert f"{strata['hash_ratio']['n_queries']:,}" in desc


def test_sampling_description_helper_uses_thousands_separators():
    from recsys_tfb.diagnosis.metric.sample import _sampling_description

    strata = {
        "take_all": {"n_queries": 1200, "weight": 1.0},
        "hash_ratio": {"n_queries": 18800, "weight": 1.85},
    }
    desc = _sampling_description(20000, 0.4, strata)
    assert "1,200" in desc
    assert "18,800" in desc
    assert "分層" in desc
