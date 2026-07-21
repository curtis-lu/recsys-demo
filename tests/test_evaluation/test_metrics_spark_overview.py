import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {}}


def _df(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1),
            ("20240331", "c1", "B", 0.1, 0),
            ("20240331", "c2", "A", 0.2, 0),
            ("20240331", "c2", "B", 0.8, 1),
            ("20240229", "c1", "A", 0.5, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_dataset_overview_totals(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    t = ov["totals"]
    assert t["n_rows"] == 5
    assert t["n_customers"] == 2
    assert t["n_products"] == 2
    assert t["n_snap_dates"] == 2
    assert t["n_positives"] == 3
    assert t["positive_rate"] == pytest.approx(3 / 5)
    assert t["avg_positives_per_customer"] == pytest.approx(1.5)


def test_dataset_overview_by_snap_and_item(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    assert ov["by_snap_date"]["20240331"]["n_rows"] == 4
    assert ov["by_snap_date"]["20240331"]["n_positives"] == 2
    assert ov["by_item"]["A"]["n_customers"] == 2
    assert ov["by_item"]["A"]["n_positives"] == 2


def _df_seg(spark):
    # 在 _df 基礎上加 seg 欄（segment 是 customer 級屬性：c1→X、c2→Y）
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1, "X"),
            ("20240331", "c1", "B", 0.1, 0, "X"),
            ("20240331", "c2", "A", 0.2, 0, "Y"),
            ("20240331", "c2", "B", 0.8, 1, "Y"),
            ("20240229", "c1", "A", 0.5, 1, "X"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "seg"],
    )


def _params_seg():
    p = _params()
    p["evaluation"] = {"segment_columns": ["seg"]}
    return p


def test_dataset_overview_by_segment(spark):
    ov = ms.compute_dataset_overview(_df_seg(spark), _params_seg())
    bs = ov["by_segment"]
    # query＝(snap_date,cust_id) distinct：X={(0331,c1),(0229,c1)}=2、Y={(0331,c2)}=1、總=3
    assert bs["X"]["n_positives"] == 2          # 0331 c1 A、0229 c1 A
    assert bs["X"]["n_queries"] == 2
    assert bs["X"]["query_share"] == pytest.approx(2 / 3)
    assert bs["Y"]["n_queries"] == 1
    assert bs["Y"]["query_share"] == pytest.approx(1 / 3)
    assert bs["X"]["positive_rate"] == pytest.approx(2 / 3)   # 2 正例 / 3 候選列
    # query_share 逐列加總＝1（segment 把 query 乾淨分群）
    assert sum(v["query_share"] for v in bs.values()) == pytest.approx(1.0)


def test_dataset_overview_no_by_segment_without_seg_col(spark):
    # 沒設 segment_columns（或欄不在資料中）→ 不產 by_segment
    ov = ms.compute_dataset_overview(_df(spark), _params())
    assert "by_segment" not in ov
