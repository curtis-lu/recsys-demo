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
