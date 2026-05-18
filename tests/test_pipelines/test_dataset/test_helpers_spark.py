import pandas as pd
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys


def _params(carry=None):
    p = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "dataset": {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio": 1.0},
        "random_seed": 42}
    if carry is not None:
        p["dataset"]["carry_columns"] = carry
    return p


def _pool(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "cust_id": [1, 2, 3, 4],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
        "label": [1, 0, 1, 0]}))


class TestSelectKeysCarry:
    def test_carry_present_no_sampling_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0, {})
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name",
                                   "cust_segment_typ"}

    def test_carry_present_overrides_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0, {"mass|a|1": 1.0})
        assert "cust_segment_typ" in df.columns

    def test_no_carry_returns_identity_only(self, spark):
        df = select_keys(_pool(spark), _params(),
                         [pd.Timestamp("2025-01-31")], 1.0, {})
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name"}
