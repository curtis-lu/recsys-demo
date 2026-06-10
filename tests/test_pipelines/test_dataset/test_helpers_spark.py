import pandas as pd
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys


def _params(carry=None, group_keys=None):
    p = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "dataset": {
            "sample_group_keys": group_keys or ["cust_segment_typ", "prod_name", "label"],
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


class TestSelectKeysOverrideLookup:
    """Broadcast-join override lookup. Using ratios in {0.0, 1.0} makes the
    crc32 bucket irrelevant (threshold 0 drops all, threshold HASH_BUCKETS keeps
    all), so these assert the group_key -> ratio mapping itself — the part the
    broadcast join replaced. In _pool: custs 1 & 3 are (mass, a, label=1),
    custs 2 & 4 are (hnw, b, 0) and (aff, b, 0).
    """

    @staticmethod
    def _survivors(df):
        return {r["cust_id"] for r in df.collect()}

    def test_override_zero_drops_only_that_group(self, spark):
        # default 1.0 keeps all; the single override drops just its group.
        df = select_keys(_pool(spark), _params(),
                         [pd.Timestamp("2025-01-31")], 1.0, {"mass|a|1": 0.0})
        assert self._survivors(df) == {2, 4}

    def test_unmatched_falls_back_to_default(self, spark):
        # default 0.0 drops all; only the overridden group survives (coalesce
        # fallback path for the unmatched rows).
        df = select_keys(_pool(spark), _params(),
                         [pd.Timestamp("2025-01-31")], 0.0, {"mass|a|1": 1.0})
        assert self._survivors(df) == {1, 3}

    def test_multiple_overrides(self, spark):
        # two groups overridden in one lookup table; default 1.0 keeps the rest.
        df = select_keys(_pool(spark), _params(),
                         [pd.Timestamp("2025-01-31")], 1.0,
                         {"mass|a|1": 0.0, "aff|b|0": 0.0})
        assert self._survivors(df) == {2}

    def test_single_group_key_no_concat(self, spark):
        # group_keys == [label] exercises the len==1 (no concat_ws) path; the
        # override key is the string-cast label value.
        df = select_keys(_pool(spark), _params(group_keys=["label"]),
                         [pd.Timestamp("2025-01-31")], 1.0, {"0": 0.0})
        assert self._survivors(df) == {1, 3}
