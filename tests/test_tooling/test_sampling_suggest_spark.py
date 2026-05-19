import pandas as pd

from recsys_tfb.tooling.sampling_suggest import profile_stats


class TestProfileStats:
    def test_groups_by_segment_product_and_counts_pos_neg(self, spark):
        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a", "a", "a", "b", "b", "b"],
            "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
            "label": [1, 0, 0, 1, 1, 0],
        }))
        stats = profile_stats(
            df, [pd.Timestamp("2025-01-31")],
            segment_col="cust_segment_typ", item_col="prod_name",
            label_col="label", time_col="snap_date")
        d = {(s, p): (np_, nn_) for (s, p, np_, nn_) in stats}
        assert d[("mass", "a")] == (1, 2)
        assert d[("hnw", "b")] == (2, 1)
