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


# --- existing_snap_date_partitions: metastore listing (no Spark needed) ---

from unittest.mock import MagicMock

from recsys_tfb.pipelines.dataset.helpers_spark import existing_snap_date_partitions


def _spark_returning(*partition_specs, raises=None):
    """SparkSession stub whose SHOW PARTITIONS yields the given spec strings."""
    spark = MagicMock()
    if raises is not None:
        spark.sql.side_effect = raises
        return spark
    spark.sql.return_value.collect.return_value = [(s,) for s in partition_specs]
    return spark


class TestExistingSnapDatePartitions:
    def test_returns_only_the_requested_base_version(self):
        # A stale month under a DIFFERENT base version must not be reported as
        # existing — that would silently skip a month that never landed here.
        spark = _spark_returning(
            "base_dataset_version=aaa11111/snap_date=2026-01-31",
            "base_dataset_version=bbb22222/snap_date=2026-02-28",
            "base_dataset_version=aaa11111/snap_date=2026-03-31",
        )
        assert existing_snap_date_partitions(spark, "db", "t", "aaa11111") == [
            "2026-01-31", "2026-03-31",
        ]

    def test_multi_level_partitions_deduplicate_to_months(self):
        # test_model_input is partitioned by snap_date AND prod_name.
        spark = _spark_returning(
            "base_dataset_version=aaa11111/snap_date=2026-01-31/prod_name=fund_stock",
            "base_dataset_version=aaa11111/snap_date=2026-01-31/prod_name=exchange_fx",
        )
        assert existing_snap_date_partitions(spark, "db", "t", "aaa11111") == [
            "2026-01-31",
        ]

    def test_escaped_partition_values_do_not_break_parsing(self):
        spark = _spark_returning(
            "base_dataset_version=aaa11111/snap_date=2026-01-31/prod_name=A%2FB%20C",
        )
        assert existing_snap_date_partitions(spark, "db", "t", "aaa11111") == [
            "2026-01-31",
        ]

    def test_hive_default_partition_sentinel_is_dropped_not_propagated(self):
        # Hive writes NULL partition values as __HIVE_DEFAULT_PARTITION__.
        # Propagating it blows up later inside pd.Timestamp() with a message
        # that names neither the table nor the column.
        spark = _spark_returning(
            "base_dataset_version=aaa11111/snap_date=__HIVE_DEFAULT_PARTITION__",
            "base_dataset_version=aaa11111/snap_date=2026-01-31",
        )
        assert existing_snap_date_partitions(spark, "db", "t", "aaa11111") == [
            "2026-01-31",
        ]

    def test_missing_table_or_unpartitioned_table_reads_as_nothing_landed(self):
        from pyspark.sql.utils import AnalysisException

        # PySpark 3.3.2 asserts desc and stackTrace are both non-None.
        spark = _spark_returning(raises=AnalysisException("Table not found", ""))
        assert existing_snap_date_partitions(spark, "db", "t", "aaa11111") == []

    def test_honours_a_non_default_time_column(self):
        spark = _spark_returning(
            "base_dataset_version=aaa11111/as_of_date=2026-01-31",
        )
        assert existing_snap_date_partitions(
            spark, "db", "t", "aaa11111", time_col="as_of_date",
        ) == ["2026-01-31"]
