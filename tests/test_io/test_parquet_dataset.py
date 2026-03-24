import os

import pandas as pd
import pytest

from recsys_tfb.io.parquet_dataset import ParquetDataset


class TestParquetDatasetPandas:
    def test_save_and_load(self, tmp_path):
        filepath = str(tmp_path / "test.parquet")
        ds = ParquetDataset(filepath=filepath, backend="pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ds.save(df)
        loaded = ds.load()
        pd.testing.assert_frame_equal(df, loaded)

    def test_save_and_load_partitioned(self, tmp_path):
        filepath = str(tmp_path / "partitioned")
        ds = ParquetDataset(filepath=filepath, backend="pandas", partition_cols=["b"])
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
        ds.save(df)
        loaded = ds.load()
        # pyarrow reads partition cols as categorical, compare values
        assert set(loaded.columns) == {"a", "b"}
        assert len(loaded) == 3
        assert set(loaded["b"]) == {"x", "y"}

    def test_exists(self, tmp_path):
        filepath = str(tmp_path / "test.parquet")
        ds = ParquetDataset(filepath=filepath, backend="pandas")
        assert ds.exists() is False
        ds.save(pd.DataFrame({"a": [1]}))
        assert ds.exists() is True

    def test_invalid_backend(self):
        with pytest.raises(ValueError, match="backend must be"):
            ParquetDataset(filepath="/tmp/x.parquet", backend="invalid")


@pytest.mark.spark
class TestParquetDatasetSpark:
    def test_save_and_load(self, spark, tmp_path):
        filepath = str(tmp_path / "spark_test.parquet")
        ds = ParquetDataset(filepath=filepath, backend="spark")
        df = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
        ds.save(df)
        loaded = ds.load()
        assert loaded.count() == 2
        assert set(loaded.columns) == {"a", "b"}

    def test_exists(self, spark, tmp_path):
        filepath = str(tmp_path / "spark_test.parquet")
        ds = ParquetDataset(filepath=filepath, backend="spark")
        assert ds.exists() is False
        df = spark.createDataFrame([(1,)], ["a"])
        ds.save(df)
        assert ds.exists() is True

    def test_save_pandas_to_spark_backend(self, spark, tmp_path):
        """Saving a pandas DataFrame with spark backend should auto-convert."""
        filepath = str(tmp_path / "auto_convert.parquet")
        ds = ParquetDataset(filepath=filepath, backend="spark")
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ds.save(pdf)
        loaded = ds.load()
        assert loaded.count() == 3
        assert set(loaded.columns) == {"a", "b"}

    def test_save_spark_to_pandas_backend(self, spark, tmp_path):
        """Saving a Spark DataFrame with pandas backend should auto-convert."""
        filepath = str(tmp_path / "auto_convert.parquet")
        ds = ParquetDataset(filepath=filepath, backend="pandas")
        sdf = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
        ds.save(sdf)
        loaded = ds.load()
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 2

    def test_save_and_load_partitioned(self, spark, tmp_path):
        filepath = str(tmp_path / "partitioned")
        ds = ParquetDataset(filepath=filepath, backend="spark", partition_cols=["b"])
        sdf = spark.createDataFrame([(1, "x"), (2, "y"), (3, "x")], ["a", "b"])
        ds.save(sdf)
        loaded = ds.load()
        assert set(loaded.columns) == {"a", "b"}
        assert loaded.count() == 3
