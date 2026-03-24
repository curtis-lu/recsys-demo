import os

from recsys_tfb.io.base import AbstractDataset


class ParquetDataset(AbstractDataset):
    """Dataset for reading and writing Parquet files.

    Supports pandas and PySpark backends, selected via the ``backend`` parameter.
    Supports partitioned writes via the ``partition_cols`` parameter.
    """

    def __init__(
        self,
        filepath: str,
        backend: str = "pandas",
        partition_cols: list[str] | None = None,
    ):
        if backend not in ("pandas", "spark"):
            raise ValueError(f"backend must be 'pandas' or 'spark', got '{backend}'")
        self._filepath = filepath
        self._backend = backend
        self._partition_cols = partition_cols

    def load(self):
        if self._backend == "pandas":
            import pandas as pd

            return pd.read_parquet(self._filepath)
        else:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            return spark.read.parquet(self._filepath)

    def save(self, data) -> None:
        if self._backend == "pandas":
            if hasattr(data, "toPandas"):
                data = data.toPandas()
            os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
            if self._partition_cols:
                import pyarrow as pa
                import pyarrow.parquet as pq

                table = pa.Table.from_pandas(data)
                pq.write_to_dataset(
                    table, self._filepath, partition_cols=self._partition_cols
                )
            else:
                data.to_parquet(self._filepath, index=False)
        else:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                data = spark.createDataFrame(data)
            writer = data.write.mode("overwrite")
            if self._partition_cols:
                writer = writer.partitionBy(*self._partition_cols)
            writer.parquet(self._filepath)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
