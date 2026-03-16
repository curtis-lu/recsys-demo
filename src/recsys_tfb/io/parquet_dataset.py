import os

from recsys_tfb.io.base import AbstractDataset


class ParquetDataset(AbstractDataset):
    """Dataset for reading and writing Parquet files.

    Supports pandas and PySpark backends, selected via the ``backend`` parameter.
    """

    def __init__(self, filepath: str, backend: str = "pandas"):
        if backend not in ("pandas", "spark"):
            raise ValueError(f"backend must be 'pandas' or 'spark', got '{backend}'")
        self._filepath = filepath
        self._backend = backend

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
            data.to_parquet(self._filepath, index=False)
        else:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                data = spark.createDataFrame(data)
            data.write.mode("overwrite").parquet(self._filepath)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
