## MODIFIED Requirements

### Requirement: ParquetDataset supports pandas backend

ParquetDataset with `backend="pandas"` SHALL read and write Parquet files using pandas/pyarrow.

#### Scenario: Load parquet as pandas DataFrame
- **WHEN** ParquetDataset is configured with `backend="pandas"` and a valid file path
- **THEN** `load()` returns a `pandas.DataFrame`

#### Scenario: Save pandas DataFrame as parquet
- **WHEN** `save()` is called with a `pandas.DataFrame`
- **THEN** a Parquet file is written to the configured path

#### Scenario: Save Spark DataFrame with pandas backend
- **WHEN** `save()` is called with a `pyspark.sql.DataFrame` on a ParquetDataset with `backend="pandas"`
- **THEN** the dataset SHALL automatically call `.toPandas()` and save as pandas Parquet

#### Scenario: Check existence
- **WHEN** `exists()` is called
- **THEN** returns `True` if the Parquet file/directory exists, `False` otherwise

### Requirement: ParquetDataset supports PySpark backend

ParquetDataset with `backend="spark"` SHALL read and write Parquet files using PySpark.

#### Scenario: Load parquet as Spark DataFrame
- **WHEN** ParquetDataset is configured with `backend="spark"` and a valid file path
- **THEN** `load()` returns a `pyspark.sql.DataFrame`

#### Scenario: Save Spark DataFrame as parquet
- **WHEN** `save()` is called with a `pyspark.sql.DataFrame`
- **THEN** Parquet files are written to the configured path (overwrite mode)

#### Scenario: Save pandas DataFrame with spark backend
- **WHEN** `save()` is called with a `pandas.DataFrame` on a ParquetDataset with `backend="spark"`
- **THEN** the dataset SHALL automatically call `spark.createDataFrame(data)` and save as Spark Parquet
