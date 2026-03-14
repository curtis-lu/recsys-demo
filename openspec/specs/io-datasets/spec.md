## ADDED Requirements

### Requirement: AbstractDataset defines load/save/exists interface

AbstractDataset SHALL be an abstract base class defining three abstract methods: `load()`, `save(data)`, and `exists()`. All concrete dataset classes MUST inherit from it.

#### Scenario: Cannot instantiate AbstractDataset directly
- **WHEN** code attempts to instantiate `AbstractDataset()` directly
- **THEN** a `TypeError` is raised

#### Scenario: Subclass must implement all methods
- **WHEN** a subclass of `AbstractDataset` does not implement `load`, `save`, or `exists`
- **THEN** instantiation raises `TypeError`

### Requirement: ParquetDataset supports pandas backend

ParquetDataset with `backend="pandas"` SHALL read and write Parquet files using pandas/pyarrow.

#### Scenario: Load parquet as pandas DataFrame
- **WHEN** ParquetDataset is configured with `backend="pandas"` and a valid file path
- **THEN** `load()` returns a `pandas.DataFrame`

#### Scenario: Save pandas DataFrame as parquet
- **WHEN** `save()` is called with a `pandas.DataFrame`
- **THEN** a Parquet file is written to the configured path

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

### Requirement: PickleDataset for arbitrary Python objects

PickleDataset SHALL serialize and deserialize Python objects using pickle.

#### Scenario: Save and load a model object
- **WHEN** `save()` is called with a Python object (e.g., a trained model) and then `load()` is called
- **THEN** the loaded object is equivalent to the saved object

#### Scenario: Check existence of pickle file
- **WHEN** `exists()` is called on a PickleDataset
- **THEN** returns `True` if the pickle file exists, `False` otherwise
