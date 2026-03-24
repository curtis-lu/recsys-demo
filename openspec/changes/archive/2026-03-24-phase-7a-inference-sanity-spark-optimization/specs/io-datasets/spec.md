## MODIFIED Requirements

### Requirement: ParquetDataset supports partitioned writes
ParquetDataset SHALL accept an optional `partition_cols` parameter. When provided, the `save` method SHALL write data using partition columns.

#### Scenario: Spark backend with partition_cols
- **WHEN** saving a Spark DataFrame with partition_cols configured
- **THEN** the system writes using `partitionBy(*partition_cols).parquet(filepath)`

#### Scenario: Pandas backend with partition_cols
- **WHEN** saving a pandas DataFrame with partition_cols configured
- **THEN** the system writes using pyarrow `write_to_dataset` with partition_cols

#### Scenario: No partition_cols (backward compatible)
- **WHEN** partition_cols is None or not provided
- **THEN** the system writes as a single Parquet file, same as before

#### Scenario: Reading partitioned data
- **WHEN** loading data written with partition_cols
- **THEN** both pandas (pyarrow) and Spark backends automatically read the partitioned directory structure

#### Scenario: Catalog YAML configuration
- **WHEN** catalog.yaml includes `partition_cols: [col1, col2]` for a ParquetDataset entry
- **THEN** DataCatalog passes partition_cols to the ParquetDataset constructor via `cls(**entry)`
