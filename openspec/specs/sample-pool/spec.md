## ADDED Requirements

### Requirement: Sample pool as independent catalog dataset
The system SHALL support a `sample_pool` dataset defined in `catalog.yaml` as a ParquetDataset, containing at minimum `cust_id`, `snap_date`, and stratification columns (e.g., `cust_segment_typ`).

#### Scenario: Sample pool registered in catalog
- **WHEN** the dataset pipeline is loaded
- **THEN** `sample_pool` SHALL be available as a registered dataset in the DataCatalog

#### Scenario: Sample pool contains required columns
- **WHEN** `sample_pool` is loaded
- **THEN** it SHALL contain at least the identity key columns (`cust_id`, `snap_date`) and the configured `sample_group_keys` columns

### Requirement: select_sample_keys accepts sample_pool
The `select_sample_keys` node function SHALL accept `sample_pool` (instead of `label_table`) as its primary input for stratified sampling.

#### Scenario: Sampling from sample_pool (pandas)
- **WHEN** `select_sample_keys(sample_pool, parameters)` is called with pandas backend
- **THEN** it SHALL perform stratified sampling on `sample_pool` using `sample_group_keys` and `sample_ratio`, returning unique identity keys

#### Scenario: Sampling from sample_pool (spark)
- **WHEN** `select_sample_keys(sample_pool, parameters)` is called with spark backend
- **THEN** it SHALL perform the same stratified sampling logic on the Spark DataFrame

### Requirement: Pipeline wiring uses sample_pool
The dataset building pipeline SHALL wire `sample_pool` as the input to the `select_sample_keys` node (replacing `label_table`).

#### Scenario: Pipeline input declaration
- **WHEN** the dataset pipeline is created
- **THEN** the `select_sample_keys` node's inputs SHALL include `sample_pool` and `parameters`

### Requirement: Synthetic data includes sample_pool
`scripts/generate_synthetic_data.py` SHALL generate a `sample_pool.parquet` file alongside existing synthetic data files.

#### Scenario: Sample pool generated
- **WHEN** `generate_synthetic_data.py` is executed
- **THEN** it SHALL produce `data/sample_pool.parquet` with columns `snap_date`, `cust_id`, `cust_segment_typ`

#### Scenario: Sample pool aligned with other data
- **WHEN** `sample_pool.parquet` is generated
- **THEN** its `cust_id` and `snap_date` values SHALL be consistent with those in `feature_table.parquet` and `label_table.parquet`
