## REMOVED Requirements

### Requirement: Spark split_keys node
**Reason**: Replaced by `split_train_keys` (train-dev-cust-split), `select_calibration_keys` (calibration-split), `select_val_keys`, and `select_test_keys` (test-split).
**Migration**: Use new node functions that mirror the pandas implementations.

## ADDED Requirements

### Requirement: Spark split_train_keys node
The system SHALL provide a function `split_train_keys(sample_keys: pyspark.sql.DataFrame, parameters: dict) -> tuple[DataFrame, DataFrame]` in `nodes_spark.py` that splits sampled keys into train and train-dev by cust_id ratio, mirroring the pandas implementation.

#### Scenario: Split by cust_id in Spark
- **WHEN** sample_keys is a Spark DataFrame and `train_dev_ratio` is 0.2
- **THEN** approximately 20% of unique cust_ids SHALL be assigned to train-dev, with all snap_date rows for each cust_id in the same split

#### Scenario: No unnecessary actions
- **WHEN** split_train_keys completes
- **THEN** no `.count()` action SHALL be triggered for logging

### Requirement: Spark select_calibration_keys node
The system SHALL provide a function `select_calibration_keys(sample_pool: pyspark.sql.DataFrame, label_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that selects calibration keys with stratified sampling.

#### Scenario: Filter and sample in Spark
- **WHEN** `calibration_sample_ratio` is 0.5 and `calibration_snap_dates` is specified
- **THEN** the function SHALL filter to calibration dates and perform stratified sampling using Window functions

### Requirement: Spark select_val_keys node
The system SHALL provide a function `select_val_keys(label_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that selects validation keys with optional random sampling.

#### Scenario: Full population in Spark
- **WHEN** `val_sample_ratio` is 1.0
- **THEN** output SHALL contain all unique identity keys for val_snap_dates

#### Scenario: Random sampling in Spark
- **WHEN** `val_sample_ratio` is 0.5
- **THEN** approximately 50% of unique cust_ids SHALL be randomly sampled

### Requirement: Spark select_test_keys node
The system SHALL provide a function `select_test_keys(label_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that selects test keys (full population, no sampling).

#### Scenario: Full population, no sampling
- **WHEN** select_test_keys is called
- **THEN** output SHALL contain ALL unique identity keys for test_snap_dates

## MODIFIED Requirements

### Requirement: Spark select_sample_keys node
The system SHALL provide a function `select_sample_keys(sample_pool: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that performs stratified sampling using PySpark Window functions. The function SHALL filter sample_pool to train dates only (excluding calibration/val/test dates) and support `sample_ratio_overrides`.

#### Scenario: Filter to train dates in Spark
- **WHEN** sample_pool has 12 months and val_snap_dates + test_snap_dates cover 2 months
- **THEN** only the remaining 10 months SHALL be considered for sampling

#### Scenario: Per-group overrides in Spark
- **WHEN** `sample_ratio_overrides` has `{"VIP": 1.0}` and `sample_ratio` is 0.5
- **THEN** VIP rows SHALL use ratio 1.0, others SHALL use 0.5

#### Scenario: No unnecessary actions
- **WHEN** select_sample_keys completes
- **THEN** no `.count()` action SHALL be triggered for logging

### Requirement: Spark prepare_model_input node
The system SHALL provide functions `prepare_model_input` (4-set) and `prepare_model_input_with_calibration` (5-set) in `nodes_spark.py` that convert Spark DataFrames to pandas/numpy for model training.

#### Scenario: Output format without calibration
- **WHEN** `prepare_model_input` is called with 4 Spark DataFrames (train, train_dev, val, test)
- **THEN** it SHALL return 10 outputs: X/y for each set + preprocessor + category_mappings

#### Scenario: Output format with calibration
- **WHEN** `prepare_model_input_with_calibration` is called with 5 Spark DataFrames
- **THEN** it SHALL return 12 outputs: X/y for each set + preprocessor + category_mappings
