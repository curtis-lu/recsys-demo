## ADDED Requirements

### Requirement: Spark select_sample_keys node
The system SHALL provide a function `select_sample_keys(label_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that performs stratified sampling using PySpark Window functions. The function signature and semantics SHALL be identical to the pandas version.

#### Scenario: Stratified sampling via Window functions
- **WHEN** label_table is a Spark DataFrame with 3 snap_dates and sample_ratio=0.5 and sample_group_keys=["snap_date"]
- **THEN** the function SHALL use `Window.partitionBy(*group_keys).orderBy(F.rand(seed))` with `row_number()` to sample approximately 50% from each group

#### Scenario: Full ratio skips Window computation
- **WHEN** sample_ratio is 1.0
- **THEN** the function SHALL return all unique (snap_date, cust_id) keys without applying Window-based sampling

#### Scenario: Output schema matches pandas version
- **WHEN** select_sample_keys completes
- **THEN** the output Spark DataFrame SHALL contain exactly columns: snap_date, cust_id

#### Scenario: Multiple group keys
- **WHEN** sample_group_keys=["snap_date", "cust_segment_typ"]
- **THEN** sampling SHALL partition by both keys, maintaining proportional representation within each group

### Requirement: Spark split_keys node
The system SHALL provide a function `split_keys(sample_keys, label_table, parameters) -> tuple[DataFrame, DataFrame, DataFrame]` in `nodes_spark.py` that splits Spark DataFrames into train, train_dev, val sets using PySpark filter operations.

#### Scenario: Filter by date sets
- **WHEN** sample_keys is a Spark DataFrame and parameters specify train_dev_snap_dates and val_snap_dates
- **THEN** the function SHALL use `F.col("snap_date").isin(dates)` to split keys into three non-overlapping sets

#### Scenario: Val uses full population
- **WHEN** split_keys is called with sample_ratio < 1.0
- **THEN** val_keys SHALL contain ALL unique (snap_date, cust_id) from label_table for val dates, not limited to sample_keys

### Requirement: Spark build_dataset node
The system SHALL provide a function `build_dataset(keys, feature_table, label_table) -> pyspark.sql.DataFrame` in `nodes_spark.py` that joins Spark DataFrames using `.join()`.

#### Scenario: Join operations
- **WHEN** build_dataset is called with Spark DataFrames
- **THEN** it SHALL perform `.join(label_table, on=["snap_date", "cust_id"], how="inner")` then `.join(feature_table, on=["snap_date", "cust_id"], how="left")`

#### Scenario: Output contains all expected columns
- **WHEN** build_dataset completes
- **THEN** the output SHALL contain feature columns, label, prod_name, snap_date, cust_id

### Requirement: Spark prepare_model_input node
The system SHALL provide a function `prepare_model_input(train_set, train_dev_set, val_set, parameters) -> tuple` in `nodes_spark.py` that converts Spark DataFrames to pandas/numpy for LightGBM.

#### Scenario: Spark to pandas conversion
- **WHEN** prepare_model_input receives three Spark DataFrames
- **THEN** it SHALL call `.toPandas()` on each, then apply the same category encoding logic as the pandas version

#### Scenario: Output format identical to pandas version
- **WHEN** prepare_model_input completes
- **THEN** it SHALL return (X_train: pd.DataFrame, y_train: np.ndarray, X_train_dev: pd.DataFrame, y_train_dev: np.ndarray, X_val: pd.DataFrame, y_val: np.ndarray, preprocessor: dict, category_mappings: dict)
