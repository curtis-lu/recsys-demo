## ADDED Requirements

### Requirement: select_sample_keys node
The system SHALL provide a pure function `select_sample_keys(label_table: DataFrame, parameters: dict) -> DataFrame` that performs stratified sampling on the label table by configurable group keys (from `parameters["dataset"]["sample_group_keys"]`), returning a DataFrame of unique (snap_date, cust_id) keys.

#### Scenario: Stratified sampling by snap_date (default)
- **WHEN** label_table has 3 snap_dates with 1000, 800, 600 customers and sample_ratio=0.5 and sample_group_keys=["snap_date"]
- **THEN** returned keys SHALL contain approximately 500, 400, 300 customers per snap_date

#### Scenario: Stratified sampling by multiple keys
- **WHEN** sample_group_keys=["snap_date", "cust_segment_typ"] and label_table has customers across multiple segments
- **THEN** sampling SHALL be performed within each (snap_date, cust_segment_typ) group, maintaining proportional representation

#### Scenario: Output contains only key columns
- **WHEN** select_sample_keys is called with any sample_group_keys configuration
- **THEN** the output DataFrame SHALL contain only columns: snap_date, cust_id (deduplicated)

#### Scenario: Deterministic with seed
- **WHEN** select_sample_keys is called twice with the same parameters (including random_seed)
- **THEN** both outputs SHALL be identical

### Requirement: split_keys node
The system SHALL provide a pure function `split_keys(sample_keys: DataFrame, label_table: DataFrame, parameters: dict) -> tuple[DataFrame, DataFrame, DataFrame]` that splits keys into three non-overlapping sets: train (in-time, sampled), train_dev (out-of-time, sampled), and val (out-of-time, full population).

#### Scenario: Three-way temporal split
- **WHEN** sample_keys has snap_dates [2024-01 through 2024-12], train_dev_snap_dates=["2024-11"], val_snap_dates=["2024-12"]
- **THEN** train_keys SHALL contain sampled keys with snap_date in 2024-01 through 2024-10, train_dev_keys SHALL contain sampled keys with snap_date 2024-11, val_keys SHALL contain ALL keys (unsampled) with snap_date 2024-12

#### Scenario: val is full population
- **WHEN** split_keys is called with sample_ratio < 1.0
- **THEN** val_keys SHALL contain ALL unique (snap_date, cust_id) pairs from label_table for val_snap_dates, not limited to sample_keys

#### Scenario: Return format
- **WHEN** split_keys is called
- **THEN** it SHALL return a tuple of three DataFrames (train_keys, train_dev_keys, val_keys), each with columns snap_date, cust_id

#### Scenario: No date overlap
- **WHEN** split_keys is called
- **THEN** the snap_dates in train_keys, train_dev_keys, and val_keys SHALL be mutually exclusive

### Requirement: build_dataset node
The system SHALL provide a pure function `build_dataset(keys: DataFrame, feature_table: DataFrame, label_table: DataFrame) -> DataFrame` that joins keys with features and labels.

#### Scenario: Inner join on keys
- **WHEN** build_dataset is called with keys containing 100 (snap_date, cust_id) pairs
- **THEN** output SHALL contain only rows matching those keys, joined with corresponding features and labels

#### Scenario: Output schema
- **WHEN** build_dataset is called
- **THEN** output SHALL contain all feature columns, label column, prod_name column, snap_date column, and cust_id column

#### Scenario: Handle missing features
- **WHEN** a key exists in label_table but not in feature_table
- **THEN** feature columns SHALL be filled with NaN (left join behavior from keys+labels to features)

### Requirement: prepare_model_input node
The system SHALL provide a pure function `prepare_model_input(train_set: DataFrame, train_dev_set: DataFrame, val_set: DataFrame, parameters: dict) -> tuple` that converts three DataFrames to model-ready arrays.

#### Scenario: Output format
- **WHEN** prepare_model_input is called
- **THEN** it SHALL return: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, category_mappings

#### Scenario: Categorical encoding of prod_name
- **WHEN** prepare_model_input is called
- **THEN** prod_name SHALL be encoded as integer category codes in X_train, X_train_dev, and X_val using the same mapping derived from train_set only

#### Scenario: Label extraction
- **WHEN** prepare_model_input is called
- **THEN** y_train, y_train_dev, and y_val SHALL be 1D arrays containing the label column values

#### Scenario: Feature columns exclude non-feature columns
- **WHEN** prepare_model_input is called
- **THEN** X_train, X_train_dev, and X_val SHALL NOT contain columns: snap_date, cust_id, label, apply_start_date, apply_end_date, cust_segment_typ

#### Scenario: Preprocessor records transformation state
- **WHEN** prepare_model_input is called
- **THEN** preprocessor SHALL contain at minimum: feature_columns (list), categorical_columns (list), category_mappings (dict), drop_columns (list)

#### Scenario: category_mappings returned separately
- **WHEN** prepare_model_input is called
- **THEN** category_mappings SHALL be returned as a separate dict output (identical to preprocessor["category_mappings"]) for independent JSON persistence


## MODIFIED Requirements

### Requirement: Column names are configurable
All dataset pipeline nodes (select_sample_keys, split_keys, build_dataset, prepare_model_input) SHALL obtain column names (time, entity, item, label) from `get_schema(parameters)` instead of using hard-coded strings. The `build_dataset` node SHALL accept `parameters` as an additional input.

#### Scenario: Default column names match current behavior
- **WHEN** nodes are called with parameters that have no `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation (snap_date, cust_id, prod_name, label)

#### Scenario: Custom column names propagate through pipeline
- **WHEN** `schema.columns.time` is set to `"month_end"` and `schema.columns.entity` is set to `["branch_id", "cust_id"]`
- **THEN** all nodes SHALL use `month_end` as the time column and `["branch_id", "cust_id"]` as the entity columns for joins, groupby, and filtering
