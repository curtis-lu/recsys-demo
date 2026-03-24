## ADDED Requirements

### Requirement: select_sample_keys node
The system SHALL provide a pure function `select_sample_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame` that performs stratified sampling on the sample pool by configurable group keys, returning unique identity keys. The function SHALL filter sample_pool to train dates only (all dates NOT in calibration_snap_dates, val_snap_dates, or test_snap_dates) before sampling. The function SHALL support `sample_ratio_overrides` for per-group custom ratios.

#### Scenario: Filter to train dates
- **WHEN** sample_pool has snap_dates [2025-01 through 2025-12], calibration_snap_dates=["2025-10-31"], val_snap_dates=["2025-11-30"], test_snap_dates=["2025-12-31"]
- **THEN** only rows with snap_date in 2025-01 through 2025-09 SHALL be considered for sampling

#### Scenario: Stratified sampling with overrides
- **WHEN** `sample_ratio` is 0.5, `sample_group_keys` is `["cust_segment_typ"]`, and `sample_ratio_overrides` is `{"VIP": 1.0}`
- **THEN** VIP customers SHALL be sampled at ratio 1.0 and other segments at ratio 0.5

#### Scenario: Output contains only key columns
- **WHEN** select_sample_keys is called
- **THEN** the output DataFrame SHALL contain only columns defined by identity_key (e.g., snap_date, cust_id)

#### Scenario: Deterministic with seed
- **WHEN** select_sample_keys is called twice with the same parameters (including random_seed)
- **THEN** both outputs SHALL be identical

### Requirement: select_val_keys node
The system SHALL provide a pure function `select_val_keys(label_table: DataFrame, parameters: dict) -> DataFrame` that selects validation identity keys from the full population for val_snap_dates.

#### Scenario: Full population by default
- **WHEN** `val_sample_ratio` is `1.0` or not set
- **THEN** output SHALL contain ALL unique identity keys from label_table for val_snap_dates

#### Scenario: Optional random sampling
- **WHEN** `val_sample_ratio` is `0.5`
- **THEN** approximately 50% of unique cust_ids for val_snap_dates SHALL be randomly sampled (not stratified), and all rows for selected cust_ids SHALL be included

#### Scenario: Sampling is by cust_id
- **WHEN** `val_sample_ratio` is less than 1.0
- **THEN** sampling SHALL be at the cust_id level — all rows for a selected cust_id across all val_snap_dates SHALL be included

#### Scenario: Output contains only identity columns
- **WHEN** select_val_keys is called
- **THEN** the output DataFrame SHALL contain only columns defined by identity_key (e.g., snap_date, cust_id)

#### Scenario: Deterministic with seed
- **WHEN** select_val_keys is called twice with the same `random_seed` and `val_sample_ratio`
- **THEN** both outputs SHALL be identical

### Requirement: Date validation
The first node in the pipeline (`select_sample_keys`) SHALL validate that `calibration_snap_dates`, `val_snap_dates`, and `test_snap_dates` are mutually non-overlapping. If any date appears in more than one list, a `ValueError` SHALL be raised.

#### Scenario: Non-overlapping dates pass
- **WHEN** calibration_snap_dates=["2025-10-31"], val_snap_dates=["2025-11-30"], test_snap_dates=["2025-12-31"]
- **THEN** validation SHALL pass silently

#### Scenario: Overlapping dates fail
- **WHEN** val_snap_dates=["2025-12-31"] and test_snap_dates=["2025-12-31"]
- **THEN** a `ValueError` SHALL be raised with a message indicating the overlapping dates

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
The system SHALL provide a pure function `prepare_model_input(train_set: DataFrame, train_dev_set: DataFrame, val_set: DataFrame, test_set: DataFrame, parameters: dict) -> tuple` that converts four DataFrames to model-ready arrays. When calibration is enabled, a separate function `prepare_model_input_with_calibration` SHALL accept an additional `calibration_set` parameter.

#### Scenario: Output format without calibration
- **WHEN** `prepare_model_input` is called with 4 sets
- **THEN** it SHALL return: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, X_test, y_test, preprocessor, category_mappings (10 outputs)

#### Scenario: Output format with calibration
- **WHEN** `prepare_model_input_with_calibration` is called with 5 sets
- **THEN** it SHALL return: X_train, y_train, X_train_dev, y_train_dev, X_calibration, y_calibration, X_val, y_val, X_test, y_test, preprocessor, category_mappings (12 outputs)

#### Scenario: Categorical encoding from train set only
- **WHEN** prepare_model_input is called
- **THEN** category_mappings SHALL be derived from train_set only, applied consistently to all sets

#### Scenario: val_sample_ratio NOT applied in prepare_model_input
- **WHEN** prepare_model_input is called
- **THEN** it SHALL NOT perform any sampling on val_set (sampling moved to select_val_keys)

#### Scenario: Preprocessor records transformation state
- **WHEN** prepare_model_input is called
- **THEN** preprocessor SHALL contain: feature_columns, categorical_columns, category_mappings, drop_columns

#### Scenario: category_mappings returned separately
- **WHEN** prepare_model_input is called
- **THEN** category_mappings SHALL be returned as a separate dict output (identical to preprocessor["category_mappings"]) for independent JSON persistence


## MODIFIED Requirements

### Requirement: Column names are configurable
All dataset pipeline nodes (select_sample_keys, split_train_keys, select_val_keys, select_test_keys, select_calibration_keys, build_dataset, prepare_model_input) SHALL obtain column names (time, entity, item, label) from `get_schema(parameters)` instead of using hard-coded strings. The `build_dataset` node SHALL accept `parameters` as an additional input.

#### Scenario: Default column names match current behavior
- **WHEN** nodes are called with parameters that have no `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation (snap_date, cust_id, prod_name, label)

#### Scenario: Custom column names propagate through pipeline
- **WHEN** `schema.columns.time` is set to `"month_end"` and `schema.columns.entity` is set to `["branch_id", "cust_id"]`
- **THEN** all nodes SHALL use `month_end` as the time column and `["branch_id", "cust_id"]` as the entity columns for joins, groupby, and filtering
