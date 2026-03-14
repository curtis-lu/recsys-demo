## ADDED Requirements

### Requirement: select_sample_keys node
The system SHALL provide a pure function `select_sample_keys(label_table: DataFrame, parameters: dict) -> DataFrame` that performs stratified sampling on the label table by snap_date, returning a DataFrame of unique (snap_date, cust_id) keys.

#### Scenario: Stratified sampling by snap_date
- **WHEN** label_table has 3 snap_dates with 1000, 800, 600 customers and sample_ratio=0.5
- **THEN** returned keys SHALL contain approximately 500, 400, 300 customers per snap_date

#### Scenario: Output contains only key columns
- **WHEN** select_sample_keys is called
- **THEN** the output DataFrame SHALL contain only columns: snap_date, cust_id (deduplicated)

#### Scenario: Deterministic with seed
- **WHEN** select_sample_keys is called twice with the same parameters (including random_seed)
- **THEN** both outputs SHALL be identical

### Requirement: split_keys node
The system SHALL provide a pure function `split_keys(sample_keys: DataFrame, parameters: dict) -> dict` that splits keys into train and validation sets by snap_date temporal boundary.

#### Scenario: Temporal split
- **WHEN** sample_keys has snap_dates [2024-01, 2024-02, ..., 2024-12] and val_snap_dates=["2024-11", "2024-12"]
- **THEN** train_keys SHALL contain keys with snap_date in 2024-01 through 2024-10, val_keys SHALL contain keys with snap_date in 2024-11 and 2024-12

#### Scenario: Return format
- **WHEN** split_keys is called
- **THEN** it SHALL return a dict with keys "train_keys" and "val_keys", each a DataFrame with columns snap_date, cust_id

#### Scenario: No overlap
- **WHEN** split_keys is called
- **THEN** train_keys and val_keys SHALL have no overlapping (snap_date, cust_id) pairs

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
The system SHALL provide a pure function `prepare_model_input(train_set: DataFrame, val_set: DataFrame, parameters: dict) -> dict` that converts DataFrames to model-ready arrays.

#### Scenario: Output format
- **WHEN** prepare_model_input is called
- **THEN** it SHALL return a dict with keys: X_train, y_train, X_val, y_val, preprocessor

#### Scenario: Categorical encoding of prod_name
- **WHEN** prepare_model_input is called
- **THEN** prod_name SHALL be encoded as integer category codes in X_train and X_val using the same mapping

#### Scenario: Label extraction
- **WHEN** prepare_model_input is called
- **THEN** y_train and y_val SHALL be 1D arrays containing the label column values

#### Scenario: Feature columns exclude non-feature columns
- **WHEN** prepare_model_input is called
- **THEN** X_train and X_val SHALL NOT contain columns: snap_date, cust_id, label, apply_start_date, apply_end_date

#### Scenario: Preprocessor records transformation state
- **WHEN** prepare_model_input is called
- **THEN** preprocessor SHALL contain at minimum: feature_columns (list), categorical_columns (list), category_mappings (dict)
