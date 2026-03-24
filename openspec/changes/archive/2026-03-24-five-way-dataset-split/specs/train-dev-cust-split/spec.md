## ADDED Requirements

### Requirement: train_dev_ratio parameter
The system SHALL support a `train_dev_ratio` parameter (float, 0.0-1.0) in `parameters_dataset.yaml` under the `dataset` section, specifying the fraction of sampled cust_ids allocated to the train-dev split.

#### Scenario: Default ratio
- **WHEN** `train_dev_ratio` is set to `0.2`
- **THEN** approximately 20% of unique sampled cust_ids SHALL be assigned to train-dev and 80% to train

### Requirement: split_train_keys node
The system SHALL provide a pure function `split_train_keys(sample_keys: DataFrame, parameters: dict) -> tuple[DataFrame, DataFrame]` that splits sampled identity keys into train and train-dev by cust_id ratio.

#### Scenario: Split by cust_id, not by row
- **WHEN** split_train_keys is called with sample_keys containing 1000 unique cust_ids across 3 snap_dates and `train_dev_ratio` is 0.2
- **THEN** approximately 200 unique cust_ids SHALL be assigned to train-dev and 800 to train, and each cust_id SHALL appear in the same split across ALL snap_dates

#### Scenario: No cust_id overlap between splits
- **WHEN** split_train_keys is called
- **THEN** the set of cust_ids in train_keys and train_dev_keys SHALL be completely disjoint

#### Scenario: Same snap_dates in both splits
- **WHEN** split_train_keys is called
- **THEN** train_keys and train_dev_keys SHALL contain the same set of snap_dates (both derived from the same date range)

#### Scenario: Output contains only identity columns
- **WHEN** split_train_keys is called
- **THEN** both output DataFrames SHALL contain only columns defined by identity_key (e.g., snap_date, cust_id)

#### Scenario: Deterministic with seed
- **WHEN** split_train_keys is called twice with the same `random_seed`
- **THEN** both outputs SHALL be identical

#### Scenario: All sampled keys preserved
- **WHEN** split_train_keys is called
- **THEN** the union of train_keys and train_dev_keys SHALL equal the input sample_keys (no rows lost)
