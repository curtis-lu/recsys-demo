## MODIFIED Requirements

### Requirement: select_sample_keys does not trigger unnecessary actions
The `select_sample_keys` function SHALL NOT call `.count()` on Spark DataFrames for logging purposes.

#### Scenario: No count actions in sample key selection
- **WHEN** select_sample_keys completes
- **THEN** no `.count()` action is triggered; log message uses only the sample_ratio and group_keys

### Requirement: split_keys does not trigger unnecessary actions
The `split_keys` function SHALL NOT call `.count()` on train_keys, train_dev_keys, or val_keys for logging purposes.

#### Scenario: No count actions in key splitting
- **WHEN** split_keys completes
- **THEN** no `.count()` action is triggered; log message uses only date lists

### Requirement: build_dataset does not trigger unnecessary actions
The `build_dataset` function SHALL NOT call `.count()` on the result DataFrame for logging purposes.

#### Scenario: No count actions in dataset building
- **WHEN** build_dataset completes
- **THEN** no `.count()` action is triggered; log message uses only column count
