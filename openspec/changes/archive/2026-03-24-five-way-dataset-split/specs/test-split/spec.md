## ADDED Requirements

### Requirement: test_snap_dates parameter
The system SHALL support a `test_snap_dates` parameter (list of date strings) in `parameters_dataset.yaml` under the `dataset` section.

#### Scenario: Test dates specified
- **WHEN** `test_snap_dates` is `["2025-12-31"]`
- **THEN** test keys SHALL be drawn from rows with snap_date matching the specified dates

#### Scenario: Test dates non-overlapping with other splits
- **WHEN** `test_snap_dates` overlaps with `calibration_snap_dates` or `val_snap_dates`
- **THEN** the system SHALL raise a `ValueError` during date validation

### Requirement: select_test_keys node
The system SHALL provide a pure function `select_test_keys(label_table: DataFrame, parameters: dict) -> DataFrame` that selects test identity keys from the full population.

#### Scenario: Full population, no sampling
- **WHEN** select_test_keys is called
- **THEN** output SHALL contain ALL unique identity keys from label_table for test_snap_dates, with no sampling applied

#### Scenario: Output contains only identity columns
- **WHEN** select_test_keys is called
- **THEN** the output DataFrame SHALL contain only columns defined by identity_key (e.g., snap_date, cust_id)

### Requirement: Test catalog entries
The system SHALL define catalog entries for test artifacts: `test_keys`, `test_set`, `X_test`, `y_test`, all under `data/dataset/${dataset_version}/`.

#### Scenario: Catalog entries exist
- **WHEN** the catalog is loaded
- **THEN** entries `test_keys`, `test_set`, `X_test`, `y_test` SHALL be defined with paths under `data/dataset/${dataset_version}/`
