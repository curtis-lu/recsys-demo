### Requirement: enable_calibration parameter
The system SHALL support an `enable_calibration` parameter (boolean) in `parameters_dataset.yaml` under the `dataset` section, with a default value of `false`.

#### Scenario: Calibration disabled by default
- **WHEN** `enable_calibration` is not set or set to `false`
- **THEN** the dataset pipeline SHALL NOT include calibration-related nodes and SHALL NOT produce calibration outputs

#### Scenario: Calibration enabled
- **WHEN** `enable_calibration` is set to `true`
- **THEN** the dataset pipeline SHALL include `select_calibration_keys`, `build_calibration_dataset`, and produce X_calibration, y_calibration outputs

### Requirement: calibration_snap_dates parameter
The system SHALL support a `calibration_snap_dates` parameter (list of date strings) in `parameters_dataset.yaml` under the `dataset` section, with a default value of `[]`.

#### Scenario: Calibration dates specified
- **WHEN** `calibration_snap_dates` is `["2025-10-31"]`
- **THEN** calibration keys SHALL be drawn from rows with snap_date matching the specified dates

#### Scenario: Calibration dates non-overlapping with other splits
- **WHEN** `calibration_snap_dates` overlaps with `val_snap_dates` or `test_snap_dates`
- **THEN** the system SHALL raise a `ValueError` during date validation

### Requirement: calibration_sample_ratio parameter
The system SHALL support a `calibration_sample_ratio` parameter (float, 0.0-1.0) in `parameters_dataset.yaml` under the `dataset` section, with a default value of `1.0`.

#### Scenario: Full calibration population
- **WHEN** `calibration_sample_ratio` is `1.0`
- **THEN** all customers in calibration dates SHALL be included

#### Scenario: Partial calibration sampling
- **WHEN** `calibration_sample_ratio` is `0.5`
- **THEN** approximately 50% of customers in calibration dates SHALL be sampled, stratified by `sample_group_keys`

### Requirement: select_calibration_keys node
The system SHALL provide a pure function `select_calibration_keys(sample_pool: DataFrame, label_table: DataFrame, parameters: dict) -> DataFrame` that selects calibration identity keys.

#### Scenario: Filter to calibration dates
- **WHEN** `calibration_snap_dates` is `["2025-10-31"]`
- **THEN** output SHALL contain only rows with snap_date `2025-10-31`

#### Scenario: Stratified sampling by sample_group_keys
- **WHEN** `calibration_sample_ratio` is `0.5` and `sample_group_keys` is `["cust_segment_typ"]`
- **THEN** sampling SHALL be stratified by cust_segment_typ

#### Scenario: Output contains only identity columns
- **WHEN** select_calibration_keys is called
- **THEN** the output DataFrame SHALL contain only columns defined by identity_key (e.g., snap_date, cust_id)

#### Scenario: Deterministic with seed
- **WHEN** select_calibration_keys is called twice with the same `random_seed`
- **THEN** both outputs SHALL be identical

### Requirement: Calibration catalog entries
The system SHALL define catalog entries for calibration artifacts: `calibration_keys`, `calibration_set`, `X_calibration`, `y_calibration`, all under `data/dataset/${dataset_version}/`.

#### Scenario: Catalog entries exist
- **WHEN** the catalog is loaded
- **THEN** entries `calibration_keys`, `calibration_set`, `X_calibration`, `y_calibration` SHALL be defined with paths under `data/dataset/${dataset_version}/`
