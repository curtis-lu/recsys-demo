## ADDED Requirements

### Requirement: sample_ratio_overrides parameter
The system SHALL support a `sample_ratio_overrides` parameter in `parameters_dataset.yaml` under the `dataset` section, with a default value of `{}` (empty dict). Keys SHALL be string representations of sample_group_keys values, using `"|"` as separator for multi-column group keys.

#### Scenario: Default empty overrides
- **WHEN** `sample_ratio_overrides` is not set or set to `{}`
- **THEN** all groups SHALL use the global `sample_ratio`

#### Scenario: Single-column group key override
- **WHEN** `sample_group_keys` is `["cust_segment_typ"]` and `sample_ratio_overrides` is `{"VIP": 1.0, "Regular": 0.3}`
- **THEN** VIP customers SHALL be sampled at ratio 1.0, Regular at 0.3, and all other segments at the global `sample_ratio`

#### Scenario: Multi-column group key override
- **WHEN** `sample_group_keys` is `["cust_segment_typ", "label"]` and `sample_ratio_overrides` is `{"VIP|1": 1.0, "VIP|0": 0.5}`
- **THEN** rows with cust_segment_typ=VIP and label=1 SHALL be sampled at ratio 1.0, VIP and label=0 at 0.5, and all other combinations at the global `sample_ratio`

### Requirement: Override key serialization
The system SHALL serialize group key values by joining column values with `"|"` in the order defined by `sample_group_keys`, converting each value to string before joining.

#### Scenario: Serialization order matches sample_group_keys
- **WHEN** `sample_group_keys` is `["cust_segment_typ", "label"]` and a row has cust_segment_typ="VIP", label=1
- **THEN** the serialized key SHALL be `"VIP|1"`

#### Scenario: Numeric values converted to string
- **WHEN** a group key column contains numeric value `1`
- **THEN** the serialized key SHALL use `"1"` (string representation)

### Requirement: Override applies to stratified sampling
The `select_sample_keys` function SHALL compute an effective sampling ratio per row by looking up the serialized group key in `sample_ratio_overrides`, falling back to `sample_ratio` if not found, then applying probabilistic sampling using `rand(seed) < effective_ratio`.

#### Scenario: Mixed ratios within same dataset
- **WHEN** `sample_ratio` is 0.5 and `sample_ratio_overrides` has `{"VIP": 1.0}`
- **THEN** VIP group rows SHALL all be retained and non-VIP group rows SHALL be sampled at approximately 50%

#### Scenario: Deterministic with seed
- **WHEN** overrides are applied twice with the same `random_seed`
- **THEN** both outputs SHALL be identical

### Requirement: Override applies to calibration sampling
The `select_calibration_keys` function SHALL also support `sample_ratio_overrides` when performing stratified sampling with `calibration_sample_ratio` as the base ratio.

#### Scenario: Calibration with overrides
- **WHEN** `enable_calibration` is true, `calibration_sample_ratio` is 0.5, and `sample_ratio_overrides` has `{"VIP": 1.0}`
- **THEN** VIP customers in calibration dates SHALL all be retained, others sampled at 50%
