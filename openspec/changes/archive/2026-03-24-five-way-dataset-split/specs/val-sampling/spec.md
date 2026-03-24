## REMOVED Requirements

### Requirement: Val set sampling in prepare_model_input
**Reason**: val_sample_ratio logic moved upstream to `select_val_keys` node. Sampling now happens at key selection time, not at prepare_model_input time.
**Migration**: Use `select_val_keys` which applies `val_sample_ratio` at the cust_id level before building the dataset.

### Requirement: Graceful fallback when group keys unavailable
**Reason**: No longer applicable since val sampling is now pure random (non-stratified) in `select_val_keys`, not stratified by sample_group_keys.
**Migration**: `select_val_keys` performs random cust_id sampling without group keys.

## MODIFIED Requirements

### Requirement: val_sample_ratio parameter
The system SHALL support a `val_sample_ratio` parameter in `parameters_dataset.yaml` under the `dataset` section, with a default value of `1.0`. This parameter is consumed by `select_val_keys` (not `prepare_model_input`).

#### Scenario: Default value preserves full volume
- **WHEN** `val_sample_ratio` is not set or set to `1.0`
- **THEN** `select_val_keys` SHALL return the full population for val_snap_dates

#### Scenario: Partial sampling
- **WHEN** `val_sample_ratio` is `0.5`
- **THEN** `select_val_keys` SHALL randomly sample approximately 50% of unique cust_ids

### Requirement: Full val set preserved on disk
The `select_val_keys` node output and `build_val_dataset` node output reflect the actual sampling. When `val_sample_ratio < 1.0`, the persisted val_keys and val_set SHALL contain only the sampled portion.

#### Scenario: Sampled val_keys persisted
- **WHEN** `val_sample_ratio` is `0.5`
- **THEN** `val_keys` saved to disk SHALL contain only the sampled cust_ids

#### Scenario: Full population when ratio is 1.0
- **WHEN** `val_sample_ratio` is `1.0`
- **THEN** `val_keys` saved to disk SHALL contain the full population
