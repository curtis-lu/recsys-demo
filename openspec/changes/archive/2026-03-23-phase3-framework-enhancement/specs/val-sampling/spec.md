## ADDED Requirements

### Requirement: val_sample_ratio parameter
The system SHALL support a `val_sample_ratio` parameter in `parameters_dataset.yaml` under the `dataset` section, with a default value of `1.0` (full volume).

#### Scenario: Default value preserves full volume
- **WHEN** `val_sample_ratio` is not set or set to `1.0`
- **THEN** `prepare_model_input` SHALL use the full val set without sampling

#### Scenario: Parameter accepted in config
- **WHEN** `val_sample_ratio: 0.5` is set in `parameters_dataset.yaml`
- **THEN** the parameter SHALL be available to `prepare_model_input` via the parameters dict

### Requirement: Val set sampling in prepare_model_input
When `val_sample_ratio` is less than 1.0, `prepare_model_input` SHALL perform stratified sampling on the val set before converting to numpy arrays.

#### Scenario: Val set sampled at prepare_model_input (pandas)
- **WHEN** `val_sample_ratio` is `0.5` and `prepare_model_input` is called with pandas backend
- **THEN** X_val and y_val SHALL contain approximately 50% of the original val set rows, stratified by `sample_group_keys`

#### Scenario: Val set sampled at prepare_model_input (spark)
- **WHEN** `val_sample_ratio` is `0.5` and `prepare_model_input` is called with spark backend
- **THEN** X_val and y_val SHALL contain approximately 50% of the original val set rows, stratified by `sample_group_keys`

#### Scenario: Deterministic sampling
- **WHEN** `prepare_model_input` is called twice with the same `val_sample_ratio` and `random_seed`
- **THEN** the resulting X_val and y_val SHALL be identical

### Requirement: Full val set preserved on disk
The `split_keys` and `build_val_dataset` nodes SHALL continue to produce full-volume val sets regardless of `val_sample_ratio`.

#### Scenario: split_keys produces full val_keys
- **WHEN** `val_sample_ratio` is `0.5`
- **THEN** `split_keys` SHALL still produce val_keys from the full population for val snapshot dates

#### Scenario: build_val_dataset produces full val_set
- **WHEN** `val_sample_ratio` is `0.5`
- **THEN** `build_val_dataset` SHALL still produce a full-volume val_set saved to disk

### Requirement: Graceful fallback when group keys unavailable
When configured `sample_group_keys` columns are not present in the val set, sampling SHALL fall back to simple random sampling.

#### Scenario: Group keys missing from val set
- **WHEN** `sample_group_keys` includes a column not present in val_set
- **THEN** `prepare_model_input` SHALL perform simple random sampling using only available group keys, or plain random sampling if none are available
