## ADDED Requirements

### Requirement: LightGBMDataset saves model in native text format

LightGBMDataset SHALL save `lgb.Booster` objects using `model.save_model(filepath)` and load them using `lgb.Booster(model_file=filepath)`. The adapter SHALL create parent directories if they do not exist.

#### Scenario: Save and load a LightGBM Booster
- **WHEN** `save()` is called with a `lgb.Booster` object and then `load()` is called
- **THEN** the loaded object SHALL be a `lgb.Booster` that produces identical predictions on the same input data

#### Scenario: Saved file is human-readable text
- **WHEN** a model is saved via LightGBMDataset
- **THEN** the output file SHALL be a text file (not binary pickle)

#### Scenario: Check existence
- **WHEN** `exists()` is called on a LightGBMDataset
- **THEN** it SHALL return `True` if the model file exists, `False` otherwise

#### Scenario: Parent directory creation
- **WHEN** `save()` is called and the parent directory does not exist
- **THEN** LightGBMDataset SHALL create the parent directory before saving

### Requirement: LightGBMDataset implements AbstractDataset interface

LightGBMDataset SHALL inherit from `AbstractDataset` and implement `load()`, `save(data)`, and `exists()`.

#### Scenario: LightGBMDataset is registered in dataset registry
- **WHEN** DataCatalog encounters `type: "LightGBMDataset"` in catalog config
- **THEN** it SHALL instantiate a `LightGBMDataset` with the provided `filepath`
