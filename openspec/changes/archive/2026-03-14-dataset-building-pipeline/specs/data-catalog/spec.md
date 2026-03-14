## MODIFIED Requirements

### Requirement: Support in-memory dataset registration

DataCatalog SHALL allow adding datasets programmatically via an `add(name, dataset)` method, enabling the Runner to store intermediate pipeline outputs. The dataset pipeline relies on MemoryDataset for intermediate results: sample_keys, train_keys, val_keys, train_set, val_set, X_train, y_train, X_val, y_val.

#### Scenario: Add and retrieve in-memory data
- **WHEN** `add("intermediate", dataset)` is called followed by `load("intermediate")`
- **THEN** the dataset is accessible via `load`

#### Scenario: Dataset pipeline intermediates stored in memory
- **WHEN** the dataset pipeline runs
- **THEN** intermediate results (sample_keys, train_keys, val_keys, train_set, val_set) SHALL be stored as MemoryDataset and NOT persisted to disk

## ADDED Requirements

### Requirement: Preprocessor dataset in catalog
The catalog config SHALL include a `preprocessor` entry of type PickleDataset that persists the preprocessor object to disk for reuse during inference.

#### Scenario: Preprocessor persistence
- **WHEN** the dataset pipeline completes
- **THEN** the preprocessor SHALL be saved to the filepath defined in catalog.yaml via PickleDataset
