## MODIFIED Requirements

### Requirement: Instantiate datasets from YAML config

DataCatalog SHALL accept a catalog config dict (from ConfigLoader) and instantiate the corresponding Dataset objects based on the `type` field in each entry.

#### Scenario: Create ParquetDataset from config
- **WHEN** catalog config contains `{"my_data": {"type": "ParquetDataset", "filepath": "/data/my.parquet", "backend": "pandas"}}`
- **THEN** DataCatalog creates a `ParquetDataset` instance with those parameters

#### Scenario: Create JSONDataset from config
- **WHEN** catalog config contains `{"my_data": {"type": "JSONDataset", "filepath": "/data/my.json"}}`
- **THEN** DataCatalog creates a `JSONDataset` instance with those parameters

#### Scenario: Unknown dataset type
- **WHEN** catalog config contains an entry with `type: "UnknownDataset"`
- **THEN** DataCatalog raises a `ValueError` with a descriptive message

### Requirement: Support in-memory dataset registration

DataCatalog SHALL allow adding datasets programmatically via an `add(name, dataset)` method, enabling the Runner to store intermediate pipeline outputs. The dataset pipeline relies on MemoryDataset for intermediate results: sample_keys, train_keys, train_dev_keys, val_keys, train_set, train_dev_set, val_set, X_train, y_train, X_train_dev, y_train_dev, X_val, y_val.

#### Scenario: Add and retrieve in-memory data
- **WHEN** `add("intermediate", dataset)` is called followed by `load("intermediate")`
- **THEN** the dataset is accessible via `load`

#### Scenario: Dataset pipeline intermediates stored in memory
- **WHEN** the dataset pipeline runs
- **THEN** intermediate results (sample_keys, train_keys, train_dev_keys, val_keys, train_set, train_dev_set, val_set) SHALL be stored as MemoryDataset and NOT persisted to disk

### Requirement: category_mappings dataset in catalog
The catalog config SHALL include a `category_mappings` entry of type JSONDataset that persists the category mappings to disk as a human-readable JSON file.

#### Scenario: category_mappings persistence
- **WHEN** the dataset pipeline completes
- **THEN** category_mappings SHALL be saved to the filepath defined in catalog.yaml via JSONDataset
