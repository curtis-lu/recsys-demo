## ADDED Requirements

### Requirement: Dataset registry includes LightGBMDataset

The `_DATASET_REGISTRY` in `catalog.py` SHALL include `"LightGBMDataset"` mapping to the `LightGBMDataset` class, alongside the existing ParquetDataset, PickleDataset, and JSONDataset entries.

#### Scenario: LightGBMDataset is available in registry
- **WHEN** DataCatalog is initialized with a catalog config containing `type: "LightGBMDataset"`
- **THEN** it SHALL successfully instantiate a `LightGBMDataset` instance
