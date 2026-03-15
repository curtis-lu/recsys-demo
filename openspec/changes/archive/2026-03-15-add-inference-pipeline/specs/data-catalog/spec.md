## ADDED Requirements

### Requirement: Inference pipeline catalog entries
DataCatalog SHALL include entries for inference pipeline datasets:
- `scoring_dataset`: ParquetDataset for the intermediate scoring dataset
- `ranked_predictions`: ParquetDataset for the final ranked output

#### Scenario: scoring_dataset persistence
- **WHEN** build_scoring_dataset node completes
- **THEN** scoring_dataset SHALL be saved to data/inference/scoring_dataset.parquet via ParquetDataset

#### Scenario: ranked_predictions persistence
- **WHEN** rank_predictions node completes
- **THEN** ranked_predictions SHALL be saved to data/inference/ranked_predictions.parquet via ParquetDataset

#### Scenario: Inference reads training artifacts
- **WHEN** inference pipeline loads model and preprocessor
- **THEN** it SHALL use the existing model and preprocessor catalog entries (PickleDataset at data/models/)
