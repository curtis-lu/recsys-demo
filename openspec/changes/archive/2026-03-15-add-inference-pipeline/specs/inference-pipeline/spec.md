## ADDED Requirements

### Requirement: Inference pipeline definition
The inference module SHALL expose a `create_pipeline() -> Pipeline` function that returns a Pipeline with 4 nodes wired in sequence: build_scoring_dataset → apply_preprocessor → predict_scores → rank_predictions.

#### Scenario: Pipeline node count and order
- **WHEN** `create_pipeline()` is called
- **THEN** the returned Pipeline SHALL contain exactly 4 nodes in the correct dependency order

#### Scenario: Pipeline inputs from catalog
- **WHEN** the inference pipeline executes
- **THEN** it SHALL read feature_table, preprocessor, and model from the DataCatalog

#### Scenario: Pipeline final output
- **WHEN** the inference pipeline completes
- **THEN** ranked_predictions SHALL be saved to the DataCatalog

### Requirement: End-to-end inference execution
The inference pipeline SHALL be executable via `python -m recsys_tfb -p inference -e local` after the training pipeline has produced model and preprocessor artifacts.

#### Scenario: Successful batch scoring
- **WHEN** model.pkl and preprocessor.pkl exist from a prior training run, and feature_table.parquet contains data for the configured snap_dates
- **THEN** the pipeline SHALL produce a ranked_predictions Parquet file with all customers scored across all configured products

#### Scenario: Missing model artifact
- **WHEN** model.pkl does not exist and inference pipeline is executed
- **THEN** the pipeline SHALL fail with a clear error indicating the model is not found
