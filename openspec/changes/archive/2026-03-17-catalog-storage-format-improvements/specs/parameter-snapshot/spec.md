## ADDED Requirements

### Requirement: Pipeline execution saves parameter snapshot as JSON

After each pipeline run, the CLI SHALL save the pipeline's parameters as a standalone JSON file in the version directory, alongside the manifest.

#### Scenario: Dataset pipeline saves parameters_dataset.json
- **WHEN** the dataset pipeline completes successfully
- **THEN** a `parameters_dataset.json` file SHALL be written to `data/dataset/{dataset_version}/`

#### Scenario: Training pipeline saves parameters_training.json
- **WHEN** the training pipeline completes successfully
- **THEN** a `parameters_training.json` file SHALL be written to `data/models/{model_version}/`

#### Scenario: Inference pipeline saves parameters_inference.json
- **WHEN** the inference pipeline completes successfully
- **THEN** a `parameters_inference.json` file SHALL be written to `data/inference/{model_version}/{snap_date}/`

#### Scenario: Parameter snapshot is valid JSON
- **WHEN** a parameter snapshot file is written
- **THEN** the file SHALL be valid JSON with indent=2 formatting and ensure_ascii=False
