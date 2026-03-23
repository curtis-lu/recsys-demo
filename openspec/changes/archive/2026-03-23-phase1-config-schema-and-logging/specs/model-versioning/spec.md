## MODIFIED Requirements

### Requirement: Model manifest includes run_id
The training pipeline manifest.json SHALL include a `run_id` field.

#### Scenario: run_id in model manifest
- **WHEN** the training pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id
