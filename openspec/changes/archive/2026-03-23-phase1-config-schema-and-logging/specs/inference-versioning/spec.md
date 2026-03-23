## MODIFIED Requirements

### Requirement: Inference manifest includes run_id
The inference pipeline manifest.json SHALL include a `run_id` field.

#### Scenario: run_id in inference manifest
- **WHEN** the inference pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id
