## MODIFIED Requirements

### Requirement: Dataset manifest includes run_id
The dataset pipeline manifest.json SHALL include a `run_id` field recording the execution run identifier.

#### Scenario: run_id in dataset manifest
- **WHEN** the dataset pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id
