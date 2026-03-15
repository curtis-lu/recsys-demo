## MODIFIED Requirements

### Requirement: Pipeline registry lookup
The system SHALL provide a registry that maps pipeline names (strings) to Pipeline factory functions. The "dataset" entry SHALL return a fully functional Pipeline that performs dataset building. The "training" entry SHALL return a Pipeline for model training. The "inference" entry SHALL return a Pipeline for batch scoring.

#### Scenario: Look up existing pipeline
- **WHEN** a caller requests the pipeline named "dataset"
- **THEN** the registry SHALL return a Pipeline object with all dataset building nodes wired correctly

#### Scenario: Look up inference pipeline
- **WHEN** a caller requests the pipeline named "inference"
- **THEN** the registry SHALL return a Pipeline object with all inference nodes wired correctly

#### Scenario: Look up non-existent pipeline
- **WHEN** a caller requests a pipeline name that is not registered
- **THEN** the registry SHALL raise a KeyError with a message listing available pipeline names
