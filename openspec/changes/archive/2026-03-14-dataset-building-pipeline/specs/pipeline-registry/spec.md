## MODIFIED Requirements

### Requirement: Pipeline registry lookup
The system SHALL provide a registry that maps pipeline names (strings) to Pipeline factory functions. The "dataset" entry SHALL return a fully functional Pipeline (not an empty stub) that performs dataset building.

#### Scenario: Look up existing pipeline
- **WHEN** a caller requests the pipeline named "dataset"
- **THEN** the registry SHALL return a Pipeline object with all dataset building nodes wired correctly

#### Scenario: Look up non-existent pipeline
- **WHEN** a caller requests a pipeline name that is not registered
- **THEN** the registry SHALL raise a KeyError with a message listing available pipeline names
