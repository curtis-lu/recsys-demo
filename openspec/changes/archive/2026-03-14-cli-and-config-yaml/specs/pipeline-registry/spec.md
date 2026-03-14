## ADDED Requirements

### Requirement: Pipeline registry lookup
The system SHALL provide a registry that maps pipeline names (strings) to Pipeline factory functions.

#### Scenario: Look up existing pipeline
- **WHEN** a caller requests the pipeline named "dataset"
- **THEN** the registry SHALL return a Pipeline object created by the corresponding factory function

#### Scenario: Look up non-existent pipeline
- **WHEN** a caller requests a pipeline name that is not registered
- **THEN** the registry SHALL raise a KeyError with a message listing available pipeline names

### Requirement: Pipeline factory convention
Each pipeline module SHALL expose a `create_pipeline() -> Pipeline` function that constructs and returns the Pipeline for that domain.

#### Scenario: Pipeline module structure
- **WHEN** a new pipeline is added at `src/recsys_tfb/pipelines/<name>/`
- **THEN** its `__init__.py` or `pipeline.py` SHALL export a `create_pipeline` function

### Requirement: List available pipelines
The registry SHALL provide a way to list all registered pipeline names.

#### Scenario: List pipelines
- **WHEN** a caller requests the list of available pipelines
- **THEN** the registry SHALL return a list of all registered pipeline name strings
