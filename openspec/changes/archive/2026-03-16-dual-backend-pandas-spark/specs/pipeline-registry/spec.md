## MODIFIED Requirements

### Requirement: Pipeline registry lookup
The system SHALL provide a registry that maps pipeline names (strings) to Pipeline factory functions. The `get_pipeline` function SHALL accept an optional `backend` parameter and pass it to the pipeline module's `create_pipeline(backend=backend)`.

#### Scenario: Look up existing pipeline with backend
- **WHEN** a caller requests `get_pipeline("dataset", backend="spark")`
- **THEN** the registry SHALL call `create_pipeline(backend="spark")` on the dataset module and return the resulting Pipeline

#### Scenario: Look up pipeline with default backend
- **WHEN** a caller requests `get_pipeline("dataset")` without specifying backend
- **THEN** the registry SHALL call `create_pipeline(backend="pandas")`

#### Scenario: Look up non-existent pipeline
- **WHEN** a caller requests a pipeline name that is not registered
- **THEN** the registry SHALL raise a KeyError with a message listing available pipeline names

### Requirement: Pipeline factory convention
Each pipeline module SHALL expose a `create_pipeline(backend: str = "pandas") -> Pipeline` function that constructs and returns the Pipeline for that domain. The `backend` parameter SHALL determine which node implementation module to use.

#### Scenario: Pipeline module structure
- **WHEN** a new pipeline is added at `src/recsys_tfb/pipelines/<name>/`
- **THEN** its `pipeline.py` SHALL export a `create_pipeline(backend)` function
