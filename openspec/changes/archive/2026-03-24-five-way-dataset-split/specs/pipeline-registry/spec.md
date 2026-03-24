## MODIFIED Requirements

### Requirement: Pipeline registry lookup
The system SHALL provide a registry that maps pipeline names to Pipeline factory functions. The `get_pipeline` function SHALL accept `backend` parameter and additional `**kwargs`, passing all to the pipeline module's `create_pipeline(backend=backend, **kwargs)`.

#### Scenario: Pass enable_calibration to dataset pipeline
- **WHEN** `get_pipeline("dataset", backend="pandas", enable_calibration=True)` is called
- **THEN** the registry SHALL call `create_pipeline(backend="pandas", enable_calibration=True)` on the dataset module

#### Scenario: Backward compatible for pipelines without extra kwargs
- **WHEN** `get_pipeline("training", backend="pandas")` is called
- **THEN** the registry SHALL call `create_pipeline(backend="pandas")` — training pipeline's create_pipeline does not accept enable_calibration and SHALL not receive it

#### Scenario: Look up non-existent pipeline
- **WHEN** a caller requests a pipeline name that is not registered
- **THEN** the registry SHALL raise a KeyError with a message listing available pipeline names
