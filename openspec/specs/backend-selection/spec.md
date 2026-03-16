## ADDED Requirements

### Requirement: Backend parameter in configuration
The system SHALL support a top-level `backend` parameter in `parameters.yaml` that accepts values `"pandas"` or `"spark"`.

#### Scenario: Default backend
- **WHEN** `backend` is not specified in parameters.yaml
- **THEN** the system SHALL default to `"pandas"`

#### Scenario: Production override
- **WHEN** `conf/production/parameters.yaml` sets `backend: spark`
- **THEN** running with `--env production` SHALL use Spark node implementations

### Requirement: Pipeline factory selects node implementation by backend
The `create_pipeline(backend)` function in each pipeline module SHALL import node functions from the corresponding backend module.

#### Scenario: Pandas backend selection
- **WHEN** `create_pipeline(backend="pandas")` is called on the dataset pipeline
- **THEN** it SHALL import all node functions from `nodes_pandas.py`

#### Scenario: Spark backend selection
- **WHEN** `create_pipeline(backend="spark")` is called on the dataset pipeline
- **THEN** it SHALL import all node functions from `nodes_spark.py`

#### Scenario: Node wiring unchanged
- **WHEN** `create_pipeline` is called with any backend value
- **THEN** the Pipeline's Node definitions (inputs, outputs, names) SHALL be identical regardless of backend — only the function references differ
