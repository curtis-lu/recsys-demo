## MODIFIED Requirements

### Requirement: CLI run command
The system SHALL provide a `run` command via `python -m recsys_tfb run` that executes a named pipeline in a specified environment. The CLI SHALL load config BEFORE building the pipeline, extract the `backend` parameter from parameters, and pass it to `get_pipeline`.

#### Scenario: Run a pipeline with default environment
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset`
- **THEN** the system loads config from `conf/base/` merged with `conf/local/` (default env), extracts `backend` from parameters (default "pandas"), builds the pipeline with that backend, builds a DataCatalog, and executes via Runner

#### Scenario: Run with production environment (spark backend)
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset --env production`
- **THEN** the system loads config with `conf/production/parameters.yaml` overriding `backend: spark`, builds the pipeline with Spark nodes, and executes

#### Scenario: Execution order
- **WHEN** the CLI run command executes
- **THEN** it SHALL follow this order: (1) load config, (2) extract backend from parameters, (3) get pipeline with backend, (4) build catalog, (5) inject parameters, (6) run pipeline
