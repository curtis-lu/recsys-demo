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

#### Scenario: Unknown pipeline name
- **WHEN** user executes `python -m recsys_tfb run --pipeline nonexistent`
- **THEN** the system SHALL exit with an error message listing available pipeline names

#### Scenario: Pipeline execution failure
- **WHEN** a pipeline node raises an exception during execution
- **THEN** the CLI SHALL log the error and exit with a non-zero exit code

### Requirement: CLI help
The system SHALL display usage information when invoked with `--help`.

#### Scenario: Show help
- **WHEN** user executes `python -m recsys_tfb --help`
- **THEN** the system SHALL display available commands and options

### Requirement: Parameters injection
The CLI SHALL load parameters from ConfigLoader and inject them into the DataCatalog as a MemoryDataset named `parameters` before pipeline execution.

#### Scenario: Parameters available to nodes
- **WHEN** a pipeline is executed via CLI
- **THEN** nodes that declare `parameters` as an input SHALL receive the merged parameters dict from all `parameters*.yaml` files

### Requirement: Conf directory resolution
The CLI SHALL resolve the `conf/` directory relative to the project root (the directory containing `pyproject.toml` or the current working directory).

#### Scenario: Default conf directory
- **WHEN** the CLI is run from the project root
- **THEN** it SHALL look for config files in `./conf/`
