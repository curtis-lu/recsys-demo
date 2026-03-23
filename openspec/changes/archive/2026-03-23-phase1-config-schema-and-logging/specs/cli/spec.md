## MODIFIED Requirements

### Requirement: CLI initializes structured logging before pipeline execution
`__main__.py` SHALL call `setup_logging()` with the logging config and RunContext before running any pipeline. The `run_id` SHALL be passed to the manifest writing logic.

#### Scenario: Logging setup on pipeline run
- **WHEN** `python -m recsys_tfb --pipeline dataset --env local` is executed
- **THEN** structured logging SHALL be initialized before the pipeline runner starts, and a log file SHALL be created in the configured log directory

#### Scenario: run_id propagated to manifest
- **WHEN** a pipeline completes
- **THEN** the manifest.json SHALL contain the same `run_id` as the log records
