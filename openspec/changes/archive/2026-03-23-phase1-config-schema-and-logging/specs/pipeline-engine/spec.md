## MODIFIED Requirements

### Requirement: Runner emits structured logs for pipeline and node execution
The Runner SHALL generate a unique `run_id` at pipeline start, create a `RunContext`, call `setup_logging()`, and emit structured log records at pipeline start/end and node start/end.

#### Scenario: Pipeline start log
- **WHEN** Runner.run() is called
- **THEN** a `pipeline_started` log record SHALL be emitted with run_id, pipeline name, and start_time

#### Scenario: Node completion log with timing
- **WHEN** a node completes successfully
- **THEN** a `node_completed` log record SHALL be emitted with node_name, duration_seconds, input_names, output_names, status="success"

#### Scenario: Node failure log
- **WHEN** a node raises an exception
- **THEN** a `node_failed` log record SHALL be emitted with error_message and exception_type, before re-raising the exception

#### Scenario: Pipeline completion log
- **WHEN** all nodes complete successfully
- **THEN** a `pipeline_completed` log record SHALL be emitted with total duration_seconds and node_count
