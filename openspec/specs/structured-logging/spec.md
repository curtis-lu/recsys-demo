## ADDED Requirements

### Requirement: RunContext carries execution metadata
The system SHALL provide a `RunContext` that holds: `run_id` (string), `pipeline` (string), `env` (string), `dataset_version` (string), `model_version` (string), `backend` (string). RunContext SHALL be set once per pipeline execution.

#### Scenario: RunContext created at pipeline start
- **WHEN** a pipeline execution begins
- **THEN** a RunContext SHALL be created with a unique `run_id` and pipeline metadata

#### Scenario: run_id format
- **WHEN** a run_id is generated
- **THEN** it SHALL follow the format `YYYYMMDD_HHMMSS_{6 hex chars}` (e.g., `20260322_120000_a1b2c3`)

### Requirement: JSON log formatter for file output
The system SHALL provide a `JsonFormatter` that outputs one JSON object per line (JSON lines format). Each log record SHALL include: `timestamp` (ISO 8601), `level`, `logger`, `message`, `run_id`, `pipeline`, `node` (if applicable), and any extra fields.

#### Scenario: Node-level log record format
- **WHEN** a node completes execution
- **THEN** the JSON log record SHALL include: `event: "node_completed"`, `node` (node name), `duration_seconds`, `input_names`, `output_names`, `status` ("success" or "failed")

#### Scenario: Pipeline-level log record format
- **WHEN** a pipeline completes execution
- **THEN** the JSON log record SHALL include: `event: "pipeline_completed"`, `duration_seconds`, `status`, `node_count`

#### Scenario: Failed node log record
- **WHEN** a node raises an exception
- **THEN** the JSON log record SHALL include: `event: "node_failed"`, `status: "failed"`, `error_message`, `exception_type`

### Requirement: Console formatter for human readability
The system SHALL provide a `ConsoleFormatter` that outputs logs in the format: `[YYYY-MM-DD HH:MM:SS] LEVEL [pipeline:node] message`. Console output SHALL prioritize readability over machine parsability.

#### Scenario: Console output format
- **WHEN** a node completes and console logging is enabled
- **THEN** the console output SHALL show: `[2026-03-22 12:00:00] INFO [dataset:select_sample_keys] completed in 12.5s`

### Requirement: Configurable logging via parameters.yaml
The system SHALL read logging configuration from `parameters.yaml` under the `logging` key. Supported settings: `level` (string, default "INFO"), `console` (bool, default true), `file.enabled` (bool, default true), `file.path` (string, default "logs/"), `file.format` (string, default "json").

#### Scenario: Default logging config when section is omitted
- **WHEN** `parameters.yaml` does not contain a `logging` section
- **THEN** the system SHALL use defaults: level=INFO, console=true, file.enabled=true, file.path="logs/", file.format="json"

#### Scenario: Disable file logging
- **WHEN** `logging.file.enabled` is set to `false`
- **THEN** the system SHALL NOT create a log file, but console logging SHALL remain active

#### Scenario: Custom log level
- **WHEN** `logging.level` is set to `"DEBUG"`
- **THEN** the system SHALL emit DEBUG-level and above log records

### Requirement: setup_logging initializes handlers
`setup_logging(config, context)` SHALL configure the root logger with appropriate handlers based on the config. It SHALL be called once per pipeline execution, before any nodes run.

#### Scenario: Both console and file handlers
- **WHEN** `setup_logging()` is called with console=true and file.enabled=true
- **THEN** the root logger SHALL have both a StreamHandler (with ConsoleFormatter) and a FileHandler (with JsonFormatter)

#### Scenario: Log file naming
- **WHEN** file logging is enabled
- **THEN** the log file SHALL be named `{pipeline}_{run_id}.jsonl` under the configured path

### Requirement: Pipeline-level logging in Runner
The Runner SHALL emit structured log records at pipeline start and pipeline end, including: run_id, pipeline name, start_time, end_time, duration_seconds, status, node_count.

#### Scenario: Successful pipeline execution
- **WHEN** a pipeline completes successfully
- **THEN** a `pipeline_completed` log record SHALL be emitted with `status: "success"` and the total duration

#### Scenario: Failed pipeline execution
- **WHEN** a pipeline fails due to a node error
- **THEN** a `pipeline_failed` log record SHALL be emitted with `status: "failed"` and the error details

### Requirement: Node-level logging in Runner
The Runner SHALL emit structured log records before and after each node execution, including: node_name, start_time, end_time, duration_seconds, input_names, output_names, status.

#### Scenario: Node timing
- **WHEN** a node completes
- **THEN** a `node_completed` log record SHALL be emitted with accurate `duration_seconds`

#### Scenario: Node input/output tracking
- **WHEN** a node completes
- **THEN** the log record SHALL include `input_names` and `output_names` as lists of dataset names
