## ADDED Requirements

### Requirement: Node wraps a function with named inputs and outputs

Node SHALL encapsulate a callable function along with a list of input dataset names and output dataset names.

#### Scenario: Create a node
- **WHEN** a Node is created with `func=my_func`, `inputs=["a", "b"]`, `outputs=["c"]`
- **THEN** the Node stores the function reference, input names, and output names

#### Scenario: Node with no inputs
- **WHEN** a Node is created with `inputs=None` or `inputs=[]`
- **THEN** the Node is valid and represents a source node (e.g., parameter injection)

#### Scenario: Node string representation
- **WHEN** `str(node)` or `repr(node)` is called
- **THEN** it returns a readable string including the function name and input/output names

### Requirement: Pipeline resolves execution order via topological sort

Pipeline SHALL accept a list of Nodes and determine execution order by analyzing input/output dependencies using topological sort (Kahn's algorithm).

#### Scenario: Linear dependency chain
- **WHEN** Pipeline has Node A (outputs=["x"]) and Node B (inputs=["x"], outputs=["y"])
- **THEN** execution order is [A, B]

#### Scenario: Independent nodes
- **WHEN** Pipeline has nodes with no shared inputs/outputs
- **THEN** all nodes are included in the execution order (order among independent nodes is deterministic but unspecified)

#### Scenario: Circular dependency detection
- **WHEN** Pipeline has Node A (inputs=["y"], outputs=["x"]) and Node B (inputs=["x"], outputs=["y"])
- **THEN** Pipeline raises a `ValueError` indicating a circular dependency

### Requirement: Pipeline supports filtering by node or output

Pipeline SHALL support filtering to run a subset of nodes, selected by output dataset name.

#### Scenario: Filter by output name
- **WHEN** `pipeline.only_nodes_with_outputs(["y"])` is called
- **THEN** a new Pipeline is returned containing only nodes needed to produce output "y" and their transitive dependencies

### Requirement: Runner executes pipeline with catalog and logging

Runner SHALL execute a Pipeline's nodes in topological order, loading inputs from and saving outputs to a DataCatalog. Each node execution SHALL be logged with structured messages including node name, duration, and input/output names.

#### Scenario: Successful pipeline run
- **WHEN** Runner executes a pipeline with all required inputs available in the catalog
- **THEN** each node runs in order, outputs are saved to catalog, and execution completes

#### Scenario: Missing input data
- **WHEN** Runner encounters a node whose input is not in the catalog and not produced by a prior node
- **THEN** a descriptive error is raised before attempting execution

#### Scenario: Node execution failure
- **WHEN** a node's function raises an exception during execution
- **THEN** Runner logs the error with the node name and re-raises the exception

#### Scenario: Execution timing
- **WHEN** Runner completes a pipeline run
- **THEN** logs include per-node duration and total pipeline duration


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
