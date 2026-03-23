## ADDED Requirements

### Requirement: MemoryDataset supports explicit release
MemoryDataset SHALL provide a `release()` method that sets its internal data to None, freeing the memory held by the stored object.

#### Scenario: Release clears data
- **WHEN** `release()` is called on a MemoryDataset that holds data
- **THEN** the internal `_data` attribute SHALL be set to None and `exists()` SHALL return False

#### Scenario: Release on empty dataset is safe
- **WHEN** `release()` is called on a MemoryDataset that has no data (`_data` is None)
- **THEN** no error SHALL be raised

### Requirement: DataCatalog provides dataset accessor
DataCatalog SHALL provide a `get_dataset(name)` method that returns the dataset instance by name, or None if not registered.

#### Scenario: Get registered dataset
- **WHEN** `get_dataset("train_set")` is called and `train_set` is registered
- **THEN** the method SHALL return the corresponding dataset instance

#### Scenario: Get unregistered dataset
- **WHEN** `get_dataset("nonexistent")` is called and the name is not registered
- **THEN** the method SHALL return None

### Requirement: Runner computes last consumer map
Runner SHALL compute a mapping from each dataset name to the last node (in topological order) that consumes it as input, before pipeline execution begins.

#### Scenario: Single consumer
- **WHEN** dataset "mid" is consumed only by node B in a pipeline [A → B → C]
- **THEN** the last consumer of "mid" SHALL be node B

#### Scenario: Multiple consumers
- **WHEN** dataset "shared" is consumed by both node B and node C (topological order: A, B, C)
- **THEN** the last consumer of "shared" SHALL be node C

### Requirement: Runner releases MemoryDataset after last consumer
After each node execution, Runner SHALL release any MemoryDataset input whose last consumer is the current node.

#### Scenario: Release after last consumer completes
- **WHEN** node B is the last consumer of MemoryDataset "mid" and node B finishes execution
- **THEN** Runner SHALL call `release()` on the MemoryDataset for "mid"

#### Scenario: Non-MemoryDataset not released
- **WHEN** node B is the last consumer of ParquetDataset "feature_table" and node B finishes execution
- **THEN** Runner SHALL NOT call `release()` on "feature_table"

#### Scenario: Shared input not released early
- **WHEN** dataset "shared" is consumed by node B and node C, and node B finishes execution
- **THEN** Runner SHALL NOT release "shared" (node C still needs it)

### Requirement: dataset_released structured log event
Runner SHALL emit a structured log event when a MemoryDataset is released, containing the dataset name and the releasing node name.

#### Scenario: Log event emitted on release
- **WHEN** MemoryDataset "sample_keys" is released after node "split_keys" completes
- **THEN** a log event SHALL be emitted with `event: "dataset_released"`, `dataset_name: "sample_keys"`, and `node: "split_keys"`
