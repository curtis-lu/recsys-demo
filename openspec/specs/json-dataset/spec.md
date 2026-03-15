## ADDED Requirements

### Requirement: JSONDataset for structured data persistence
JSONDataset SHALL serialize and deserialize Python objects (dicts, lists) as JSON files, inheriting from AbstractDataset.

#### Scenario: Save and load a dictionary
- **WHEN** `save()` is called with a Python dict and then `load()` is called
- **THEN** the loaded object SHALL be equivalent to the saved object

#### Scenario: JSON file is human-readable
- **WHEN** `save()` is called with data
- **THEN** the output file SHALL be formatted with indent=2 for readability

#### Scenario: Parent directory auto-creation
- **WHEN** `save()` is called and the parent directory does not exist
- **THEN** the parent directory SHALL be created automatically

#### Scenario: Check existence
- **WHEN** `exists()` is called on a JSONDataset
- **THEN** returns `True` if the JSON file exists, `False` otherwise
