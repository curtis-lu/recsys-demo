## MODIFIED Requirements

### Requirement: parameters.yaml supports schema and logging sections
The ConfigLoader SHALL pass through `schema` and `logging` sections from `parameters.yaml` without modification. No special handling is required — these are consumed by `get_schema()` and `setup_logging()` respectively.

#### Scenario: Schema section loaded
- **WHEN** `parameters.yaml` contains a `schema` section
- **THEN** `get_parameters()` SHALL include it in the returned dict

#### Scenario: Logging section loaded
- **WHEN** `parameters.yaml` contains a `logging` section
- **THEN** `get_parameters()` SHALL include it in the returned dict
