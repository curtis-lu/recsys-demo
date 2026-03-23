## ADDED Requirements

### Requirement: Centralized column schema in parameters.yaml
The system SHALL provide a `schema.columns` section in `parameters.yaml` that defines all column names used across pipelines. The section SHALL include: `time` (string), `entity` (string or list of strings), `item` (string), `label` (string), `score` (string), `rank` (string).

#### Scenario: Default schema when section is omitted
- **WHEN** `parameters.yaml` does not contain a `schema` section
- **THEN** `get_schema()` SHALL return default values: time="snap_date", entity=["cust_id"], item="prod_name", label="label", score="score", rank="rank"

#### Scenario: Partial schema override
- **WHEN** `parameters.yaml` contains `schema.columns.time: month_end` but omits other keys
- **THEN** `get_schema()` SHALL return time="month_end" with all other keys at default values

#### Scenario: Full schema override
- **WHEN** `parameters.yaml` contains all `schema.columns` keys with custom values
- **THEN** `get_schema()` SHALL return all custom values exactly as specified

### Requirement: Entity column supports multiple columns
The `entity` field in `schema.columns` SHALL accept both a single string and a list of strings. `get_schema()` SHALL normalize it to always return a list.

#### Scenario: Entity as single string
- **WHEN** `schema.columns.entity` is set to `"cust_id"` (a string)
- **THEN** `get_schema()` SHALL return `entity: ["cust_id"]` (a list)

#### Scenario: Entity as list of strings
- **WHEN** `schema.columns.entity` is set to `["branch_id", "cust_id"]`
- **THEN** `get_schema()` SHALL return `entity: ["branch_id", "cust_id"]` unchanged

### Requirement: Automatic identity_columns derivation
`get_schema()` SHALL compute `identity_columns` as `[time] + entity + [item]`. This field SHALL NOT be manually configurable.

#### Scenario: Default identity columns
- **WHEN** schema uses default values
- **THEN** `identity_columns` SHALL be `["snap_date", "cust_id", "prod_name"]`

#### Scenario: Multi-entity identity columns
- **WHEN** `entity` is `["branch_id", "cust_id"]` and `time` is `"snap_date"` and `item` is `"prod_name"`
- **THEN** `identity_columns` SHALL be `["snap_date", "branch_id", "cust_id", "prod_name"]`

### Requirement: get_schema is a pure function
`get_schema(parameters)` SHALL be a pure function with no side effects. It SHALL accept a `parameters` dict and return a new dict. It SHALL NOT modify the input.

#### Scenario: Repeated calls return same result
- **WHEN** `get_schema()` is called twice with the same parameters
- **THEN** both calls SHALL return identical results

#### Scenario: Input dict is not mutated
- **WHEN** `get_schema()` is called with a parameters dict
- **THEN** the input dict SHALL remain unchanged after the call
