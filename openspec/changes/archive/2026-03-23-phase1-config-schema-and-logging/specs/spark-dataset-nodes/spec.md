## MODIFIED Requirements

### Requirement: Column names are configurable (Spark backend)
All Spark dataset pipeline nodes SHALL obtain column names from `get_schema(parameters)` instead of hard-coded strings. Changes SHALL mirror the pandas backend modifications exactly.

#### Scenario: Default column names match current behavior
- **WHEN** nodes are called with parameters that have no `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Custom entity columns in Spark joins
- **WHEN** `schema.columns.entity` is `["branch_id", "cust_id"]`
- **THEN** Spark join conditions SHALL use both columns
