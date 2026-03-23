## MODIFIED Requirements

### Requirement: Spark inference nodes use schema for column names
All Spark inference nodes SHALL obtain column names from `get_schema(parameters)`. Changes SHALL mirror the pandas backend modifications.

#### Scenario: Default column names
- **WHEN** called with parameters without `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Identity columns in Spark predict_scores
- **WHEN** `schema.columns.entity` is `["branch_id", "cust_id"]`
- **THEN** identity columns preserved during scoring SHALL be `["snap_date", "branch_id", "cust_id", "prod_name"]`
