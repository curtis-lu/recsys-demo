## MODIFIED Requirements

### Requirement: evaluate_model uses schema for identity columns
The `evaluate_model` function SHALL obtain identity columns from `get_schema(parameters)` instead of hard-coding `["snap_date", "cust_id", "prod_name"]`.

#### Scenario: Default identity columns
- **WHEN** called with parameters without `schema` section
- **THEN** SHALL use `["snap_date", "cust_id", "prod_name"]` (identical to current behavior)

#### Scenario: Custom identity columns
- **WHEN** called with `schema.columns.entity: ["branch_id", "cust_id"]`
- **THEN** SHALL use `["snap_date", "branch_id", "cust_id", "prod_name"]` for groupby and ranking
