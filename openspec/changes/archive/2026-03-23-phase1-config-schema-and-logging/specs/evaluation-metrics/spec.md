## MODIFIED Requirements

### Requirement: Metric computation uses schema for groupby keys
`compute_all_metrics()` and related functions SHALL accept schema-driven column names for groupby operations (time + entity columns for per-query grouping, item column for per-product metrics).

#### Scenario: Default groupby keys
- **WHEN** called without schema overrides
- **THEN** SHALL group by `["snap_date", "cust_id"]` for per-query metrics (identical to current behavior)

#### Scenario: Custom entity columns in metric groupby
- **WHEN** schema specifies `entity: ["branch_id", "cust_id"]`
- **THEN** SHALL group by `["snap_date", "branch_id", "cust_id"]` for per-query metrics
