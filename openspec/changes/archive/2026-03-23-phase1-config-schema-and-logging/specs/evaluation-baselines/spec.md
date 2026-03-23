## MODIFIED Requirements

### Requirement: Baseline generation uses schema for column names
`generate_global_popularity_baseline()` and `generate_segment_popularity_baseline()` SHALL use schema-driven column names instead of hard-coded `snap_date`, `cust_id`, `prod_name`, `label`.

#### Scenario: Default column names
- **WHEN** called without schema overrides
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Custom label column
- **WHEN** schema specifies `label: "is_applied"`
- **THEN** baseline generation SHALL use `is_applied` as the positive/negative indicator column
