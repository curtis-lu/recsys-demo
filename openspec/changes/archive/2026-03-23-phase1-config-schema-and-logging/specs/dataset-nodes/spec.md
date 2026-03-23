## MODIFIED Requirements

### Requirement: Column names are configurable
All dataset pipeline nodes (select_sample_keys, split_keys, build_dataset, prepare_model_input) SHALL obtain column names (time, entity, item, label) from `get_schema(parameters)` instead of using hard-coded strings. The `build_dataset` node SHALL accept `parameters` as an additional input.

#### Scenario: Default column names match current behavior
- **WHEN** nodes are called with parameters that have no `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation (snap_date, cust_id, prod_name, label)

#### Scenario: Custom column names propagate through pipeline
- **WHEN** `schema.columns.time` is set to `"month_end"` and `schema.columns.entity` is set to `["branch_id", "cust_id"]`
- **THEN** all nodes SHALL use `month_end` as the time column and `["branch_id", "cust_id"]` as the entity columns for joins, groupby, and filtering
