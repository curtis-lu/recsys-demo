## MODIFIED Requirements

### Requirement: Inference nodes use schema for column names
All pandas inference nodes (build_scoring_dataset, apply_preprocessor, predict_scores, rank_predictions) SHALL obtain column names from `get_schema(parameters)`.

#### Scenario: Default column names
- **WHEN** called with parameters without `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Custom item column in ranking
- **WHEN** `schema.columns.item` is set to `"channel_type"`
- **THEN** ranking SHALL group by the custom item column instead of `prod_name`
