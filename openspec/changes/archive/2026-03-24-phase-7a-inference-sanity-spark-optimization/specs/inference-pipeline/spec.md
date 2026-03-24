## MODIFIED Requirements

### Requirement: Inference pipeline includes validation node
The inference pipeline SHALL include a `validate_predictions` node as the final step, after `rank_predictions`.

#### Scenario: Pipeline node count
- **WHEN** creating the inference pipeline
- **THEN** the pipeline contains 5 nodes: build_scoring_dataset, apply_preprocessor, predict_scores, rank_predictions, validate_predictions

#### Scenario: Validation node inputs and outputs
- **WHEN** the validate_predictions node is defined
- **THEN** it takes inputs ["ranked_predictions", "scoring_dataset", "parameters"] and outputs "validated_predictions"

#### Scenario: Both backends include validation
- **WHEN** creating the pipeline with backend "pandas" or "spark"
- **THEN** both backends import and register the validate_predictions function
