## MODIFIED Requirements

### Requirement: Inference pipeline definition
The inference module SHALL expose a `create_pipeline(backend: str = "pandas") -> Pipeline` function that accepts a `backend` parameter and returns a Pipeline with 4 nodes wired in sequence: build_scoring_dataset -> apply_preprocessor -> predict_scores -> rank_predictions. Node functions SHALL be imported from `nodes_pandas.py` or `nodes_spark.py` based on the backend parameter.

#### Scenario: Pipeline node count and order
- **WHEN** `create_pipeline()` is called with any backend value
- **THEN** the returned Pipeline SHALL contain exactly 4 nodes in the correct dependency order, identical regardless of backend

#### Scenario: Pipeline inputs from catalog
- **WHEN** the inference pipeline executes
- **THEN** it SHALL read feature_table, preprocessor, and model from the DataCatalog

#### Scenario: Pipeline final output
- **WHEN** the inference pipeline completes
- **THEN** ranked_predictions SHALL be saved to the DataCatalog

#### Scenario: Backend parameter selects node source
- **WHEN** `create_pipeline(backend="spark")` is called
- **THEN** all node functions SHALL be imported from `nodes_spark.py`
