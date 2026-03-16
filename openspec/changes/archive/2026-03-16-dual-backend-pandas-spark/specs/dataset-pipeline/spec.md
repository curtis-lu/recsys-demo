## MODIFIED Requirements

### Requirement: Dataset pipeline definition
The system SHALL define a Pipeline that chains dataset building nodes in the correct dependency order: select_sample_keys → split_keys → build_train_dataset / build_train_dev_dataset / build_val_dataset → prepare_model_input. The `create_pipeline` function SHALL accept a `backend` parameter (`"pandas"` or `"spark"`, default `"pandas"`) and import node functions from the corresponding module (`nodes_pandas.py` or `nodes_spark.py`).

#### Scenario: Pipeline node order
- **WHEN** the dataset pipeline is created via `create_pipeline(backend="pandas")` or `create_pipeline(backend="spark")`
- **THEN** it SHALL contain 6 nodes in topologically valid order with correct input/output wiring, identical regardless of backend

#### Scenario: Pipeline inputs
- **WHEN** the dataset pipeline is inspected
- **THEN** its required external inputs SHALL be: feature_table, label_table, parameters

#### Scenario: Pipeline outputs
- **WHEN** the dataset pipeline is inspected
- **THEN** its final outputs SHALL include: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, category_mappings

#### Scenario: Backend parameter selects node source
- **WHEN** `create_pipeline(backend="spark")` is called
- **THEN** all node functions SHALL be imported from `nodes_spark.py`

#### Scenario: Default backend is pandas
- **WHEN** `create_pipeline()` is called without backend argument
- **THEN** node functions SHALL be imported from `nodes_pandas.py`
