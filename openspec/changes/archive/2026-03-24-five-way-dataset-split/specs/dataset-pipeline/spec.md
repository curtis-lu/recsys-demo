## MODIFIED Requirements

### Requirement: Dataset pipeline definition
The system SHALL define a Pipeline that chains dataset building nodes. The `create_pipeline` function SHALL accept `backend` (str, default `"pandas"`) and `enable_calibration` (bool, default `False`) parameters. When `enable_calibration` is False, the pipeline SHALL contain nodes: select_sample_keys → split_train_keys → select_val_keys → select_test_keys → build_train_dataset / build_train_dev_dataset / build_val_dataset / build_test_dataset → prepare_model_input. When `enable_calibration` is True, additional nodes SHALL be included: select_calibration_keys → build_calibration_dataset, and `prepare_model_input_with_calibration` SHALL replace `prepare_model_input`.

#### Scenario: Pipeline node order without calibration
- **WHEN** `create_pipeline(backend="pandas", enable_calibration=False)` is called
- **THEN** it SHALL contain nodes in topologically valid order: select_sample_keys, split_train_keys, select_val_keys, select_test_keys, build_train_dataset, build_train_dev_dataset, build_val_dataset, build_test_dataset, prepare_model_input

#### Scenario: Pipeline node order with calibration
- **WHEN** `create_pipeline(backend="pandas", enable_calibration=True)` is called
- **THEN** it SHALL additionally contain select_calibration_keys and build_calibration_dataset nodes, and use prepare_model_input_with_calibration

#### Scenario: Pipeline inputs
- **WHEN** the dataset pipeline is inspected
- **THEN** its required external inputs SHALL be: sample_pool, feature_table, label_table, parameters

#### Scenario: Pipeline outputs without calibration
- **WHEN** `enable_calibration` is False
- **THEN** final outputs SHALL include: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, X_test, y_test, preprocessor, category_mappings

#### Scenario: Pipeline outputs with calibration
- **WHEN** `enable_calibration` is True
- **THEN** final outputs SHALL additionally include: X_calibration, y_calibration

#### Scenario: Backend parameter selects node source
- **WHEN** `create_pipeline(backend="spark")` is called
- **THEN** all node functions SHALL be imported from `nodes_spark.py`

#### Scenario: Default parameters
- **WHEN** `create_pipeline()` is called without arguments
- **THEN** backend SHALL default to `"pandas"` and enable_calibration SHALL default to `False`

### Requirement: Dataset parameters configuration
The system SHALL support dataset-specific parameters via `conf/base/parameters_dataset.yaml`.

#### Scenario: Required parameters
- **WHEN** parameters_dataset.yaml is loaded
- **THEN** it SHALL contain at minimum: sample_ratio (float), sample_group_keys (list), sample_ratio_overrides (dict), train_dev_ratio (float), enable_calibration (bool), calibration_snap_dates (list), calibration_sample_ratio (float), val_snap_dates (list), val_sample_ratio (float), test_snap_dates (list)

#### Scenario: Removed parameter
- **WHEN** parameters_dataset.yaml is loaded
- **THEN** it SHALL NOT contain `train_dev_snap_dates` (replaced by `train_dev_ratio`)
