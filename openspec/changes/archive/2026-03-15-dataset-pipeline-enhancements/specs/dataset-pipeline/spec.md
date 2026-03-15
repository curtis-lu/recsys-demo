## MODIFIED Requirements

### Requirement: Dataset pipeline definition
The system SHALL define a Pipeline that chains dataset building nodes in the correct dependency order: select_sample_keys → split_keys → build_train_dataset / build_train_dev_dataset / build_val_dataset → prepare_model_input.

#### Scenario: Pipeline node order
- **WHEN** the dataset pipeline is created via `create_pipeline()`
- **THEN** it SHALL contain 7 nodes in topologically valid order with correct input/output wiring

#### Scenario: Pipeline inputs
- **WHEN** the dataset pipeline is inspected
- **THEN** its required external inputs SHALL be: feature_table, label_table, parameters

#### Scenario: Pipeline outputs
- **WHEN** the dataset pipeline is inspected
- **THEN** its final outputs SHALL include: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, category_mappings

### Requirement: Dataset parameters configuration
The system SHALL support dataset-specific parameters via `conf/base/parameters_dataset.yaml`.

#### Scenario: Required parameters
- **WHEN** parameters_dataset.yaml is loaded
- **THEN** it SHALL contain at minimum: sample_ratio (float), sample_group_keys (list of strings), train_dev_snap_dates (list of date strings), val_snap_dates (list of date strings)

#### Scenario: Parameters merged into global
- **WHEN** ConfigLoader loads parameters
- **THEN** dataset parameters SHALL be accessible under the `dataset` key in the merged parameters dict
