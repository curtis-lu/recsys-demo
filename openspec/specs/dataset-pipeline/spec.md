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

### Requirement: Pipeline outputs 版本化路徑
Dataset pipeline 的 `preprocessor` 和 `category_mappings` 產出 SHALL 儲存在 dataset 版本目錄（`data/dataset/${dataset_version}/`）中，而非 model 目錄。

#### Scenario: preprocessor 寫入 dataset 版本目錄
- **WHEN** dataset pipeline 的 prepare_model_input node 完成
- **THEN** preprocessor.pkl SHALL 寫入 `data/dataset/{dataset_version}/preprocessor.pkl`

#### Scenario: category_mappings 寫入 dataset 版本目錄
- **WHEN** dataset pipeline 的 prepare_model_input node 完成
- **THEN** category_mappings.json SHALL 寫入 `data/dataset/{dataset_version}/category_mappings.json`

#### Scenario: Backend parameter selects node source
- **WHEN** `create_pipeline(backend="spark")` is called
- **THEN** all node functions SHALL be imported from `nodes_spark.py`

#### Scenario: Default backend is pandas
- **WHEN** `create_pipeline()` is called without backend argument
- **THEN** node functions SHALL be imported from `nodes_pandas.py`

### Requirement: Dataset parameters configuration
The system SHALL support dataset-specific parameters via `conf/base/parameters_dataset.yaml`.

#### Scenario: Required parameters
- **WHEN** parameters_dataset.yaml is loaded
- **THEN** it SHALL contain at minimum: sample_ratio (float), sample_group_keys (list of strings), train_dev_snap_dates (list of date strings), val_snap_dates (list of date strings)

#### Scenario: Parameters merged into global
- **WHEN** ConfigLoader loads parameters
- **THEN** dataset parameters SHALL be accessible under the `dataset` key in the merged parameters dict
