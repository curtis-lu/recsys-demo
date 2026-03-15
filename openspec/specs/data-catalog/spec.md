## ADDED Requirements

### Requirement: Instantiate datasets from YAML config

DataCatalog SHALL accept a catalog config dict (from ConfigLoader) and instantiate the corresponding Dataset objects based on the `type` field in each entry.

#### Scenario: Create ParquetDataset from config
- **WHEN** catalog config contains `{"my_data": {"type": "ParquetDataset", "filepath": "/data/my.parquet", "backend": "pandas"}}`
- **THEN** DataCatalog creates a `ParquetDataset` instance with those parameters

#### Scenario: Create JSONDataset from config
- **WHEN** catalog config contains `{"my_data": {"type": "JSONDataset", "filepath": "/data/my.json"}}`
- **THEN** DataCatalog creates a `JSONDataset` instance with those parameters

#### Scenario: Unknown dataset type
- **WHEN** catalog config contains an entry with `type: "UnknownDataset"`
- **THEN** DataCatalog raises a `ValueError` with a descriptive message

### Requirement: Provide unified load/save/exists interface

DataCatalog SHALL provide `load(name)`, `save(name, data)`, and `exists(name)` methods that delegate to the corresponding Dataset instance.

#### Scenario: Load a registered dataset
- **WHEN** `load("my_data")` is called and `my_data` is registered in the catalog
- **THEN** the corresponding Dataset's `load()` method is called and its result returned

#### Scenario: Save to a registered dataset
- **WHEN** `save("my_data", df)` is called
- **THEN** the corresponding Dataset's `save(df)` method is called

#### Scenario: Load unregistered dataset
- **WHEN** `load("nonexistent")` is called and the name is not in the catalog
- **THEN** a `KeyError` is raised

### Requirement: Support in-memory dataset registration

DataCatalog SHALL allow adding datasets programmatically via an `add(name, dataset)` method, enabling the Runner to store intermediate pipeline outputs. The dataset pipeline relies on MemoryDataset for intermediate results: sample_keys, train_keys, train_dev_keys, val_keys, train_set, train_dev_set, val_set, X_train, y_train, X_train_dev, y_train_dev, X_val, y_val.

#### Scenario: Add and retrieve in-memory data
- **WHEN** `add("intermediate", dataset)` is called followed by `load("intermediate")`
- **THEN** the dataset is accessible via `load`

#### Scenario: Dataset pipeline intermediates stored in memory
- **WHEN** the dataset pipeline runs
- **THEN** intermediate results (sample_keys, train_keys, train_dev_keys, val_keys, train_set, train_dev_set, val_set) SHALL be stored as MemoryDataset and NOT persisted to disk

### Requirement: Preprocessor dataset in catalog
The catalog config SHALL include a `preprocessor` entry of type PickleDataset that persists the preprocessor object to disk for reuse during inference.

#### Scenario: Preprocessor persistence
- **WHEN** the dataset pipeline completes
- **THEN** the preprocessor SHALL be saved to the filepath defined in catalog.yaml via PickleDataset

### Requirement: category_mappings dataset in catalog
The catalog config SHALL include a `category_mappings` entry of type JSONDataset that persists the category mappings to disk as a human-readable JSON file.

#### Scenario: category_mappings persistence
- **WHEN** the dataset pipeline completes
- **THEN** category_mappings SHALL be saved to the filepath defined in catalog.yaml via JSONDataset

### Requirement: Training pipeline catalog entries
DataCatalog SHALL 包含以下 training pipeline 輸出的定義：
- `best_params`：JSONDataset，儲存 Optuna 搜索的最佳超參數
- `evaluation_results`：JSONDataset，儲存 mAP 等評估指標
- `model`：PickleDataset，儲存訓練完成的 LightGBM Booster（已存在）

#### Scenario: best_params 持久化
- **WHEN** tune_hyperparameters 節點執行完畢
- **THEN** best_params SHALL 以 JSON 格式存入 data/models/best_params.json

#### Scenario: evaluation_results 持久化
- **WHEN** evaluate_model 節點執行完畢
- **THEN** evaluation_results SHALL 以 JSON 格式存入 data/models/evaluation_results.json

#### Scenario: 各環境路徑可設定
- **WHEN** 使用 production 環境設定
- **THEN** 所有 training artifact 的路徑 SHALL 切換至 HDFS 路徑
