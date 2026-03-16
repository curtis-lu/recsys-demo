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

### Requirement: Training artifacts with template variables
Catalog entries for `model`, `best_params`, `evaluation_results` SHALL use `${model_version}` placeholder。`preprocessor` 和 `category_mappings` SHALL 使用 `${dataset_version}` placeholder，路徑為 `data/dataset/${dataset_version}/`。

#### Scenario: preprocessor 使用 dataset_version
- **WHEN** catalog 被載入並解析 runtime_params
- **THEN** `preprocessor` 的路徑 SHALL 為 `data/dataset/${dataset_version}/preprocessor.pkl`

#### Scenario: category_mappings 使用 dataset_version
- **WHEN** catalog 被載入並解析 runtime_params
- **THEN** `category_mappings` 的路徑 SHALL 為 `data/dataset/${dataset_version}/category_mappings.json`

#### Scenario: model artifacts 維持 model_version
- **WHEN** catalog 被載入
- **THEN** `model`、`best_params`、`evaluation_results` 的路徑 SHALL 繼續使用 `${model_version}`

#### Scenario: 各環境路徑可設定
- **WHEN** 使用 production 環境設定
- **THEN** 所有 training artifact 的路徑 SHALL 切換至 HDFS 路徑（如 `hdfs:///data/recsys/models/${model_version}/model.pkl`）

### Requirement: Inference pipeline catalog entries
DataCatalog SHALL include entries for inference pipeline datasets:
- `scoring_dataset`: ParquetDataset for the intermediate scoring dataset
- `ranked_predictions`: ParquetDataset for the final ranked output

Inference 產出的 catalog 路徑 SHALL 使用 `${model_version}` 和 `${snap_date}` template variables。

#### Scenario: inference 產出路徑包含 model_version 和 snap_date
- **WHEN** catalog 被載入且 runtime_params 包含 `model_version` 和 `snap_date`
- **THEN** `scoring_dataset` 和 `ranked_predictions` 的路徑 SHALL 解析為 `data/inference/{model_version}/{snap_date}/` 下的對應檔案

#### Scenario: Inference reads training artifacts from best
- **WHEN** inference pipeline loads model and preprocessor
- **THEN** `${model_version}` SHALL 被解析為 `"best"`，從 `data/models/best/` 目錄讀取

### Requirement: Dataset 產出使用 dataset_version template
所有 dataset pipeline 中間產出的 catalog 路徑 SHALL 使用 `${dataset_version}` template variable。

#### Scenario: dataset 產出路徑包含版本
- **WHEN** catalog 被載入且 runtime_params 包含 `dataset_version`
- **THEN** sample_keys、train_keys、train_dev_keys、val_keys、train_set、train_dev_set、val_set、X_train、y_train、X_train_dev、y_train_dev、X_val、y_val 的路徑 SHALL 解析為 `data/dataset/{dataset_version}/` 下的對應檔案
