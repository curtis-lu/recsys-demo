## MODIFIED Requirements

### Requirement: Training artifacts with template variables
Catalog entries for `model` SHALL use `LightGBMDataset` type with filepath `data/models/${model_version}/model.txt`. `best_params` and `evaluation_results` SHALL continue using `JSONDataset`. `preprocessor` SHALL continue using `PickleDataset`. `category_mappings` SHALL continue using `JSONDataset`.

#### Scenario: model 使用 LightGBMDataset
- **WHEN** catalog 被載入並解析 runtime_params
- **THEN** `model` 的 type SHALL 為 `LightGBMDataset`，路徑 SHALL 為 `data/models/${model_version}/model.txt`

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
- **THEN** 所有 training artifact 的路徑 SHALL 切換至 HDFS 路徑（如 `hdfs:///data/recsys/models/${model_version}/model.txt`）

### Requirement: Dataset 產出使用 dataset_version template
所有 dataset pipeline 的 X/y preprocessed arrays SHALL 使用 `ParquetDataset` type（而非 PickleDataset），filepath 使用 `.parquet` 副檔名。

#### Scenario: X_train 使用 ParquetDataset
- **WHEN** catalog 被載入
- **THEN** `X_train` 的 type SHALL 為 `ParquetDataset`，路徑 SHALL 為 `data/dataset/${dataset_version}/X_train.parquet`

#### Scenario: y_train 使用 ParquetDataset
- **WHEN** catalog 被載入
- **THEN** `y_train` 的 type SHALL 為 `ParquetDataset`，路徑 SHALL 為 `data/dataset/${dataset_version}/y_train.parquet`

#### Scenario: 所有 X/y artifacts 使用 ParquetDataset
- **WHEN** catalog 被載入
- **THEN** `X_train`、`y_train`、`X_train_dev`、`y_train_dev`、`X_val`、`y_val` 的 type SHALL 全部為 `ParquetDataset` with `backend: pandas`
