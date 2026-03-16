## MODIFIED Requirements

### Requirement: Training artifacts with template variables
Catalog entries for `model`, `best_params`, `evaluation_results` SHALL use `${model_version}` placeholder。`preprocessor` 和 `category_mappings` SHALL 改為使用 `${dataset_version}` placeholder，路徑從 `data/models/` 移至 `data/dataset/`。

#### Scenario: preprocessor 使用 dataset_version
- **WHEN** catalog 被載入並解析 runtime_params
- **THEN** `preprocessor` 的路徑 SHALL 為 `data/dataset/${dataset_version}/preprocessor.pkl`

#### Scenario: category_mappings 使用 dataset_version
- **WHEN** catalog 被載入並解析 runtime_params
- **THEN** `category_mappings` 的路徑 SHALL 為 `data/dataset/${dataset_version}/category_mappings.json`

#### Scenario: model artifacts 維持 model_version
- **WHEN** catalog 被載入
- **THEN** `model`、`best_params`、`evaluation_results` 的路徑 SHALL 繼續使用 `${model_version}`

## ADDED Requirements

### Requirement: Dataset 產出使用 dataset_version template
所有 dataset pipeline 中間產出的 catalog 路徑 SHALL 使用 `${dataset_version}` template variable。

#### Scenario: dataset 產出路徑包含版本
- **WHEN** catalog 被載入且 runtime_params 包含 `dataset_version`
- **THEN** sample_keys、train_keys、train_dev_keys、val_keys、train_set、train_dev_set、val_set、X_train、y_train、X_train_dev、y_train_dev、X_val、y_val 的路徑 SHALL 解析為 `data/dataset/{dataset_version}/` 下的對應檔案

### Requirement: Inference 產出使用版本化路徑
Inference pipeline 產出的 catalog 路徑 SHALL 使用 `${model_version}` 和 `${snap_date}` template variables。

#### Scenario: inference 產出路徑包含 model_version 和 snap_date
- **WHEN** catalog 被載入且 runtime_params 包含 `model_version` 和 `snap_date`
- **THEN** `scoring_dataset` 和 `ranked_predictions` 的路徑 SHALL 解析為 `data/inference/{model_version}/{snap_date}/` 下的對應檔案
