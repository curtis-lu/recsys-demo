## MODIFIED Requirements

### Requirement: Pipeline outputs
Dataset pipeline 的 `preprocessor` 和 `category_mappings` 產出 SHALL 儲存在 dataset 版本目錄（`data/dataset/${dataset_version}/`）中，而非 model 目錄。

#### Scenario: preprocessor 寫入 dataset 版本目錄
- **WHEN** dataset pipeline 的 prepare_model_input node 完成
- **THEN** preprocessor.pkl SHALL 寫入 `data/dataset/{dataset_version}/preprocessor.pkl`

#### Scenario: category_mappings 寫入 dataset 版本目錄
- **WHEN** dataset pipeline 的 prepare_model_input node 完成
- **THEN** category_mappings.json SHALL 寫入 `data/dataset/{dataset_version}/category_mappings.json`
