## Requirements

### Requirement: Dataset 產出版本化目錄
Dataset pipeline 的所有產出 SHALL 儲存在 `data/dataset/${dataset_version}/` 版本目錄中，其中 `${dataset_version}` 為 `parameters_dataset.yaml` 內容的 hash。

#### Scenario: 執行 dataset pipeline 建立版本目錄
- **WHEN** 執行 `python -m recsys_tfb run -p dataset`
- **THEN** 系統 SHALL 計算 dataset_version hash，將所有產出寫入 `data/dataset/{hash}/`，包含 sample_keys.parquet、train_keys.parquet、train_dev_keys.parquet、val_keys.parquet、train_set.parquet、train_dev_set.parquet、val_set.parquet、X_train.pkl、y_train.pkl、X_train_dev.pkl、y_train_dev.pkl、X_val.pkl、y_val.pkl、preprocessor.pkl、category_mappings.json

#### Scenario: 相同參數重跑覆蓋同一版本
- **WHEN** 以相同的 parameters_dataset.yaml 執行 dataset pipeline 兩次
- **THEN** 兩次 SHALL 寫入同一個版本目錄（hash 相同），第二次覆蓋第一次的產出

#### Scenario: 不同參數建立不同版本
- **WHEN** 修改 parameters_dataset.yaml 後再次執行 dataset pipeline
- **THEN** SHALL 建立新的版本目錄（不同 hash），舊版本目錄保留

### Requirement: Dataset latest symlink
Dataset pipeline 完成後 SHALL 自動更新 `data/dataset/latest` symlink 指向本次產出的版本目錄。

#### Scenario: 首次執行建立 latest
- **WHEN** dataset pipeline 首次成功完成且 latest symlink 不存在
- **THEN** SHALL 建立 latest symlink 指向本次版本目錄

#### Scenario: 再次執行更新 latest
- **WHEN** dataset pipeline 成功完成且 latest symlink 已存在
- **THEN** SHALL 更新 latest symlink 指向本次版本目錄

### Requirement: Dataset manifest 自動寫入
Dataset pipeline 成功完成後 SHALL 在版本目錄中寫入 manifest.json。

#### Scenario: manifest 包含完整追溯資訊
- **WHEN** dataset pipeline 成功完成
- **THEN** manifest.json SHALL 包含 version、pipeline="dataset"、created_at、git_commit、parameters（parameters_dataset.yaml 完整內容）、artifacts（產出檔案清單）

#### Scenario: pipeline 失敗不寫入 manifest
- **WHEN** dataset pipeline 執行失敗
- **THEN** SHALL 不寫入 manifest.json


## MODIFIED Requirements

### Requirement: Dataset manifest includes run_id
The dataset pipeline manifest.json SHALL include a `run_id` field recording the execution run identifier.

#### Scenario: run_id in dataset manifest
- **WHEN** the dataset pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id
