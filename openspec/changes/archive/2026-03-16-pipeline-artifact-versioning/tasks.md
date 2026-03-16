## 1. VersionManager 核心模組

- [x] 1.1 建立 `src/recsys_tfb/core/versioning.py`，實作 `compute_dataset_version(params: dict) -> str` 和 `compute_model_version(params: dict, dataset_version: str) -> str`（SHA-256 hash 前 8 字元）
- [x] 1.2 實作 `write_manifest(version_dir: Path, metadata: dict) -> None`，將 metadata 寫入 manifest.json
- [x] 1.3 實作 `read_manifest(version_dir: Path) -> dict`，讀取 manifest.json
- [x] 1.4 實作 `update_symlink(target: Path, link: Path) -> None`，處理 symlink 建立/更新/取代舊目錄
- [x] 1.5 實作 `resolve_dataset_version(dataset_dir: Path, version: str | None) -> str` 和 `resolve_model_version(models_dir: Path, version: str | None) -> str`
- [x] 1.6 實作 `get_git_commit() -> str | None`，取得當前 git HEAD short hash
- [x] 1.7 為上述所有函式撰寫單元測試 `tests/core/test_versioning.py`

## 2. Catalog 設定變更

- [x] 2.1 更新 `conf/base/catalog.yaml`：所有 dataset 產出路徑加入 `${dataset_version}`，preprocessor/category_mappings 路徑移至 `data/dataset/${dataset_version}/`，inference 產出路徑加入 `${model_version}/${snap_date}`
- [x] 2.2 更新 `conf/local/catalog.yaml`：與 base 同步變更
- [x] 2.3 更新 `conf/production/catalog.yaml`：HDFS 路徑同步加入 template variables

## 3. ConfigLoader 擴展

- [x] 3.1 在 `src/recsys_tfb/core/config.py` 新增 `get_parameters_by_name(name: str) -> dict` 方法
- [x] 3.2 為 `get_parameters_by_name` 撰寫單元測試

## 4. CLI 版本解析邏輯

- [x] 4.1 重構 `src/recsys_tfb/__main__.py` 的 `run` 命令：新增 `--dataset-version` 選項
- [x] 4.2 實作 dataset pipeline 的版本解析：計算 hash → 注入 runtime_params → 執行完成後 write_manifest + update_symlink(latest)
- [x] 4.3 實作 training pipeline 的版本解析：resolve dataset_version（latest 或指定）→ 計算 model_version hash → 注入 runtime_params → 執行完成後 write_manifest
- [x] 4.4 實作 inference pipeline 的版本解析：model_version="best" → 從 model manifest 讀取 dataset_version → 注入 runtime_params（含 snap_date）→ 執行完成後 write_manifest
- [x] 4.5 所有 pipeline 啟動時 log 輸出版本 ID

## 5. Model 比較與 Promotion 更新

- [x] 5.1 更新 `src/recsys_tfb/pipelines/training/nodes.py` 中 `compare_model_versions`：版本目錄正則同時支援 hash 格式（`^[0-9a-f]{8}$`）和舊的時間戳格式（`^\d{8}_\d{6}$`）
- [x] 5.2 更新 `scripts/promote_model.py`：版本正則支援新舊格式、改用 symlink 取代 copy、複製/保留 manifest.json
- [x] 5.3 更新 `tests/scripts/test_promote_model.py`：測試 symlink 行為、hash 格式支援

## 6. 整合測試與驗證

- [x] 6.1 端到端測試：依序執行 dataset → training → inference pipeline，驗證版本目錄建立、manifest 內容、symlink 正確性
- [x] 6.2 驗證舊版本相容性：確認既有 `YYYYMMDD_HHMMSS` 格式目錄不受影響，compare_model_versions 和 promote 都能處理
- [x] 6.3 執行完整測試套件 `pytest tests/` 確認無回歸
