## Why

Catalog artifacts 目前的儲存格式不一致且不夠理想：model 使用 pickle（不可讀、跨版本脆弱）、X/y 資料類 artifacts 也用 pickle（應與其他資料集一致使用 parquet）、pipeline parameters 沒有獨立 snapshot（manifest 中雖有但不方便單獨檢視）。改善儲存格式可提升可讀性、跨版本相容性，並讓所有資料類 artifacts 統一使用 parquet。

## What Changes

- **新增 LightGBM 原生格式 I/O adapter**：model 從 PickleDataset 改為 LightGBMDataset，使用 `model.save_model()` / `lgb.Booster(model_file=...)` 原生文字格式。**BREAKING**: 既有 `model.pkl` 檔案不再相容，需重新執行 training pipeline。
- **X/y preprocessed arrays 改用 ParquetDataset**：`X_train`、`y_train` 等 6 個 artifacts 從 PickleDataset 改為 ParquetDataset。`y_train` 型別從 `np.ndarray` 改為 `pd.DataFrame`（單欄 `"label"`）。**BREAKING**: 既有 `.pkl` 檔案不再相容。
- **Pipeline parameters JSON snapshot**：每次 pipeline 執行後，在版本目錄中儲存獨立的 `parameters_*.json` 檔案，方便直接檢視和比較不同版本的參數設定。

## Capabilities

### New Capabilities
- `lightgbm-dataset`: LightGBM 原生格式的 I/O adapter，支援 `lgb.Booster` 的 save/load/exists
- `parameter-snapshot`: Pipeline 執行後自動儲存 parameters JSON snapshot 至版本目錄

### Modified Capabilities
- `io-datasets`: 新增 LightGBMDataset 至 dataset type registry
- `data-catalog`: model entry 改用 LightGBMDataset，X/y entries 改用 ParquetDataset
- `dataset-nodes`: `prepare_model_input` 的 y 回傳型別從 `np.ndarray` 改為 `pd.DataFrame`
- `training-nodes`: 所有接收 y 的函式需適配 DataFrame 輸入
- `spark-dataset-nodes`: 同 `dataset-nodes` 的 y 型別變更

## Impact

- **I/O 層**：新增 `src/recsys_tfb/io/lightgbm_dataset.py`，修改 `io/__init__.py` 和 `core/catalog.py` registry
- **Pipeline nodes**：`dataset/nodes_pandas.py`、`dataset/nodes_spark.py`、`training/nodes.py` 的 y 型別和傳遞方式
- **Config**：`conf/base/catalog.yaml` 的 model 和 X/y entries
- **CLI**：`__main__.py` post-run 區塊新增 parameter snapshot 寫入
- **Tests**：dataset nodes、training nodes、CLI 相關測試需更新
- **Notebook**：`inspect_artifacts.ipynb` 已在先前修正中處理 DataFrame 型別
- **Dependencies**：無新增依賴（lightgbm 已是既有 dependency）
- **Breaking**：既有 model.pkl 和 X/y .pkl 檔案不再相容，需重新執行 pipeline
