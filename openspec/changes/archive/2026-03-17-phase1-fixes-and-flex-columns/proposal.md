## Why

目前存在三個已知問題影響正確性和可維護性：(1) README 文件說明 `--env` 預設行為與實際程式碼不符；(2) inference pipeline output 路徑使用 `"best"` 字串而非實際 model hash，導致版本追溯困難且多版本 output 互相覆蓋；(3) dataset pipeline 的 `drop_cols` 和 `categorical_cols` 是 hard-coded，不符合「Externalize configuration」設計原則。這些問題在進入後續 Phase（結構化日誌、版本管理增強）前應先修正。

## What Changes

- **修正 README `--env` 說明**：更新 README.md 環境覆蓋機制段落，說明 `--env` 預設值為 `local`（非僅載入 `base/`）
- **Inference output 使用實際 model hash**：將 `__main__.py` 中 inference 的 `runtime_params["model_version"]` 從 `"best"` 改為 `resolve_model_version()` 回傳的實際 hash，使 output 路徑為 `data/inference/<hash>/<snap_date>/`。model 讀取仍透過 `best` symlink。inference output 目錄新增 `latest` symlink 維護。
- **Dataset pipeline 欄位彈性化**：將 `prepare_model_input()` 中 hard-coded 的 `drop_columns` 和 `categorical_columns` 抽取到 `parameters_dataset.yaml`，保留現有值作為程式碼內預設值（向後相容）

## Capabilities

### New Capabilities
- `configurable-model-input`: 將 dataset pipeline `prepare_model_input` 的 drop_columns 和 categorical_columns 從 hard-coded 改為可透過 YAML 設定

### Modified Capabilities
- `cli`: inference pipeline 的 `model_version` runtime param 改用實際 hash 而非 `"best"` 字串
- `inference-versioning`: inference output 路徑改用實際 model hash，新增 `latest` symlink

## Impact

- **程式碼**：`__main__.py`（inference 區塊）、`nodes_pandas.py`、`nodes_spark.py`、`parameters_dataset.yaml`
- **文件**：`README.md`
- **測試**：`test_cli.py` 中 `test_inference_uses_best_model_version` 需更新驗證邏輯
- **向後相容**：欄位彈性化保留預設值，不影響未提供設定的使用者；inference output 路徑結構改變（`best/` → `<hash>/`）
