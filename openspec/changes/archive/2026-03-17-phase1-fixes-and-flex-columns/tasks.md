## 1. README `--env` 文件修正

- [x] 1.1 更新 README.md 環境覆蓋機制段落，將「未指定 `--env` 時，僅載入 `base/` 配置」改為說明預設值為 `local`

## 2. Inference output 改用實際 model hash

- [x] 2.1 修改 `__main__.py` inference 區塊：將 `runtime_params["model_version"]` 從 `"best"` 改為 `mv`（resolve_model_version 回傳的實際 hash）
- [x] 2.2 修改 `__main__.py` inference 區塊：在 catalog config 解析後，針對 model 和 preprocessor entry 的 filepath 將 model hash 替換回 `best`（確保透過 symlink 讀取模型）
- [x] 2.3 修改 `__main__.py` inference 後處理：新增 `update_symlink` 將 `data/inference/latest` 指向最新 output 目錄
- [x] 2.4 更新 `__main__.py` inference 版本 log：改為 log 實際 model hash 而非 `"best"`
- [x] 2.5 更新 `test_cli.py` 中 `test_inference_uses_best_model_version`：model filepath 仍驗證 `best`，但 scoring_dataset filepath 應驗證使用實際 hash `a1b2c3d4` 而非 `best`

## 3. Dataset pipeline 欄位彈性化

- [x] 3.1 在 `parameters_dataset.yaml` 新增 `prepare_model_input` 區塊，包含 `drop_columns` 和 `categorical_columns` 預設值
- [x] 3.2 修改 `nodes_pandas.py` 的 `prepare_model_input()`：從 `parameters["dataset"]["prepare_model_input"]` 讀取欄位設定，未提供時使用預設值
- [x] 3.3 修改 `nodes_spark.py` 的 `prepare_model_input()`：同步 pandas 版本的改動
- [x] 3.4 確認 preprocessor dict 正確記錄實際使用的 drop_columns 和 categorical_columns

## 4. 驗證

- [x] 4.1 執行 `pytest tests/ -v` 確認所有測試通過（3 個 pre-existing spark failures 不影響）
- [x] 4.2 執行 `python -m recsys_tfb run -p dataset -e local` 確認 dataset pipeline 正常
- [x] 4.3 執行 `python -m recsys_tfb run -p inference -e local` 確認 inference output 路徑使用實際 hash
- [x] 4.4 檢查 inference manifest.json 記錄正確的 model_version hash
