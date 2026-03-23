## 1. Config Schema 核心模組

- [x] 1.1 建立 `src/recsys_tfb/core/schema.py`，實作 `get_schema(parameters)` 函式：預設值、entity 正規化為 list、identity_columns 自動推導
- [x] 1.2 建立 `tests/test_core/test_schema.py`，覆蓋：defaults、partial override、full override、entity string→list、entity list、identity_columns 推導、input dict 不被 mutate
- [x] 1.3 更新 `conf/base/parameters.yaml`，新增 `schema.columns` section（time / entity / item / label / score / rank）

## 2. Structured Logging 核心模組

- [x] 2.1 建立 `src/recsys_tfb/core/logging.py`，實作 RunContext dataclass、JsonFormatter、ConsoleFormatter、setup_logging(config, context)
- [x] 2.2 建立 `tests/test_core/test_logging.py`，覆蓋：run_id 格式驗證、JsonFormatter 輸出為合法 JSON、ConsoleFormatter 格式、setup_logging 建立正確 handlers、file logging 開關
- [x] 2.3 更新 `conf/base/parameters.yaml`，新增 `logging` section（level / console / file.enabled / file.path / file.format）

## 3. Dataset Pipeline Nodes 改用 Schema

- [x] 3.1 修改 `src/recsys_tfb/pipelines/dataset/nodes_pandas.py`：所有 hard-coded key 改用 `get_schema(parameters)`
- [x] 3.2 修改 `src/recsys_tfb/pipelines/dataset/nodes_spark.py`：同上
- [x] 3.3 修改 `src/recsys_tfb/pipelines/dataset/pipeline.py`：build_dataset nodes 新增 `parameters` 作為輸入
- [x] 3.4 更新 `tests/test_pipelines/test_dataset/test_nodes.py`：test fixtures 加入 schema，確認 default 行為不變

## 4. Training Pipeline Nodes 改用 Schema

- [x] 4.1 修改 `src/recsys_tfb/pipelines/training/nodes.py`：evaluate_model 中 identity columns 改用 `get_schema(parameters)`
- [x] 4.2 更新 `tests/test_pipelines/test_training/test_nodes.py`：確認 default 行為不變

## 5. Inference Pipeline Nodes 改用 Schema

- [x] 5.1 修改 `src/recsys_tfb/pipelines/inference/nodes_pandas.py`：所有 hard-coded key 改用 `get_schema(parameters)`
- [x] 5.2 修改 `src/recsys_tfb/pipelines/inference/nodes_spark.py`：同上
- [x] 5.3 更新 `tests/test_pipelines/test_inference/test_nodes.py` 和 `test_nodes_spark.py`：確認 default 行為不變

## 6. Evaluation 模組改用 Schema

- [x] 6.1 修改 `src/recsys_tfb/evaluation/metrics.py`：groupby keys 改用 schema 參數
- [x] 6.2 修改 `src/recsys_tfb/evaluation/baselines.py`：column references 改用 schema 參數
- [x] 6.3 更新相關 evaluation 測試，確認 default 行為不變

## 7. Runner 整合 Structured Logging

- [x] 7.1 修改 `src/recsys_tfb/core/runner.py`：整合 RunContext、pipeline-level log（start/end/duration/status）、node-level log（name/duration/inputs/outputs/status/error）
- [x] 7.2 更新 `tests/test_core/test_runner.py`：驗證 log 記錄被正確 emit

## 8. CLI 整合

- [x] 8.1 修改 `src/recsys_tfb/__main__.py`：在 pipeline 執行前呼叫 setup_logging()、run_id 寫入 manifest.json
- [x] 8.2 更新 `tests/test_cli.py`：確認 logging 初始化不影響既有 CLI 行為

## 9. 驗證與收尾

- [x] 9.1 執行全量測試 `pytest tests/ -v`，確認所有既有測試通過（向後相容）（test_promote_model 7 個失敗為既有問題）
- [x] 9.2 手動執行三條 pipeline（dataset / training / inference），確認 logs/ 下產生 JSON log
- [x] 9.3 確認 manifest.json 包含 run_id 欄位
- [x] 9.4 確認 JSON log 可被 `python -m json.tool` 正確解析
