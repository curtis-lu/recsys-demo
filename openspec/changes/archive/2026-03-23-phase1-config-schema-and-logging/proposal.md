## Why

所有 pipeline nodes 中的欄位名稱（snap_date、cust_id、prod_name、label 等）散落在 ~40 處 hard-coded strings，無法彈性替換，也讓框架難以適應不同的上游資料結構。同時，目前僅有基礎 print/logging.info，缺乏結構化日誌與 run_id 追蹤，production 環境中難以定位問題與追蹤 pipeline 執行歷程。此兩項是後續所有重構（演算法抽象、Spark 優化、evaluation pipeline 化等）的共同基礎，需優先建立。

## What Changes

- **新增 config-driven column schema**：在 `parameters.yaml` 中新增 `schema.columns` section，定義 time / entity / item / label / score / rank 等欄位名稱，entity 支援多欄位組合（list）
- **新增 `core/schema.py` 模組**：提供 `get_schema(parameters)` 函式，所有 pipeline nodes 統一透過此函式取得欄位名稱，預設值完全向後相容
- **重構所有 pipeline nodes**：將 ~40 處 hard-coded column names 替換為 `get_schema()` 呼叫
- **新增 structured logging 框架**：`core/logging.py` 提供 JsonFormatter（JSON lines，可接 ELK/Splunk）、ConsoleFormatter（人類可讀）、RunContext（run_id + pipeline/node context）
- **Runner 整合 logging**：pipeline-level 與 node-level 結構化日誌，包含 duration、input/output names、status
- **Config 新增 logging section**：log level / console / file path / format 均可設定
- **manifest.json 新增 run_id 欄位**：每次執行可追蹤

## Capabilities

### New Capabilities
- `column-schema`: 集中式欄位名稱管理，透過 `parameters.yaml` 的 `schema.columns` section 定義所有 pipeline 使用的欄位名稱，支援 entity 多欄位組合
- `structured-logging`: JSON structured logging 框架，包含 run_id 追蹤、pipeline-level / node-level 日誌、console + file 雙輸出

### Modified Capabilities
- `dataset-nodes`: nodes 改用 `get_schema()` 取得欄位名稱，不再 hard-code
- `spark-dataset-nodes`: 同上，Spark 版本
- `training-nodes`: evaluate_model 中 identity columns 改用 schema
- `inference-nodes`: 欄位名稱改用 schema
- `spark-inference-nodes`: 同上，Spark 版本
- `evaluation-metrics`: groupby keys 改用 schema
- `evaluation-baselines`: column references 改用 schema
- `pipeline-engine`: Runner 整合 structured logging（run_id、node timing、status）
- `config-loader`: parameters.yaml 新增 schema + logging sections
- `dataset-versioning`: manifest.json 新增 run_id 欄位
- `model-versioning`: manifest.json 新增 run_id 欄位
- `inference-versioning`: manifest.json 新增 run_id 欄位
- `cli`: __main__.py 整合 setup_logging()

## Impact

- **修改檔案**：~13 個既有檔案 + 2 個新檔案 + 2 個新測試檔
- **Config 變更**：`conf/base/parameters.yaml` 新增 `schema` + `logging` sections（向後相容，有預設值）
- **向後相容**：所有既有測試應不需修改即通過（get_schema 有預設值）；建議同步在 test fixtures 加入 schema section
- **無 breaking changes**：所有預設值與目前 hard-coded 值完全一致
- **依賴**：僅使用 Python 標準庫（logging、json、uuid），不需安裝額外套件
