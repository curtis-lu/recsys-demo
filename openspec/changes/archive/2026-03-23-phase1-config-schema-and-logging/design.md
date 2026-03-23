## Context

recsys_tfb 是一個 Kedro-inspired 的推薦模型框架，包含 dataset / training / inference 三條 pipeline。目前所有欄位名稱（snap_date、cust_id、prod_name、label 等）hard-coded 在約 40 處，分散於 6 個 pipeline node 檔案與 2 個 evaluation 模組中。日誌僅使用基礎 `logging.info()`，無 run_id、無 JSON 格式、無 node-level timing。

Production 環境為 PySpark 3.3.2 on Hadoop，CPU 4 core / 128GB RAM，不可安裝額外套件。此次變更僅使用 Python 標準庫。

## Goals / Non-Goals

**Goals:**
- 將所有 hard-coded column names 集中到 `parameters.yaml` 的 `schema.columns` section
- 提供 `get_schema()` 函式，預設值與目前 hard-coded 完全一致，確保向後相容
- entity 欄位支援多欄位組合（list 格式）
- 建立 JSON structured logging 框架，支援 console 可讀 + file JSON 雙輸出
- Runner 整合 run_id、pipeline-level / node-level 結構化日誌
- Log schema 穩定可機器解析，未來可接 ELK / Splunk

**Non-Goals:**
- MemoryDataset cleanup（Phase 2）
- 演算法抽象 LightGBM / XGBoost（Phase 2）
- Probability calibration（Phase 2）
- Spark toPandas 優化（Phase 2）
- Evaluation pipeline 化（Phase 3）
- Data quality logging / artifact lineage（Phase 3）
- Sample pool table 分離 / group-specific sampling（Phase 2）

## Decisions

### D1: Schema 結構設計 — 扁平 columns dict + 自動推導 identity_columns

**選擇**：`schema.columns` 為扁平 dict（time / entity / item / label / score / rank），identity_columns 由程式自動組合 `[time] + entity + [item]`。

**替代方案**：
- A) 每個 pipeline 各自定義 keys → 重複設定、容易不一致
- B) 深度巢狀結構（schema.dataset.keys / schema.training.keys / ...）→ 過度複雜，目前三條 pipeline 用的 key 完全相同

**理由**：目前且可預見的未來，所有 pipeline 共用同一組 key。扁平結構最簡單、最不容易出錯。

### D2: entity 欄位用 list 表示

**選擇**：`entity: [cust_id]`，永遠為 list。`get_schema()` 自動將 string 轉為 list。

**理由**：使用者表示未來可能需要多欄位組合 entity（如 `[branch_id, cust_id]`）。統一為 list 避免下游 if/else。

### D3: get_schema() 設計 — 純函式 + 預設值

**選擇**：`get_schema(parameters) -> dict`，從 `parameters["schema"]` 取值，缺失時 fallback 到硬編碼預設值。

**替代方案**：
- A) 全域 singleton / class instance → 增加複雜度，測試不易
- B) 要求 parameters.yaml 必填 schema section → 破壞向後相容

**理由**：純函式最容易測試和理解。預設值確保既有測試和設定檔不需改動即可正常運作。

### D4: Logging 架構 — Python 標準 logging + 自訂 Formatter

**選擇**：基於 Python 標準 `logging` 模組，新增 `JsonFormatter`（JSON lines 格式）和 `ConsoleFormatter`（人類可讀），透過 `setup_logging()` 設定 handlers。

**替代方案**：
- A) structlog → 需要額外安裝，production 不允許
- B) 自建 logging 框架 → 過度設計，標準庫已經夠用
- C) 純 print → 無法控制 level、無法導向檔案

**理由**：Python 標準 `logging` 模組功能充足，可自訂 Formatter，不需額外依賴。

### D5: run_id 格式 — timestamp + random hex

**選擇**：`YYYYMMDD_HHMMSS_{6 chars random hex}`，例如 `20260322_120000_a1b2c3`。

**替代方案**：
- A) UUID4 → 太長，不利人類閱讀
- B) 純 timestamp → 可能在同秒內重複

**理由**：timestamp 便於人類閱讀和排序，random hex 避免衝突，總長度適中。

### D6: RunContext — thread-local context

**選擇**：`RunContext` dataclass 存於 module-level variable，`setup_logging()` 初始化，log formatter 自動注入。

**理由**：框架為單執行緒順序執行，不需要 thread-local storage 的複雜度。Module-level variable 最簡單。

### D7: Log file 路徑策略

**選擇**：log 目錄由 config 指定（`logging.file.path`），檔名自動為 `{pipeline}_{run_id}.jsonl`。

**理由**：方便按 pipeline + 時間排序查找，`.jsonl` 副檔名標示 JSON lines 格式。

## Risks / Trade-offs

- **[Risk] get_schema() 呼叫頻率高** → 每個 node 都會呼叫，但函式為純 dict 操作，效能可忽略。如需優化，可在 Runner 層快取。
- **[Risk] schema 改動未同步到 evaluation CLI script** → evaluate_model.py 也有 hard-coded columns。Phase 1 僅修改 pipeline nodes 和 evaluation 模組內部，evaluate_model.py 的 CLI 參數保持不變，Phase 3 再整合。
- **[Risk] 既有測試 fixture 未加 schema** → get_schema() 有預設值，不會 break。但建議同步更新 fixture 以確保測試覆蓋。
- **[Trade-off] Log file 會累積** → 不實作自動清理。Production 環境由運維團隊管理 log retention。
- **[Trade-off] Console 和 file 兩種格式需維護一致性** → 以 JsonFormatter 為主要格式，ConsoleFormatter 僅為可讀性輔助，兩者共用同一組 LogRecord extra fields。
