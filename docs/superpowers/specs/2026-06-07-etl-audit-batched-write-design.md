# etl_audit_log 批次化寫入設計

> **狀態**：設計定稿，待轉 implementation plan。
> **範圍**：`src/recsys_tfb/pipelines/source_etl/audit.py`（`AuditWriter`）與其在
> `sql_runner.py` 的呼叫點；`etl_audit_log` Hive 表 schema；相關測試與 `docs/pipelines/source_etl.md`。

## 問題

`AuditWriter.write_record()` 對每一筆稽核紀錄各跑一條獨立的
`INSERT INTO ... PARTITION(snap_date) SELECT <literals>`。在 Spark/Hive 上**每次
INSERT 動作至少產生一個檔案**，所以：

- 正常 run：每個 snap_date × 每張 table 一筆（success/failed）＋每個 snap_date 一筆
  `__summary__`。以 feature_etl ~6 張表、12 個 snap_date 為例 → 一次 run 就產出
  **12 ×(6+1)= 84 個只有 1 row 的 parquet 檔**。
- 又是 `INSERT INTO`（append）、按 `snap_date STRING` 分區、從不 compaction → 每次重跑
  同一天再往該分區塞新碎檔，**無上限累積**。

附帶問題：`write_record` 以 `replace("'", "\\'")` 手動跳脫 error message，對換行與其他
特殊字元脆弱，可能讓拼出來的 SQL 失效。

## 目標

1. 一次 run 的稽核紀錄**批次化、一次寫出**，把檔案數從 `O(dates × tables)` 降到
   **每 run 1 檔**。
2. 移除手動 SQL 字串跳脫（改用 DataFrame 寫入，由 Spark 處理跳脫）。
3. fail-fast 中止時，已產生的稽核紀錄仍必須落地。
4. audit 寫入本身的失敗**不得**壓掉或蓋過真正的 ETL 例外。

## 設計決策

### D1：取消 `snap_date` 分區

`etl_audit_log` 資料量極小，按 `snap_date STRING` 分區本身就是小檔放大器（每天一個目錄、
每次寫入至少一檔）。改成**不分區**，`snap_date` 降為一般欄位、欄序不變。查詢仍可
`WHERE snap_date = '...'`，只是失去分區裁剪——對極小的 audit 表可忽略。

source_etl 尚未在公司環境部署（見專案記憶 `project_inference_not_yet_deployed.md`），
**無 backward-compat 包袱**，可乾淨重建表。

```sql
CREATE TABLE IF NOT EXISTS {database}.{table} (
    run_id           STRING,
    snap_date        STRING,
    table_name       STRING,
    status           STRING,
    row_count        BIGINT,
    duration_seconds DOUBLE,
    error_message    STRING,
    created_at       TIMESTAMP
)
STORED AS PARQUET
```

### D2：`write_record` / `write_summary` 改為 buffer，新增 `flush()`

- `write_record(record)`：只 `self._buffer.append(record)`，並維持原本的 `logger.info`
  結構化日誌（即時可觀測，不依賴 flush）。
- `write_summary(...)`：建出 `__summary__` 的 `AuditRecord` 後同樣走 `write_record`
  （即 append 到 buffer）。
- `flush()`：buffer 非空時，`spark.createDataFrame(rows, schema)` →
  `.coalesce(1).write.mode("append").insertInto(fqn)` → 清空 buffer；空 buffer 直接
  no-op。

`created_at` 由 SQL 的 `CURRENT_TIMESTAMP()` 改為 Python 在 `flush()` 時填
`datetime.now(timezone.utc)`（`audit.py` 已 import `datetime, timezone`），對同一次 flush
的所有 row 用同一時間戳。

### D3：欄序單一真實來源

`insertInto` 按**位置**對齊（忽略 DataFrame 欄名），因此 CREATE DDL 的欄序與
`createDataFrame` 用的 `StructType` 欄序必須一致。用**一個欄位順序定義**同時驅動兩者，
避免錯位。`StructType`：

```python
StructType([
    StructField("run_id",           StringType(),    True),
    StructField("snap_date",        StringType(),    True),
    StructField("table_name",       StringType(),    True),
    StructField("status",           StringType(),    True),
    StructField("row_count",        LongType(),      True),
    StructField("duration_seconds", DoubleType(),    True),
    StructField("error_message",    StringType(),    True),
    StructField("created_at",       TimestampType(), True),
])
```

每筆 `AuditRecord` 依此欄序轉成一個 tuple（含 Python 端填入的 `created_at`）。

### D4：`flush()` 呼叫點（`sql_runner.py`）

兩個入口都在最外層保證 flush：

- **`run()`**：snap_date 迴圈外再包一層 `try/finally`，`finally: if not self._dry_run
  and audit: audit.flush()`。既有 per-snap_date 的 `write_summary` 維持在內層 finally，
  只是現在是 append 到 buffer。
- **`run_source_checks()`**：寫完失敗紀錄後，以 `try/finally` 包住 `raise
  SourceCheckError(...)`，`finally` 裡 `if audit: audit.flush()`——確保例外拋出前
  紀錄已落地。

dry_run 時 `_initialize_context` 回傳 `(None, None)`，`audit` 為 `None`，flush 被
`if audit` 擋掉。

### D5：audit 失敗不致命

`flush()` 內部以 `try/except Exception` 包住實際 Spark 寫入，失敗時只
`logger.error("Failed to flush audit records: %s", exc)`、**不 re-raise**。理由：

- flush 在 `finally` 執行；若它拋例外會**蓋掉**正在傳播的真正 ETL 例外。
- 稽核紀錄不該拖垮 ETL 本身。

代價：audit 寫入失敗只進 log、不中斷 run。對稽核用途可接受。

## 不做（YAGNI）

- **跨 run compaction**（read-modify-write / `INSERT OVERWRITE` 合併舊檔）：不分區 +
  每 run 1 檔已足夠；compaction 增加並發覆蓋風險，對 audit log 過度。
- **多槽 / 多檔策略**：單檔/run 已解決問題。
- **改 `AuditRecord` 資料模型**：欄位不變，只是寫入路徑改變。

## 受影響檔案

- **改**：`src/recsys_tfb/pipelines/source_etl/audit.py`
  - `_CREATE_AUDIT_TABLE_SQL`：移除 `PARTITIONED BY`、加 `snap_date STRING` 欄。
  - 移除 `_INSERT_AUDIT_SQL` 與手動跳脫。
  - 加欄序常數 + `StructType`、`self._buffer`、`flush()`。
  - `write_record` / `write_summary` 改為 buffer append（保留 logger.info）。
- **改**：`src/recsys_tfb/pipelines/source_etl/sql_runner.py`
  - `run()` 外層加 `try/finally` flush。
  - `run_source_checks()` 加 `try/finally` flush。
- **改**：`tests/test_pipelines/test_source_etl/test_audit.py`
  - 不再斷言 `INSERT INTO` 字串與 `PARTITIONED BY`。
  - 改斷言：CREATE 不含 `PARTITIONED BY`、含 `snap_date STRING` 欄；`write_record`
    後未立即 `spark.sql`（只 buffer）；`flush()` 觸發 `createDataFrame` 帶正確 row 數
    與內容、`.coalesce(1)...insertInto`；空 buffer flush 為 no-op；含特殊字元的
    error_message 原樣帶入 row（不需手動跳脫）。
- **改**：`tests/test_pipelines/test_source_etl/test_sql_runner.py`
  - 對齊 buffer/flush：原本 assert `audit.write_record` 立即寫的測試，改為驗證
    record 進 buffer 且 run/`run_source_checks` 結束有呼叫 `flush`。
- **改**：`docs/pipelines/source_etl.md`
  - §稽核：把「每輪寫一筆 summary」更新為 buffer/flush（每 run 一次寫出、單檔）與
    不分區 schema。

## 驗證

- 單元測試（worktree 絕對 venv python + `PYTHONPATH=<wt>/src`）：
  `test_audit.py`、`test_sql_runner.py` 全綠。
- dev-cluster 實跑一次 `feature_etl --env production`（local conf 視 pipeline 表而定），
  確認 `createDataFrame → coalesce(1) → insertInto` 對未分區 Hive managed table 正常
  寫入、且 `etl_audit_log` 一次 run 只新增 1 個 parquet 檔；`SELECT * FROM
  ml_recsys.etl_audit_log` 內容正確、`created_at` 有值。無 UDF。
