# Source ETL：feature_table 加欄位（schema evolution）設計

- 日期：2026-06-02
- 分支：`feat/source-etl-add-column`
- 狀態：設計（待 review）

## 1. 問題與目標

讓使用者在調整 feature ETL SQL（主要是 `conf/sql/etl/feature/feature_concat.sql`）**新增一個 feature 欄位**時，能把新欄位**寫進既有的 `feature_table`（與鏈上其他既有表）**，而不需要砍表重建、也不破壞既有模型。

情境：**同一個 `snap_date`**，SQL 多 SELECT 一個欄位（或 join 一張新的 `feature_*` 來源表），重跑 feature ETL 後既有表就多那一欄。

### 可重現性範圍：只保證 L1 + L2

可重現性拆三層（本設計只承諾前兩層）：

- **L1 打分可重現**：既有已訓練模型現在跑 inference，分數與以前一致。
- **L2 產物保存**：既有模型的 `model.txt` / preprocessor / calibrator / manifest 仍在、載得起來。
- L3 從來源重算（不承諾）：能重跑 dataset+training 得到等價產物。`INSERT OVERWRITE` 就地覆寫會破壞 L3，本設計不處理。

為何加欄位在 L1+L2 下安全（已驗證）：

- **L1**：inference 只挑訓練時釘住的 `preprocessor_metadata["feature_columns"]`，`select(*feature_columns)`，多出來的新欄被忽略（`src/recsys_tfb/preprocessing/_spark.py:447-466`）。
- **L2**：下游 dataset pipeline 以 `base_dataset_version` 目錄分版；加欄重訓只會**新增**版本目錄，不覆蓋舊的。

## 2. 範圍

### In scope

- source_etl 寫入路徑：當 rendered SELECT 產生**既有表沒有的新欄位**時，先 `ALTER TABLE ... ADD COLUMNS`，再 `INSERT OVERWRITE`。
- INSERT 投影**一律照「目標表的欄序」**（既有欄按表序、新欄補在最後、partition 最後），確保 positional `INSERT OVERWRITE` 不錯位。
- 偵測到「SELECT 缺了既有表的欄位」（移除欄位）→ **fail loud**，明確擋下並提示走版本化重建。
- 純函式（`build_*`）以 TDD 單元測試覆蓋；runner 行為以既有 Spark 測試模式覆蓋。

### Out of scope（本次刻意不做）

- 任何**版本化／版本標記 guard**。下游 `base_dataset_version` 由 dataset pipeline 從表 schema 自動重算，本路徑不碰。
- **「改既有欄位邏輯」的偵測**（同名同型、值改變）。本路徑只**加欄**；改邏輯雖然 `INSERT OVERWRITE` 會生效，但**不偵測、不防護**——這是已知未防護 gap（指紋 schema-only 看不到值），需文件註明，且此路徑不得被當成改邏輯的工具。
- **既有欄位的型別變更**（SELECT 把某既有欄從 int 變 double 等）：`ADD COLUMNS` 不涵蓋，本路徑不處理。
- **物理減欄 / 版本化重建**。
- 歷史分區回填：`ADD COLUMNS` 後**未被覆寫的舊分區，新欄讀 NULL**（屬維運動作，不在程式範圍）。

## 3. 現況（已驗證）：為何加欄位現在會直接報錯

ETL 寫入鏈：`feature_aum/sav/ccard/info → feature_concat → feature_table`。
使用者加欄改 `feature_concat.sql`；`feature_table.sql` 是 `SELECT * FROM feature_concat`，自動帶下去。

寫入核心 `_process_single_table`（`src/recsys_tfb/pipelines/source_etl/sql_runner.py:215-242`）：

1. `columns = spark.sql("SELECT * FROM (body) LIMIT 0").columns` — 取 SELECT 自己的欄（SELECT 順序）。
2. `aligned_select = build_aligned_select(select_sql, columns, partition_by)` — 只對齊「SELECT 自己的欄」，**不與既有表對帳**。
3. 表不存在 → `build_hive_ctas`（CTAS）；表已存在 → `build_insert_overwrite`。

Spark 3.3.2 的 `INSERT OVERWRITE ... SELECT` 是 **positional**、欄數必須相符。SELECT 多一欄（N+1）灌進 N 欄的既有表 → **AnalysisException 欄數不符**。
→ 「加欄寫進既有表」**目前不支援**，必須新增 schema-evolution 邏輯。

附帶發現（潛在既有風險）：現行投影照「SELECT 順序」而非「表順序」。若使用者只是**重排** SELECT 欄序（集合不變），positional INSERT OVERWRITE 也會**靜默錯位**。本設計改為「照表序投影」後，這個既有風險一併修掉。

## 4. 設計

核心不變量：**寫入既有表時，INSERT 投影一律以「目標表欄序」by-name 對齊；新欄是唯一被 append 的東西。**

### 4.1 `_process_single_table`（表已存在分支）新流程

在 `build_insert_overwrite` 之前插入 schema 對帳：

1. 取既有表欄位（保序、去掉 partition 欄）：`existing_nonpart`（list[str]，表序）。
2. 取 SELECT 的 **schema**（名稱＋型別）：把現有 `LIMIT 0` 查詢的 `.columns` 改用 `.schema`（StructField），不需額外查詢。去掉 partition 欄得 `select_nonpart`（保序）與其型別。
3. 比對（case-insensitive，Hive 會 lowercase）：
   - `new_cols = select_nonpart − existing_nonpart`（保留 SELECT 出現順序）。
   - `removed_cols = existing_nonpart − select_nonpart`。
4. `removed_cols` 非空 → `raise SourceETLError`（fail loud）：訊息說明「移除欄位不支援，請走版本化重建」，列出哪些欄。
5. `new_cols` 非空 → 執行 `ALTER TABLE <db>.<name> ADD COLUMNS (<col> <type>, ...)`（型別取自 SELECT schema，`dataType.simpleString()`，與 Hive 型別相容）。log 出新增了哪些欄。
6. 目標表欄序（ALTER 後）= `existing_nonpart + new_cols`。用 order-aware 的投影函式產生 INSERT SELECT：依此順序 by-name 取欄、partition casts 最後。
7. `INSERT OVERWRITE`（沿用 `build_insert_overwrite`，但傳入 order-aware 的 aligned select）。

表不存在分支（CTAS）維持不變：CTAS 以 SELECT 順序建表，確立該表的 canonical 欄序。

### 4.2 `sql_renderer.py` 新增/調整

- **新增** `build_alter_add_columns(table_config, new_cols: list[tuple[str, str]], target_db) -> str`
  產生 `ALTER TABLE <db>.<name> ADD COLUMNS (c1 t1, c2 t2)`。純字串組裝，可單測。
- **新增** order-aware 投影（二選一，實作時定）：
  - 方案 a：新增 `build_aligned_select_in_order(select_sql, ordered_nonpartition_cols, partition_by)`，明確吃「目標欄序」。
  - 方案 b：擴充 `build_aligned_select` 接受可選 `target_order` 參數，省一個函式但動到既有簽名。
  傾向 **方案 a**（保留 `build_aligned_select` 給 dry-run / CTAS 不動），降低 regression 面。

### 4.3 讀既有表欄位（保序、去 partition）

新增小 helper（runner 內或 utils）：以 `spark.table(fqn).schema` 取 fields，扣掉 `partition_by` 的 key（case-insensitive），回傳保序的 non-partition 欄名 list。`spark.table().schema` 的欄序對 Hive 表即表定義順序（partition 欄在末段）。

## 5. 邊界與正確性

- **Positional INSERT OVERWRITE**：靠「照表序 by-name 投影 + 新欄 append」保證對齊。這是本設計最關鍵的正確性點。
- **型別映射**：`simpleString()` 產生 `int/bigint/double/float/string/boolean/date/timestamp/decimal(p,s)` 等，皆為合法 Hive 型別。
- **Partition 欄**：絕不 ALTER；由 `partition_by` + PARTITION 子句處理；比對時排除。
- **Case-insensitivity**：所有欄名比對 lowercase（與 `build_aligned_select` 既有作法一致，`sql_renderer.py:59-60`）。
- **既有欄型別變更**：不在範圍；只比對「欄名集合」，不比型別。若既有欄型別在 SELECT 改變，沿用 Spark `INSERT OVERWRITE` 行為（可能隱式 cast 或報錯），不額外處理。
- **dry-run**：維持現狀（不檢查表是否存在，無法得知 delta）；不讓 dry-run schema-aware。
- **歷史分區 NULL**：ALTER 後未覆寫的舊分區新欄為 NULL；舊模型不讀它沒事，需涵蓋舊 snap_date 的新模型由維運回填。

## 6. 測試策略（TDD）

- **純函式**（無 Spark，快）：
  - `build_alter_add_columns`：單欄、多欄、型別含 `decimal(10,2)`、空 new_cols 不該被呼叫（呼叫端負責）。
  - order-aware 投影：表序 ≠ SELECT 序時，投影照表序；新欄 append；partition casts 最後；缺 partition 欄 raise。
- **runner 行為**（沿用 `tests/test_pipelines/test_source_etl/` 既有 Spark/mock 模式，如 `test_sql_runner.py`/`test_sql_renderer.py`）：
  - 既有表 N 欄 + SELECT N+1 欄 → 觸發 ALTER，且 INSERT 後資料落在正確欄（值對位驗證）。
  - SELECT 缺既有欄 → raise `SourceETLError`，訊息含被移除欄名。
  - 欄序不變、無新欄 → 不 ALTER，行為等同今日（但投影改表序仍正確）。
- 測試只跑 `tests/test_pipelines/test_source_etl/`（秒級），不跑整包；驗證以 `git diff` + 針對性執行為主。

## 7. 決策與非目標

- **D1**：寫入既有表一律照表序 by-name 投影（順帶修掉重排靜默錯位的既有風險）。
- **D2**：移除欄位 fail loud，不靜默 drop。
- **D3**：改邏輯不偵測（版本化延後）；此路徑只准加欄，文件明令不得用於改邏輯。
- **D4**：order-aware 投影採新函式（方案 a），不動 `build_aligned_select` 既有簽名。
- 非目標：版本化、L3、既有欄型別變更、物理減欄、歷史回填。

## 8. 待確認

- 無重大未決項。實作時於 4.2 在「新函式 vs 擴充既有簽名」做最終取捨（傾向新函式）。
