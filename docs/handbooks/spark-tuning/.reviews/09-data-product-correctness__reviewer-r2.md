# 09 章 §9.6 聚焦補審（reviewer-r2）

> 範圍：**只審** `## 9.6 進階：資料版本化的設計模式與歷史回補`（檔案 09-data-product-correctness.md 第 316–456 行），不審其他節。
> 環境基準：Spark 3.3.2 + Hive 3.1.3（CDP 7.1.9，YARN+HDFS），external Parquet/ORC（非 ACID），無 Iceberg/Delta。
> 來源限官方（Spark 3.3.x / Cloudera CDP / dbt / Airflow 官方）、Spark 核心開發者文章、指定書籍。逐條 ✅/❌/⚠️。

---

## 逐條查證

### 1. bitemporal：snapshot_date=valid time、build_version=transaction time —— ✅ 已驗證

主張（第 322–329 行）：把 `snapshot_date` 對應 **valid time（業務時間）**、`build_version` 對應 **transaction time（處理時間）**，雙軸合稱 **bitemporal**。

- 通用定義查證一致：「A bitemporal database is a database supporting valid time and transaction time.」「**Valid time** is the period during which a fact was true in the real world.」「**Transaction time** is the period during which a fact was recorded in the database system / the point at which data arrives into the database, giving us an audit trail.」
- 文中對應方向**正確**：
  - `snapshot_date`＝「這份資料**描述**的是哪個時間點的客戶狀態」＝ fact 在真實世界為真的時間 ＝ **valid time** ✅
  - `build_version`＝「這份資料**什麼時候被算出來**」＝ fact 被記錄進系統的時間 ＝ **transaction time** ✅
- 「同時拿兩條軸記資料在資料倉儲裡叫 bitemporal」與「雙層分區是它最樸素的實作」皆為通用設計模式陳述，方向正確（文中 footer 已誠實標示為「通用資料倉儲設計模式、無單一官方逐字出處」）。
- 📚 出處：bitemporal / valid time / transaction time 標準定義（ScienceDirect Topics — Bitemporal Data；XTDB Bitemporality docs；IJCSIT「A Survey on Bitemporal Data Warehousing System」）。屬通用 temporal-database 術語，非 Spark/CDP 功能——文中無誤掛為官方功能。

### 2. 核心限制：純 Hive/Parquet external 表在 Spark 3.3 不支援 UPDATE/DELETE/MERGE 改單列，row-level DML 需 DataSource v2（Iceberg/Delta）—— ✅ 已驗證（本節立論關鍵，成立）

主張（第 389 行 + footer 第 456 行）：「純 external Parquet 在 Spark 3.3 本來就不支援 `UPDATE`／`MERGE` 改單列（那要 Iceberg/Delta 那種支援列級操作的格式）」；故 promote 用 append-only 發佈紀錄表或整表 `INSERT OVERWRITE`。

- Spark 對 v1（檔案式 Hive/Parquet）表執行 DELETE 會直接丟 `AnalysisException: DELETE is only supported with v2 tables`（這是實際 runtime 錯誤訊息，多處實證：databrickslabs/discoverx#123、delta-io/delta#774）。
- DELETE/UPDATE/MERGE 的列級語意自 Spark 3.0 起經 **DataSource V2** 介面實作（`SupportsDelete` 等；SPARK-28351）；v1 file-based table 不具該能力。Iceberg/Delta 才提供列級 mutation（Iceberg：filter 命中整分區→metadata-only delete，命中個別列→rewrite 受影響 data files）。
- 故文中「要改『對外版本指標』只能 (a) append-only 發佈紀錄表 或 (b) 整表 `INSERT OVERWRITE`」的替代作法**因果方向正確**：因為不能 row-level update，所以改用 append 或整表覆寫。✅
- 一個**精確度註記（非錯誤）**：文中與 §8.2 連動的措辭聚焦 `UPDATE`／`MERGE`／`DELETE`，與官方 v2-only 限制一致。footer 自承「已對 Spark 3.3 行為查證」屬實。
- 📚 出處：SPARK-28351 [SQL] Support DELETE in DataSource V2（apache/spark#25115）；Spark `SupportsDelete` JavaDoc；多個 `DELETE is only supported with v2 tables` 實證 issue。footer 掛的 Spark SQL — DELETE FROM 參考頁存在於 version-locked archive（主站 `/docs/latest/` 已升 4.x、該檔名 404 屬正常版本漂移，不影響主張）。

### 3. 回補走查 SQL：INSERT OVERWRITE ... PARTITION(snapshot_date='...', build_version='...') 只覆寫該 (snapshot,build) 子分區、舊 build 不受影響 —— ✅ 已驗證（但成立的前提需留意）

主張（第 263–267、362–365 行）：靜態指定**兩個**分區值的 `INSERT OVERWRITE … PARTITION (snapshot_date='2026-03-31', build_version='20260612T0200')` 只覆寫該子分區，舊 build 子分區原封不動。

- 靜態分區覆寫語意：當 partition spec 給定具體值時，Spark（預設 `spark.sql.sources.partitionOverwriteMode=static`）只截斷/覆寫**符合該 spec 的分區**，不動其他分區（SPARK-20236；waitingforcode「Overwriting partitioned tables in Apache Spark SQL」）。
- 文中 SQL **兩個分區欄都是完全靜態**（都給了字面值），所以只命中 `(2026-03-31, 20260612T0200)` 這一個葉子子分區 → 同一 snapshot 的其他 `build_version` 子分區不受影響。✅ 主張正確。
- ⚠️ **成立前提（文中 SQL 已滿足，但讀者易踩的隱含條件）**：此安全性**依賴 build_version 也寫成靜態值**。官方語意：`INSERT OVERWRITE tbl PARTITION (a=1, b)`（b 為**動態**）會「truncate all the partitions that start with a=1」——亦即若把 `build_version` 留成動態欄，會把該 snapshot 底下**所有** build 都清掉，正好打爛本節要保護的東西。文中範例兩欄皆靜態、寫法正確；此點屬「正確但脆弱」的邊界，非缺陷（footer 未明示此前提，可加強）。
- 📚 出處：SPARK-20236「Overwrite a partitioned data source table should only overwrite related partitions」；Spark 3.3.2 INSERT 參考頁（archive 3.3.2 確認支援多欄靜態 `PARTITION (col=val [, ...])` 與 `INSERT INTO … VALUES`）。

### 4. current view 的 ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY promoted_at DESC) rn=1 選出每 snapshot 最後一次 promote 的 build —— ✅ 已驗證

主張（第 412–427 行）：用 `ROW_NUMBER()`＋`PARTITION BY snapshot_date ORDER BY promoted_at DESC` 取 `rn=1`，選出每個 snapshot「最後一次被 promote」的 `build_version`；Spark 3.3 支援此語法。

- Spark 3.3.2 Window Functions 頁逐字確認：ranking 函式含 `ROW_NUMBER`；`OVER` 子句語法支援 `PARTITION BY … ORDER BY expression [ ASC | DESC ]`。`ORDER BY promoted_at DESC` 後取 `rn=1` 即每分區 `promoted_at` 最大（最後一次）那列。邏輯**正確**、語法**合法**。✅
- ⚠️ tie-break 邊界（文中 footer 第 456 行已自承）：同一 `promoted_at` 時刻多次 promote 時 `ROW_NUMBER` 的勝出列不確定，需另加序號 tie-break。屬已揭露的正確 caveat，非缺陷。
- 📚 出處：Spark 3.3.2 SQL — Window Functions（archive 3.3.2 逐字確認 `ROW_NUMBER` 與 `ORDER BY … DESC`）。

### 5. promote = INSERT INTO … VALUES（append 一列）、rollback = 再 append 一列指回舊版 —— ✅ 已驗證

主張（第 404–407、431 行）：promote 是 `INSERT INTO feature_version_promotion VALUES (...)` append 一列；rollback 是再 append 一列、`reason='rollback'` 指回舊 build。

- `INSERT INTO … VALUES (...)` 為 Spark 3.3.2 合法語法（INSERT 參考頁逐字含 `VALUES ( { value | NULL } [ , ... ] )`，且範例示範）。append 一列到 append-only log 表完全合法。✅
- 邏輯成立：rollback 只是再 append 一列、把 `ROW_NUMBER` 取最新的結果撥回舊 build——「資料一點沒動，只是把對外指標撥回去」描述正確；對應 blue/green 撥流量回舊版的類比方向正確。✅
- 一致性註記：此處未碰 row-level update，與第 2 條的「不能改單列」限制**自洽**（promote/rollback 全靠 append + view 選最新，繞開 v1 表無列級 DML 的限制）——立論前後一致。
- 📚 出處：Spark 3.3.2 SQL — INSERT（archive 3.3.2，`INSERT INTO … VALUES`）。

### 6. SCD Type 2（effective_from/to、is_current）描述準確、且「讀某時間點要 as-of join」正確 —— ✅ 已驗證

主張（第 452 行）：列級版本化＝在每列加 `effective_from`／`effective_to`／`is_current`，比分區級 `build_version` 細；讀「某時間點的樣子」要回到 as-of join、也較重；適用「維度表少量列零星變動」。

- SCD Type 2 標準定義一致：為每次變更建新列，以 `effective_date`／`end_date`（current 常用 `9999-12-31`）／`is_current` 標記哪版有效（Wikipedia「Slowly changing dimension」；Microsoft Fabric SCD Type 2）。文中欄位命名與語意**準確**。✅
- 「讀某時間點要 as-of join」**正確**：point-in-time 查詢以 `dim.effective_from <= 目標時間 AND 目標時間 < dim.effective_to` 做 range/不等值對齊，即 as-of join（與 §9.3 第 180 行所述「對每個 label 時間點找當時最新值的不等值對齊」一致；Spark 3.3 無原生 AS OF，需 window 模擬——見第 09 章 §9.3 已查證的 join 類型清單）。✅
- 「分區級 build_version 對得上『整批重算一個 snapshot』的粒度、更簡單；列級 SCD2 留給少量列零星變動」為合理的設計取捨陳述，方向正確。
- 📚 出處：Wikipedia — Slowly Changing Dimension；Microsoft Learn — SCD Type 2（Fabric）。屬通用 DW 術語，文中未誤掛為官方功能。

### 7. Iceberg/Delta 內建 snapshot/tag/branch + time-travel 可取代手搭機制 —— ✅ 已驗證（方向正確）

主張（第 453 行）：Iceberg／Delta 內建 snapshot／tag／branch ＋ time-travel，能一行語法取代本節整套手搭（回補＝寫新 snapshot、可命名 tag、可 time-travel 查任意版）；本書環境目前沒有，故只點到。

- Iceberg 官方/生態確認：每次寫入建一個 immutable snapshot；可用 snapshot id／timestamp／named ref（branch/tag）time-travel 查任意歷史版；rollback 是純 metadata 操作（撥 current-snapshot-id）；**tag**＝指向某 snapshot 的具名不可變指標（如 `v1.0_ml_model_training_data`，正對應本節 ML 可重現用途）；**branch**＝可再 commit 的具名分支。
- 文中對應**方向正確**且未 overclaim：明確說「本書環境目前沒有，故只在此點到」，並把它定位為「值得評估的一步」，未把它當成 Spark 3.3 既有能力。✅
- 📚 出處：Apache Iceberg™ docs — Branching and Tagging；Iceberg Spark Procedures（rollback）；Iceberg Spec — Time Travel。

### 8. 把「通用模式」誤寫成「Spark/CDP 官方功能」／因果方向／出處掛錯 —— ✅ 未發現實質誤標

- bitemporal、產出/發佈分離（blue/green、promote 指標）、SCD Type 2 三者，footer（第 456 行）皆**明確標為「通用資料倉儲設計模式，無單一官方逐字出處、方向明確」**——定位誠實，無 overclaim。
- 唯一掛在「Spark 官方行為」的硬主張＝「純 Hive/Parquet 表不支援 row-level DML、需 v2」——已於第 2 條查證**屬實**，出處掛 Spark SQL — DELETE FROM ＋ §8.2 MERGE 同款說明，掛對。
- 因果方向檢查：第 382–389 行「MAX 會自動把壞版本推上線 → 所以要 promote 指標分離 → 而 promote 不需改任何資料、因為 v1 表本來就不能改單列 → 故用 append-only」整條因果鏈**自洽且正確**。
- Spark 文件連結版本漂移（`/docs/latest/` 已為 4.x、3.3.2 需走 `archive.apache.org/dist/spark/docs/3.3.2/…`）為全書已知並於章末「資料來源與精確度說明」揭露的事項，非本節缺陷。

---

## 結尾彙整（三級）

### A. 真缺陷（必補）
- **無。** §9.6 全部七條主張查證後均成立；無事實錯誤、無因果倒置、無把通用模式誤掛為 Spark/CDP 官方功能。

### B. 可加強（非錯誤，建議補強）
1. **§9.6 回補走查的靜態分區前提沒點明（第 263–267、362–365 行）**：`INSERT OVERWRITE … PARTITION(snapshot_date='…', build_version='…')` 只覆寫單一葉子子分區的安全性，**前提是 build_version 也寫成靜態值**。官方語意下，若 `build_version` 留成**動態**欄，`PARTITION (snapshot_date='…', build_version)` 會截斷該 snapshot 底下**所有** build（SPARK-20236 / waitingforcode），正好打爛本節要保護的舊版。文中範例兩欄皆靜態、寫法正確，但 footer 可補一句「兩個分區欄都必須給靜態字面值；任一留動態會擴大覆寫範圍」以防讀者照抄時漏寫。
2. **promote 時間單調性 caveat 位置**：tie-break（同一 `promoted_at` 多次 promote 需加序號）footer 已述，但 `ROW_NUMBER` 那段正文（第 416 行）可就近一句提示，避免讀者只看正文照抄。

### C. 誤讀風險（讀者可能會錯意，但文稿本身沒錯）
1. **「blue/green 部署」類比**：文中（第 434 行）類比正確（新版備好→切→出事切回），但讀者可能誤以為有對應的 Spark/CDP 內建發佈機制；實際上是純靠「append 發佈紀錄表＋view 選最新」手搭。footer 已標為通用模式，誤讀風險低。
2. **「一行語法取代整套手搭」（Iceberg/Delta，第 453 行）**：方向正確，但讀者可能低估遷移成本（external Parquet/ORC→Iceberg 的表格式轉換、CDP 版本支援度）。文中已用「評估換表格式是值得的一步」收口，未過度承諾。

---

*查證日期：2026-06-21。所有官方來源見各條 📚。版本基準 Spark 3.3.2；Spark 文件以 archive 3.3.2 逐字頁為準（主站 `/docs/latest/` 已升 4.x）。*
