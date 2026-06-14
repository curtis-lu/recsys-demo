# Spark 優化參考手冊 — 實作計畫

> **For agentic workers:** 用 `superpowers:subagent-driven-development`（推薦）或 `superpowers:executing-plans` 逐 task 執行。步驟用 `- [ ]` checkbox 追蹤。
> 設計來源：`.worktrees/spark-handbook/docs/superpowers/specs/2026-06-14-spark-tuning-handbook-design.md`（spec 為單一真實來源，方向調整先改 spec）。

**Goal:** 產出一份給數據部門 SQL-first 同事自行參考的 Spark 優化手冊（9 章 + index 的 `.md`，全部審定後轉離線 `.html`）。

**Architecture:** 純文件任務，在 worktree `.worktrees/spark-handbook`（分支 `feat/spark-tuning-handbook`）進行，**不跑 python/Spark/pytest**。每章的「測試閘」＝兩個審稿 subagent（A 技術 reviewer 驗真實性、B 目標讀者 reader 驗易讀性）並行審 + 使用者人工審。一章一審、審過才動下一章。

**Tech Stack:** Markdown + Mermaid（概念圖）；HTML 階段內嵌 mermaid.js（離線可看）。權威來源：Spark 3.3 官方文件、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》、Databricks / Cloudera CDP 官方文件。

---

## 路徑與環境（每個 task 都適用）

- 工作根目錄：`/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook`
- 手冊輸出目錄：`docs/handbooks/spark-tuning/`
- 審稿日誌目錄：`docs/handbooks/spark-tuning/.reviews/`
- 寫作規範：`docs/handbooks/handbook-writing-guide.md`（可轉移者：具體數字落地、結論誠實、不洩漏鷹架、流程可操作、§11/§12 審稿清單）
- **給使用者的檔案路徑一律帶 `.worktrees/spark-handbook/` 前綴**（否則他開到主目錄打不開）。
- git 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook ...` 或先 `cd` 該 worktree root。
- **每次 commit 後**：`git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook checkout -- graphify-out/GRAPH_REPORT.md`（post-commit hook 會弄髒它，不還原會擋住後續 checkout/merge）。

## 寫作慣例（每章共用）

- 繁體中文；專有名詞用英文原文（SparkSession 不譯）。
- 範例以 **SQL** 為主（DataFrame API 僅第 07 章）。
- 一節一概念；每個抽象主張用**具體數字 / 銀行資料量**（客戶 ~1000 萬、信用卡帳務 ~3000 萬/月、App ~1000 萬筆/天）落地。
- 每個 config 主張附 **Spark 3.3 預設值 + 出處**；每個「做 X → 變快」先確認因果方向。
- 遇取捨（時間/記憶體/儲存）就地明講。
- 每章結構：H1 標題 → 「本章前提（讀者已讀哪些章）」一句話 → 內文小節 → 章末「上一章 / 下一章」導覽連結。
- 概念圖用 Mermaid code block（` ```mermaid `）。

## 標準「每章撰寫循環」（Task 1–9 共用，每章的 Step A–F）

- **Step A — 查證並記錄關鍵事實**：對該章「須查證重點」清單，用 WebFetch/WebSearch 查 Spark 3.3 官方文件/指定書籍，把確認的預設值/行為/出處記下來（供寫稿與 reviewer 比對）。
- **Step B — 寫 `NN-name.md` 草稿**：依該章內容大綱 + 寫作慣例，含 Mermaid 概念圖、前提註記、章末導覽。
- **Step C — 並行派兩個審稿 subagent**：在同一則訊息內用 Agent tool 同時派 reviewer 與 reader（`superpowers:dispatching-parallel-agents`），各自即時寫日誌到 `.reviews/NN-name__reviewer.md` / `.reviews/NN-name__reader.md`。prompt 用下方「審稿 subagent prompt 模板」，填入 `{CHAPTER}` 與 `{PRIOR_CHAPTERS}`。
- **Step D — triage 並修**：把兩份回報按「真缺陷（必補）/ 可加強（斟酌）/ 誤讀（不改或微調）」分類；真缺陷必修，可加強斟酌，誤讀記錄不改。重大取捨不明才回頭問使用者。
- **Step E — 送使用者審**：把該章（帶 `.worktrees/` 前綴路徑）送使用者，納入回饋；**若使用者調整了方向，append 到本檔末「Direction Log」**。
- **Step F — 標記進度 + commit**：在「Progress Tracker」把該章打勾，commit（訊息 `docs(spark-handbook): 第 NN 章 <主題>`），然後 graphify reset。

### 審稿 subagent prompt 模板

**Reviewer（subagent_type: general-purpose）**
```
你是《Spark 優化參考手冊》的技術審查員。

[背景]
- 讀者：數據部門 SQL-first、無 DE 背景的分析師/科學家。
- 環境：Spark 3.3.x（AQE 預設開）+ Hive 3.1.3（CDP 7.1.9，YARN+HDFS，另有 Impala）；範例以 SQL 為主。
- 這份文件會被同事當行動依據，錯一個預設值或因果方向就會誤導真實調優。
- 寫作規範：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/handbook-writing-guide.md（特別 §12）。

[目標]
逐條檢查本章所有技術主張的正確性，找出不正確/不精確/無權威來源支撐處。

[素材]
- 待審章節：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/{CHAPTER}.md
- 權威來源（限）：Spark 3.3 官方文件（SQL Performance Tuning Guide、Configuration、SQL reference）、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》、Databricks / Cloudera CDP 官方文件。用 WebFetch/WebSearch 查證。

[限制]
- 只查證、不改稿。
- 來源限權威，不引未認證部落格。
- 對齊 Spark 3.3.x、Hive 3.1.3/CDP，不可套別版預設值/行為。
- 每條判定必附出處（URL 或書名+章節）；查不到標「無法查證」、不臆測。
- 不評文筆易讀性（那是 reader 的事）。
- 邊查邊把每條發現「即時 append」到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reviewer.md（每查一條就寫一條，讓人能同步看進度）。

[完成的定義]
日誌檔內：逐條主張 + ✅已驗證(附出處) / ❌錯誤(正確值+出處) / ⚠️無法查證；點出「建議被寫成硬限制」「因果正負號可疑」「引用出處錯」；結尾按 真缺陷(必補)/可加強(斟酌)/誤讀(不改或微調) 三級彙整。回傳一段摘要。
```

**Reader（subagent_type: general-purpose）**
```
你是《Spark 優化參考手冊》的目標讀者審稿人。

[人設＝背景]
你是銀行的資料分析師：會寫 SQL、懂業務資料，但沒學過分散式系統，不知道 shuffle/executor/partition 底層怎麼運作，看到沒解釋的英文術語會卡。嚴格扮演這個讀者，不可因為你其實懂 Spark 就放水。

[目標]
從頭讀到尾，標出讓你讀不懂、卡關、缺脈絡、太抽象、不自明之處。

[素材]
- 待審章節：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/{CHAPTER}.md
- 你「已經讀過、可假設已懂」的前置章：{PRIOR_CHAPTERS}
- 讀者審查清單：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/handbook-writing-guide.md §11。

[限制]
- 只回報讀者視角問題，不查技術對錯（那是 reviewer）。
- 不改稿。
- 任何第一次出現、沒當場解釋的術語都要標（即使你知道那是什麼）。
- 邊讀邊把卡關點「即時 append」到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reader.md。

[完成的定義]
日誌檔內：逐段/逐節卡關點 + 類型（缺脈絡/太抽象＝只有形容詞沒數字例子/術語沒先定義/概念圖不自明/步驟不可操作/鷹架洩漏）+「我會這樣想、我會問什麼」；全章主旨是否一致、範疇有無失衡；結尾按 真缺陷/可加強/誤讀 三級彙整。回傳一段摘要。
```

---

## Progress Tracker

- [ ] Task 0：scaffold（目錄 + index 骨架 + .reviews/）
- [ ] Task 1：`01-how-spark-runs-your-sql.md`（心智模型）
- [ ] Task 2：`02-diagnose-with-spark-ui.md`（Spark UI 診斷）
- [ ] Task 3：`03-sql-tuning.md`（SQL 寫法）
- [ ] Task 4：`04-spark-config.md`（Spark 設定 AQE-first）
- [ ] Task 5：`05-storage-efficiency.md`（儲存效率）
- [ ] Task 6：`06-engine-selection.md`（引擎選用）
- [ ] Task 7：`07-pyspark-dataframe-api.md`（DataFrame API 進階）
- [ ] Task 8：`08-scenario-playbooks.md`（場景對應）
- [ ] Task 9：`09-cheatsheet-and-glossary.md`（速查與名詞表）
- [ ] Task 10：完稿 `index.md`（導覽/連結/如何使用）
- [ ] Task 11：轉 HTML（內嵌 mermaid.js、離線檢查、回頂鈕、跨章導覽）
- [ ] Task 12：全書 reader subagent 通讀 + 修正

---

## Direction Log（append-only；每次使用者調整方向就加一行）

- 2026-06-14：骨架定為 9 章（依調優槓桿分層 + 診斷章 + 場景章）。
- 2026-06-14：PySpark DataFrame API 納入（第 07 章），仍不碰 RDD 低階 API。
- 2026-06-14：每章需派 reviewer + reader 兩個 subagent 審，按「目標/背景/素材/限制/DoD」五項給 prompt，並即時寫日誌可同步審核。
- 2026-06-14：離線 HTML 預設內嵌 mermaid.js（單一來源）；要「連 JS 都不依賴」才切 SVG 預渲染。
- 2026-06-14：純文件任務改用 worktree（避免與使用者其他 session 的 code 改動互相干擾）；給使用者的路徑一律帶 `.worktrees/spark-handbook/` 前綴。

---

## Task 0：Scaffold

**Files:**
- Create: `docs/handbooks/spark-tuning/index.md`（骨架）
- Create: `docs/handbooks/spark-tuning/.reviews/.gitkeep`

- [ ] **Step 1：建目錄與 .gitkeep**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
mkdir -p docs/handbooks/spark-tuning/.reviews
touch docs/handbooks/spark-tuning/.reviews/.gitkeep
```

- [ ] **Step 2：寫 `index.md` 骨架**（含 9 章導覽連結 placeholder、環境前提摘要、讀者假設、「如何使用本手冊」）。各章連結先指向預定檔名（Task 10 再補一句話地圖與完稿）。

- [ ] **Step 3：commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
git add docs/handbooks/spark-tuning/
git commit -m "docs(spark-handbook): scaffold 目錄與 index 骨架"
git checkout -- graphify-out/GRAPH_REPORT.md
```

---

## Task 1：第 01 章 — 心智模型：Spark 怎麼跑你的 SQL

**File:** Create `docs/handbooks/spark-tuning/01-how-spark-runs-your-sql.md`
**前置章（PRIOR_CHAPTERS）：** 無（第一章；只假設讀者會 SQL）

**內容大綱：**
- 從一條熟悉的 SQL 出發，講它在 Spark 裡發生什麼。
- cluster = driver + executors（在 YARN 上）；資料切成 partitions 平行處理。
- query 生命週期：SQL → logical plan → Catalyst 優化 → physical plan → jobs → stages → tasks。
- 窄依賴（map-like，便宜，不搬資料）vs 寬依賴 = shuffle（貴，跨網路重分佈）。
- shuffle 為什麼是頭號敵人（用 3000 萬筆帳務 `GROUP BY` 客戶舉例）。
- lazy evaluation：transformation 累積、action 才觸發。
- 主軸預告：多數優化＝減少/減輕 shuffle 與掃描量。

**概念圖（Mermaid）：** ① cluster（driver + N executors on YARN）；② SQL→logical→physical→jobs→stages→tasks 流程；③ narrow vs wide（shuffle）對照。

**須查證重點（reviewer 會查）：** 窄/寬依賴定義；shuffle 對應 physical plan 的 `Exchange`；transformation vs action 的 lazy 機制；stage 邊界由 shuffle 切。出處：Spark 3.3 官方文件 + Definitive Guide/Learning Spark。

- [ ] Step A：查證並記錄關鍵事實
- [ ] Step B：寫 `01-how-spark-runs-your-sql.md` 草稿（大綱 + 3 張 Mermaid 圖 + 章末導覽）
- [ ] Step C：並行派 reviewer + reader（`{CHAPTER}=01-how-spark-runs-your-sql`，`{PRIOR_CHAPTERS}=無`）
- [ ] Step D：triage 三級並修
- [ ] Step E：送使用者審，納回饋（必要時記 Direction Log）
- [ ] Step F：Progress Tracker 打勾 + commit + graphify reset

---

## Task 2：第 02 章 — 用 Spark UI 與 EXPLAIN 找瓶頸

**File:** Create `docs/handbooks/spark-tuning/02-diagnose-with-spark-ui.md`
**前置章：** 01

**內容大綱：**
- 心法：先量再調，不憑感覺。
- 在 CDP 上怎麼開 Spark UI（History Server / Cloudera Manager 入口）。
- `EXPLAIN` / `EXPLAIN FORMATTED` 讀重點：找 `Exchange`(=shuffle)、`BroadcastHashJoin` vs `SortMergeJoin`、Scan 的 partition filter 是否生效。
- Spark UI 各頁籤看什麼：Jobs / Stages / SQL / Executors / Storage；SQL-first 的人主看 SQL 頁籤 query plan + Stages 的 task 時間/資料分佈。
- 認症狀：shuffle 過大、skew（少數 task 特別久）、spill（記憶體不足落磁碟）、小檔/掃太多（partition 沒裁到）。
- 「症狀 → 看哪裡 → 翻到哪章」對照表（呼應第 09 章）。

**概念圖（Mermaid）：** ① Spark UI 頁籤地圖；② 一個 stage 內 task 時間分佈（正常 vs skew）示意；③ 症狀→對策決策流。

**須查證重點：** Spark 3.3 web UI 頁籤組成與 SQL tab 的 query plan 視圖；`EXPLAIN FORMATTED`（3.0+）；spill 指標欄位（memory/disk spill）；skew 在 UI 的呈現。出處：Spark 3.3「Web UI」「SQL ref – EXPLAIN」官方文件。

- [ ] Step A–F（同標準循環；`{CHAPTER}=02-diagnose-with-spark-ui`，`{PRIOR_CHAPTERS}=01`）

---

## Task 3：第 03 章 — SQL 寫法優化

**File:** Create `docs/handbooks/spark-tuning/03-sql-tuning.md`
**前置章：** 01, 02

**內容大綱：**
- 只讀需要的：partition 裁剪（`WHERE` 帶 partition column）、projection（別 `SELECT *`）、predicate pushdown。
- join 策略：broadcast（小表，省 shuffle）vs sort-merge（大表×大表）；AQE 自動選，但要喂對統計、別寫法擋住 pushdown。
- 手動 `/*+ BROADCAST(t) */` 何時用、threshold 多少。
- join key 型別一致（型別不符→隱式轉型→pushdown 失效）。
- 避免笛卡兒積 / 一對多爆量 join（連結 `aligning-on-table-joins` 精神）。
- `GROUP BY` / `DISTINCT` / `COUNT(DISTINCT)` 成本與 `approx_count_distinct` 取捨。
- window function 成本（每個 `PARTITION BY` 一次 shuffle）。
- 處理 skew：salting、AQE skew join、熱點 key 分流。
- 每招格式：原理 → SQL before/after → Spark UI 看到什麼變化 → 取捨。

**概念圖（Mermaid）：** ① partition 裁剪（掃全表 vs 只掃命中分區）；② broadcast vs sort-merge join；③ skew 與 salting。

**須查證重點：** `spark.sql.autoBroadcastJoinThreshold` 預設（10MB）；broadcast hint 語法；AQE skew join（`spark.sql.adaptive.skewJoin.*`）；`approx_count_distinct` 為 HyperLogLog 近似；predicate/projection pushdown 條件。出處：Spark 3.3 SQL Performance Tuning + SQL ref（hints / functions）。

- [ ] Step A–F（`{CHAPTER}=03-sql-tuning`，`{PRIOR_CHAPTERS}=01, 02`）

---

## Task 4：第 04 章 — Spark 設定（AQE-first）

**File:** Create `docs/handbooks/spark-tuning/04-spark-config.md`
**前置章：** 01, 02, 03

**內容大綱：**
- 心法：3.3 AQE 預設開，先別亂調靜態旋鈕。AQE 自動做：合併 shuffle 分區、動態切 broadcast、處理 skew join。
- 確認 AQE 開著（`spark.sql.adaptive.enabled`）。
- 仍要懂的少數旋鈕：`spark.sql.shuffle.partitions`（AQE 下角色變了）、`spark.sql.autoBroadcastJoinThreshold`、executor memory/cores/數量（CDP/YARN 上怎麼給）、`spark.sql.files.maxPartitionBytes`、dynamic allocation。
- 怎麼在 Hue/notebook 用 `SET`。
- 記憶體模型一句話：executor memory 分 execution/storage，spill 是不夠的徵兆。
- 取捨：更多記憶體/核心 vs 叢集併發；broadcast threshold 調大 vs driver OOM。
- 強調：對 SQL-first 的人，調 SQL 寫法 + 喂統計多半比硬調 config 有效。

**概念圖（Mermaid）：** ① AQE 自動做的三件事；② executor 記憶體區塊（execution/storage/overhead）。

**須查證重點：** `spark.sql.adaptive.enabled` 預設 true（3.2+）；`spark.sql.shuffle.partitions` 預設 200；`autoBroadcastJoinThreshold` 10MB；`spark.sql.files.maxPartitionBytes` 預設 128MB；AQE coalesce partitions config；dynamic allocation 開關；unified memory（execution/storage）。出處：Spark 3.3 Configuration + Performance Tuning + Tuning guide。

- [ ] Step A–F（`{CHAPTER}=04-spark-config`，`{PRIOR_CHAPTERS}=01, 02, 03`）

---

## Task 5：第 05 章 — 儲存效率

**File:** Create `docs/handbooks/spark-tuning/05-storage-efficiency.md`
**前置章：** 01, 03

**內容大綱：**
- 檔案格式：Parquet/ORC 為何比 text/CSV 快又省（列式、壓縮、謂詞下推、只讀需要的欄）。
- 壓縮：snappy（快）vs zstd/gzip（小）取捨。
- partition 設計：選對 partition column（帳務按 month/date）、不要過度分割（1000 萬客戶別按 `cust_id` 分割→小檔災難）；目標檔案大小 ~128MB–1GB。
- 小檔問題：成因、徵兆、解法（寫出前 `repartition`/`coalesce`、定期 compaction）。
- bucketing：何時有用、Hive 3 注意。
- 統計：`ANALYZE TABLE ... COMPUTE STATISTICS` 為何關鍵（AQE/CBO 靠它）、怎麼跑。
- Hive 3.x ACID/transactional table 提醒（delta 檔、compaction）。
- 取捨：分割細→掃描省但小檔/metadata 爆；壓縮強→省儲存但耗 CPU。

**概念圖（Mermaid）：** ① 列式 vs 列存只讀需要欄；② partition 裁剪在磁碟層；③ 小檔成因（過度分割 / 太多 shuffle 分區寫出）。

**須查證重點：** Parquet/ORC 列式 + 謂詞下推；Spark parquet 預設壓縮 snappy；`ANALYZE TABLE ... COMPUTE STATISTICS` 語法 + `spark.sql.cbo.enabled`（預設 false！需確認）；bucketing 行為；Hive 3 ACID/transactional 預設。出處：Spark 3.3 SQL data sources / Performance Tuning；Cloudera CDP / Hive 3 官方文件。

- [ ] Step A–F（`{CHAPTER}=05-storage-efficiency`，`{PRIOR_CHAPTERS}=01, 03`）

---

## Task 6：第 06 章 — 引擎選用：Spark vs Hive/Tez vs Impala

**File:** Create `docs/handbooks/spark-tuning/06-engine-selection.md`
**前置章：** 01, 02, 05

**內容大綱：**
- 三引擎定位：Spark SQL（大型 ETL/複雜轉換/與 ML 整合）、Hive on Tez（穩定批次/既有 HQL）、Impala（低延遲互動 ad-hoc/BI）。
- 決策表：資料量 / 延遲需求 / 併發 / 查詢複雜度 / 是否寫回大表。
- 各引擎診斷工具：Spark UI / Tez UI(Hue) / Impala query profile。
- CDP 實務：同一 Hive table 三引擎都讀；Impala metadata `INVALIDATE`/`REFRESH`、ACID 表 Impala 支援限制。
- 取捨：Impala 快但吃記憶體、不適合超大 shuffle；Spark 通用但啟動/排程成本高。

**概念圖（Mermaid）：** 引擎決策樹（資料量/延遲/併發/複雜度 → Spark / Hive-Tez / Impala）。

**須查證重點：** Impala `INVALIDATE METADATA` vs `REFRESH` 語意；Impala 對 Hive ACID/transactional 表的支援限制；三引擎定位（vendor 文件）。出處：Cloudera CDP 官方文件（Impala / Hive / Spark on CDP）。

- [ ] Step A–F（`{CHAPTER}=06-engine-selection`，`{PRIOR_CHAPTERS}=01, 02, 05`）

---

## Task 7：第 07 章 — 進階：何時與如何改用 PySpark DataFrame API

**File:** Create `docs/handbooks/spark-tuning/07-pyspark-dataframe-api.md`
**前置章：** 01, 03, 04

**內容大綱：**
- 何時值得從 SQL 改用 DataFrame API：複雜可重用邏輯、要單元測試、動態組查詢、與 ML pipeline/Python 生態整合（本 repo pipeline 即一例）。
- SQL ↔ DataFrame 心智對照（同一 query 兩種寫法並排）；破除「API 比 SQL 快/慢」迷思＝底層同一個 Catalyst、效能等價。
- API 特有效能注意：`cache()`/`persist()` 何時用與記憶體取捨；避免 `collect()`/`toPandas()` 拉回 driver（OOM）；`repartition` vs `coalesce`；UDF 成本（呼應本 repo 生產禁 UDF）；lazy 與 action。
- 取捨：可測試/可維護 vs 純 SQL 簡潔。
- 範圍界線：只到 DataFrame API，不碰 RDD。

**概念圖（Mermaid）：** SQL 與 DataFrame 都進同一個 Catalyst → 同一 physical plan（效能等價）。

**須查證重點：** SQL 與 DataFrame 編譯到相同 logical/physical plan（Catalyst）；`cache`/`persist` storage levels 與預設；`collect`/`toPandas` 把資料拉回 driver；`coalesce` vs `repartition`（是否觸發 shuffle）；Python UDF 序列化成本。出處：Spark 3.3 官方文件 + Definitive Guide。

- [ ] Step A–F（`{CHAPTER}=07-pyspark-dataframe-api`，`{PRIOR_CHAPTERS}=01, 03, 04`）

---

## Task 8：第 08 章 — 場景對應

**File:** Create `docs/handbooks/spark-tuning/08-scenario-playbooks.md`
**前置章：** 01–07

**內容大綱：**
- 場景 1 ad-hoc：先 Impala/小樣本、partition 裁剪、`LIMIT`、別 `SELECT *`、別全表 `COUNT(DISTINCT)`。
- 場景 2 排程產表：可重跑、控輸出檔大小、partition 設計、`ANALYZE`、用 Spark/Hive、用 Spark UI 抓退化。
- 場景 3 特徵運算：寬表多 join、多 window、易 skew；broadcast 維度表、預聚合、控 shuffle、cache 中間結果取捨（SQL 或第 07 章的 DataFrame API）。
- 每場景：典型陷阱 → 對策 → 引用前面哪章。

**概念圖（Mermaid）：** 三場景各一張「典型陷阱 → 對策」流程（或一張總表）。

**須查證重點：** 本章主要綜合前面已查證內容；只需確認跨章引用指對、無新的未查證主張。

- [ ] Step A–F（`{CHAPTER}=08-scenario-playbooks`，`{PRIOR_CHAPTERS}=01, 02, 03, 04, 05, 06, 07`）

---

## Task 9：第 09 章 — 速查與名詞表

**File:** Create `docs/handbooks/spark-tuning/09-cheatsheet-and-glossary.md`
**前置章：** 01–08

**內容大綱：**
- 取捨速查表：時間 ↔ 記憶體 ↔ 儲存（每個手段三維度影響）。
- config 速查表（名稱 / Spark 3.3 預設 / 何時調 / 風險）。
- 症狀→對策速查（呼應第 02 章）。
- 名詞對照表（partition/shuffle/executor/skew/spill/broadcast/AQE/CBO… 中英對照 + 一句話）。

**概念圖（Mermaid）：** 視需要，可不放（本章以表格為主）。

**須查證重點：** config 速查表每一列的預設值要與第 04/03/05 章一致且對齊 Spark 3.3；名詞定義精確。

- [ ] Step A–F（`{CHAPTER}=09-cheatsheet-and-glossary`，`{PRIOR_CHAPTERS}=01–08`）

---

## Task 10：完稿 index.md

**File:** Modify `docs/handbooks/spark-tuning/index.md`

- [ ] **Step 1：** 補各章「一句話地圖」、確認 9 章連結正確、補「如何使用本手冊」（依場景/問題快速導向哪一章）、環境前提摘要、讀者假設。
- [ ] **Step 2：** 全書跨章引用檢查（grep「第 NN 章」「見 §」指對沒）。
- [ ] **Step 3：** commit + graphify reset。

---

## Task 11：轉 HTML（離線可看）

**Files:** Create `docs/handbooks/spark-tuning/*.html`（與各 `.md` 成對）；可建一支建置 script（如 `scripts/` 下，若沿用既有手冊 HTML 產法則沿用之）。

- [ ] **Step 1：** 確認既有手冊 HTML 的產法（看 `docs/handbooks/*_offline.html` 怎麼來的；沿用同套樣式/工具，含 anchor 目錄、右下角浮動回頂鈕）。
- [ ] **Step 2：** 把 9 章 + index 的 `.md` 轉 HTML，**內嵌 mermaid.js**（vendored/inline，不靠 CDN），渲染 ` ```mermaid ` 區塊。
- [ ] **Step 3：** 離線驗證：斷網 / `file://` 開啟，確認所有 Mermaid 圖正常渲染、跨章連結可走、回頂鈕可用。
- [ ] **Step 4：** commit + graphify reset。

---

## Task 12：全書 reader subagent 通讀 + 修正

- [ ] **Step 1：** 派一個 reader subagent（人設同 §reader 模板）從 index 一路讀到第 09 章，檢查全書一致性、導覽是否好走、有無跨章脈絡斷裂；即時寫日誌到 `.reviews/whole-book__reader.md`。
- [ ] **Step 2：** triage 三級並修。
- [ ] **Step 3：** 送使用者最終確認。
- [ ] **Step 4：** commit + graphify reset；（使用者要求時）用 `superpowers:finishing-a-development-branch` 收尾（PR/merge）。

---

## Self-Review（撰計畫後自查，已做）

- **Spec 覆蓋**：spec §5 的 index + 9 章 → Task 0/10 + Task 1–9 全覆蓋；§8 交付流程（先 .md 後 HTML）→ Task 1–10 為 .md、Task 11 為 HTML；§10 審稿 subagent → 每章 Step C/D + Task 12；§9 跨 session 機制 → 本檔 Progress Tracker + Direction Log + commit 節奏；§7 離線 mermaid → Task 11。
- **Placeholder 掃描**：每章 task 都有具體內容大綱、概念圖、須查證重點、前置章；subagent prompt 為完整可貼模板（僅 `{CHAPTER}`/`{PRIOR_CHAPTERS}` 為刻意的填空）。
- **一致性**：章節編號 01–09 與檔名、PRIOR_CHAPTERS、index 連結一致；config 預設值集中在 04/03/05 並由 09 速查表彙整，避免各章各說。
