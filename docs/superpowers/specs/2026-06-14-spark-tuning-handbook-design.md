# Spark 優化參考手冊 — 設計 spec

> 狀態：設計定稿待使用者審閱。實作進度見 `docs/superpowers/plans/2026-06-14-spark-tuning-handbook-plan.md`（計畫產生後）。
> 工作環境：`feat/spark-tuning-handbook` 分支，worktree 在 `.worktrees/spark-handbook`（純文件，不跑 python/Spark）。

## 1. 目標與讀者

產出一份**給數據部門同事自行參考**的 Spark 優化手冊，讓他們能自己據以調整 **SQL 寫法、Spark 設定、與資料處理工具選用**，提升資料處理效率。

「資料處理效率」涵蓋三個面向，**遇到兩難一律明講取捨**：

1. 運算時間（壁鐘時間）
2. 記憶體使用效率
3. 資料儲存效率

**讀者**：資料分析師、資料科學家。**無專業資料工程背景** —— 對系統架構、基礎設施、網路通訊只有淺薄認知。因此凡是 partition / shuffle / executor / skew / spill 這類概念，第一次出現都要用易懂方式建立心智模型，搭配概念圖，**不碰原始碼、不假設 DE 知識**。

**深度定位（重要）**：起點易懂，但**不止於 ad-hoc 分析**。要假設讀者未來會**自己營運資料排程、經營多人共用的資料產品（如特徵庫 / feature store）**，因此每章在打好直覺後，要帶到進階概念與**營運層取捨**：資源配置（executor core/instance/memory）、併發與多租戶、可靠度與可重跑、schema 演進與維護。寫法上「先易懂直覺 → 再進階／營運取捨」分層遞進，讓只做 ad-hoc 的人讀前半段就夠用，要營運資料產品的人能讀到後半段。

## 2. 已確認前提（環境與使用方式）

| 項目 | 結論 |
|---|---|
| 計算引擎 | **Spark 3.3.x**（AQE 預設開）+ **Hive 3.1.3**（CDP Private Cloud Base 7.1.9）；底層 YARN + HDFS，另有 Impala |
| 介面 | 同事以 **SQL 為主**：Spark SQL 字串、Hive/Hue、Impala；**少用 PySpark DataFrame API**（但使用者要求納入一章） |
| 手冊範例方言 | 以 **SQL** 為主（非 `df.filter()` 鏈式 API）；DataFrame API 集中在專章 |
| 引擎範圍 | **Spark 為主軸** + 一章引擎選用（Spark vs Hive/Tez vs Impala） |
| 診斷工具 | 主推 **Spark UI** + `EXPLAIN`；Impala/Hive 的等價工具（query profile / Tez UI）在引擎選用章帶過 |
| 使用場景 | ① ad-hoc 分析 ② 排程定期產表 ③ 模型訓練特徵運算 |
| 資料量級 | 客戶 ~1000 萬；信用卡帳務 ~3000 萬筆/月；App ~100 萬 session/天、~1000 萬筆/天 |

> AQE 預設開的後果：手冊**引導同事善用 AQE 自動處理 shuffle 分區合併 / skew join / 動態 broadcast**，而不是教一堆手動靜態調參。靜態旋鈕只講 AQE 之後仍真正要懂的少數幾個。

## 3. 非目標（Out of Scope，YAGNI）

- 不教 **RDD 低階 API** 與低階分散式程式設計（partitioner、mapPartitions…）；DataFrame API 有專章，但只到「SQL-first 的人何時/如何改用」的程度。
- 不深入 Spark 原始碼 / JVM 內部 / Catalyst 實作細節。
- 不寫叢集建置、YARN/HDFS 運維、安全/權限設定（讀者非平台管理者）。
- 不做 Hive-Tez 與 Impala 的**深度**優化（僅在引擎選用章談定位與何時改用）。
- 不寫 streaming / structured streaming（場景全為批次）。

## 4. 組織方式

採 **「依調優槓桿分層」為骨幹**（A），吸收「診斷流程」（B）與「場景對應」（C）的長處：診斷獨立成早期一章（B 的行動導向），最後用場景對應章把槓桿綁到三個使用情境（C，但不重教概念）。這與《Learning Spark, 2nd ed》《Spark: The Definitive Guide》《High Performance Spark》的編排一致（概念 → 優化 → 調校）。

## 5. 檔案結構與各章大綱

放在 `docs/handbooks/spark-tuning/`，一個 index + 分章檔（階層拆分，不塞同一份）：

### `index.md` — 總覽與導覽
如何使用本手冊、環境前提（§2 摘要）、讀者假設、章節導覽、各章一句話地圖。

### `01-how-spark-runs-your-sql.md` — 心智模型：Spark 怎麼跑你的 SQL
- 從一條熟悉的 SQL 出發，講它在 Spark 裡發生什麼。
- cluster = driver + executors（在 YARN 上）；資料切成 **partitions** 平行處理。
- **執行的層級關係：application → job（每個 action 一個）→ stage（每次 shuffle 切一刀）→ task（一 partition 一個）**，並對應到第 02 章 Spark UI 看到的頁籤。
- query 生命週期：SQL → logical plan → Catalyst 優化 → physical plan → stages → tasks。
- **executor 的形狀：core 數（＝可同時跑幾個 task）/ instance 台數（＝總平行度）/ memory size 三者的取捨**（fat vs thin executor、平行度＝executors×cores、記憶體被同時跑的 task 分掉→spill 風險、HDFS 吞吐與 GC、多租戶）；建立直覺，操作細節 forward 到第 04 章。
- 兩種運算：**窄依賴**（map-like，便宜，不搬資料）vs **寬依賴 = shuffle**（貴，跨網路重分佈資料）。
- **shuffle 為什麼是頭號敵人**（用 3000 萬筆帳務 `GROUP BY` 客戶舉例）。
- lazy evaluation：action 才觸發。
- 概念圖：cluster 圖、application→job→stage→task 層級圖、SQL→plan→stage→task 流程圖、narrow vs wide 圖。
- 主軸預告：多數優化＝減少或減輕 shuffle 與掃描量。

### `02-diagnose-with-spark-ui.md` — 用 Spark UI 與 EXPLAIN 找瓶頸
- 心法：**先量再調，不憑感覺**。
- 在 CDP 上怎麼開 Spark UI（History Server / Cloudera Manager）。
- `EXPLAIN` / `EXPLAIN FORMATTED` 讀重點：找 `Exchange`(=shuffle)、`BroadcastHashJoin` vs `SortMergeJoin`、Scan 的 partition filter 有沒有生效。
- Spark UI 各頁籤該看什麼（SQL-first 的人主看 SQL 頁籤的 query plan + Stages 的 task 時間/資料分佈）。
- 認症狀：shuffle 過大、**skew**（少數 task 特別久）、**spill**（記憶體不足落磁碟）、小檔/掃太多（partition 沒裁到）。
- 產出「症狀 → 看哪裡 → 翻到哪章」對照表（呼應 §10）。

### `03-sql-tuning.md` — SQL 寫法優化
- 只讀需要的：partition 裁剪（`WHERE` 帶 partition column）、projection（別 `SELECT *`）、predicate pushdown。
- join 策略：broadcast join（小表，省 shuffle）vs sort-merge（大表×大表）；AQE 自動選，但你要喂對統計、別寫法擋住 pushdown。
- 手動 `/*+ BROADCAST(t) */` 何時用、threshold 多少。
- join key 型別一致（型別不符→隱式轉型→pushdown 失效）。
- 避免笛卡兒積 / 一對多爆量 join（連結 `aligning-on-table-joins` 的精神）。
- `GROUP BY` / `DISTINCT` / `COUNT(DISTINCT)` 成本與 `approx_count_distinct` 取捨。
- window function 成本（每個 `PARTITION BY` 是一次 shuffle）。
- 處理 skew：salting、AQE skew join、熱點 key 分流。
- 每招格式：原理 → SQL before/after → 在 Spark UI 看到什麼變化 → 取捨（時間/記憶體/儲存）。

### `04-spark-config.md` — Spark 設定（AQE-first）
- 心法：3.3 AQE 預設開，先別亂調靜態旋鈕。AQE 自動做：合併 shuffle 分區、動態切 broadcast、處理 skew join。
- 確認 AQE 開著（`spark.sql.adaptive.enabled`）。
- 仍要懂的少數旋鈕：`spark.sql.shuffle.partitions`（AQE 下角色變了）、`spark.sql.autoBroadcastJoinThreshold`、`spark.sql.files.maxPartitionBytes`、dynamic allocation。
- **executor 資源配置（深化第 01 章直覺）**：`spark.executor.cores` / `spark.executor.memory` / `spark.executor.memoryOverhead` / instance 數的實際設定與取捨；fat vs thin executor 的工作範例（給定 YARN 額度怎麼切）；execution/storage 統一記憶體模型、spill 徵兆與救法。
- **營運/多租戶**：dynamic allocation（隨需求伸縮、把資源讓回叢集）、靜態大額配置會餓死同事的風險、排程作業該怎麼要資源才穩定又不擾鄰。
- 怎麼在 Hue/notebook 用 `SET` 設定（哪些可在 session 設、哪些要在 submit/啟動時給）。
- 取捨：更多記憶體/核心 vs 叢集併發；broadcast threshold 調大 vs driver OOM。
- 強調：對 SQL-first 的人，調 SQL 寫法 + 喂統計，多半比硬調 config 有效。

### `05-storage-efficiency.md` — 儲存效率
- 檔案格式：Parquet/ORC 為何比 text/CSV 快又省（列式、壓縮、謂詞下推、只讀需要的欄）。
- 壓縮：snappy（快）vs zstd/gzip（小）取捨。
- partition 設計：選對 partition column（帳務按 month/date）、**不要過度分割**（1000 萬客戶別按 `cust_id` 分割→小檔災難）；目標檔案大小 ~128MB–1GB。
- 小檔問題：成因、徵兆、解法（寫出前 `repartition`/`coalesce`、定期 compaction）。
- bucketing：何時有用（固定 join key 反覆 join）、Hive 3 注意事項。
- 統計：`ANALYZE TABLE ... COMPUTE STATISTICS` 為何關鍵（AQE/CBO 靠它選 join 策略）、怎麼跑。
- Hive 3.x ACID/transactional table 提醒（delta 檔、compaction）。
- **營運共用資料表（深度）**：給多人/多作業共用的表，partition 與檔案大小要為「下游怎麼讀」設計；schema 演進（加欄/型別）怎麼不打爛既有讀者；定期維護（compaction、`ANALYZE` 重算統計）；併發寫入與覆寫的安全。
- 取捨：分割細→掃描省但小檔/metadata 爆；壓縮強→省儲存但耗 CPU。

### `06-engine-selection.md` — 引擎選用：Spark vs Hive/Tez vs Impala
- 三引擎定位：Spark SQL（大型 ETL/複雜轉換/與 ML 整合）、Hive on Tez（穩定批次、既有 HQL）、Impala（低延遲互動 ad-hoc/BI）。
- 決策表：資料量 / 延遲需求 / 併發 / 查詢複雜度 / 是否寫回大表。
- 各引擎診斷工具：Spark UI / Tez UI(Hue) / Impala query profile。
- CDP 實務：同一 Hive table 三引擎都讀；Impala metadata `INVALIDATE`/`REFRESH`、ACID 表 Impala 支援限制。
- 取捨：Impala 快但吃記憶體、不適合超大 shuffle；Spark 通用但啟動/排程成本高。

### `07-pyspark-dataframe-api.md` — 進階：何時與如何改用 PySpark DataFrame API
- 何時值得從 SQL 改用 DataFrame API：複雜可重用邏輯、要單元測試、動態組查詢、與 ML pipeline / Python 生態整合（本 repo 的 pipeline 即一例）。
- SQL ↔ DataFrame 心智對照（同一 query 兩種寫法並排）；**破除「API 比 SQL 快/慢」迷思 ＝ 底層同一個 Catalyst，效能等價**。
- API 特有效能注意：`cache()`/`persist()` 何時用與記憶體取捨；避免 `collect()`/`toPandas()` 把資料拉回 driver（OOM）；`repartition` vs `coalesce`；UDF 成本（呼應本 repo 生產禁 UDF）；lazy 與 action。
- 取捨：可測試/可維護 vs 純 SQL 的簡潔。
- 範圍界線：只到 DataFrame API；**不碰 RDD 低階 API**（§3）。

### `08-operating-data-pipelines.md` — 營運資料排程與資料產品（營運專章）
> 由 architecture round-1 補上：終極目標「能長期營運排程＋特徵庫」原本散落在場景章條列、無教學主體。本章用 01 章深度補齊整條營運線。
- 定位：把產出的表/特徵當成**要長期營運的服務**，不是一次性查詢；正確、可靠、可維護優先於快。
- **冪等與可重跑**：用「整個 partition 覆寫」（`INSERT OVERWRITE ... PARTITION`、dynamic partition overwrite）而非 append——失敗重跑不重複、可安全補跑；對照 append 在重跑時造成重複的坑。
- **回填（backfill）**：補某段歷史；按 partition 分批、控資源、可中斷續跑。
- **排程相依與資料就緒**：上游沒齊不要跑下游；以「partition 是否存在/列數是否到位」當 gate。
- **資料品質驗證（補 C12，§11 明文要求）**：產表後基本檢查（列數量級、null 比例、key 唯一性、值域、跟昨天比的漂移），不過就擋下游、發警報。
- **時間點正確性 / 特徵洩漏（C11，特徵庫命門）**：某時間點的特徵只能用「該時間點之前」的資料；常見洩漏（用到未來、用到 label 期間）；以 snapshot date 為界、as-of join 的概念。
- **監控與退化**：用 Spark UI/歷史看作業時間、資料量、shuffle 是否隨時間惡化；資料量成長導致的退化與因應。
- **表的生命週期維護**：定期 compaction（小檔）、重算 `ANALYZE` 統計、清過期 partition、schema 演進不打爛下游（呼應 §05）。
- **多人共用的資料產品**：schema/SLA 契約、版本、文件、別人怎麼讀你的表。
- 取捨就地：冪等覆寫 vs append 成本；驗證嚴格 vs 誤擋；回填一次到位 vs 分批。
- 概念圖：① 冪等覆寫 vs append（重跑後結果對照）；② 排程相依 gate（上游就緒才跑下游）；③ 時間點正確性（snapshot date 切線，只能用左邊資料）。

### `09-scenario-playbooks.md` — 場景對應（索引）
- 角色：純**索引/指路**，不重教概念——把前面各章技巧，按三大情境串成「遇到這種工作，照哪些章、最常踩什麼雷」。
- 場景 1 ad-hoc：先 Impala/小樣本、partition 裁剪、`LIMIT`、別 `SELECT *`、別全表 `COUNT(DISTINCT)` → 主要引 §02/§03/§06。
- 場景 2 排程產表：可重跑/冪等、控檔大小、資源要得穩 → 主要引 **§08**（營運）＋ §03/§04/§05。
- 場景 3 特徵運算/特徵庫：寬表多 join/window、易 skew、時間點正確性 → 主要引 **§08**（時間點/品質）＋ §03/§05/§07。
- 每場景：典型流程 → 對應章節清單 → 該情境最常踩的雷。

### `10-cheatsheet-and-glossary.md` — 速查與名詞表
- 取捨速查表：時間 ↔ 記憶體 ↔ 儲存（每個手段三維度影響）。
- config 速查表（名稱/預設/何時調/風險）。
- 症狀→對策速查（呼應 §02）。
- 名詞對照表（partition/shuffle/executor/skew/spill/broadcast… 中英對照＋一句話）。

> 「記憶體 vs 時間 vs 儲存」取捨**就地點在各章**（如 broadcast join 省 shuffle 但吃記憶體；過度 partition 省掃描但爆小檔），最後在 §10 收成速查表。
>
> 章數彈性：若某章寫起來太薄，允許合併（如 04 併入 03、10 併入 index），定案以實作計畫為準。

## 6. 寫作慣例與權威來源

**權威來源**（限定，不引用未認證部落格）：
- Spark 官方文件，特別是 **SQL Performance Tuning Guide** 與 **Configuration**（對齊 3.3.x 的預設值與 config 名稱）。
- 《Learning Spark, 2nd ed》(Damji 等)、《Spark: The Definitive Guide》(Chambers & Zaharia)、《High Performance Spark》(Karau & Warren)。
- Databricks 官方文件/課程。
- 需要精確處（預設值、config 名稱、行為）以 **WebFetch 對 Spark 3.3 官方文件核對**。

**風格**（沿用 `docs/handbooks/handbook-writing-guide.md` 可轉移者，並加運維手冊特例）：
- **繁體中文**為主；專有名詞用英文原文（SparkSession 不譯）。
- 短、敘述性、易懂；一節一個概念。
- 每個抽象主張用**具體數字 / 銀行資料量**落地；每個 config 主張附**來源 + Spark 3.3 預設值**；每個「做 X → 變快」的方向性主張先確認因果。
- 結論誠實，遇取捨明講，不寫漂亮但偏頗的單一結論。
- 不洩漏寫作鷹架（無 TODO/暫名/後設旁白）。
- 流程用「第幾步 → 看什麼 → 得到什麼 → 再決定什麼」的具體步驟，不用密碼式縮寫。
- 圖表自明（不用未定義箭頭）。
- **資料來源可查證（每章必做）**：每個重要概念的段落末尾附「📚 來源」footer（代表性出處＋連結；多來源取一個有代表性的即可）；章末加「資料來源與精確度說明」段，列出刻意簡化／無官方逐字數字之處、與版本對齊說明。**目的：讓讀者不照單全收、能自行驗證、看得出哪些段落不完全精確。** 來源限官方文件（Spark / Apache Hadoop / Cloudera CDP）、Spark 核心開發者文章（如 Databricks）、指定書籍；**不引未認證個人部落格**。連結用可達頁面（自動工具對 3.3.x 版頁回 404，故用 `docs/latest` 並註明「改網址版本號即對齊」；引用的預設值已對 3.3 核對）。逐條查證以 reviewer subagent 日誌為憑（`.reviews/<chapter>__reviewer.md`）。

## 7. 圖表方案

- `.md` 內用 **Mermaid**（GitHub/VSCode 可預覽，便於內容檢閱）畫概念圖（flowchart / 簡單架構圖）。
- **離線瀏覽可行**：轉 HTML 時把 **mermaid.js 函式庫直接內嵌（vendored/inline）進 HTML**，不靠 CDN，從本機 `file://` 開啟即可渲染（沿用既有 `*_offline.html` 自包含做法）。維持「`.md` 的 mermaid 原始碼 ＝ HTML 內同一份」單一來源。
  - 替代方案（更穩、多一道建置）：建置時用 mermaid-cli 把每張圖**預渲染成內嵌 SVG**，HTML 完全不依賴 JS（連 JS 關閉也能看）；代價是圖原始碼與 `.md` 分離、建置端需 node/puppeteer。
  - **預設採內嵌 mermaid.js**；若要「連 JS 都不依賴」的最強離線，再切預渲染 SVG。
- 圖以「簡單概念圖」為原則：cluster、query→stage→task、narrow vs wide shuffle、broadcast vs sort-merge join、partition 裁剪、小檔成因、引擎決策樹等。

## 8. 交付流程

1. **先全部產 `.md`**（方便內容檢閱）。每章節奏：`我寫草稿 → 兩個審稿 subagent 並行審（§11）→ 我 triage 修 → 使用者審 → 打勾 commit`。
2. 全部 `.md` 經使用者確認 OK 後，**再一次轉成 `.html`**（方便概念圖呈現，內嵌 mermaid.js 離線可看），與 `.md` 成對放在 `docs/handbooks/spark-tuning/`。
3. HTML 完成後做一次整體檢查：跨章導覽/anchor 連結、離線渲染、回頂鈕。

## 9. 跨 session / 跨日持續工作機制

確保工作能中斷後無縫接續，並記得使用者中途的調整方向：

1. **設計 spec（本檔）** = 範圍/大綱/風格的單一真實來源；方向調整**先改這裡**。
2. **實作計畫** `docs/superpowers/plans/2026-06-14-spark-tuning-handbook-plan.md` = 分階段（每章一階段或數階段），每階段含狀態勾選（未開始/進行中/已完成/已審）、產出檔案、驗收點。**新 session 開頭先讀此計畫**知道進度。
3. **Direction Log（方向日誌）**：計畫檔內 append-only 區段，每次 session 記下使用者的調整與決定（日期＋一句話），跨 session 不忘修正方向。
4. **project memory**：寫一條 memory 記錄本任務關鍵不變項（環境前提、骨架、風格、檔案位置、進度指標、worktree/分支）；context reset 仍找得回。方向大改時更新它。
5. **每章交付節奏**：一章 `.md` 完成 → 審稿 subagent → 使用者審 → OK 才動下一章；每完成一章在計畫打勾 + commit。
6. **每次 session 收尾**：更新計畫進度 + Direction Log + commit。

## 10. 審稿 subagent 工作流程

每章 `.md` 草稿完成後、送使用者審閱前，**派兩個 subagent 各自通讀該章**（可並行、彼此獨立；用 `superpowers:dispatching-parallel-agents`）。沿用 `docs/handbooks/handbook-writing-guide.md` §11–§12 的審稿精神，拆成兩個互補角色。**每次派發都把下列五項（目標／背景／素材／限制／完成的定義）寫進 subagent prompt**，避免跑偏。

### 10.1 兩角色共用背景（每次都帶）
- 這是一份 **Spark 優化參考手冊**，讀者是數據部門 **SQL-first、無 DE 背景**的分析師/科學家（§1）。
- 環境：**Spark 3.3.x（AQE 預設開）+ Hive 3.1.3（CDP 7.1.9，YARN+HDFS，另有 Impala）**；範例以 SQL 為主；效率含時間/記憶體/儲存三面向（§2）。
- 寫作對齊 `docs/handbooks/handbook-writing-guide.md`。
- 本次審第 N 章；**前置章節清單**＝讀者讀到這章前「已建立的心智模型」（用來判斷有無 forward-reference 還沒教的概念）。

### 10.2 角色 A — 技術審查員（reviewer，驗真實性）
- **目標**：找出該章所有技術上不正確、不精確、或無權威來源支撐的主張，給出可據以修正的具體回報。
- **背景**：共用背景（§10.1）＋ 這份文件會被同事當**行動依據**，錯一個預設值或因果方向就會誤導真實調優。
- **素材**：待審章 `.md`（帶 `.worktrees/spark-handbook/` 前綴路徑）；§6 權威來源清單；可用 **WebFetch/WebSearch** 查證；`handbook-writing-guide.md` §12（軟建議 vs 硬限制、方向性宣稱先確認正負號、查證引用）。
- **限制**：(1) 只查證、**不改稿**（回報給我）；(2) 來源限權威，不引未認證部落格；(3) 對齊 **Spark 3.3.x**、**Hive 3.1.3/CDP**，不可套別版的預設值/行為；(4) 每條判定**必附出處**（URL 或書名＋章節），查不到就標「無法查證」、**不臆測**；(5) 不評文筆易讀性（那是角色 B）；(6) 全程即時寫日誌（§10.4）。
- **完成的定義**：逐條列出可查證主張（config 名稱/預設值/版本特性/因果方向/數字），每條標 ✅已驗證(附出處) / ❌錯誤(給正確值+出處) / ⚠️無法查證；明確點出「建議被寫成硬限制」「方向性宣稱正負號可疑」「引用出處錯」之處；結尾按**真缺陷（必補）／可加強（斟酌）／誤讀（不改或微調）**三級彙整；日誌檔已落地。

### 10.3 角色 B — 目標讀者（reader，驗易讀性）
- **目標**：以無 DE 背景的目標讀者身分通讀，標出讀不懂、卡關、缺脈絡、太抽象、不自明之處，讓我知道哪裡要補。
- **背景**：共用背景（§10.1）＋ **人設扮演到位**：你是銀行資料分析師，會寫 SQL、懂業務資料，但沒學過分散式系統，不知道 shuffle/executor/partition 底層怎麼運作，看到沒解釋的英文術語會卡。
- **素材**：待審章 `.md`；**前置章節清單**（可假設已懂的概念）；`handbook-writing-guide.md` §11 讀者審查清單（主旨一致性/抽象未落地/鷹架洩漏/範疇失衡）。
- **限制**：(1) 只回報讀者視角問題、**不查技術對錯**（那是角色 A）；(2) **不改稿**；(3) **不可因自己其實懂 Spark 就放水**——嚴格扮演無 DE 背景讀者，任何第一次出現、沒當場解釋的術語都要標；(4) 全程即時寫日誌（§10.4）。
- **完成的定義**：逐段/逐節標卡關點，每點註明類型（缺脈絡 / 太抽象＝只有形容詞沒數字例子 / 術語沒先定義 / 概念圖不自明 / 步驟不可操作 / 鷹架洩漏），並寫「我會這樣想、我會問什麼」讓我知道怎麼補；指出全章主旨是否一致、範疇有無失衡（邊緣主題佔太多 / 核心太淺）；結尾按**真缺陷／可加強／誤讀**三級彙整；日誌檔已落地。

### 10.4 角色 C — 完整度與架構審查員（architecture / curriculum，驗邏輯與深度）
- **目標**：從**整本手冊**的角度，檢查邏輯架構是否清楚、章節順序是否合理、深度是否足夠，確保一個 **Spark 新手讀完，能具備長期穩定營運資料排程與特徵庫（feature store）的能力**。找出：缺漏的主題、順序/依賴問題（前面用到後面才教的概念）、深度不足以支撐營運之處。
- **背景**：共用背景（§10.1）＋ 終極學習目標（新手 → 能長期營運 data scheduling + feature store）＋ §1 深度定位 ＋ 手冊骨架（index + 9 章，§5）。
- **素材**：目前**已寫**的章節 `.md`（依 plan 的 Progress Tracker 判斷哪些已寫）；尚未寫的章節看 §5 大綱；spec 全文（§1、§5、§11）。
- **限制**：(1) 不查單點技術對錯（reviewer A 的事）、不挑逐句易讀性（reader B 的事）——**只看整體架構、順序、覆蓋度、深度是否達成終極能力目標**；(2) 不改稿；(3) 明確區分「已寫章節的實況」與「尚未寫、只能評 outline」；(4) 即時寫日誌（§10.5）。
- **完成的定義**：產出 (a) **能力地圖**——把「長期營運排程／特徵庫」需要的能力逐項對應到「由哪章哪節支撐」，標出**無人覆蓋的缺口**；(b) 章節順序/依賴是否合理；(c) 各章深度是否足以支撐營運（不只 ad-hoc）；(d) 具體補強建議（新增章/節、調順序、加深何處）；按「真缺陷（必補）／可加強／誤讀」三級彙整。

### 10.5 同步可審核（即時日誌）
每個 subagent 邊讀邊把發現**即時寫入** companion 日誌檔：逐章審稿用 `docs/handbooks/spark-tuning/.reviews/<chapter>__<role>.md`（如 `03-sql-tuning__reviewer.md`）；角色 C 的全書審查用 `docs/handbooks/spark-tuning/.reviews/_architecture__round-N.md`。使用者可隨時打開看進度，不必等最終回報（對齊既有「subagent 過程可同步審核」偏好）。

### 10.6 回饋處理與節奏
- **逐章（A+B）**：每章草稿完成 → A 技術 reviewer ＋ B 目標 reader 並行審 → 我按**真缺陷（必補）／可加強（斟酌）／誤讀（不改或微調）**三級 triage 修 → 送使用者審 → 打勾 commit。重大分歧或取捨不明才回頭問使用者。
- **里程碑（C）**：角色 C 在下列時點各跑一次——①outline/骨架剛定或大改時；②每累積寫完數章時；③全書 `.md` 定稿的最終 pass（Task 12）。用來確保整體邏輯與深度持續對準終極能力目標、及早發現結構缺口。
- 全書 `.md` 定稿、轉 HTML 後，最終 pass 同時跑 reader（跨章通讀導覽/連結）與 architecture（C）兩種審查。

## 11. 成功標準

- 一位無 DE 背景的分析師，能照手冊**自行**：讀懂自己 SQL 的 Spark UI、判斷瓶頸類型、改寫 SQL 或調少數 config、選對引擎、設計合理的 partition/儲存；需要時知道何時改用 DataFrame API。
- **終極能力目標**：一個 Spark 新手從頭讀完整本手冊後，具備**長期穩定營運資料排程與特徵庫（feature store）**的能力——不只會跑 ad-hoc，還懂資源配置與多租戶、可靠/可重跑、schema 演進與維護、時間點正確性等營運課題。整本的邏輯架構、章節順序與深度都要服務這個終點（由審稿角色 C 在里程碑持續把關）。
- 每個建議都有權威來源、具體數字、與明確取捨；每章經 reviewer(A) + reader(B) 審過並 triage 修正，整體架構/深度經 architecture reviewer(C) 在里程碑把關。
- 階層化、可分章查閱；`.md` 與 `.html` 成對交付，HTML 離線可看（內嵌 mermaid.js）。

## 12. 章節撰寫基準（以第 01 章為範本，後續每章對齊）

第 01 章經三輪 A/B 審 + 架構審 C + 使用者多輪回饋定版，立為**深度與體例範本**。後續章節照下列基準寫（這份是 §6 寫作慣例的「實戰版補充」）：

### 12.1 深度與分層
- **三層遞進**：每章先給「無 DE 背景也懂的直覺」→ 再進階機制 → 再帶「營運／取捨」。只做 ad-hoc 的人讀前段就夠用，要營運排程／特徵庫的人讀得到後段。
- **具體數字落地**：每個抽象主張配可手算的具體數字，優先用銀行資料量（客戶 1000 萬、信用卡帳務 3000 萬/月、App 1000 萬筆/天）。範例：`30GB ÷ 128MB ≈ 240 partition`、`100 core / 400GB → 胖 5×20×80 vs 瘦 20×5×20`。
- **取捨就地講**：遇時間／記憶體／儲存的兩難，當場把兩邊代價講清楚，不含糊帶過。
- **長度**：~8–10 節可接受（第 01 章 10 節）；寧可拆多個聚焦小節，不要一節做太多事；注意密度別前輕後重擠成一團。

### 12.2 體例結構（每章固定骨架）
- 開頭 `> **本章前提**`：列讀者「已讀哪些章、可假設已懂什麼」（對齊 §10 reader subagent 的 PRIOR_CHAPTERS）。
- 一節一概念；小節標題用描述性命名，不放給作者自己的備註。
- 視需要放一個「**把它全部串起來**」的貫穿範例節（如 §1.8 端到端 SQL 旅程）：用前面的零件組裝、不引入新理論——這種節通常最有收穫。
- 結尾「**一句話帶走**」收斂主軸 + 指向後續章節。
- 章末「上一章 / 下一章 / 回首頁」導覽連結。

### 12.3 概念圖（Mermaid）
- 每章 2–5 張簡單概念圖；圖要**自明**（節點／箭頭看得懂、不靠未定義符號）；跨節／跨 stage 的配對關係要用文字點明（如 §1.8「shuffle write 與下一個 stage 的 shuffle read 是同一次 shuffle 的兩半」）。

### 12.4 資料來源（見 §6，每章必做）
- 每個重要概念段落末附 `📚 來源` footer（代表性出處＋連結）；章末加「資料來源與精確度說明」（列刻意簡化／無官方逐字數字之處 + 版本對齊說明）。來源限官方／核心開發者文章／指定書籍，不引未認證部落格。

### 12.5 審稿流程（每章）
- 節奏：`我寫草稿 → A 技術 reviewer + B 目標 reader 並行審（即時日誌到 .reviews/）→ 我 triage（真缺陷必補／可加強斟酌／誤讀不改）→ 送使用者審 → commit`。
- 第 01 章因是範本審了三輪；後續章節通常一輪 A+B 即可，內容大改才再審。架構審 C 在里程碑跑（§10.6）。

### 12.6 第 01 章審查反覆抓到、後續要主動避免的坑
- **術語／縮寫第一次出現就當場解釋**（CDP／AQE／spill／HDFS／GC 都曾被抓「憑空出現」）；嚴禁「先用後定義」。
- **跨節引用要指對**：「下一節會講」「§X 會講」改結構後回頭檢查（第 01 章曾把 shuffle 指錯到下一節）。
- **圖與文要對齊**：圖上出現的方塊／節點，文字要解釋到（第 01 章流程圖曾有方塊沒對應到文字）。
- **不過度宣稱**：「X 比 Y 快／好」要有但書、先確認因果方向；把文件的「建議」寫成「硬限制」是過度宣稱。
- **簡化要明說**：凡為好懂而簡化、或無官方逐字數字的，章末精確度段要列出來，讓讀者自行斟酌。
- **方向性宣稱先確認正負號**；量級無來源就標「方向正確、無逐字數字」，不要編倍率。
