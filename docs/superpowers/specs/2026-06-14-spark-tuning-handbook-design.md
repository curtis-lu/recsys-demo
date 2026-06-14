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
- query 生命週期：SQL → logical plan → Catalyst 優化 → physical plan → jobs → stages → tasks。
- 兩種運算：**窄依賴**（map-like，便宜，不搬資料）vs **寬依賴 = shuffle**（貴，跨網路重分佈資料）。
- **shuffle 為什麼是頭號敵人**（用 3000 萬筆帳務 `GROUP BY` 客戶舉例）。
- lazy evaluation：action 才觸發。
- 概念圖：cluster 圖、SQL→stage→task 圖、narrow vs wide 圖。
- 主軸預告：多數優化＝減少或減輕 shuffle 與掃描量。

### `02-diagnose-with-spark-ui.md` — 用 Spark UI 與 EXPLAIN 找瓶頸
- 心法：**先量再調，不憑感覺**。
- 在 CDP 上怎麼開 Spark UI（History Server / Cloudera Manager）。
- `EXPLAIN` / `EXPLAIN FORMATTED` 讀重點：找 `Exchange`(=shuffle)、`BroadcastHashJoin` vs `SortMergeJoin`、Scan 的 partition filter 有沒有生效。
- Spark UI 各頁籤該看什麼（SQL-first 的人主看 SQL 頁籤的 query plan + Stages 的 task 時間/資料分佈）。
- 認症狀：shuffle 過大、**skew**（少數 task 特別久）、**spill**（記憶體不足落磁碟）、小檔/掃太多（partition 沒裁到）。
- 產出「症狀 → 看哪裡 → 翻到哪章」對照表（呼應 §09）。

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
- 仍要懂的少數旋鈕：`spark.sql.shuffle.partitions`（AQE 下角色變了）、`spark.sql.autoBroadcastJoinThreshold`、executor memory/cores/數量（CDP/YARN 上怎麼給）、`spark.sql.files.maxPartitionBytes`、dynamic allocation。
- 怎麼在 Hue/notebook 用 `SET` 設定。
- 記憶體模型一句話：executor memory 分 execution/storage，spill 是不夠的徵兆。
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

### `08-scenario-playbooks.md` — 場景對應
- 場景 1 ad-hoc：先 Impala/小樣本、partition 裁剪、`LIMIT`、別 `SELECT *`、別全表 `COUNT(DISTINCT)`。
- 場景 2 排程產表：可重跑、控輸出檔大小、partition 設計、`ANALYZE`、用 Spark/Hive、用 Spark UI 抓退化。
- 場景 3 特徵運算：寬表多 join、多 window、易 skew；broadcast 維度表、預聚合、控 shuffle、cache 中間結果的取捨（可用 SQL 或 §07 的 DataFrame API）。
- 每場景：典型陷阱 → 對策 → 引用前面哪章。

### `09-cheatsheet-and-glossary.md` — 速查與名詞表
- 取捨速查表：時間 ↔ 記憶體 ↔ 儲存（每個手段三維度影響）。
- config 速查表（名稱/預設/何時調/風險）。
- 症狀→對策速查（呼應 §02）。
- 名詞對照表（partition/shuffle/executor/skew/spill/broadcast… 中英對照＋一句話）。

> 「記憶體 vs 時間 vs 儲存」取捨**就地點在各章**（如 broadcast join 省 shuffle 但吃記憶體；過度 partition 省掃描但爆小檔），最後在 §09 收成速查表。
>
> 章數彈性：若某章寫起來太薄，允許合併（如 04 併入 03、09 併入 index），定案以實作計畫為準。

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

### 10.4 同步可審核（即時日誌）
每個 subagent 邊讀邊把發現**即時寫入** companion 日誌檔 `docs/handbooks/spark-tuning/.reviews/<chapter>__<role>.md`（如 `03-sql-tuning__reviewer.md`），使用者可隨時打開看進度，不必等最終回報（對齊既有「subagent 過程可同步審核」偏好）。

### 10.5 回饋處理與節奏
- 我收兩份報告後，按**真缺陷（必補）／可加強（斟酌）／誤讀（不改或微調）**三級處理，修完才把該章送使用者審；重大分歧或取捨不明處才回頭問使用者。
- 節奏與「一章一審」對齊：`我寫草稿 → A+B 並行審 → 我 triage 修 → 使用者審 → 打勾 commit`。
- 全部 `.md` 定稿、轉 HTML 後，可選擇再派 reader subagent 做一次跨章通讀（導覽/連結/全書一致性）。

## 11. 成功標準

- 一位無 DE 背景的分析師，能照手冊**自行**：讀懂自己 SQL 的 Spark UI、判斷瓶頸類型、改寫 SQL 或調少數 config、選對引擎、設計合理的 partition/儲存；需要時知道何時改用 DataFrame API。
- 每個建議都有權威來源、具體數字、與明確取捨；每章經 reviewer + reader 兩個 subagent 審過並 triage 修正。
- 階層化、可分章查閱；`.md` 與 `.html` 成對交付，HTML 離線可看（內嵌 mermaid.js）。
