# 02 章 round-2 升級（D-UI：手把手 UI 診斷核心）reviewer 比對檔

本檔記錄 round-2 改寫第 02 章時的 **Step A 官方查證**，供後續 reviewer 比對「畫面欄位語意」是否與 Spark 官方一致。
版本對齊：本手冊鎖定 **Spark 3.3.2**；官方 `/docs/3.3.2/` 版本化頁面在查證當下回 404，改查 `/docs/latest/` 可達頁
（web-ui / sql-performance-tuning / EXPLAIN 三頁），這些頁籤組成與 metric 名稱自 Spark 3.x 起穩定、與 3.3 行為一致。
逐字句以 latest 頁為準、本章正文同時標「對齊 3.3」。

---

## Step A 查證 — Web UI 各頁籤欄位語意（來源：Spark `docs/latest/web-ui.html`）

### Jobs 頁籤
- Summary page 欄位（**已查證**）：Job ID、Description、Submitted、Duration、Stages（summary）、Tasks（progress bar）、Status（Active/Completed/Failed）。
- Job detail 內每個 stage 列出：Input＝"Bytes read from storage in this stage"、Output＝"Bytes written in storage in this stage"、
  Shuffle read＝"Total shuffle bytes and records read, includes both data read locally and data read from remote executors"、
  Shuffle write＝"Bytes and records written to disk in order to be read by a shuffle in a future stage"。（皆**已查證逐字**）
- 定位：Jobs 是「入口/概覽」——一個 application 觸發哪些 job、各花多久、紅色 Failed 一眼可見。

### Stages 頁籤（Stage detail → Summary Metrics）
- 官方原文：**"Summary metrics for all task are represented in a table and in a timeline"**（注意官方用 "all task" 不是 "Completed Tasks"；
  正文已據此把標題寫成「Summary Metrics 摘要表」，不杜撰 "for Completed Tasks" 字樣）。
- **已查證逐字的 metric 定義**：
  - `Shuffle spill (memory)` = "is the size of the deserialized form of the shuffled data in memory"
  - `Shuffle spill (disk)` = "is the size of the serialized form of the data on disk"
  - `Shuffle Read Size / Records` = "Total shuffle bytes read, includes both data read locally and data read from remote executors"
  - `GC time` = "is the total JVM garbage collection time"
  - `Peak execution memory` = "is the maximum memory used by the internal data structures created during shuffles, aggregations and joins"
  - 另列：Tasks deserialization time、Duration of tasks、Result serialization time、Getting result time、Scheduler delay、
    Shuffle Read Fetch Wait Time、Shuffle Remote Reads、Shuffle Write Time。
- **percentile 欄位（Min / 25th percentile / Median / 75th percentile / Max）**：latest web-ui.html 主文未逐字列出這幾個列標，
  但官方 monitoring 生態與多個對齊 latest 文件的二手來源一致確認此即 Stage Summary Metrics 的標準五分位呈現
  （"Min, 25th percentile, Median, 75th percentile, Max"；對應 0/25/50/75/100 百分位）。正文採此為**標準呈現**並在精確度說明標註「主文未逐字、屬標準呈現」。
- `Input Size / Records`：定義為「從 Hadoop 或 Spark storage 讀入的 bytes 與 records」（inputMetrics.bytesRead / recordsRead）。
  二手來源並指出讀法：input size 的 max 若遠大於 median＝資料 skew。正文據此用於「掃太多/小檔」診斷，標保守。

### SQL / DataFrame 頁籤（query detail）
- 顯示：query 執行時間/duration、關聯 jobs、**query execution DAG（每個算子掛 metrics）**、logical/physical plan。
- **已查證逐字/近逐字**：
  - `number of output rows` = "the number of output rows of the operator"
  - Exchange 區塊：官方原文 "The second block 'Exchange' shows the metrics on the shuffle exchange, including number of written shuffle records, total data size, etc."
  - 完整 SQL metrics 欄位含：`number of output rows`、`data size`、`shuffle bytes written`、`shuffle records written`、
    `shuffle write time`、`remote/local blocks read`、`remote/local bytes read`、`fetch wait time`、`records read`、
    `sort time`、`peak memory`、`spill size`。
- 正文用的兩個關鍵字 `number of output rows`（抓爆量 join）與 Exchange 的 shuffle 量（抓最貴 shuffle）皆有官方依據。
  正文沿用既有「shuffle bytes written total」描述，標為對齊官方 `shuffle bytes written` metric 的口語化。

### Executors 頁籤
- **已查證欄位**：Executor ID、Address、Status、RDD Blocks、Storage Memory、Disk Used、Cores、
  Active Tasks、Failed Tasks、Complete Tasks、Total Tasks、Task Time、Input、Shuffle Read、Shuffle Write、GC Time。
- `Storage Memory` = "shows the amount of memory used and reserved for caching data"（**已查證逐字**）。
- 官方定位（**已查證逐字**）："resource information (amount of memory, disk, and cores used by each executor)
  but also performance information (GC time and shuffle information)"。
- 正文用法：看 Active/Failed Tasks（是否一直在重試）、Shuffle Read/Write、GC Time（紅＝GC 壓力）、Storage Memory（cache 佔用）。
  注意：web-ui.html 主文未把「Shuffle Spill」列為 Executors 頁獨立欄；正文據此**收斂**——spill 的權威讀法回到 Stages 頁
  Summary Metrics 的 `Shuffle spill (memory/disk)`，Executors 頁只當「誰記憶體吃緊（GC、Storage Memory）」的旁證，不杜撰 Executors 有 spill 欄。

### Storage 頁籤
- 顯示 persisted RDDs/DataFrames、storage level、size、partitions；明確警語（**已查證逐字**）：
  "the newly persisted RDDs or DataFrames are not shown in the tab before they are materialized ... make sure an action operation has been triggered"。
- 正文定位：只有用到 `CACHE`/`persist` 才看；對 SQL-first 排程多數人少用，故輕量帶過並把「cache 何時值得」留給後續章。

### Environment 頁籤
- 五區（**已查證**）：Runtime Information（Java/Scala 版本）、Spark Properties（如 spark.app.name、spark.driver.memory）、
  Hadoop Properties、System Properties、Classpath Entries。
- 正文用法：驗「我用 `SET` 改的設定 / broadcast 門檻到底生效沒」——在 Spark Properties 找該 key 的實際值。

---

## Step A 查證 — SQL 效能與計畫節點（來源：`docs/latest/sql-performance-tuning.html`、`sql-ref-syntax-qry-explain.html`）

### AQE（與 SQL 頁籤 isFinalPlan 對讀）
- `spark.sql.adaptive.enabled` **default = true（since Spark 3.2.0）**（**已查證逐字**）：
  "Adaptive Query Execution (AQE) ... is enabled by default since Apache Spark 3.2.0."
- 三件事（**已查證逐字**）：
  - Coalescing post-shuffle partitions（`spark.sql.adaptive.coalescePartitions.enabled` default true）。
  - "AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side are smaller than
    the adaptive broadcast hash join threshold."（→ 解釋為何 EXPLAIN 看到 SortMergeJoin、跑完 SQL 頁卻變 broadcast）。
  - Skew join optimization（`spark.sql.adaptive.skewJoin.enabled` default true）："dynamically handles skew in sort-merge join
    by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks."
- `AdaptiveSparkPlan` 節點 / `isFinalPlan` 旗標：**sql-performance-tuning.html 與 EXPLAIN 頁主文均未逐字載明**。
  → 正文沿用既有第 02 章既有處理：標為「行為已知、官方主文未逐字」，並引 Databricks AQE 文（Spark 核心團隊撰）＋ SPARK-33850 為佐證；不誇大為官方逐字。

### Join / broadcast 門檻
- `spark.sql.autoBroadcastJoinThreshold` **default = 10485760（10 MB）**（**已查證逐字**）：
  "the maximum size in bytes for a table that will be broadcast ... By setting this value to -1, broadcasting can be disabled."
- `spark.sql.shuffle.partitions` **default = 200**（**已查證逐字**）。
- 上述兩個 default 僅作「畫面看到值是否異常」的對照，**修法本身留第 04 章**（本章不教調法）。

### EXPLAIN 模式與輸出
- `EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement`（**已查證逐字**）。
- `FORMATTED` = "Generates two sections: a physical plan outline and node details."（**已查證逐字**）。
- ⚠️ EXPLAIN 頁的範例只示範到 `Exchange`/`HashAggregate`/`LocalTableScan`，**未逐字列出** `PartitionFilters`/`PushedFilters`/
  `BroadcastHashJoin`/`SortMergeJoin`。這些是 Spark physical plan 的標準輸出（見《Spark: The Definitive Guide》Ch.8/15
  與 sql-performance-tuning），正文標為「示意、欄位以你環境跑出來為準」，不宣稱逐字引自官方 EXPLAIN 範例。

---

## 無法查證 / 保守處理清單（明確標記，正文已對齊）
1. Stage Summary Metrics 的 percentile **列標字樣**（Min/25th/Median/75th/Max）——latest web-ui 主文未逐字，
   採「標準呈現」說法，精確度說明標註。
2. `AdaptiveSparkPlan` / `isFinalPlan` 字樣——官方主文未逐字，沿用佐證來源、標保守。
3. Executors 頁有無獨立「Shuffle Spill」欄——web-ui 主文未列，正文**不杜撰**，spill 權威讀法收斂到 Stages 頁。
4. EXPLAIN 範例的 `PartitionFilters`/`PushedFilters`/join 算子字樣——官方小範例未含，標「示意、以你環境為準」。
5. CDP History Server port 18088 vs Apache 預設 18080——沿用既有 §2.2 已查證處理，兩者各自情境皆對。

---

## 邊界自我確認（D-UI 不越界）
- 第 02 章 round-2 只做「畫面症狀 → 該查哪一格 → 翻第 03/04/05 哪一節」的**診斷與路由**。
- 所有「怎麼修」（broadcast hint、salting、AQE 旋鈕、partition 設計、小檔合併、ANALYZE TABLE…）一律**只給連結指到 03/04/05**，
  本章正文不重述修法步驟。三張 checklist 每一項都收束到一個第 03/04/05 章的具體 §。
