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

---

# Step B 查證 — reviewer 逐條技術正確性核對（接 Step A）

審查員：技術審查（只查技術正確、不評易讀性）。環境＝Spark 3.3.2 + Hive 3.1.3 + CDP 7.1.9。
方法：WebFetch 抓 `docs/latest/` 三頁（web-ui / sql-performance-tuning / sql-ref-syntax-qry-explain）逐句比對。
（註：web-ui WebFetch 回傳頁面標頭顯示為 4.1.2，但所引欄位/逐字定義字句自 Spark 3.x 起穩定、與 Step A 對 3.3 的查證完全一致，採用。）

## 重點查證 1 — 每處「某格顯示 X → 代表 Y」對照官方欄位語意

### §2.3 EXPLAIN
- ✅ `EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement` 語法逐字正確。出處：EXPLAIN 頁逐字。
- ✅ `FORMATTED` 產出「physical plan outline + node details 兩段」逐字正確（官方："Generates two sections: a physical plan outline and node details."）。正文 §2.3「`FORMATTED` 額外把每個步驟的細節分區整理」與 章末來源「physical plan outline + node details」一致。
- ✅ `Exchange hashpartitioning(...)`＝一次 shuffle；`BroadcastExchange` 不算 shuffle 次數——此區分正確（BroadcastExchange 是廣播交換、非 hash repartition shuffle）。正文 §2.3 明確提醒「別把 BroadcastExchange 算進 shuffle 次數」，技術上正確。出處：概念見 sql-performance-tuning + DEF Guide，正文已標示意。
- ✅ `PartitionFilters` / `PushedFilters` 語意正確（PartitionFilters＝用來只讀需要的分區目錄；PushedFilters＝下推到讀檔層提早篩）。⚠️ 但**官方 EXPLAIN 範例只示範到 `HashAggregate`/`Exchange`/`LocalTableScan`**，未逐字出現 `PartitionFilters`/`PushedFilters`/`BroadcastHashJoin`/`SortMergeJoin`——正文 §2.3 來源段與章末精確度說明#2 已誠實標註「示意、欄位以你環境跑出來為準」。處理正確、無杜撰。出處：EXPLAIN 頁逐字確認範例只含三算子。
- ⚠️（小）正文範例 plan 用 `Exchange hashpartitioning(segment, 200)` 的 `200`＝`spark.sql.shuffle.partitions` 預設（官方逐字 200），與示意一致、無誤導。

### §2.4 Jobs 頁
- ✅ Summary 欄位（Job ID／Description／Submitted／Duration／Stages／Tasks progress／Status）正確。官方原文為合併句："Job ID, description (with a link to detailed job page), submitted time, duration, stages summary and tasks progress bar"——逐欄對得上（Status 屬 job 狀態分組 Active/Completed/Failed，Step A 已查證）。
- ✅ Job detail 內每個 stage 的 Input／Output／Shuffle Read／Shuffle Write 逐字定義正確：
  - Input＝"Bytes read from storage in this stage" ✅
  - Output＝"Bytes written in storage in this stage" ✅
  - Shuffle read＝"...includes both data read locally and data read from remote executors" ✅（正文章末來源引此句，正確）
- ✅ 「FAILED→第一個要點開」「Duration 特別長→主嫌」「Stages 卡在 2/3、進度條停住→下游 skew/spill」——皆為合理因果定位，未寫成硬規則。Jobs 頁定位為「分流台、只定位不給病因」正確。

### §2.5 SQL 頁
- ✅ `number of output rows`＝"the number of output rows of the operator" 逐字正確。出處：web-ui SQL Tab 逐字。
- ✅ Exchange 區塊 metrics：官方原文 "The second block 'Exchange' shows the metrics on the shuffle exchange, including number of written shuffle records, total data size, etc."——正文用 `shuffle bytes written`/`shuffle records written`/`data size` 對齊官方 SQL metrics 清單（官方另列 `shuffle bytes written`="the number of bytes written"），正確。正文把口語化 metric「shuffle bytes written total」標為對齊官方 `shuffle bytes written`，誠實。
- ✅「`Join` 後 output rows ≫ 輸入＝爆量/一對多/近笛卡兒」因果正確；「`Filter` 後列數驟降正常」正確。
- ✅「最大 Exchange 位元組＝最貴 shuffle」為合理判讀依據，非硬門檻（未給絕對 GB 閾值）。

### §2.6 Stages 頁
- ✅ `Shuffle spill (memory)`＝"the size of the deserialized form of the shuffled data in memory" 逐字正確。
- ✅ `Shuffle spill (disk)`＝"the size of the serialized form of the data on disk" 逐字正確。
- ✅ `Shuffle Read Size / Records`＝"Total shuffle bytes read, includes both data read locally and data read from remote executors" 逐字正確。
- ✅ `GC time`＝"the total JVM garbage collection time" 逐字正確（此處正文章末把 GC time 歸到 Executors 頁來源，但 Stages 頁 summary 亦有 GC time 列，兩處皆官方有，無誤）。
- ✅ `Input Size / Records`＝從 Hadoop/Spark storage 讀入 bytes 與 records——語意正確（官方 inputMetrics）。
- ✅ 「Summary metrics for all task ... in a table and in a timeline」逐字正確；正文標題寫「Summary Metrics 摘要表」、未杜撰 "for Completed Tasks"，正確。
- ✅ 因果方向正確：skew＝Max≫Median（看 Duration/Shuffle Read/Input 的 Max vs Median）；spill＝兩個 Shuffle spill 列任一非零。皆為「招牌訊號」式判讀、非硬門檻；「Max 比 Median 大一個量級」是描述性、不是硬規則。

### §2.7 Executors 頁
- ✅ 欄位（Cores／Active／Failed／Complete Tasks／Task Time／Input／Shuffle Read/Write／GC Time／Storage Memory／Disk Used）正確。
- ✅ `Storage Memory`＝"the amount of memory used and reserved for caching data" 逐字正確。
- ✅ `GC time`＝"the total JVM garbage collection time" 逐字正確。
- ✅ 定位句逐字正確：官方 "The Executors tab provides not only resource information (amount of memory, disk, and cores used by each executor) but also performance information (GC time and shuffle information)."——正文章末引此句，正確。
- ✅ 「GC Time 佔 Task Time 高比例＝記憶體吃緊」「Failed Tasks 長大＝重試/OOM」「拿到的 executor 太少＝資源沒配到/多租戶」——皆合理因果、未寫硬門檻（10% 是文中示意數字、非規定閾值，措辭「紅色警訊」是描述）。

### §2.8 Storage / Environment 頁
- ✅ Storage 警語逐字正確："the newly persisted RDDs or DataFrames are not shown in the tab before they are materialized ... make sure an action operation has been triggered."
- ✅ Environment 五區正確：Runtime Information／Spark Properties／Hadoop Properties／System Properties／Classpath Entries（官方逐字 "This environment page has five parts"）。Spark Properties 例 `spark.app.name`/`spark.driver.memory` 與官方一致。
- ✅ 「用 `SET` 改設定後到 Spark Properties 區對質實際值」用法正確。

## 重點查證 2 — 實作者自標「無法逐字查證」四點專核

1. **Executors 頁有無獨立 Shuffle Spill 欄** → ✅ 實作者判斷**正確**。官方 web-ui Executors 段**未**把 "Shuffle Spill" 列為獨立欄（只有 Storage Memory / Disk Used / Input / Shuffle Read / Shuffle Write / GC Time 等）。正文把 spill 判定**收斂到 Stages 頁** `Shuffle spill (memory/disk)`、Executors 頁只當「誰記憶體吃緊（GC/Failed/Storage Memory）」旁證——**沒有杜撰 Executors 有 spill 欄**，§2.7 末提醒框與精確度說明#6 處理正確。
   - 補充（可加強，非缺陷）：官方 Executors 定位句確實含 "shuffle information"，但指的是 Shuffle Read/Write，**不含 spill**；正文未把 "shuffle information" 誤讀成 spill，正確。
2. **五分位列標 Min/25th/Median/75th/Max** → ⚠️ 官方 web-ui 主文**確未逐字列**這五個列標（WebFetch 確認 Stages 段未出現 percentile 列標字樣）。實作者標為「Stage Summary Metrics 標準呈現、主文未逐字」並於精確度說明#5 註記——**處理正確、不臆測為官方逐字**。此為業界與多版本 Spark UI 一致的標準五分位呈現，標 ⚠️「標準呈現、非官方逐字」是誠實且恰當的層級。
3. **`AdaptiveSparkPlan isFinalPlan=true` 字樣來源** → ⚠️ 官方 sql-performance-tuning 與 EXPLAIN 兩頁**均未**出現字串 "AdaptiveSparkPlan" / "isFinalPlan"（WebFetch 兩頁皆確認 No）。正文 §2.5 與精確度說明#3 標為「官方主文未逐字載明，依 Databricks AQE 文＋SPARK-33850 佐證、以你環境 SQL 頁實際顯示為準」——**處理正確、未誇大為官方逐字**。佐證來源（Databricks AQE 文＋JIRA）屬可接受的核心團隊來源。
   - ✅ 連帶查證：AQE 把 sort-merge join 轉 broadcast 確為官方逐字（"AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side are smaller than the adaptive broadcast hash join threshold."），正文據此解釋「EXPLAIN 看 SortMergeJoin、跑完 SQL 頁變 broadcast」的因果**正確**。
   - ✅ `spark.sql.adaptive.enabled` default true since 3.2.0 逐字正確（對齊 3.3.2 成立）。
4. **EXPLAIN 範例 PartitionFilters/join 算子字樣** → ⚠️ 同查證 1 §2.3：官方範例只到 `HashAggregate`/`Exchange`/`LocalTableScan`，未含 `PartitionFilters`/`PushedFilters`/`BroadcastHashJoin`/`SortMergeJoin`。正文已標「示意、以你環境為準」，**未宣稱逐字引自官方**。處理正確。

## 重點查證 3 — §2.9 路由總表 9 列 ＋ §2.10 三張 checklist 16 項

### §2.9 路由總表（9 列）逐列核「在哪格看出來」+ 指向章節
1. shuffle 過大｜SQL 頁 Exchange 的 shuffle bytes｜✅ 格正確（SQL 頁 Exchange metric）｜指 §3.5/§3.8 ✅ 對症（join 策略/聚合減 shuffle）。
2. skew｜Stages 頁 Duration/Shuffle Read 分位數 Max≫Median｜✅ 格正確｜指 §3.10 salting + §4.2 AQE skew join ✅ 對症。
3. spill｜Stages 頁 Shuffle spill(memory/disk) 任一非零｜✅ 格正確（spill 權威讀法在 Stages 頁，與 §2.7 收斂一致）｜指 第03章減量 + §4.4/§4.5 ✅ 對症。
4. 掃太多/沒裁分區｜EXPLAIN Scan 缺 PartitionFilters；Stages 頁 Input Size 遠大於預期｜✅ 兩個入手點皆正確｜指 §3.2/§3.4 + §5.4 ✅ 對症。
5. 小檔太多｜Stages 頁讀檔 stage task 數異常多、每個 Input 很小｜✅ 格正確（小檔→多 task、每 task input 小，因果對）｜指 §5.5 ✅ 對症。
6. broadcast 沒生效｜EXPLAIN/SQL 頁 小表處 SortMergeJoin 而非 BroadcastHashJoin｜✅ 格正確｜指 §3.6 hint + §4.4 門檻 ✅ 對症。
7. 爆量 join｜SQL 頁 Join 的 number of output rows ≫ 輸入｜✅ 格正確（number of output rows 官方逐字）｜指 §3.7 ✅ 對症。
8. job 失敗/OOM｜Jobs 頁 FAILED；Executors 頁 Failed Tasks/GC Time 高｜✅ 兩個入手點正確｜指 第03章 + §4.5/§4.6 ✅ 對症。
9. 設定沒生效｜Environment 頁 Spark Properties 找 key｜✅ 格正確（Environment 唯一能對質實際值處）｜指 §4.3 ✅ 對症。
→ 9 列「在哪格看」全部與官方欄位語意一致；指向章節與症狀修法對得上。無缺陷。

### §2.10 三張 checklist（A 6 項 + B 5 項 + C 5 項 = 16 項）
A 改 SQL（→第03章）：
- A1 SQL 頁 Exchange 個數 → §3.5/§3.8 ✅
- A2 EXPLAIN/SQL 頁 BroadcastHashJoin vs SortMergeJoin → §3.6 ✅
- A3 EXPLAIN Scan 的 PartitionFilters/PushedFilters → §3.2/§3.4 ✅（PushedFilters 為下推、PartitionFilters 為裁分區，分流正確）
- A4 SQL 頁 Join number of output rows ≫ 輸入 → §3.7 ✅
- A5 SQL 頁 讀檔算子輸入列數/欄位過多（SELECT */沒裁）→ §3.3/§3.2 ✅
- A6 Stages 頁 Duration Max≫Median（skew）→ §3.10 ✅
B 改設定（→第04章）：
- B1 Stages 頁 Shuffle spill 非零 → §4.5 ✅
- B2 Stages 頁 shuffle 後分區過多/每 task Shuffle Read 很小（碎分區）或 AQE 已合併 → §4.2/§4.4 ✅（碎分區徵兆＝每 task shuffle read 小，正確；且提醒 AQE coalesce 可能已處理，誠實）
- B3 Environment 頁 autoBroadcastJoinThreshold/adaptive.enabled 實際值 → §4.3/§4.4 ✅（兩 key 官方 default 10485760 / true，正文未在本章寫死門檻、留第04章，正確）
- B4 Executors 頁 GC Time 佔比高/Failed Tasks 長大 → §4.5 ✅
- B5 Executors 頁 實際 executor 數/core vs 申請 → §4.6/§4.7 ✅
C 改寫表/儲存（→第05章）：
- C1 Stages 頁 Input Size 掃了不該掃的分區 → §5.4 ✅
- C2 Stages 頁 task 數暴增/每 Input 很小（小碎檔）→ §5.5 ✅
- C3 Jobs 頁 寫出階段 Output 大小/檔案數→一堆小檔 → §5.5 ✅（Output＝"Bytes written in storage in this stage" 官方逐字，用於看寫出量正確；惟「檔案數」非 Jobs 頁直接欄位，屬由 Output/task 數推斷——見下「可加強」）
- C4 SQL/EXPLAIN 該裁分區卻整表掃（查詢沒寫對 or partition 欄選錯）→ §5.4 ✅
- C5 SQL 頁 讀檔慢/PushedFilters 沒生效（格式不對 or 沒餵統計）→ §5.2/§5.6 ✅
→ 16 項「在某格看」與「指向 §」全部對得上、無錯指。一個輕微保留見下。

## 重點查證 4 — 硬規則/門檻、因果正負號、出處錯

- ✅ 全章未把「建議」寫成硬門檻：所有數字（12 秒/9.2 分、12.4GB、10%、260MB vs 6GB）均明確標「示意數字」；判讀用「Max≫Median」「特別大」「非零」等相對描述，不是絕對閾值。
- ✅ 因果正負號全部正確：spill＝記憶體相對資料太小（非反向）；Max≫Median＝skew；output rows 暴增＝爆量；GC 佔比高＝記憶體壓力；BroadcastHashJoin＝免 join shuffle（便宜）、SortMergeJoin＝兩邊 shuffle（貴）方向正確。
- ✅ 出處引用正確、分層誠實：逐字定義引官方、概念引 DEF Guide、未逐字者標佐證來源（Databricks/JIRA）。CDP 18088 vs Apache 18080 的雙 port 處理（精確度說明#1）正確（CDP 文件確用 18088）。
- ✅ §2.2 History Server：`spark.history.fs.update.interval` 預設 10s、incomplete 含崩潰未收尾者、需 `spark.eventLog.enabled`——與 monitoring 頁一致（Step A 已查證，本輪未重抓 monitoring 頁，沿用 Step A）。

---

# 結尾彙整：三級分類

## A. 真缺陷（必補）
**無。** 逐條對官方欄位語意、因果方向、章節指向均正確；四個「無法逐字查證」點全部以恰當層級（⚠️ 標註 + 佐證來源）誠實處理，無杜撰、無把建議寫成硬門檻、無出處錯置。

## B. 可加強（非阻擋，作者自行斟酌）
1. **§2.10 C3「寫出階段的 Output 大小/檔案數」**：官方 Jobs/Stage 頁有 `Output`＝"Bytes written in storage in this stage"（bytes，逐字），但**「輸出檔案數」並非 UI 直接欄位**，需由 Output bytes + 寫出 task 數推斷。建議微調措辭為「Output 大小（檔案數需由寫出 task 數推斷）」，避免讀者誤以為 UI 有「檔案數」一格。屬精確度微調、非錯誤。
2. **§2.5 metric 口語化字樣**：正文用 `shuffle bytes written total: 12.4 GB`，官方 SQL metric 名為 `shuffle bytes written`（無 "total" 後綴；"total" 是 SQL DAG 上多 task 加總的呈現慣例）。作者已在來源段標為「口語化」，可保留；若要更貼官方可去掉 "total" 字面或加註「total 為加總呈現」。
3. **percentile 列標 / AdaptiveSparkPlan / isFinalPlan**：三者官方主文確無逐字（本輪已二次確認），現行 ⚠️ 標註已足夠；若日後 HTML 版能補上一張公司環境真實截圖（精確度說明#4 已預告），可把這三點從「標準呈現/佐證」升級為「截圖實證」，更硬。

## C. 誤讀（澄清，非作者錯）
1. 本輪 web-ui WebFetch 回傳頁面標頭顯示 **Spark 4.1.2** 而非 3.3.2——這是 `docs/latest/` 目前指向的版本，**不影響本章查證**：所引欄位名與逐字定義（spill/Storage Memory/GC time/number of output rows/Exchange 區塊/五區 Environment 等）自 3.x 起穩定、與 Step A 對 3.3 的查證逐字一致。作者章末「版本對齊」說明（latest→3.3.2 改網址字串）已涵蓋此點，無需改。
2. 官方 Executors 定位句含 "shuffle information"，易被誤讀為含 spill；正文**未**犯此誤讀，明確把 spill 收斂到 Stages 頁，正確。

（Step B 查證完。所有官方逐字句出處：web-ui.html / sql-performance-tuning.html / sql-ref-syntax-qry-explain.html，`docs/latest/`，2026-06 抓取。）
