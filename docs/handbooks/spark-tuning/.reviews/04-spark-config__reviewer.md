# 04 章「Spark 設定（AQE-first）」技術審查日誌

審查員：claude（技術審查 subagent）
審查日：2026-06-16
基準版本：Spark 3.3.x / Hive 3.1.3（CDP PvC Base 7.1.9）
方法：逐條主張 → WebFetch/WebSearch 查證權威來源 → ✅/❌/⚠️

> 本日誌邊查邊 append，每查一條寫一條。

---

## 進度

(以下逐條 append)

---

### [版本鎖定備註] Spark 3.3.2 版本化文件頁 404
- `https://spark.apache.org/docs/3.3.2/sql-performance-tuning.html` → HTTP 404（與本章「自動工具無法直接驗證 3.3.2 頁」一致）。
- 改用 `docs/latest/`（撰寫時指向 4.x）查證；user 已確認上列關鍵值 3.3→4.x 未變，以下查證以 latest 為準並逐一比對 since-version。

---

### §4.2 ② SortMergeJoin → BroadcastHashJoin（特別注意點）— ✅ 已驗證
- 章節主張：「就算計畫階段排了昂貴的 SortMergeJoin，執行時 shuffle 完發現某一邊其實小於廣播門檻，AQE 會當場改走 broadcast，省掉那次 shuffle。」
- 官方原文（sql-performance-tuning.html / latest，與 3.3 同）：
  > "AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side are smaller than the adaptive broadcast hash join threshold."
- 判定 ✅：這是 AQE 「執行期動態」改寫，**不是**一開始就 broadcast；章節描述方向正確。
  - 細微一點：官方比對的是「adaptive broadcast hash join threshold」（`spark.sql.adaptive.autoBroadcastJoinThreshold`），預設 fall back 到 `spark.sql.autoBroadcastJoinThreshold`(10MB)；章節以「廣播門檻」一語帶過，無誤導，屬可接受簡化。
- 出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （Adaptive Query Execution）

### §4.2 / §4.9 AQE 三件事 — ✅ 已驗證
- ① coalescing post shuffle partitions、② SMJ→BHJ、③ optimizing skew join，三者皆官方明列。
- AQE「利用 runtime statistics 選最有效率計畫」原文：
  > "Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0."
- skew join 原文補充：「by splitting (**and replicating if needed**) skewed tasks」——章節 §4.2③ 只說「切成幾小塊並行處理」，未提 replicate；屬可接受簡化（不誤導）。
- 出處：同上。

### §4.2 / §4.3 `adaptive.enabled` 預設 true「自 3.2 起」— ✅ 已驗證（但 since-version 欄易誤讀）
- 章節說「自 Spark 3.2 起預設開啟」✅ 正確。
- ⚠️ 注意：官方表 `spark.sql.adaptive.enabled` 的「Since Version」欄是 **1.6.0**（指 config 存在的版本），但**預設值變 true 是 3.2.0**。章節用詞「自 3.2 起預設 true」精準、未踩這個坑，無需改。
- `advisoryPartitionSizeInBytes`=64MB ✅；官方註明「splits skewed shuffle partition」時也用此 advisory size（與章節「只併不拆」的但書不衝突：coalesce 端只併小，skew 端才會 split，章節已在 §1.6/§4.4 區分）。
- `coalescePartitions.enabled` 預設 true、since 3.0.0 ✅；`coalescePartitions.minPartitionSize`=1MB、since 3.2.0 ✅。
- 出處：同上。

### §4.5 / §4.6 記憶體模型各預設值（configuration.html）— ✅ 多數驗證
- `spark.executor.memory`=1g ✅（since 0.7.0）。
- `spark.executor.cores`= **"1 in YARN mode, all the available cores on the worker in standalone mode"** ✅；章節 §4.6 與 §4.3 表述（YARN 預設 1、standalone 為全部 core）正確。
- `spark.executor.memoryOverhead` 預設＝`executorMemory * spark.executor.memoryOverheadFactor, with minimum of spark.executor.minMemoryOverhead`（latest 措辭）✅。
- `spark.executor.memoryOverheadFactor`=0.10（K8s 非 JVM 為 0.40）✅。
- `spark.memory.fraction`=0.6、描述含「Fraction of (heap space - 300MB)」「Leaving this at the default value is recommended」✅ 完全對得上章節 §4.5。
- `spark.memory.storageFraction`=0.5、描述「Amount of storage memory immune to eviction, expressed as a fraction of the size of the region set aside by spark.memory.fraction」「Leaving this at the default value is recommended」✅。
- 出處：https://spark.apache.org/docs/latest/configuration.html

### [版本鎖定確認] Spark 3.3 memoryOverhead 原文 — ✅（WebSearch 佐證，版本化頁 404）
- 所有 `docs/3.3.x/`、`docs/3.3.0/` 版本化頁經 WebFetch 皆回 404（fetch 基礎設施問題，非真 404；章節已自承）。改以 WebSearch 佐證：
  - Spark 3.3 `spark.executor.memoryOverhead` 預設＝「executorMemory * spark.executor.memoryOverheadFactor, **with minimum of 384**」（MB）——3.3 直接寫死 384。
  - `spark.executor.minMemoryOverhead`（把下限抽成獨立 config）在 latest 標 **Since 4.0.0**，3.3 沒有這個 config 名。
- 結論：章節「至少約 384MB」「heap × 0.10」對 3.3 數值正確；章節只寫數值、未引用 `minMemoryOverhead` config 名，OK（若 HTML 版要引 config 名，對 3.3 是 anachronism，勿出現）。
- 出處：WebSearch（多來源一致；原始權威為 Spark 3.3 configuration.html，因 fetch 404 未逐字截圖，與 latest 數值一致）。

### §4.5 統一記憶體模型方向性（特別注意點）— ✅ 方向完全正確（tuning.html）
- 章節主張：「execution 不夠就 spill」「execution 可以逼退 storage 一直到 R 為止，反過來 storage 不能逼退 execution」「沒人 cache 時 execution 可用滿整個 M」「R = M × storageFraction」。
- 官方原文（tuning.html, Memory Management Overview，latest 與 3.3 同）：
  > "execution and storage share a unified region (M). When no execution memory is used, storage can acquire all the available memory and vice versa."
  > "Execution may evict storage if necessary, but only until total storage memory usage falls under a certain threshold (R)."
  > "Storage may not evict execution due to complexities in implementation."
  > "applications that do not use caching can use the entire space for execution, obviating unnecessary disk spills."
  > execution = "computation in shuffles, joins, sorts and aggregations"；storage = "caching and propagating internal data across the cluster"。
- 判定 ✅：章節方向（execution→可逼退 storage 到 R；storage→不可逼退 execution；無 cache → execution 用滿 M）與官方逐字一致，正負號正確。
- 出處：https://spark.apache.org/docs/latest/tuning.html （Memory Management Overview）

### ⚠️ §4.5 overhead 用途措辭「給 JVM／網路／shuffle 緩衝用」— 可加強（不算錯，但不精準）
- 章節兩處（mermaid 圖與內文）說 overhead「給 JVM／網路／shuffle 等非 heap 用途」「給 JVM 自己、網路傳輸、shuffle」。
- 官方 `memoryOverhead` 描述：「VM overheads, interned strings, **other native overheads**, etc.」並含「PySpark executor memory（未設 pyspark.memory 時）與同 container 內其他非 executor process 用的記憶體」。
- 判定 ⚠️：官方沒明列「網路傳輸／shuffle 緩衝」字樣。off-heap shuffle buffer 多由 `spark.memory.offHeap`／netty 直接記憶體管，並非嚴格等於 memoryOverhead。章節把 overhead 約等於「JVM/網路/shuffle」是**常見的科普近似**，方向不錯（這些非 heap 開銷確實要靠 overhead 與系統記憶體 cover），但用詞比官方更具體、略嫌過度具體化。建議改貼近官方：「JVM 自身開銷、interned strings、其他原生（native）開銷，以及 PySpark/同 container 其他行程」。屬「可加強」級，非缺陷。
- 出處：https://spark.apache.org/docs/latest/configuration.html （spark.executor.memoryOverhead）

### §4.3 SET 語法三型 — ✅ 已驗證
- 章節：`SET key=value`（設值）、`SET key`（查值）、（隱含）`SET`（列全部）。
- 官方 sql-ref SET：`SET property_key=property_value`「Sets the value for a given property key. If an old value exists … it gets overridden」；`SET property_key`「Returns the value of specified property key」；`SET`「List all SQLConf properties …for current session」。✅ 三型描述正確。
- 出處：https://spark.apache.org/docs/latest/sql-ref-syntax-aux-conf-mgmt-set.html

### §4.3「SQL 層 SET 即時生效 / 資源層 SET 不生效」二分（特別注意點）— ⚠️ 過度簡化，有真實反例（建議補但書）
- 章節表格把旋鈕二分：`spark.sql.*`（SQL 層）→「✅ 能，下一條查詢就生效」；資源層 → 「❌ 不能」。並斷言 SQL 層那欄=「`shuffle.partitions`、`autoBroadcastJoinThreshold`、`adaptive.enabled`」。
- **反例（查證確認）**：並非所有 `spark.sql.*` 都能用 SET 即時改。Spark 區分 **runtime SQL config（per-session, mutable）** 與 **static SQL config（`StaticSQLConf`，cross-session, immutable）**；對 static SQL config 下 SET 會丟 `Cannot modify the value of a static config`。
  - static SQL config 範例：`spark.sql.warehouse.dir`、`spark.sql.catalogImplementation`、`spark.sql.extensions`、`spark.sql.queryExecutionListeners`、`spark.sql.hive.metastore.version` 等。
  - 章節點名的三個（`shuffle.partitions`、`autoBroadcastJoinThreshold`、`adaptive.enabled`）**都是 runtime SQL config，確實可即時 SET ✅**——所以章節給的具體例子沒錯，錯的是把規則寫成「凡 `spark.sql.*` 一律可即時改」這個全稱。
- 判定 ⚠️：二分作為「日常 SQL 旋鈕 vs 啟動定死的資源旋鈕」的**心法**是對的、對讀者有用；但「`spark.sql.*` → 一律 ✅ 即時改」是**全稱過度宣稱**（§12「軟建議 vs 硬限制／全稱壓力測試」）。章節 §4.3 的 📚 來源註已有 hedge（「個別 config 是否真的即時生效，以 Environment 頁籤實際值為準」），但那條 hedge 講的是「生效與否」，沒點出「有一類 `spark.sql.*`（static SQL config）SET 會直接報錯、根本改不動」這個**質**的反例。
- 建議（可加強，偏真缺陷邊緣）：在 §4.3 表格或註腳補一句——「少數 `spark.sql.*` 屬 static SQL config（如 `warehouse.dir`、`catalogImplementation`），同樣只能在 session 啟動時定、SET 會報 `Cannot modify the value of a static config`；你日常會碰的 `shuffle.partitions`／`autoBroadcastJoinThreshold`／`adaptive.*` 都是可即時改的 runtime config。」
- 出處：WebSearch 多來源一致（Databricks KB「Cannot modify the value of an Apache Spark config」、Jacek Laskowski "The Internals of Spark SQL — StaticSQLConf"、SPARK-31532）；概念在 Spark 官方 configuration.html「Spark properties …Runtime SQL configuration … static SQL configuration」一節亦有。sql-ref SET 頁本身**未**提此限制（章節若只依該頁，無從得知此反例）。

### §4.1 / §4.3「config 分啟動時定 vs 執行期可改」兩類框架 — ✅ 已驗證（官方 Spark properties 原文）
- 官方 configuration.html「Spark Properties」開頭原文：
  > "Spark properties mainly can be divided into two kinds: one is related to deploy, like \"spark.driver.memory\", \"spark.executor.instances\", this kind of properties may not be affected when setting programmatically through `SparkConf` in runtime … so it would be suggested to set through configuration file or `spark-submit` command line options; another is mainly related to Spark runtime control, like \"spark.task.maxFailures\", this kind of properties can be set in either way."
- 判定 ✅：章節 §4.1（兩類旋鈕）與 §4.3 來源註「Spark properties 分兩類」與官方逐字對得上；官方點名的 deploy 類正是 `spark.executor.instances` 等資源旋鈕，支持章節「資源層啟動時定死」。
- 補強我上一條：官方 configuration.html 目錄確有 **"Runtime SQL Configuration"** 與 **"Static SQL Configuration"** 兩節並列，印證 §4.3 的反例（static SQL config 不可 SET）是官方概念，非杜撰。
- 出處：https://spark.apache.org/docs/latest/configuration.html （Spark Properties 開頭、Runtime/Static SQL Configuration 目錄）

### §4.4 / §4.3 表 SQL 旋鈕預設值 — ✅ 全數驗證
- `spark.sql.shuffle.partitions`=200 ✅。
- `spark.sql.autoBroadcastJoinThreshold`=10485760（10MB）；「By setting this value to -1, broadcasting can be disabled」✅——章節「預設 10MB、設 -1 關閉自動廣播」正確；§4.3 Environment 示意表的 `10485760` 也對。
- `spark.sql.files.maxPartitionBytes`=134217728（128MB）；「maximum number of bytes to pack into a single partition when reading files …effective only …file-based sources such as Parquet, JSON and ORC」✅——章節「讀檔時每個 partition 多大、預設 128MB」正確。
- 出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html

### §4.7 dynamic allocation 開源預設 — ✅ 已驗證
- `spark.dynamicAllocation.enabled`=false、`executorIdleTimeout`=60s、`minExecutors`=0、`maxExecutors`=infinity、`shuffleTracking.enabled`=false ✅（全數對得上章節 §4.7 與來源註）。
- 前提（需 external shuffle service 或 shuffleTracking）✅：shuffleTracking 自 Spark 3.0 起為 external shuffle service 的替代；章節「靠 external shuffle service 或 shuffle tracking 保住 shuffle 資料」正確。
- 出處：WebSearch 多來源一致（japila-books/apache-spark-internals、DZone）；原始權威 Spark 3.3 configuration.html（fetch 截斷未逐字，數值與 user 預查一致）。

### §4.7 CDP 預設啟用 dynamic allocation（特別注意點：與開源相反）— ✅ 已驗證
- 章節主張：「開源 Spark 預設 false，但 CDP 預設是開的」。
- Cloudera CDP 原文：
  > "In Cloudera Data Platform (CDP), dynamic allocation is enabled by default."
  > "To disable dynamic allocation, set `spark.dynamicAllocation.enabled` to `false`."
- 判定 ✅：CDP 預設 true、與開源 false 相反，章節正確；關閉方式（設 false）也對。章節說「在 Cloudera Manager 設」——CDP 頁未明指 Cloudera Manager vs spark-defaults，屬無傷的操作細節。
- 出處：https://docs.cloudera.com/runtime/7.2.18/running-spark-applications/topics/spark-yarn-dynamic-allocation.html

### §4.7 streaming 應停用 dynamic allocation — ✅ 已驗證
- 章節：「別對 streaming 作業開 dynamic allocation（Cloudera 明確提醒兩者會衝突）」。
- Cloudera 專頁原文：
  > "Cloudera recommends that you disable dynamic allocation by setting `spark.dynamicAllocation.enabled` to `false` when running streaming applications."
  > 理由：「executor idle timeout 小於 batch duration → executor 反覆增減；大於 → 永不釋放。」
- 判定 ✅：Cloudera 確有此明確建議；章節「兩者會衝突」措辭與專頁的 timing 衝突理由一致，無過度宣稱。
- 出處：https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/developing-spark-applications/topics/spark-streaming-dynamic-allocation.html

### §4.6「每台約 5 core」與 Cloudera「keep cores below 5」（特別注意點）— ⚠️ 措辭偏寬，建議微調（§1.7 同源問題）
- 章節 §4.6 配「每台 5 core」，並說 §1.7 講「4～5 core 是平衡點」。
- Cloudera Tuning Resource Allocation 原文：
  > "The HDFS client has difficulty processing many concurrent threads. At most, five tasks per executor can achieve full write throughput, so keep the number of cores per executor **below that number**."
- 判定 ⚠️：官方是「最多 5 個 task 能達滿吞吐，所以把 core 數**壓在 5 以下**」——嚴格讀是「< 5」（即 ≤4 較保險，5 是上界）。章節（含 §1.7）以「4～5 core／每台 5 core」呈現，落在「at most 5」的邊界、屬業界常見近似（源自 Cloudera 經典部落格「How-to: Tune Your Apache Spark Jobs」的『~5 cores』），不算錯，但與官方「below five」字面有張力。
  - 既非真缺陷（5 是 Cloudera 自己說的 at-most 上界），但若要對齊官方字面，可在 §4.6／§1.7 把「5」說成「**至多約 5（再多 HDFS 吞吐反降）**」更貼近原文。屬「可加強」。
- 出處：https://docs.cloudera.com/runtime/7.2.18/tuning-spark/topics/spark-admin-tuning-resource-allocation.html

### §4.6 worked example 算術自洽性（特別注意點）— ✅ 自洽
- 章節：100 core ÷ 5 = 20 台；400 GB ÷ 20 = 20 GB/台；heap ≈ 20 ÷ 1.1 ≈ 18 GB；overhead ≈ 18 × 0.10 = 1.8 GB；18 + 1.8 = 19.8 ≈ 20 GB/台。
- 復算 ✓：100/5=20 ✓；400/20=20 ✓；20/1.1=18.18→「18 GB」✓（取整保守，留 0.2GB 餘裕）；18×0.10=1.8 ✓；合計 19.8≈20 ✓。算術完全自洽。
- 章節也誠實標註此為「乾淨對切示意，未含 driver／ApplicationMaster／節點其他開銷」（§4.6 ⚠️）✅。
- 注意：overhead 下限 384MB 在此不咬到（1.8GB > 384MB），所以「heap×0.10」在這個例子直接適用、無需被下限蓋過 ✅。
- 出處：算術復核；overhead 公式見 configuration.html。

### §4.5/§4.6 ↔ 第 01 章 §1.7 跨章一致性（特別注意點）— ✅ 一致（有一處框架張力，建議對齊用語）
- §1.7 表「瘦 executor = 20 台 × 5 core × **20 GB**」，§1.7 ⚠️ 註明該 80／20 GB 是「把總額度乾淨對切的示意，**未扣 overhead**（實務每台還要再扣約 10% overhead，無法整包配成 heap）」。
- §4.6 說「和 §1.7 那個 20 台×5 core×20 GB 的瘦 executor 對上了」，並把 20 GB **拆成 18g heap + 1.8g overhead**。
- 判定 ✅ 一致：§1.7 的「20 GB」是**每台總額**（=heap+overhead），§4.6 把同一個 20 GB 總額拆成 18g+1.8g，數字接得上、台數/核數一致（20 台、5 core）。
- ⚠️ 框架小張力（可加強，非缺陷）：§1.7 ⚠️ 把 20 GB 講成「未扣 overhead 的乾淨對切」，語感像「20 GB 全是 heap、之後還要再扣 overhead」；§4.6 則把 20 GB 當「heap+overhead 的總額、拆出 18g heap」。兩者數字相容，但「20 GB 是不是已含 overhead」這點兩章口徑可更一致：建議 §1.7 ⚠️ 改成「此 20 GB 是每台總額，實務還要再從中切出約 10% 當 overhead、heap 只剩約 18 GB」，與 §4.6 完全咬合。
- 出處：01-how-spark-runs-your-sql.md §1.7（本機檔案，行 199-217）；04-spark-config.md §4.5/§4.6。

### §4.5 spill 救法優先序 / execution 不夠就 spill — ✅ 方向正確
- 章節：execution 不夠→spill；救法序＝先減量(§03) > 提高平行度 > 最後加記憶體。
- 與 tuning.html「no caching → 全 M 給 execution、減少 spill」「memory.fraction 越低 spill 越頻繁」一致；減量優先是對 SQL-first 讀者正確的方向性建議（章節 §4.9 也誠實標為「方向性、非任何情況皆然」）✅。
- 出處：https://spark.apache.org/docs/latest/tuning.html

---

## 結尾彙整（三級）

### A. 真缺陷（建議必補）
- **無硬性事實錯誤**。所有 config 預設值、AQE 行為、記憶體模型方向性、dynamic allocation 開源/CDP 差異、SET 三型語法，皆查證為正確。
- 唯一**逼近**缺陷級的是 **§4.3 的「`spark.sql.*` → 一律可即時 SET」全稱**：存在真實反例（static SQL config 如 `warehouse.dir`/`catalogImplementation`，SET 會丟 `Cannot modify the value of a static config`）。章節點名的三個具體 config 都沒錯，但規則寫成全稱、且現有 hedge 只談「生效與否」未涵蓋「這類根本改不動且報錯」。**建議在 §4.3 表格或註腳補一句區分 runtime SQL config vs static SQL config**（這是 §12「軟建議/全稱壓力測試」最該補的一點）。

### B. 可加強（斟酌）
1. **§4.5 overhead 用途措辭**：「給 JVM／網路／shuffle 緩衝」比官方更具體；官方是「VM overheads, interned strings, other native overheads, PySpark/同 container 其他行程」。建議改貼官方用語（網路/shuffle buffer 非嚴格等於 memoryOverhead）。
2. **§4.6／§1.7「5 core」**：Cloudera 字面是「keep cores **below five**（at most 5 tasks 才滿吞吐）」；「每台 5 core」落在上界、業界常見近似，但可改「至多約 5」更貼原文。
3. **§4.5 ↔ §1.7 「20 GB 是否含 overhead」口徑**：兩章數字相容但語感不同步（§1.7 ⚠️ 像「20GB 全 heap」、§4.6 當「含 overhead 總額」）。建議統一成「20 GB 是每台總額，內含約 10% overhead、heap≈18 GB」。
4. **§4.5 `minMemoryOverhead` config 名（前瞻提醒）**：章節目前只寫數值 384MB（對 3.3 正確）。若日後轉 HTML 或補充，**勿引用 `spark.executor.minMemoryOverhead` 這個 config 名**——它 Since 4.0.0，3.3 沒有（3.3 是直接寫死「minimum of 384」）。

### C. 誤讀／不改或微調（其實正確、列出以免後續被誤改）
1. **§4.2② SMJ→broadcast「執行期動態改」**：✅ 官方原文「AQE converts sort-merge join to broadcast hash join when the runtime statistics …smaller than …threshold」，確是執行期改、非一開始 broadcast。維持。
2. **§4.5 execution 可逼退 storage 到 R、反之不行**：✅ 官方「Execution may evict storage …until …R」「Storage may not evict execution」逐字吻合。正負號正確，維持。
3. **§4.5「要 8g 佔約 8.8g」**：✅ 8×1.10=8.8，且 0.8GB>384MB 下限不咬到；算術示意正確（章節已標 ⚠️）。維持。
4. **§4.6 算術（20台×5core×18g+1.8g）**：✅ 自洽、且與 §1.7 接得上。維持。
5. **`adaptive.enabled` 自 3.2 起預設 true**：✅ 章節用詞精準（官方 Since 欄 1.6.0 指 config 存在，預設變 true 是 3.2，章節沒踩此坑）。維持。
6. **§4.7 CDP 預設 true / streaming 應停用**：✅ Cloudera 官方逐字確認。維持。
7. **§4.1/§4.9「改 SQL+喂統計 > 硬調 config」**：方向性建議，章節已誠實標為「非任何情況皆然」（§4.9 精確度說明 #3），符合 §12「誠實、不過度宣稱」。維持。

