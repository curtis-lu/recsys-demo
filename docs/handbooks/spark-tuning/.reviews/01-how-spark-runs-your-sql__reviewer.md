# 審查日誌：01 · Spark 怎麼跑你的 SQL

> 技術正確性審查（不評文筆）。對齊 Spark 3.3.x。逐條 append；每條附判定 + 出處。
> 判定符號：✅ 已驗證（附出處）／❌ 錯誤（給正確說法+出處）／⚠️ 無法查證。

審查員：技術審查 subagent
開始時間：2026-06-14
權威來源限：Spark 3.3 官方文件、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》、Databricks 官方文件。

---

（逐條 append 中…）

> 來源取得備註：WebFetch 對 `https://spark.apache.org/docs/3.3.2|3.3.0|3.3.1|3.3.4/...` 路徑回 404（fetcher 端問題；WebSearch 已確認 3.3.1 頁面實際存在）。可成功抓取的是 `docs/latest/`（目前對應 Spark 4.x），但本章查核的核心定義（driver/executor/task/stage/transformation/action/shuffle）在 RDD Programming Guide 與 Cluster Overview 中自 2.x 起文字穩定、3.3.x 與 latest 一致，故引用 `docs/latest/` 的逐字句仍對齊 3.3.x 行為。AQE/Catalyst 等版本敏感項另行對齊 3.3 SQL guide。

---

## §1.1 你的查詢，其實是一群機器一起做

**主張 1.1-a：「Driver…只有一個」「解析你的 SQL、安排工作、把結果收回來」**
✅ 已驗證。Spark Cluster Overview Glossary：Driver program =「The process running the main() function of the application and creating the SparkContext」；Job =「a parallel computation … spawned in response to a Spark action (e.g. save, collect)」；collect action =「Return all the elements of the dataset as an array **at the driver program**」。每個 application 一個 driver/SparkContext，"只有一個" 對單一 application 成立。解析/排程/收結果三項職責與 driver 定義相符。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary）。

**主張 1.1-b：「資料被切成很多塊、散在多台機器上，每一塊叫一個 partition」**
✅ 已驗證（概念正確）。RDD Programming Guide：「During computations, a single task will operate on a single partition」隱含資料以 partition 為單位分散。partition 為 Spark 資料切分單位是標準定義（亦見《Spark: The Definitive Guide》Ch.2「A partition is a collection of rows that sit on one physical machine in your cluster」）。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html （Shuffle operations / Background）。
⚠️ 細節提醒（非錯誤）：「散在多台機器上」對叢集成立；單機 local 模式下 partition 在同一台機器。文中語境是 CDP 叢集，無誤導。

**主張 1.1-c：「Executor…實際讀 partition、做運算的機器…有很多個，可以同時開工」**
⚠️ 用詞需注意（可加強，非硬錯）：Glossary 定義 Executor =「A **process** launched for an application on a worker node, that runs tasks and keeps data…」。executor 是「行程（process）」，不是「機器」。一台機器（worker node）上可跑多個 executor 行程。文中把 executor 說成「機器」是給 SQL-first 讀者的簡化，方向不致誤導，但嚴格說 executor≠機器（machine/node）。建議微調為「行程」或「工人（跑在機器上的行程）」。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Executor）。

---

## §1.2 Spark 不會馬上算：先攢計畫，再一次跑

**主張 1.2-a：「SELECT/WHERE/JOIN/GROUP BY 這些 Spark 只是記下來、攢成待辦計畫、先不動手 = lazy evaluation」**
✅ 已驗證。RDD Programming Guide：「All transformations in Spark are *lazy*, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset… The transformations are only computed when an action requires a result to be returned to the driver program.」《STDG》Ch.2「Lazy Evaluation」：「Lazy evaluation means that Spark will wait until the very last moment to execute the graph of computation instructions… you build up a plan of transformations…」。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html ；《Spark: The Definitive Guide》Ch.2「Lazy Evaluation」。

**主張 1.2-b：action 範例「存表/寫檔、collect、count」會觸發執行**
✅ 已驗證。RDD Programming Guide actions 表列 `collect`(Return all the elements … at the driver program)、`count`(Return the number of elements)；Cluster Overview Job 定義「spawned in response to a Spark action (e.g. `save`, `collect`)」。《STDG》Ch.2「Actions」明列三類 action：view in console、collect to native objects、write to output data sources，與文中三項對應（撈回來看 / 算 count / 存表寫檔）。「Hue 按執行去顯示資料」屬「view/collect」類，合理。
出處：同上 RDD guide + Cluster Overview（Glossary: Job）；《STDG》Ch.2「Actions」。

**主張 1.2-c：「Spark 看得到整份計畫才動手 → 有機會優化（把 WHERE 提早、砍用不到的欄位）」**
✅ 已驗證（方向正確）。《STDG》Ch.2「Lazy Evaluation」明確舉同一例：「An example of this is something called **predicate pushdown**… Spark will actually optimize this for us by pushing the filter down automatically.」「把用不到的欄位整段砍掉」= column/projection pruning，由 Catalyst 執行（見 §1.3 查核）。
出處：《Spark: The Definitive Guide》Ch.2；Databricks Catalyst blog（projection pruning / predicate pushdown）。

---

## §1.3 從 SQL 到一群 task：計畫怎麼變成實際工作

**主張 1.3-a：流程「SQL → Logical Plan → Catalyst 優化 → Physical Plan → 切 Stage → 切 Task」**
✅ 已驗證。Databricks Catalyst blog：Catalyst 四階段「analyzing a logical plan to resolve references, logical plan optimization, physical planning, and code generation」；「generates one or more physical plans, using physical operators that match the Spark execution engine」。圖中 logical→optimize→physical 順序正確。Stage/Task 切分見 1.3-c/1.3-d。
出處：https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html

**主張 1.3-b：「Catalyst 是查詢優化器，會把查詢改寫成更省的等價形式（例如自動把過濾條件下推到讀檔階段）」**
✅ 已驗證。Databricks Catalyst blog：列出 Catalyst 套用「constant folding, **predicate pushdown**, projection pruning, null propagation, Boolean expression simplification, and other rules」並能「push operations from the logical plan into data sources that support predicate or projection pushdown」。「下推到讀檔階段」＝ predicate pushdown to data source，精確。
出處：同上 Databricks Catalyst blog。

**主張 1.3-c：「Stage = 一段不需在機器間搬資料就能連續做完的工作；一旦需要搬資料就切下一個 stage」**
✅ 已驗證。《STDG》Ch.15「Stages」逐字：「Stages in Spark represent groups of tasks that can be executed together to compute the same operation on multiple machines… the engine starts new stages after operations called **shuffles**… **Spark starts a new stage after each shuffle**, and keeps track of what order the stages must run in…」。文中說法與此一致。
出處：《Spark: The Definitive Guide》Ch.15「Stages」。

**主張 1.3-d：「一個 partition 對應一個 task；100 partition 就是 100 task，由眾多 executor 平行跑」**
✅ 已驗證。《STDG》Ch.15「Tasks」逐字：「Each task corresponds to a combination of blocks of data and a set of transformations that will run on a single executor. If there is one big partition in our dataset, we will have one task. **If there are 1,000 little partitions, we will have 1,000 tasks that can be executed in parallel.** A task is just a unit of computation applied to a unit of data (the partition).」對齊官方 RDD guide「a single task will operate on a single partition」與 Cluster Overview「Task = A unit of work that will be sent to one executor」。partition↔task 一對一、task 跑在單一 executor，全部正確。
出處：《Spark: The Definitive Guide》Ch.15「Tasks」；https://spark.apache.org/docs/latest/rdd-programming-guide.html ；https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Task）。

---

## §1.4 兩種運算：窄依賴(便宜) vs 寬依賴(貴)

**主張 1.4-a：「窄依賴 = 每個 partition 自己算自己的，不用看別的 partition；例 WHERE 過濾、SELECT 取欄位、逐列計算」**
✅ 已驗證。《STDG》Ch.2 逐字：「Transformations consisting of narrow dependencies (we'll call them narrow transformations) are those for which **each input partition will contribute to only one output partition**. In the preceding code snippet, the `where` statement specifies a narrow dependency…」。WHERE(filter)/SELECT(projection)/逐列算術皆 narrow，無誤。RDD guide 亦把 `map`/`filter` 列為 transformation。
出處：《Spark: The Definitive Guide》Ch.2「Transformations / Narrow dependencies」。
⚠️ 精確度註記（可加強，非錯）：嚴格定義是「每個 input partition 只**貢獻到一個 output partition**」（one-to-one / one parent→one child），文中「自己算自己、不看別的 partition」是讀者友善的等價直覺，方向正確；但要留意 `coalesce`（不增分區）是 narrow 卻會「多個 input→一個 output」——文中沒舉 coalesce，故不構成錯誤，僅標示「narrow≠永遠一對一輸出」這個邊角，本章不必展開。

**主張 1.4-b：「寬依賴 = 要把同 key 的資料跨 partition 聚到一起；例 GROUP BY、JOIN、DISTINCT、ORDER BY」**
✅ 已驗證。《STDG》Ch.2：「A wide dependency (or wide transformation)… will have input partitions contributing to many output partitions. You will often hear this referred to as a **shuffle** whereby Spark will exchange partitions across the cluster.」官方 RDD guide「Operations which can cause a shuffle include repartition…, 'ByKey operations (except for counting) like groupByKey and reduceByKey, and join operations like cogroup and join」。ORDER BY/sort 為 wide：《STDG》Ch.2「the sort of our data is actually a wide transformation because rows will need to be compared with one another」（explain plan 顯示 Exchange rangepartitioning）。GROUP BY→aggregation 為 wide：《STDG》Ch.2「an aggregation (a wide transformation)」。DISTINCT 需去重跨分區比對，屬 shuffle 類（標準，亦與 distinct 觸發 Exchange 的物理計畫一致）。
出處：《Spark: The Definitive Guide》Ch.2；https://spark.apache.org/docs/latest/rdd-programming-guide.html （Shuffle operations）。

**主張 1.4-c：「這個把同 key 資料跨機器重新分配的動作就是 shuffle；它就是切 stage 的那一刀」**
✅ 已驗證。RDD guide：「The shuffle is Spark's mechanism for re-distributing data so that it's grouped differently across partitions. This typically involves copying data across executors and machines…」。《STDG》Ch.15：「A shuffle represents a physical repartitioning of the data… Spark starts a new stage after each shuffle」→ shuffle 確為 stage 邊界。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html ；《Spark: The Definitive Guide》Ch.15。

---

## §1.5 為什麼 shuffle 是頭號敵人

**主張 1.5-a：「Shuffle write：每個 task 按 key 分桶，寫到本機磁碟」「Shuffle read：負責某些 key 的 task 從其他機器跨網路把桶拉過來」**
✅ 已驗證。RDD guide「Background」：「results from individual map tasks are kept in memory until they can't fit. Then, these are sorted based on the target partition and **written to a single file**. On the reduce side, **tasks read the relevant sorted blocks**.」《STDG》Ch.2：「When we perform a shuffle, **Spark writes the results to disk**.」write 端落本機磁碟、read 端跨網路拉，與 map-side write / reduce-side fetch 的標準描述一致。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html （Shuffle operations / Background）；《Spark: The Definitive Guide》Ch.2。
⚠️ 精確度註記（可加強，非錯）：官方原文是「kept in memory **until they can't fit**, then … written to a single file」——也就是 map output 先在記憶體、放不下才落地（且 sort-shuffle 寫成單一檔＋index，而非「每個 key 一個獨立檔」）。文中「按 cust_id 分好桶、寫到本機磁碟」對 SQL-first 讀者是合理簡化，不誤導因果；唯「分好桶」字面易讓人以為「一桶一檔」，與 sort-based shuffle 的「單一檔內分區」實作有出入。屬可斟酌的精確度，不是硬錯。

**主張 1.5-b：「WHERE month=... 是窄依賴，每個 partition 各自篩、完全不搬資料，幾乎不花成本」**
✅ 已驗證。filter = narrow（§1.4-a 出處）；且 Catalyst 會把此 predicate 下推到 FileScan（§1.3-b），讀檔即過濾、不產生 Exchange。方向正確。
出處：《Spark: The Definitive Guide》Ch.2（narrow + predicate pushdown）。

**主張 1.5-c（因果核心）：「CPU 算數很快，但寫磁碟、過網路慢得多 → shuffle 把大量資料推去做這兩件慢事，所以最花時間/最容易出問題（記憶體不夠、資料傾斜）」**
✅ 已驗證（因果正負號正確）。RDD guide「Performance Impact」逐字：「The **Shuffle** is an expensive operation since it involves **disk I/O, data serialization, and network I/O**.」shuffle 之所以貴＝磁碟 I/O＋序列化＋網路 I/O，正是文中「落地磁碟＋過網路」的因果，方向無誤。「記憶體不夠、資料傾斜」為 shuffle 常見問題亦屬標準認知（RDD guide 提到 shuffle 可大量耗用記憶體、放不下會 spill）。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html （Shuffle operations / Performance Impact）。
⚠️ 補充（可加強，非錯）：官方把 shuffle 成本拆成三項（disk I/O、**data serialization**、network I/O）；文中只點了「磁碟＋網路」兩項，漏掉「序列化（CPU 反序列化/序列化）」。對「比 CPU 運算貴」的核心因果無影響，但若要更貼官方，序列化也是成本來源之一。此外「CPU 算數很快、寫磁碟/過網路慢得多」是常見且大致正確的量級直覺，但官方文件未給具體倍率數字；此句屬合理常識性陳述，非逐字可引，標記為「方向正確、量級未逐字背書」。

---

## §1.6 一句話帶走：優化＝少搬、少讀

**主張 1.6-a：「優化 = 讓 Spark 少搬資料（減少/減輕 shuffle）、少讀資料（只讀需要的 partition 與欄位）」**
✅ 已驗證（為合理收斂，不過度宣稱）。「少搬」對應 shuffle 是頭號成本（§1.5-c 官方背書）；「少讀」對應 predicate pushdown（少讀列）＋ projection/column pruning（少讀欄）＋ partition pruning（少讀分區），皆 Catalyst 標準優化（Databricks Catalyst blog）。此為章節主軸的軟性收斂、非硬性規則，符合寫作指引 §12「軟建議 vs 硬限制」。
出處：Databricks Catalyst blog；《Spark: The Definitive Guide》Ch.2。
⚠️ 前瞻指向（屬後續章節，非本章硬錯）：§1.6 提到「AQE 已經自動幫你處理了哪些 shuffle 問題」（指向第 04 章）。AQE 在 Spark 3.2+ 預設開啟（`spark.sql.adaptive.enabled=true`），與背景設定（3.3.x、AQE 預設開）一致；本章未對 AQE 行為做具體技術主張，故僅備註留待 04 章查核，本章無需改。
出處：Spark SQL Performance Tuning（AQE 自 3.2.0 預設開）— 待 04 章逐字查核。

---

## 補充查核：AQE 預設開（§1.6 前瞻指向）
✅ 已驗證。Spark SQL Performance Tuning：「Adaptive Query Execution (AQE)… is **enabled by default since Apache Spark 3.2.0**」，`spark.sql.adaptive.enabled` 預設 `true`。與背景（Spark 3.3.x、AQE 預設開）一致；本章對 AQE 不做具體技術主張，僅指向 04 章，無誤。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html

---

# 三級彙整

## A. 真缺陷（必補）
- **無。** 本章所有核心技術主張（driver/executor/partition/task、lazy evaluation 的 transformation vs action、logical→Catalyst→physical 流程、stage 由 shuffle 切分、partition↔task 一對一、narrow vs wide 定義與例子、shuffle write/read 機制、「shuffle 因落地磁碟+過網路而比 CPU 貴」的因果正負號）逐條都有 Spark 官方文件或《Spark: The Definitive Guide》逐字背書，無觀念或機制錯誤、無因果反向、無引用出處張冠李戴。

## B. 可加強（斟酌，不影響正確性）
1. **§1.1-c「Executor 是機器」**：官方定義 executor 是 worker node 上的「行程（process）」，一台機器可跑多個 executor。給 SQL-first 讀者把它叫「工人」可接受；若要嚴謹，建議點一句「executor 是行程、跑在機器上」。
2. **§1.5-a「按 cust_id 分好桶、寫到本機磁碟」**：官方原文是 map output「先在記憶體、放不下才落地，且 sort-based 寫成單一檔＋index」，並非「一桶一獨立檔」。現寫法為合理簡化，若要更貼實作可補「先在記憶體、滿了才 spill 落地」。
3. **§1.5-c 漏列「序列化」成本**：官方把 shuffle 貴拆成三項 disk I/O + **data serialization** + network I/O；文中只點「磁碟+網路」兩項。核心因果不受影響，補一句序列化會更完整。
4. **§1.5-c「CPU 很快、寫磁碟/過網路慢得多」的量級**：方向正確、屬常識性陳述，但官方未給具體倍率；若想加數字落地（呼應寫作指引「抽象主張要有具體數字」）需另尋可引來源，目前無官方逐字倍率。

## C. 誤讀 / 不改或微調（嚴謹但讀者語境下成立）
1. **§1.1-b「partition 散在多台機器」**：叢集成立；local 模式在同一台。本章語境是 CDP 叢集，不誤導。
2. **§1.4-a narrow「自己算自己」vs 嚴格定義「每 input partition 只貢獻一個 output partition」**：讀者友善等價直覺，方向正確。邊角（coalesce 是 narrow 卻多對一）本章未舉，不必展開。
3. **§1.4-b DISTINCT/ORDER BY 列為 wide**：正確（sort 為 wide 有 STDG 逐字；distinct 去重需跨分區比對屬 shuffle 類）。

---

# 查核出處清單（權威）
- Spark RDD Programming Guide（latest，文字自 2.x 起對 driver/task/transformation/action/shuffle 定義穩定、與 3.3.x 一致）：https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Spark Cluster Overview / Glossary（driver/executor/task/job/stage 定義）：https://spark.apache.org/docs/latest/cluster-overview.html
- Spark SQL Performance Tuning（AQE 預設開、自 3.2.0）：https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Databricks「Deep Dive into Spark SQL's Catalyst Optimizer」（Catalyst 四階段、predicate pushdown/projection pruning）：https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
- 《Spark: The Definitive Guide》(Chambers & Zaharia)：Ch.2「Transformations / Narrow & Wide dependencies / Lazy Evaluation / Actions」；Ch.15「Stages / Tasks / Pipelining」（逐字引用自書本 PDF）。

> 版本對齊備註：3.3.x 專屬 doc URL 在本次 fetcher 回 404，改引 docs/latest；上列各定義在 3.3.x 與 latest 無行為差異（核心執行模型自 2.x 穩定，AQE 預設開亦自 3.2.0 起，涵蓋 3.3.x）。如需 100% 對齊 3.3.x 逐字頁，可改抓 https://spark.apache.org/docs/3.3.1/ （WebSearch 已證該頁存在）。

---

## 第二輪：§1.3 層級 + §1.4 executor 形狀

審查時間：2026-06-15（第二輪）
範圍：本輪因新增兩節，章節編號整體下移——新 §1.3「工作分成四層」、新 §1.4「executor 該多大」為新增節；原 §1.3→現 §1.5、原 §1.4→現 §1.5（窄/寬依賴）、原 §1.5→現 §1.6（shuffle）、原 §1.6→現 §1.7（收斂）。第一輪對舊 §1.1/§1.2 及窄寬依賴/shuffle/收斂的逐字背書仍成立，本輪只查新節 + 確認改寫未引入新錯。
新增權威來源：Spark 3.3 Configuration、Cluster Overview、Tuning、RDD Programming Guide（皆 docs/latest，定義對 3.3.x 穩定）；Cloudera CDP「Tuning Resource Allocation」官方文件（runtime 7.2.10，與本手冊目標環境 CDP 7.1.9 同系）；Cloudera 官方部落格「How-to: Tune Your Apache Spark Jobs (Part 2)」。

### §1.3 工作分成四層（application / job / stage / task）

**主張 1.3(新)-a：「Application = 你這一次連上 Spark 的整個工作階段；一個 application 從頭到尾共用同一批 executor」**
✅ 已驗證。Cluster Overview Glossary：「Application = User program built on Spark. Consists of a driver program and executors on the cluster.」；「Executor = A process launched for an application on a worker node, that runs tasks and keeps data… **Each application has its own executors.**」一個 application 擁有自己一批 executor，與「共用同一批」一致。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Application / Executor）。

**主張 1.3(新)-b：「Job = 每觸發一次 action，就產生一個 job；三次 count + 一次寫表 = 四個 job」**
✅ 已驗證（主敘述正確）⚠️附 nuance。Cluster Overview Glossary：「Job = A parallel computation consisting of multiple tasks that gets spawned **in response to a Spark action** (e.g. save, collect)」。一 action → 一 job 是官方定義的標準對應，作為心智模型正確。
⚠️ nuance（非錯，簡化可接受）：某些**單一 action 實際會觸發多個 job**——最典型是讀 CSV 開 `inferSchema`/`header` 時，schema 推斷會額外掃檔，使一個 `.count()` 在 Spark UI 顯示為多個 job；其他如部分需要先取樣/邊界的操作（如 sortByKey 的 range partition 取樣）也會多開 job。文中是 Parquet/Hive 表為主的批次語境（schema 來自 metastore，不需推斷），且「一 action 一 job」是教學心智模型的正解，**此簡化可接受**；若要 hedge 可加一句「少數情況一個 action 會被拆成多個 job（例如讀 CSV 推斷 schema）」。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Job）；CSV inferSchema 額外 job 行為見 Spark CSV data source 文件 https://spark.apache.org/docs/latest/sql-data-sources-csv.html （inferSchema「requires one extra pass over the data」）。

**主張 1.3(新)-c：「Stage = job 內一段不用跨機器搬資料就能連續做完的工作；每遇一次 shuffle 就切下一個 stage」**
✅ 已驗證。Cluster Overview Glossary：「Stage = Each job gets divided into smaller sets of tasks called stages that depend on each other (similar to the map and reduce stages in MapReduce)」。「shuffle 為 stage 邊界」第一輪已有《STDG》Ch.15 逐字背書（「Spark starts a new stage after each shuffle」）。兩者合起來＝文中說法，正確。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Stage）；《Spark: The Definitive Guide》Ch.15「Stages」。

**主張 1.3(新)-d：「Task = stage 裡最小工作單位；一個 partition 對應一個 task；100 partition = 100 task，由眾多 executor 平行跑」**
✅ 已驗證。Cluster Overview Glossary：「Task = A unit of work that will be sent to one executor」。partition↔task 一對一第一輪已有《STDG》Ch.15 逐字（「If there are 1,000 little partitions, we will have 1,000 tasks that can be executed in parallel」）＋官方 RDD guide「a single task will operate on a single partition」。正確。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Glossary: Task）；《Spark: The Definitive Guide》Ch.15「Tasks」。

### §1.4 一個 executor 該多大

**主張 1.4(新)-①：「一個 core 同時跑一個 task；5 core 的 executor 一次做 5 個 task」**
✅ 已驗證。Spark Configuration：`spark.task.cpus` 預設 `1`（「Number of cores to allocate for each task」）、`spark.executor.cores`（「The number of cores to use on each executor」）。同 executor 並行 task 數 = `executor.cores / task.cpus`，預設即 = core 數。5 core → 同時 5 task，正確（前提是 `spark.task.cpus=1`，預設成立；文中為預設語境，無需點出）。
出處：https://spark.apache.org/docs/latest/configuration.html （spark.executor.cores / spark.task.cpus）。

**主張 1.4(新)-②：「同時能跑的 task 數 = executor 台數 × 每台 core 數」**
✅ 已驗證。承①，每台並行 = core 數（task.cpus=1），總並行 = 台數 × 每台 core 數。文中 10 台 × 5 core = 50 並行、200 task 約 4 個 wave，算術自洽。
出處：同①（spark.executor.cores / spark.task.cpus 推導）。

**主張 1.4(新)-③：「廣播的小表每台 executor 各複製一份（台數越多總記憶體越凶）」**
✅ 已驗證。RDD Programming Guide：「Broadcast variables allow the programmer to keep a read-only variable **cached on each machine** rather than shipping a copy of it with tasks.」廣播是「每台快取一份、而非每 task 一份」，故 executor 台數越多、總副本記憶體越多，與文中因果一致。（嚴格說是「每 executor/每 machine 一份」，文中「每台 executor 各一份」用詞精準。）
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html （Broadcast Variables）。

**主張 1.4(新)-④：「每台 executor 保留一塊固定的管理用記憶體（overhead）；台數越多被吃掉的總量越多」**
✅ 已驗證。Spark Configuration：`spark.executor.memoryOverhead` 預設 `executorMemory * spark.executor.memoryOverheadFactor`（factor 預設 `0.10`），「accounts for things like VM overheads, interned strings, other native overheads」；YARN 容器總記憶體 = overhead + executor.memory + offHeap + pyspark.memory。Cloudera CDP 文件亦逐字：overhead「is added to the executor memory to determine the full memory request to YARN for each executor」。每台都要另計一塊 overhead，台數越多總開銷越大，正確。
⚠️ 用詞精確度（非錯）：overhead 嚴格不是「固定值」而是「executor 記憶體 × factor（預設 10%）＋下限」，會隨 executor 變大而增加（官方：「tends to grow with the executor size, typically 6-10%」）；文中「固定的一塊」是對「每台都要再額外保留一份」的友善簡化，方向（台數↑→總 overhead↑）正確，不誤導。若要嚴謹可改「每台額外保留一塊（約佔該 executor 記憶體 10%）」。
出處：https://spark.apache.org/docs/latest/configuration.html （spark.executor.memoryOverhead / memoryOverheadFactor）；Cloudera CDP「Tuning Resource Allocation」https://docs.cloudera.com/runtime/7.2.10/tuning-spark/topics/spark-admin-tuning-resource-allocation.html 。

**主張 1.4(新)-⑤：「太多 core/太胖會讓 HDFS 吞吐卡住；大 heap 造成 GC 長停頓」**
✅ 已驗證（兩條皆有官方背書）。
- HDFS 吞吐：Cloudera CDP 官方文件逐字「**At most, five tasks per executor can achieve full write throughput, so keep the number of cores per executor below that number.**」根因為「The HDFS client has trouble with tons of concurrent threads」（Cloudera 部落格 Part 2）。文中「一台同時對 HDFS 開太多讀取、吞吐卡住」方向正確（官方語境強調 write throughput；對 read 並行過高同屬 HDFS client 多執行緒問題，方向一致）。
- 大 heap GC：Cloudera CDP 文件逐字「**Running executors with too much memory often results in excessive garbage-collection delays**」，並建議 executor 記憶體上限約 64 GB。Spark Tuning guide 亦指大 heap 需調 G1 region size、GC 目標是避免 full GC。文中「記憶體開很大→GC 長停頓」正確。
出處：Cloudera CDP「Tuning Resource Allocation」https://docs.cloudera.com/runtime/7.2.10/tuning-spark/topics/spark-admin-tuning-resource-allocation.html ；Cloudera 部落格「How-to: Tune Your Apache Spark Jobs (Part 2)」https://www.cloudera.com/blog/technical/how-to-tune-your-apache-spark-jobs-part-2.html ；Spark Tuning https://spark.apache.org/docs/latest/tuning.html （Garbage Collection Tuning）。

**主張 1.4(新)-⑥【務必查權威來源】：「每台 executor 抓大約 4～5 個 core」heuristic**
✅ 已驗證——**有合格權威來源**。此 heuristic 直接源於 Cloudera 官方，且本手冊目標環境正是 CDP：
- Cloudera CDP 官方文件（runtime 7.2.10「Tuning Resource Allocation」）逐字：「**At most, five tasks per executor can achieve full write throughput, so keep the number of cores per executor below that number.**」
- Cloudera 官方部落格「How-to: Tune Your Apache Spark Jobs (Part 2)」逐字：「the HDFS client has trouble with tons of concurrent threads」「at most five tasks per executor can achieve full write throughput」，並在其工作範例直接採用 `--executor-cores 5`，並指出 `--executor-cores 15` 會「lead to bad HDFS I/O throughput」。
判定：**「4～5 core」屬 Cloudera 官方背書的標準起手值（上界 5、實務取 4～5 留餘裕），非部落格臆測**。文中把它定位為「常見起手建議（heuristic）」並接「在 HDFS 吞吐與管理開銷之間取平衡」，與官方根因一致，正確。無需改成純推理+hedge。
出處：Cloudera CDP「Tuning Resource Allocation」https://docs.cloudera.com/runtime/7.2.10/tuning-spark/topics/spark-admin-tuning-resource-allocation.html ；Cloudera 部落格 Part 2 https://www.cloudera.com/blog/technical/how-to-tune-your-apache-spark-jobs-part-2.html 。

**主張 1.4(新)-⑦：工作範例算術（YARN 共 100 core / 400 GB → 胖 5台×20core×80GB、瘦 20台×5core×20GB）是否自洽**
✅ 已驗證（算術自洽）。
- 胖：5×20 = 100 core ✅、5×80 = 400 GB ✅。
- 瘦：20×5 = 100 core ✅、20×20 = 400 GB ✅。
兩極端皆等於總額度，內部一致。
⚠️ 提醒（非錯，已被同節文字涵蓋）：此處把 80 GB / 20 GB 當「每台 executor 記憶體」是把總量乾淨對切的教學示意；實務上每台還要額外扣 memoryOverhead（主張④，預設約 10%），故無法把整 400 GB 全當 heap 配下去。但本節正文已在「太瘦」代價裡明說 overhead 要另計，且本表刻意只示意兩個極端的形狀（台數×core×記憶體）以講取捨，**不構成錯誤**；對齊 Cloudera 範例（其 63G 容量配 19G executor 即為留 overhead/AM 的結果）精神一致。
出處：算術自驗；overhead 留量精神見 Cloudera 部落格 Part 2 工作範例（--executor-memory 19G 而非 21G，為留 overhead/AM）https://www.cloudera.com/blog/technical/how-to-tune-your-apache-spark-jobs-part-2.html 。

### §1.5–§1.7 改寫後快速回歸（確認未引入新錯）
✅ §1.5（窄/寬依賴，原 §1.4）：定義與例子（WHERE/SELECT narrow；GROUP BY/JOIN/DISTINCT/ORDER BY wide）第一輪逐字背書仍成立，文字未改變語意。
✅ §1.6（shuffle 頭號敵人，原 §1.5）：shuffle write→落本機磁碟、read→跨網路拉、貴在 disk/serialize/network、傾斜，皆第一輪已驗；本輪確認「它就是上一節說的切 stage 的那一刀」改成回指新 §1.5 寬依賴節，指向正確（shuffle = 寬依賴 = stage 邊界三者一致）。
✅ §1.7（收斂，原 §1.6）：主軸新增「並把有限的 executor 用在刀口上」一句，正好呼應新 §1.4，與第 04 章前瞻一致；AQE 自 3.2.0 預設開（第一輪已驗）不變。無新錯。
⚠️ §1.7 前瞻措辭（非本章硬錯）：「§1.4 那些 executor 資源該怎麼實際設」指向第 04 章，屬合理 forward-reference，待 04 章查核 dynamic allocation 等細節。

---

# 第二輪三級彙整

## A. 真缺陷（必補）
- **無。** 新增的 §1.3（四層）與 §1.4（executor 形狀）所有受查主張（含①～⑦）皆有 Spark 官方文件、Cloudera CDP 官方文件或 Cloudera 官方部落格逐字背書，無觀念/機制錯誤、無因果反向。算術（胖 5×20×80、瘦 20×5×20）自洽。

## B. 可加強（斟酌，不影響正確性）
1. **§1.3-b「一 action 一 job」的 nuance**：少數單一 action 會被拆成多個 job（最常見：讀 CSV 開 inferSchema/header 的額外掃檔；range-partition 取樣）。本手冊批次語境（Parquet/Hive、schema 來自 metastore）幾乎不觸發，作為心智模型的簡化可接受；若要 hedge 可加一句「少數情況一個 action 會被拆成多個 job（如讀 CSV 推斷 schema）」。
2. **§1.4-④ overhead「固定的一塊」用詞**：overhead 嚴格是 executor 記憶體 × factor（預設 10%）＋下限，會隨 executor 變大而增加，並非絕對固定值。「每台都要再保留一份」的方向（台數↑→總開銷↑）正確；若要嚴謹可改「每台額外保留一塊（約佔該 executor 記憶體 10%）」。
3. **§1.4-⑦ 範例記憶體未扣 overhead**：80/20 GB 是把 400 GB 乾淨對切的示意；實務每台還要扣 ~10% overhead，無法整 400 GB 全配為 heap。同節「太瘦」代價已提 overhead，故不誤導；若想更貼實務可在表下加半句「實際可配的 heap 會比這再少一塊 overhead」。

## C. 誤讀 / 不改（嚴謹但讀者語境下成立）
1. **§1.4-① 未點明 `spark.task.cpus=1` 前提**：預設即 1，「一 core 一 task」在預設語境完全成立；SQL-first 讀者語境不必展開多 cpu/task 的邊角。
2. **§1.4-⑤ HDFS 吞吐官方語境是 write throughput**：文中以「讀取」舉例，根因（HDFS client 多執行緒）同源、方向一致，不誤導；無需特別區分讀/寫。
3. **§1.4-⑥「4～5 core」**：Cloudera 官方上界為 5（取 4～5 留餘裕），定位為 heuristic 起手值正確，**不需**改成純推理+hedge。

---

# 第二輪新增查核出處清單（權威）
- Spark Cluster Overview / Glossary（Application/Job/Stage/Task/Executor 定義）：https://spark.apache.org/docs/latest/cluster-overview.html
- Spark Configuration（spark.executor.cores / spark.task.cpus / spark.executor.memoryOverhead / memoryOverheadFactor，3.3.0 起 factor 預設 0.10）：https://spark.apache.org/docs/latest/configuration.html
- Spark RDD Programming Guide（Broadcast Variables：cached on each machine rather than shipping a copy with tasks）：https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Spark Tuning（Garbage Collection Tuning：大 heap / G1 region / full GC）：https://spark.apache.org/docs/latest/tuning.html
- Cloudera CDP「Tuning Resource Allocation」（runtime 7.2.10，與目標環境 CDP 7.1.9 同系）—「at most five tasks per executor can achieve full write throughput」「too much memory → excessive garbage-collection delays」、overhead 加總入 YARN 請求：https://docs.cloudera.com/runtime/7.2.10/tuning-spark/topics/spark-admin-tuning-resource-allocation.html
- Cloudera 官方部落格「How-to: Tune Your Apache Spark Jobs (Part 2)」（HDFS client 多執行緒、--executor-cores 5、num-executors/executor-memory 工作範例、留 overhead/AM）：https://www.cloudera.com/blog/technical/how-to-tune-your-apache-spark-jobs-part-2.html
- Spark CSV data source（inferSchema 額外掃檔→多 job 佐證）：https://spark.apache.org/docs/latest/sql-data-sources-csv.html

> 版本對齊備註（第二輪）：Configuration 的 `spark.executor.memoryOverheadFactor` 自 **3.3.0** 起引入、預設 0.10（涵蓋本手冊 3.3.x），舊版（如 Cloudera 文件引的 `spark.yarn.executor.memoryOverhead = max(384, .1*executorMemory)`）為同一概念的舊鍵/舊式，數值口徑（~10%）一致。其餘 Glossary / broadcast / tuning 定義自 2.x 穩定，docs/latest 與 3.3.x 無行為差異。

---

## 第三輪：§1.2 / §1.6 新增 / §1.8 / §1.9

審查時間：2026-06-15（第三輪）
範圍：本輪查核新增/改寫段落 —— §1.2「partition 從哪來」（maxPartitionBytes、partition 數近似、太少/太多的取捨）、§1.6 新增兩段（stage barrier、shuffle 後 partition 重設成 200 + AQE coalesce + skew 例子）、§1.8 端到端範例（JOIN+GROUP BY+WHERE(partition 欄)、兩個 shuffle、多 stage、partition 裁剪、broadcast 免 shuffle、stage DAG 拆法）、§1.9 Spark vs Hive（MR 落地 HDFS、Spark DAG 記憶體 pipeline、Hive on Tez 縮小差距 + 但書）。前兩輪已驗的舊主張不重查。
新增權威來源：Spark 3.3 SQL Performance Tuning、Configuration、RDD Programming Guide（docs/latest，3.3.2 專頁本次仍 404；下方版本對齊備註說明）；Cloudera CDP「Hive on Tez introduction」（runtime 7.2.10 / CDP-PvC-Base 7.1.9，與目標環境同系）；Spark DAGScheduler / ShuffleMapStage 內部語意（The Internals of Spark Core）；Spark SQL SortMergeJoinExec 物理計畫。

### §1.2 你的資料被切成幾塊？partition 從哪來

**主張 1.2-a【關鍵事實：maxPartitionBytes 預設】：「讀檔一塊預設約 128MB，由 `spark.sql.files.maxPartitionBytes` 控制」**
✅ 已驗證——**預設值正確**。Spark SQL Performance Tuning 逐字：`spark.sql.files.maxPartitionBytes` **預設 `134217728 (128 MB)`**，「The maximum number of bytes to pack into a single partition when reading files. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC.」文中「約 128MB」與 134217728 bytes = 128 MiB 完全一致。此值在 3.3.2 與 latest 一致（WebSearch 對 3.3.2 二次確認）。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （Other Configuration Options：spark.sql.files.maxPartitionBytes，預設 134217728 (128 MB)）。

**主張 1.2-b：「讀檔 partition 數 ≈ 資料大小 ÷ maxPartitionBytes」（30GB ÷ 128MB ≈ 240）這個近似站不站得住**
✅ 算術正確、⚠️ **但近似有 nuance，文中已部分提醒、可再補一兩點**。30GB ÷ 128MB ≈ 240 算術自洽。此「÷ maxPartitionBytes」是「教學用一階近似」，實際 Spark 的 `FilePartition` 打包公式還受三件事影響：
  1. **openCostInBytes**：`spark.sql.files.openCostInBytes` 預設 **`4194304 (4 MB)`**，逐字「The estimated cost to open a file… used when putting multiple files into a partition. It is better to over-estimate…」。打包時每個檔案會額外加上 4MB「開檔成本」，所以**多個小檔**的情境下，partition 數會比「純資料大小 ÷ 128MB」**多**（每檔多算 4MB）。
  2. **檔案不可切分（non-splittable）**：gzip 壓縮的 CSV/text 不可切分，**整個檔案只能進一個 partition**，再大也不切——此時「÷ maxPartitionBytes」完全失效（單檔 > 128MB 也只有 1 個 partition、1 個 task）。Parquet 本身可切分（按 row group），故本手冊 Parquet/ORC 語境下此問題較輕，但仍值得一句提醒。
  3. **多檔 / `maxSplitBytes` 公式**：實際切分大小 = `min(maxPartitionBytes, max(openCostInBytes, totalBytes/defaultParallelism))`，再把 split 與小檔 bin-pack 成 partition。故 partition 數也受 `defaultParallelism`（≈ 總 core 數）下限影響——資料很小但 core 很多時，partition 數可能被 defaultParallelism 抬高、而非單純 ÷128MB。
  判定：文中已寫「Spark **大致**按固定大小切」「**大約** 240 個」「partition 數**不是固定的**」，方向正確、用詞夠 hedge，**不構成錯誤**。建議（可加強，非必補）：補一句「很多小檔時實際塊數會更多（每開一個檔有約 4MB 的固定成本），且 gzip 這類不可切分的檔，再大也只能算一塊」——這正是 §1.2/§05 小檔問題的伏筆，且糾正讀者「30GB 一定剛好 240 塊」的過度精確期待。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （spark.sql.files.maxPartitionBytes、spark.sql.files.openCostInBytes 預設 4194304 (4 MB)）；切分/打包與 splittable 行為見 ASF JIRA SPARK-29102（gzip 單檔不可切分）、Spark `FilePartition.maxSplitBytes` 公式（社群多方一致，原始碼層級）。

**主張 1.2-c：「太少（每塊太大）→ 平行度不足、機器閒著、單 task 資料太多記憶體不夠就 spill；極端 1 個 partition＝一個 task 扛全部」**
✅ 已驗證（方向正確）。partition↔task 一對一（前兩輪 STDG Ch.15 逐字背書）→ partition 太少＝可同時跑的 task 太少＝平行度不足，與「同時能跑 task 數 = 台數×core」（§1.7，前輪已驗）一致。單 task 資料過大→記憶體放不下→spill，與 §1.6 spill 因果同源。「1 個 partition＝1 個 task 扛全部」是 partition↔task 一對一的直接推論，正確。
出處：《Spark: The Definitive Guide》Ch.15「Tasks」（前輪逐字）；spill 因果見 RDD guide「Shuffle operations / Performance Impact」（前輪）。

**主張 1.2-d：「太多（每塊太小）→ 每 task 固定啟動/排程開銷；幾萬個幾 KB 的 task 光開銷就拖垮、寫出產生一堆小檔」**
✅ 已驗證（方向正確）。Cluster Overview：Task = sent to one executor，每個 task 有調度成本（driver 發送、序列化 task、結果回收）為標準認知。Spark Tuning guide 亦把「task 太小、調度開銷佔比過高」列為反模式（建議每 core 2-3 個 task 為宜的脈絡）。「寫出產生小檔」＝ output partition 數 ≈ 寫出檔數，partition 過多→小檔，與 §05 小檔問題一致。方向無誤。
出處：https://spark.apache.org/docs/latest/cluster-overview.html （Task）；https://spark.apache.org/docs/latest/tuning.html （Level of Parallelism / 排程開銷脈絡）。
⚠️ 量級註記（非錯）：「幾萬個幾 KB 的 task」「光開銷就拖垮」屬合理常識性誇示，官方未給逐字倍率；方向正確、不需改。

### §1.6 新增段：stage barrier + shuffle 後 partition 重設

**主張 1.6-barrier【關鍵事實：stage barrier】：「一個 stage 的所有 task 沒全部跑完，下一個 stage 一個都不能開始——因為 shuffle read 必須等所有 shuffle write 都寫好才能拉」**
✅ 已驗證（shuffle 依賴下正確）。Spark 排程語意：上游是 **ShuffleMapStage**，下游（reduce/result stage）必須等 ShuffleMapStage 的**所有 map output 都可用（all partitions have shuffle map outputs available）**才被視為 ready、下游才能開始 fetch。這正是 shuffle 邊界的天然 barrier：「all map tasks in a ShuffleMapStage must complete before any downstream reduce stages can begin execution.」文中因果（read 要等所有 write 寫好）精確。
  範圍提醒（非錯，文中語境已成立）：嚴格說 barrier 是「**有 shuffle 依賴的** stage 之間」才有——文中此段在「shuffle 為什麼是頭號敵人」脈絡下講，討論對象正是 shuffle 切出來的 stage，**語境正確**。窄依賴在同一 stage 內 pipeline、不存在跨 stage 等待，文中也未宣稱「任意兩 stage 都要等齊」，無過度宣稱。
出處：Spark DAGScheduler / ShuffleMapStage 執行語意（The Internals of Spark Core：「When all partitions have shuffle map outputs available, ShuffleMapStage is considered ready/available」；下游 stage 須待其 ready）：https://books.japila.pl/apache-spark-internals/scheduler/ShuffleMapStage/ ；shuffle map-write / reduce-read 機制 RDD guide「Shuffle operations / Background」（前輪逐字）。

**主張 1.6-skew：skew 例子「某超級大戶或 NULL key 集中到少數 task → 199 個 3 秒做完、剩 1 個肥 task 跑 5 分鐘、其他乾等」是否合理**
✅ 已驗證（合理、為標準 skew 病徵）。shuffle 按 key 的 hash 分配到 reduce partition；單一 key（大戶 / `NULL` 被當成一個 key 集中）資料量遠大於其他 key 時，承接該 key 的 task 變肥，stage 卡在最慢 task = stage barrier 的直接後果（與上一條 barrier 一致）。「NULL key 集中」是 join/group by skew 的經典來源（NULL 全 hash 到同一 partition）。「199 個快、1 個慢」呼應 §1.6 的「shuffle 後 200 partition」，數字內部自洽。Spark 3.3 AQE 的 skew join 處理（`spark.sql.adaptive.skewJoin.enabled`，預設 true）正是為此而設——文中把細節留待 03 章，本章只描述病徵，定位正確。
出處：skew 病徵與 AQE skew 處理 https://spark.apache.org/docs/latest/sql-performance-tuning.html （Optimizing Skew Join）；NULL/單一 key 集中為 shuffle hash 分配的直接後果（RDD guide「Shuffle operations」按 key 重分配，前輪）。

**主張 1.6-200【關鍵事實：shuffle.partitions 預設 200】：「shuffle 輸出 partition 數不延續輸入，被重設成固定值 `spark.sql.shuffle.partitions`，預設 200」**
✅ 已驗證——**預設值與「與輸入無關」皆正確**。Spark SQL Performance Tuning 逐字：`spark.sql.shuffle.partitions` **預設 `200`**，「Configures the number of partitions to use when shuffling data for joins or aggregations.」此值是 shuffle/join/aggregation 的輸出 partition 數，由 config 決定、**不延續輸入 partition 數**（輸入 240 → shuffle 後仍 200），與文中「重設、與輸入無關」一致。3.3.2 與 latest 同值（WebSearch 二次確認）。物理計畫佐證：SortMergeJoin 兩側 `Exchange hashpartitioning(key, 200)`，200 即此 config。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （spark.sql.shuffle.partitions，預設 200）；物理計畫 Exchange hashpartitioning(…, 200) 見 SortMergeJoinExec（The Internals of Spark SQL）。

**主張 1.6-aqe：「Spark 3.3 的 AQE 會在執行時自動把過多的小 partition 合併（coalesce）」**
✅ 已驗證——**正確、且預設開**。Spark SQL Performance Tuning「Coalescing Post Shuffle Partitions」逐字：「This feature coalesces the post shuffle partitions based on the map output statistics when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` configurations are true… You do not need to set a proper shuffle partition number to fit your dataset. Spark can pick the proper shuffle partition number at runtime…」。`spark.sql.adaptive.coalescePartitions.enabled` **預設 `true`**、`spark.sql.adaptive.enabled` 自 3.2.0 預設 true（前輪已驗）→ 3.3.x「AQE 自動合併過多小 partition」預設生效，與文中「好消息是…AQE 會自動合併、你多半不必手動煩惱」一致。
  ⚠️ 精確度提醒（非錯）：AQE coalesce 只**合併（減少）**過多的小 partition，**不會把過大的 partition 拆細**（拆大 partition 是 skew join 那條路徑做的、且僅限 join skew）。文中「對好幾百 GB 的大結果，200 塊＝每塊太大、狂 spill」這個「太大」的情境，**AQE coalesce 並不能救**（它只往下合併、不往上拆）。文中緊接著只說 coalesce 解「太多小 partition」、未宣稱它解「太大」，故**不構成錯誤**；但讀者可能誤以為「AQE 兩頭都顧」。建議（可加強，非必補）：點一句「AQE 自動合併處理的是『太多小塊』那頭；結果太大要靠調高 shuffle.partitions 或 AQE 的 skew 處理（03/04 章）」。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （Coalescing Post Shuffle Partitions：coalescePartitions.enabled 預設 true；只 coalesce、不 split）。

### §1.8 端到端範例（JOIN + GROUP BY + WHERE(partition 欄)）

**主張 1.8-a：「WHERE t.month='2026-05' 是窄依賴、跟讀檔一起做、且因為是 partition 欄位根本不會去讀其他月份（partition 裁剪）」**
✅ 已驗證。filter = narrow + predicate pushdown（前輪逐字）。「partition 欄位的 WHERE → 不讀其他月份」= **partition pruning（分區裁剪）**，Catalyst 標準優化：當過濾條件打在表的分區欄（此處 `month`）時，Spark 在 file listing 階段就排除不符的分區目錄、根本不掃。文中定位為「第 03、05 章的 partition 裁剪」，前瞻指向正確。
出處：partition pruning 為 Catalyst/FileScan 標準行為，見 Spark SQL（Partition Discovery / 分區裁剪）https://spark.apache.org/docs/latest/sql-data-sources-parquet.html （Partition Discovery）；predicate pushdown 前輪 Databricks Catalyst blog。
⚠️ 前提註記（非錯）：partition pruning 成立的前提是 `month` 確為**表的實體分區欄**（Hive 分區 / Parquet 目錄分區）。文中範例語境（card_txn 按 month 分區的帳務表）此前提成立；若 month 只是普通欄位則只有 predicate pushdown（少讀列）、無「不讀其他月目錄」。文中以分區欄立論，合理。

**主張 1.8-b：「JOIN 和 GROUP BY 各是一次 shuffle → 這條查詢有兩個 shuffle、被切成多個 stage」**
✅ 已驗證。JOIN（sort-merge / shuffle hash）與 GROUP BY（aggregation）各為 wide dependency＝各一次 shuffle（前輪逐字），shuffle 為 stage 邊界（前輪逐字）→ 兩個 shuffle 把 job 切成多個 stage。文中「兩個 shuffle、被切成多個 stage」正確。
出處：JOIN/GROUP BY 為 wide＋shuffle 切 stage，前輪《STDG》Ch.2 / Ch.15 逐字；SortMergeJoin 兩側 Exchange 見 SortMergeJoinExec。

**主張 1.8-c：stage DAG 圖的拆法（大表 join：S1 讀 card_txn→過濾→按 cust_id shuffle write；S2 讀 dim_customer→按 cust_id shuffle write；S3 兩邊 shuffle read 對齊→JOIN→按 segment shuffle write；S4 按 segment shuffle read→SUM→寫出）是否合理**
✅ 已驗證（拆法正確，對齊 sort-merge join 物理計畫）。`dim_customer` 為大表時走 **sort-merge join**：物理計畫兩側各一個 `Exchange hashpartitioning(cust_id, 200)`（＝兩個獨立的 shuffle write stage，對應圖中 S1、S2），下游 stage 把兩邊按 cust_id 對齊（shuffle read）後 SortMergeJoin（S3）。GROUP BY segment 再觸發一次 `Exchange hashpartitioning(segment)`（S3 尾的 shuffle write → S4 的 shuffle read + SUM）。圖中「S1、S2 各自 shuffle write → S3 read 對齊 join → S3 write → S4 read 聚合」與真實物理計畫的 stage 邊界一致。**S1、S2 可並行**（彼此無依賴，皆為 S3 的上游），圖用 `S1→S3`、`S2→S3` 兩條入邊表達，正確。
出處：SortMergeJoinExec 物理計畫「兩側各 Exchange hashpartitioning + Sort，再 SortMergeJoin」https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-SortMergeJoinExec.html ；shuffle 切 stage 前輪《STDG》Ch.15。
⚠️ 細節（非錯，AQE 開時的小出入）：sort-merge join 在 Exchange 之後、join 之前各側還有一個 **Sort** 算子（圖中併入「JOIN」這一格未單獨畫，對 SQL-first 讀者合理省略）；又 §1.8 范例 `dim_customer` 設為大表才有此「兩邊都 shuffle」的圖，若實務上 dim_customer 不大、AQE 可能在 runtime 轉成 broadcast（即 1.8-d 講的情形），圖會塌成更少 stage——文中下一段正是用「換成小表 broadcast」對照，銜接正確、無矛盾。

**主張 1.8-d【關鍵事實：broadcast 免 shuffle】：「若 dim_customer 很小，Spark 可把它廣播到每台 executor，JOIN 就地完成、完全不用為 join 做 shuffle → 少掉一整個 shuffle、少切好幾個 stage」**
✅ 已驗證——**broadcast join 免去 join 的 shuffle 正確**。Spark SQL Performance Tuning：`spark.sql.autoBroadcastJoinThreshold`「Configures the maximum size in bytes for a table that will be **broadcast to all worker nodes when performing a join**.」BROADCAST hint：以 t1 為 build side 做 broadcast hash join。AQE 文件亦逐字佐證 broadcast 省 shuffle：把 sort-merge 轉 broadcast hash join 可「**avoid sorting both join sides and read shuffle files locally to save network traffic**」。broadcast hash join 把小表送到各 executor、與大表的本地分區就地 join，大表**不需按 join key shuffle**→少掉一整個（其實是兩邊各一個）shuffle、stage 數下降。文中「完全不用為 join 做 shuffle」精確（注意是免去 *join 的* shuffle；後續 GROUP BY 的 shuffle 仍在——文中只說「少掉一整個 shuffle」對應 join 那個，未宣稱全程零 shuffle，無誤）。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html （autoBroadcastJoinThreshold「broadcast to all worker nodes when performing a join」；BROADCAST hint；AQE「avoid sorting both join sides」）；定位第 03 章 broadcast join，前瞻正確。

**主張 1.8-e：「每個 stage 的 task 數＝它的 partition 數：讀 card_txn 約 240 個；shuffle 之後的 stage 預設 200 個」**
✅ 已驗證（自洽）。讀檔 stage task 數 = 讀檔 partition 數 ≈ 240（承 1.2-b，含其 nuance）；shuffle 後 stage = `spark.sql.shuffle.partitions` 預設 200（承 1.6-200）。partition↔task 一對一（前輪）。內部一致。
出處：承 1.2-b（maxPartitionBytes）、1.6-200（shuffle.partitions）。
⚠️ 一致性提醒（非錯）：此處「shuffle 後 200」未提 AQE coalesce 會在 runtime 把 200 往下合併——但 §1.6 已明說 AQE 會合併、且 §1.8 是「預設規劃階段」的靜態視角（計畫 plan 的 partition 數 = 200，AQE 是 runtime 調整），兩節並不矛盾。SQL-first 讀者用「預設 200」建立心智模型即可，AQE 細節留 04 章，定位正確。

### §1.9 為什麼 Spark 通常比老 Hive（MapReduce）快

**主張 1.9-a【關鍵事實：MR 落地 HDFS】：「老式 Hive 跑在 MapReduce 上時，每個步驟（map、reduce）把整批中間結果寫回 HDFS、下一步再讀回；多步驟查詢＝串起多個 MR job，每個都落地一次磁碟」**
✅ 已驗證（方向正確）。MapReduce 模型在 job 之間、以及多 MR step 的查詢中，**依賴 HDFS 存放每一步輸出**：「MapReduce-based query engines like Hive materialize intermediate data to disk… when queries need multiple MapReduce steps… rely on HDFS to store the output of each step.」一條複雜 Hive 查詢被編譯成串接的多個 MR job，每個 job 的輸出落 HDFS 供下一個讀。文中描述與此一致。
出處：MR 多 step 之間以 HDFS 落地中間結果為 MapReduce 標準行為（Cloudera/業界一致；亦見 Tez 設計動機文件，下條）。
⚠️ 精確度註記（非錯，常見簡化）：嚴格說 MR **單一 job 內** map→reduce 之間的 shuffle 是寫 **mapper 本地磁碟**（不是 HDFS）；落 **HDFS** 的是「**job 與 job 之間**的最終/中間輸出」。文中「每一個步驟（map、reduce）都會把整批中間結果寫回 HDFS」字面把「map↔reduce 的本地 spill」與「job↔job 的 HDFS 落地」混為一談，略有過度——但 Spark 比 MR 快的**主因確實是「多 job/多 step 之間省去 HDFS 來回」**，文中結論（少掉「寫 HDFS→讀 HDFS」來回）正確。建議（可加強，非必補）：把「每個 map、reduce 步驟都寫 HDFS」收斂為「每個 MapReduce job 之間把結果落地 HDFS」更精準（單 job 內 shuffle 是本地磁碟，這點與 Spark shuffle 落本地磁碟其實同類）。

**主張 1.9-b：「Spark 把整條查詢規劃成一張 DAG，一個 stage 內把多個窄依賴運算在記憶體裡串著做、中間不落地，只有遇到 shuffle 才寫磁碟 → 少掉大量寫/讀 HDFS 來回，是更快主因」**
✅ 已驗證。stage 內窄依賴 pipeline（不落地）為標準執行模型（《STDG》Ch.15「Pipelining」：narrow transformations 在一個 stage 內 pipeline、不寫中間結果）；只有 shuffle 才落（本機）磁碟（前輪 RDD guide / STDG 逐字）。「比 MR 快主因＝省去步驟間 HDFS 來回」與 1.9-a 出處的「avoid writing intermediate data to HDFS / pipeline consecutive steps」一致。正確。
出處：《Spark: The Definitive Guide》Ch.15「Pipelining」（前輪）；shuffle 才落磁碟，前輪 RDD guide「Shuffle operations / Performance Impact」。

**主張 1.9-c：「別過度宣稱：Spark 不是永遠較快；對非常單純的大批次掃描差距有限；現在 Hive 多半跑在 Tez 上（不是 MapReduce），Tez 也用 DAG、把差距縮小了不少」（但書）**
✅ 已驗證——**但書站得住、且對 CDP 7.1.9 尤其正確**。
  - 「Hive 多半跑在 Tez 上」：Cloudera CDP **Tez 是 Hive 的預設（且唯一）執行引擎，MapReduce 已被取代**——CDP 文件逐字「Tez is the default execution engine for Hive in CDP, as the MapReduce execution engine has been replaced by Tez」「MapReduce is not supported, and if a legacy script… specifies MapReduce… an exception occurs」。本手冊目標環境正是 CDP 7.1.9（Hive 3.1.x on Tez），故「現在 Hive 多半跑在 Tez」對讀者環境**不只成立、是強成立**（根本不能用 MR）。
  - 「Tez 也用 DAG、縮小差距」：Tez 把計算建成 DAG、**中間結果儘量留記憶體、reducer 輸出可直接 pipe 進下一個 mapper 而不經 HDFS**——「Tez keeps the intermediate results in memory rather than keeping it in disk」「pipe the output of a reducer into the input of a subsequent mapper without any intervening HDFS activity」。正是「用 DAG 縮小與 Spark 的差距」，文中但書精確、無過度宣稱（反而沒過度褒 Spark）。
  - 「對單純大批次掃描差距有限」：合理常識性陳述（純掃描無多步 shuffle 時，引擎間差距本就小），方向正確；官方未給逐字倍率，標記為合理陳述。定位「哪個引擎適合哪種工作，第 06 章」前瞻正確。
出處：Cloudera CDP「Hive on Tez introduction」（runtime 7.2.10 / CDP-PvC-Base 7.1.9，Tez 為預設、MR 不支援）https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/managing-hive/topics/hive_query_progress.html 及 https://docs-archive.cloudera.com/runtime/7.2.10/hive-introduction/topics/hive-on-tez.html ；Tez 記憶體 pipeline / 不經 HDFS（Tez 設計動機，業界與 Hortonworks/Cloudera 一致）。

---

# 第三輪三級彙整

## A. 真缺陷（必補）
- **無。** 本輪所有受查主張的關鍵事實都正確：**maxPartitionBytes 預設 128MB（134217728）✅、shuffle.partitions 預設 200 ✅、stage barrier（shuffle 依賴下下游須等上游所有 map output 寫好）✅、broadcast join 免去 join 的 shuffle ✅、MR 步驟間落地 HDFS ✅、CDP Hive on Tez ✅**。§1.8 stage DAG 拆法對齊 sort-merge join 真實物理計畫（兩側各 Exchange、再對齊 join、再按 segment 聚合）。§1.9 但書（Tez 縮小差距）不僅成立、對 CDP 7.1.9 是強成立。無觀念/機制錯誤、無因果反向、無預設值錯。

## B. 可加強（斟酌，不影響正確性）
1. **§1.2-b partition 近似的 nuance**：文中「30GB÷128MB≈240」已用「大致/大約」hedge，方向對。可補一句涵蓋兩個真實偏差源：①**很多小檔時實際更多塊**（每開一檔約 +4MB openCostInBytes 成本）；②**gzip 等不可切分檔，再大也只算一塊**（Parquet 可切，故本語境較輕但值得提）。這同時是 §05 小檔問題的伏筆。
2. **§1.6-aqe coalesce 只往下合併、不往上拆**：文中說 AQE「自動合併過多小 partition」正確，但同段「結果太大→每塊太大狂 spill」這頭 **coalesce 救不了**（只合併小塊、不拆大塊；拆是 skew-join 路徑、且僅限 join）。文中未宣稱 AQE 兩頭都顧，不構成錯誤；可點一句「coalesce 處理『太多小塊』那頭；『結果太大』要靠調高 shuffle.partitions 或 03/04 章的 skew 處理」避免讀者誤會。
3. **§1.9-a「每個 map、reduce 步驟都寫 HDFS」字面略過度**：嚴格說 MR **單一 job 內** map↔reduce 的 shuffle 寫的是 **mapper 本地磁碟**（與 Spark shuffle 同類），落 **HDFS** 的是 **job↔job 之間**的輸出。Spark 比 MR 快的主因（省 job 間 HDFS 來回）文中結論正確；若要精準可改「每個 MapReduce job 之間把結果落地 HDFS」。

## C. 誤讀 / 不改（嚴謹但讀者語境下成立）
1. **§1.6-barrier 範圍**：barrier 是「**有 shuffle 依賴的** stage 之間」才有；文中此段全在 shuffle 脈絡下講、討論對象就是 shuffle 切出的 stage，未宣稱「任意兩 stage 都等齊」，語境正確、不改。
2. **§1.8-a partition pruning 前提**：成立前提是 month 為表的實體分區欄；範例語境（按 month 分區的帳務表）已滿足，不改。
3. **§1.8-c Sort 算子省略 / dim_customer 設為大表**：sort-merge join 在 Exchange 後有 Sort 算子，圖中併入「JOIN」格、對 SQL-first 讀者合理省略；「dim_customer 是大表」是為了示範兩邊都 shuffle 的最壞情形，緊接的小表 broadcast 段做對照，銜接正確、不改。
4. **§1.8-e「shuffle 後 200」未提 AQE coalesce**：§1.8 是規劃階段靜態視角（plan 的 partition=200），AQE 是 runtime 調整、§1.6 已交代，兩節不矛盾，不改。
5. **§1.9-c「單純掃描差距有限」量級**：合理常識性陳述，官方無逐字倍率；方向對、不改。

---

# 第三輪新增查核出處清單（權威）
- Spark SQL Performance Tuning（**maxPartitionBytes 預設 134217728/128MB、openCostInBytes 預設 4194304/4MB、shuffle.partitions 預設 200、AQE Coalescing Post Shuffle Partitions / coalescePartitions.enabled 預設 true、autoBroadcastJoinThreshold「broadcast to all worker nodes」、BROADCAST hint、AQE broadcast「avoid sorting both join sides」、Optimizing Skew Join**）：https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Spark Cluster Overview / Tuning（Task 調度成本、Level of Parallelism）：https://spark.apache.org/docs/latest/cluster-overview.html ；https://spark.apache.org/docs/latest/tuning.html
- Spark DAGScheduler / ShuffleMapStage 執行語意（**stage barrier**：下游須待上游所有 map output 可用）：https://books.japila.pl/apache-spark-internals/scheduler/ShuffleMapStage/
- Spark SQL SortMergeJoinExec 物理計畫（**§1.8 DAG 拆法**：兩側各 Exchange hashpartitioning + Sort，再 SortMergeJoin）：https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-SortMergeJoinExec.html
- Spark Parquet data source（**partition pruning / Partition Discovery**）：https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
- ASF JIRA SPARK-29102（**gzip 單檔不可切分**，partition 近似失效情境）：https://issues.apache.org/jira/browse/SPARK-29102
- Cloudera CDP「Hive on Tez introduction」/「Tracking Hive on Tez query execution」（**CDP 7.1.9 Tez 為 Hive 預設且唯一引擎、MR 不支援**）：https://docs-archive.cloudera.com/runtime/7.2.10/hive-introduction/topics/hive-on-tez.html ；https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/managing-hive/topics/hive_query_progress.html

> 版本對齊備註（第三輪）：3.3.2 專屬 doc URL（sql-performance-tuning / configuration）本次 WebFetch 仍回 404（前兩輪同症，fetcher 端問題）；改引 docs/latest 並以 WebSearch 對 3.3.2 二次確認三個關鍵預設值（maxPartitionBytes=134217728、openCostInBytes=4194304、shuffle.partitions=200）皆與 latest 一致——這些值自 2.x/3.0 起穩定、3.3.x 無變動。AQE coalescePartitions.enabled 預設 true、adaptive.enabled 自 3.2.0 預設 true（前輪已驗）涵蓋 3.3.x。Cloudera 引 runtime 7.2.10 與目標 CDP-PvC-Base 7.1.9 同系、Hive-on-Tez 行為一致（兩版皆 Tez 為預設、MR 不支援）。
