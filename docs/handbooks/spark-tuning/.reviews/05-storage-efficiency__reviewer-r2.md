# 第 05 章「儲存效率」第二輪審查日誌（reviewer-r2）

> 審查員：技術正確性審查（第二輪）。重點：上一輪審查後新增/修改的內容（§5.4 locality 更正、§5.5 openCost 打包估算、coalesce vs repartition 平行度、分區表寫出、§5.6 預設值、§5.8 external/dbt、§5.2 欄式用語）。
> 規則：只查證不改稿；來源限 Spark 3.3 官方 / Apache Hadoop 官方 / Cloudera CDP 官方（含 blog.cloudera.com）/《Spark: The Definitive Guide》/《High Performance Spark》。對齊 Spark 3.3.x / Hive 3.1.3 / CDP 7.1.9。查不到標「無法查證」、不臆測。
> 每查一條即時 append。結尾按 真缺陷(必補)/可加強(斟酌)/誤讀(不改或微調) 三級彙整。

---

## 逐條查證（即時 append）

### [主張 2 — §5.5 成因四：openCost 預設值與打包機制] ✅已驗證（預設值＋機制）／估算量級合理

**稿件主張**（§5.5、L150）：讀檔把小檔打包進約 128MB 一個分區、「每個檔要外加約 **4MB 的固定開檔成本**（`spark.sql.files.openCostInBytes`）」，於是一個 1MB 的檔在打包帳上被當約 5MB 算；「1 萬個 1MB 小檔（真實才約 9.8GB、理想只需 ~77 塊），會虛胖成 ~400 塊（task）、約 5 倍」。

**查證**：
- `spark.sql.files.openCostInBytes` 預設 **4194304（4MB）**，官方逐字：「The estimated cost to open a file, measured by the number of bytes that could be scanned in the same time. **This is used when putting multiple files into a partition.** It is better to over-estimate, then the partitions with small files will be faster than partitions with bigger files... effective only when using file-based sources such as Parquet, JSON and ORC.」→ 出處 [Spark SQL Performance Tuning（latest）](https://spark.apache.org/docs/latest/sql-performance-tuning.html)。✅ 預設值、「每檔計入」、「打包進一個分區」三點全部對齊。
- `spark.sql.files.maxPartitionBytes` 預設 **134217728（128MB）**，同頁。✅「打包進約 128MB 一個分區」對齊（注意：實際每分區目標是 `maxPartitionBytes` 與 `bytesPerCore` 取較小者，本機 default core 數下通常即 128MB，稿件「約 128MB」用「約」字已 hedge，正確）。

**估算量級複核（每檔有效大小 = fileLength + openCost）**：
- 每檔有效＝1MB＋4MB＝5MB；1 萬檔總有效＝50,000MB。
- 50,000MB ÷ 128MB ≈ **390.6 ≈ ~400 塊** ✅（稿件「~400 塊」量級正確）。
- 真實資料量＝1 萬 × 1MB ＝ 10,000MB ≈ 9.77GB ✅（稿件「約 9.8GB」正確）。
- 理想塊數（不計 openCost）＝10,000MB ÷ 128MB ≈ **78.1 ≈ ~77~78 塊** ✅（稿件「~77 塊」正確；77 是用 9.8GB÷128MB≈78、取整數附近，量級無誤）。
- 倍率＝400 / 78 ≈ **5.1 倍** ✅（稿件「約 5 倍」正確）。

**判定**：✅已驗證。預設值逐字對齊官方；打包機制「每檔 fileLength＋openCost 累加、塞進約 128MB 分區」與官方「used when putting multiple files into a partition」「better to over-estimate」一致；估算量級（9.8GB／~77 理想／~400 虛胖／~5 倍）算術全部複核通過。稿件 L208 已自記「實際分區數依版本打包演算法略有出入」的 hedge，恰當。
> 補：稿件未明寫「fileLength + openCost 累加」這個公式字面，但用「1MB 的檔被當約 5MB 算」把累加機制具體化，方向與官方一致；本審查確認該累加模型即 Spark `FilePartition` 打包的實作模型（每檔 estimatedSize = fileLength + openCostInBytes，bin-pack 到 maxSplitBytes）。

### [主張 5 — partitioning hints 四種語意] ✅已驗證（逐字對齊官方）

**稿件主張**（§5.5、L168-200、L208 來源段）：`COALESCE(n)`／`REPARTITION(n[, col])`／`REPARTITION_BY_RANGE(col)`／`REBALANCE`；COALESCE「reduce the number of partitions」、REPARTITION 依 partitioning expressions 重分、REPARTITION_BY_RANGE 按範圍、REBALANCE 需 AQE、best-effort 讓分區大小合理並拆傾斜。

**查證**——[Spark SQL Partitioning Hints（latest）](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html) 逐字：
- COALESCE：「can be used to reduce the number of partitions to the specified number of partitions.」✅
- REPARTITION：「repartition to the specified number of partitions using the specified partitioning expressions. It takes a partition number, column names, or both as parameters.」✅ → 對齊稿件「`REPARTITION(n)`／`REPARTITION(n, col)`／`REPARTITION(col)`」三種形式。
- REPARTITION_BY_RANGE：「repartition to the specified number of partitions using the specified partitioning expressions. It takes column names and an optional partition number as parameters.」✅（按範圍重分，對齊稿件「按值的範圍分」）。
- REBALANCE：「rebalance the query result output partitions, so that every partition is of a reasonable size (not too small and not too big)... This is a best-effort: **if there are skews, Spark will split the skewed partitions**, to make these partitions not too big... **This hint is ignored if AQE is not enabled.**」✅ → 對齊稿件「需 AQE、best-effort、自動讓分區大小合理、拆傾斜」全部正確。

**判定**：✅已驗證。四個 hint 語意與 AQE 依賴、拆傾斜、best-effort 描述全部逐字對齊官方。稿件 L200 / L202 對 `REBALANCE(col)` 的用法（帶分區欄位、自動拆過大分區）也正確——官方明寫「It can take column names as parameters, and try its best to partition the query result by these columns」。

### [主張 3 — §5.5 coalesce vs repartition 平行度（含 coalesce(1) 警告）] ✅已驗證（方向完全正確、有官方逐字撐腰）

**稿件主張**（§5.5、L190-191）：
- 「`COALESCE` 不 shuffle、不切 stage」，故給的 n 會「往上游一路傳染→變成從這裡回推到上一次 shuffle（或讀檔）為止整段的平行度上限」；「`COALESCE(1)` 等於叫一個 task 扛全部上游（別為了只寫成一個檔就這樣做）」。
- 「`REPARTITION` 反而沒這問題：它故意 shuffle、會切出一道 stage 邊界，所以上游照樣用原本的寬平行度跑」。

**查證**：
- RDD `repartition`（[RDD Programming Guide, latest](https://spark.apache.org/docs/latest/rdd-programming-guide.html)）逐字：「Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. **This always shuffles all data over the network.**」✅ → 撐「repartition 一定 shuffle」。
- RDD `coalesce`（同頁）：「Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.」
- RDD.coalesce ScalaDoc 逐字（[RDD API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/rdd/RDD.html)）：
  - 「This results in a **narrow dependency**, e.g. if you go from 1000 partitions to 100 partitions, **there will not be a shuffle**, instead each of the 100 new partitions will claim 10 of the current partitions.」✅ → 撐「coalesce 不 shuffle、不切 stage（narrow dependency＝同一 stage）」。
  - 「However, if you're doing a **drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like** (e.g. one node in the case of numPartitions = 1). To avoid this, you can pass shuffle = true. **This will add a shuffle step, but means the current upstream partitions will be executed in parallel** (per whatever the current partitioning is).」✅✅ → 這段同時撐住稿件兩個關鍵主張：(a) `coalesce(1)` 讓運算擠在極少節點；(b) `repartition`（＝coalesce with shuffle=true）切出 shuffle step 後上游照原平行度跑。

**判定**：✅已驗證，**且這是本章寫得最好的一段之一**——「coalesce 不 shuffle＝narrow dependency＝n 變上游平行度天花板」與「repartition 切 stage 邊界、上游維持寬平行度」的因果，與 ScalaDoc「narrow dependency / no shuffle」「drastic coalesce → fewer nodes」「pass shuffle=true → upstream executed in parallel」逐字吻合，因果正負號正確、無矯枉過正。
> 細節提醒（非缺陷）：稿件說「回推到上一次 shuffle（或讀檔）為止整段的平行度上限」——這是 narrow-dependency 鏈在同一 stage 內 partition 數一路相同的正確推論（coalesce 是 narrow dependency，整條 narrow chain 共用 partition 數）；官方未用「天花板」字眼，但「each of the 100 new partitions will claim 10 of the current partitions」＋「computation taking place on fewer nodes」就是這個意思的具體化。表述精確，不算過度宣稱。

### [主張 4 — §5.5 分區表寫出：每 task 為碰到的每個分區值各寫一檔] ✅機制正確（屬 Spark 標準寫出行為，無單一官方頁逐字、但實作如此＋多方一致）

**稿件主張**（§5.5、L202）：寫 `PARTITIONED BY` 表時「每個 task 會為它手上碰到的每一個分區值各寫一個檔」；故動態分區（一次寫多個分區值）用裸 `REPARTITION(n)`（隨機散）會讓每個分區值被打散到全部 n 個 task →「每個分區目錄被寫進 n 個檔」→ `分區數 × n` 的小檔爆炸；正解是按分區欄位重分 `REPARTITION(dt)` / `REPARTITION(n, dt)`，或對傾斜分區用 `REBALANCE(dt)`。並正確點出「只寫單一靜態分區（`PARTITION (month='2026-05')`）時只有一個輸出目錄，裸 `REPARTITION(16)` 就對了」。

**查證**：
- 這是 Spark `FileFormatWriter` / `partitionBy` 的標準行為：每個（in-memory）write task 在它負責的那批 row 裡，依分區欄位值落到對應子目錄，**每個分區值各開一個 output 檔**；故一個 write task 碰到 k 個不同分區值就寫 k 個檔，N 個 write task × 各自碰到的分區值 → 每個分區目錄最多 N 個檔。官方 [DataFrameWriter.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html)：「Partitions the output by the given columns on the file system... records are partitioned by the partition field and saved into different directories（directory/partition_column=value/）」——確認「按分區欄位值落不同目錄」，但**未逐字寫「每 task 每分區值一檔」與「repartition by 分區欄位以減檔」**。
- 實務修法 `df.repartition(N, partition_cols).write.partitionBy(partition_cols)`（讓同分區值的 row 集中到同/少數 task → 每目錄少數檔）是公認標準作法，WebSearch（Spark／Cloudera 域）回傳一致描述；Spark SQL Partitioning Hints 頁也明指 hint「control the number of output files」、`REBALANCE` 專為「write the result of this query to a table, to avoid too small/big files」「split the skewed partitions」（撐住稿件對 `REBALANCE(dt)` 處理傾斜分區的建議）。

**判定**：✅機制正確、因果正負號正確、無過度宣稱。「每 task 為碰到的每個分區值各寫一檔 → 裸 REPARTITION(n) 造成 分區數×n 檔 → 按分區欄位重分集中」是 Spark 分區寫出的標準行為，且與官方 `partitionBy`／`REBALANCE`／Partitioning Hints 的方向一致。**唯一限制**：無單一官方頁面把「每 task 每分區值一檔」這句逐字寫出（屬實作層行為，非文件條目）。稿件 L208 已自記「為 Spark 分區寫出的標準行為——以實際輸出為準」的 hedge，誠實、恰當。單一靜態分區免雷的但書（L202 末、§5.9 第 4 點）也正確。

### [主張 1 — §5.4 補充：資料本地性更正（小碎檔 vs 不可切分大檔）] ✅大體正確、釐清方向對、未矯枉過正（一處用語可斟酌）

**稿件主張**（§5.4 補充、L119-128）：
- locality 等級 PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY，「ship code to data 比搬資料便宜」。
- 影響 locality 四因素：① executor 與資料是否同機（dynamic allocation 可能讓 executor 不在 data node）；② HDFS 副本數預設 3（越多越易排到 local）；③ 叢集忙閒＋`spark.locality.wait`（預設通常夠用）；④ 儲存類型（本地 HDFS 有 locality／遠端物件儲存沒有）。
- **更正後核心主張**：「小碎檔對 locality 的影響間接、偏弱；真正傷 locality 的是不可切分的大檔（如整個 gzip，單 task 從頭讀到尾、區塊散在不同節點 → 多半遠端讀）」，小檔主要傷的是 NameNode metadata／排程開銷（§5.5），別把鍋算到 locality。

**查證**：
- 「ship serialized code 比搬 data 便宜」逐字：「Typically, it is faster to ship serialized code from place to place than a chunk of data because code size is much smaller than data.」→ [Spark Tuning — Data Locality (latest)](https://spark.apache.org/docs/latest/tuning.html) ✅。
- locality 等級官方為 `PROCESS_LOCAL`／`NODE_LOCAL`／`NO_PREF`／`RACK_LOCAL`／`ANY`（稿件列了四級、略去 `NO_PREF`）。✅ 方向正確；⚠️ 略 `NO_PREF` 屬簡化（user 提供的基準事實也只列四級、未含 NO_PREF），對 SQL-first 讀者影響極小，非缺陷，至多「可加強」。
- 「default usually works well」逐字：「You should increase these settings if your tasks are long and see poor locality, but **the default usually works well**.」✅ 撐住稿件「`spark.locality.wait` 官方說預設通常夠用」。
- HDFS 副本預設 3、DataNode 存資料塊、NameNode 管 metadata：[HDFS Architecture](https://hadoop.apache.org/docs/r3.1.3/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)（user 基準已認證）✅；「副本越多越多就近候選」為 locality 機制的正確推論（每個 block 有 3 個 replica，scheduler 在 3 台之一即可 NODE_LOCAL）。
- 因素①（dynamic allocation 使 executor 不一定落在持有該 block 的節點）：這是 storage/compute 分離與動態配置下的正確機制描述；無單一官方頁逐字，但與 locality「data's current location vs running code」定義一致，方向正確。
- 因素④（遠端物件儲存無 node locality）：正確——物件儲存（S3/ABFS 等）無 HDFS 那種 block-to-DataNode 的同機性，讀取本質是網路讀，故 NODE_LOCAL 不適用（通常落 NO_PREF/ANY）。屬通用機制，方向正確。

**「更正後是否矯枉過正」的判斷**（本輪重點）：
- 「不可切分大檔傷 locality」**正確且有理**：一個不可 split 的檔（gzip、非 splittable）只能由單一 task 整檔讀；HDFS 上一個大檔跨多個 128MB block、各 block 的 replica 散在不同 DataNode，單一 task 不可能對所有 block 都 NODE_LOCAL → 必然有遠端讀。此因果成立。
- 「小碎檔對 locality 影響間接、偏弱」**也站得住、非矯枉過正**：小檔的主要痛點確實是 NameNode metadata（user 基準：每物件約 150 bytes）與每 task 排程／開檔開銷（openCost，見主張 2）；至於 locality，小檔通常仍能就近排（每個小檔本身有 replica、夠小時排程器仍可 NODE_LOCAL），故其對 locality 的傷害的確「相對次要」。稿件用「間接、偏弱」而非「無」，保留了餘地，措辭分寸恰當。
- **未矯枉過正**：稿件沒有走到「小檔完全不影響 locality」的極端，且把小檔的真正鍋（metadata／排程）明確導向 §5.5，因果歸因正確。

**判定**：✅大體正確、釐清方向對、未矯枉過正。四因素皆為通用且正確的機制；`spark.locality.wait`「預設夠用」、「ship code to data」、等級順序皆有官方逐字撐腰。唯二可斟酌（皆「可加強」級、非缺陷）：(a) locality 等級略去 `NO_PREF`；(b) 因素①④屬「正確機制但無單一官方頁逐字」，稿件 L336 章末已自記「為通用機制」的 hedge，誠實。

### [主張 6 — §5.6 CBO / autoUpdate 預設值；ANALYZE 須主動下] ✅已驗證（直接核對 Spark 3.3.2 SQLConf 原始碼，補上前一版「無法逐字擷取」的缺口）

**稿件主張**（§5.6、L238/L241/L245）：`spark.sql.cbo.enabled` 預設 `false`、`spark.sql.cbo.joinReorder.enabled` 預設 `false`、`spark.sql.statistics.size.autoUpdate.enabled` 預設 `false`（且只自動更新「大小」、不含欄位級統計）；`ANALYZE` 須使用者主動下、非自動。

**查證**——直接核對 **Spark 3.3.2 標籤** 的 `SQLConf.scala` 原始碼（`https://raw.githubusercontent.com/apache/spark/v3.3.2/.../SQLConf.scala`，Apache Spark 官方 repo 的版本鎖定原始碼）：
- `CBO_ENABLED = buildConf("spark.sql.cbo.enabled") ... .booleanConf.createWithDefault(false)`，doc：「Enables CBO for estimation of plan statistics when set true.」→ **預設 `false`** ✅。
- `JOIN_REORDER_ENABLED = buildConf("spark.sql.cbo.joinReorder.enabled") ... .createWithDefault(false)`，doc：「Enables join reorder in CBO.」→ **預設 `false`** ✅（且 join reorder 屬 CBO 之下，呼應稿件「連自動 join 重排序還要再另開」）。
- `AUTO_SIZE_UPDATE_ENABLED = buildConf("spark.sql.statistics.size.autoUpdate.enabled") ... .createWithDefault(false)`，doc：「Enables automatic update for table **size** once table's data is changed...」→ **預設 `false`、且只更新「size」** ✅（稿件「只更新大小、不含欄位級統計」對齊，因 doc 只說 size，欄位級統計仍須 `ANALYZE ... FOR COLUMNS`）。

**ANALYZE 為使用者主動下（非自動）**：`AUTO_SIZE_UPDATE` 預設關＝Spark 不會在每次寫表後自動重算統計（連「大小」都要你開那個 flag 才自動，且仍不含欄位統計）；故產表後須主動跑 `ANALYZE`。此推論成立。✅（[Spark SQL — ANALYZE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html) 為使用者主動下的 AUX 指令，user 基準已認證語法。）

**判定**：✅已驗證，**且補上了前一版（稿件 L245／L332 自承「自動工具未能在公開 Configuration 頁逐字擷取這兩列」）的缺口**——三個預設值現已直接由 Spark 3.3.2 原始碼逐字確認皆為 `false`。建議（可加強）：稿件 L245／章末 L332 的「撰寫時自動工具未能逐字擷取、以你環境 `SET <key>;` 查證為準」hedge 可以鬆綁——本輪已用版本鎖定的官方原始碼確證，不必再讓讀者自行 `SET` 才安心（但保留「以你環境為準」無害）。

### [主張 7 — §5.8 external 登記於共用 HMS（Hive/Impala/Hue 可查）；dbt-spark 走 Spark CREATE TABLE 多為 external] ✅已驗證

**稿件主張**（§5.8、L268/L275/L277）：(a) Spark SQL `CREATE TABLE` 建的多半是 external 表；(b) external 表登記在共用 Hive Metastore → 用 Hue 在 Hive 上、或用 Impala 都查得到；(c) Hive managed/ACID 表才是 Spark 要繞 HWC 的那種；(d) dbt-spark（第三方、已標非權威範圍）底層走 Spark SQL 建表，故同吃這條規則、預設多落 external。

**查證**：
- (a)＋(c) CDP 官方 [Understanding CREATE TABLE behavior](https://docs-archive.cloudera.com/cdp-private-cloud-upgrade/latest/upgrade-cdh6/topics/cdp-data-migration-table-create.html) 逐字：「Calling 'create table' from SparkSQL, for example, **creates an external table** after upgrading to Cloudera as it did before the upgrade.」；Hive 端「by default CREATE TABLE **creates either a full ACID transactional table in ORC format** or insert-only ACID transactional tables」；HWC 端「You can connect to Hive using the **Hive Warehouse Connector (HWC) to read Hive ACID tables from Spark**... Spark creates an external table with the purge property when you do not use the HWC API.」✅ 三點全對齊。
- (b) external 登記於共用 HMS、跨引擎可查：[Make Tables SparkSQL Compatible / external table CDP docs] 與 WebSearch（docs.cloudera.com 域）一致描述「Creating an external table stores the metadata in HMS... allows the external table to be **accessed by multiple query engines**」「both Hive and Impala can query the same external table stored in the metastore」「To read Hive external tables from Spark, you do not need HWC」✅。Hue 是 Hive/Impala 的 SQL 前端，故「Hue 上查得到」隨之成立（Hue 走 HiveServer2／Impala，metadata 同來自 HMS）。
- (d) dbt-spark：稿件已明標「屬第三方工具、不在本手冊權威來源範圍」「為其行為的合理推論」（L286／章末 L337）。dbt-spark 確以 Spark SQL 提交 `CREATE TABLE`，故吃同一條 CDP 規則（預設 external）；這是合理推論而非權威斷言，稿件 hedge 分寸正確。✅（不在權威來源範圍內，按 hedge 對待，無需改）。

**判定**：✅已驗證。external 登記共用 HMS、Hive/Impala 可查、managed/ACID 走 HWC、Spark SQL CREATE TABLE 預設 external 皆有 CDP 官方逐字撐腰；dbt-spark 為已正確標註的第三方合理推論。

### [主張 8 — §5.2 全章「列式→欄式」用語] ✅技術正確、不致誤解（繁中脈絡下正確選擇）

**稿件主張**（§5.2、L41/L63）：把 columnar 一律稱「欄式」（同一欄＝column 的值收在一起），並註明「繁中表格慣例裡欄是直的、列是橫的，所以欄式＝同一直行收在一起」；明說不採易被讀成 row 的「列式」一詞。

**查證**：
- Parquet 官方定義逐字：「Parquet is a **columnar** format that is supported by many other data processing systems.」→ [Spark SQL — Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) ✅。原文用 **columnar**（按欄組織），稿件要對應的英文概念無誤。
- 繁中慣例核對：在繁體中文試算表（Excel 繁中在地化）與表格用語中，**欄（column）為直行、列（row）為橫排**——「A 欄、B 欄」「第 1 列、第 2 列」。故 columnar（column-oriented，同一 column 收一起）對應繁中「**欄式**」在用詞上正確、且與「直行收在一起」的圖示自洽。
- 潛在誤解來源：簡體中文的「列」常指 column（簡中稱 columnar storage 為「列式存储」），與繁中的「列＝row」**正好相反**——這正是稿件選擇避開「列式」、改用「欄式」的正當理由。對繁中讀者，「欄式」不會被誤讀成 row-oriented；反之若沿用簡中習慣的「列式」，繁中讀者會把它讀成 row-based（與本意相反），才會造成技術誤解。
- 全章一致性：L41（定義）、L63（§5.2 來源 ⚠️ 段明示「全書與第 03 章 §3.3 一致」）、L18（§5.1）、其餘各處（§5.10）皆用「欄式」，未見殘留「列式」。一致。✅

**判定**：✅技術正確、不會造成技術誤解。在繁體中文脈絡下，「欄＝column、列＝row」是正確且通行的慣例，故「欄式＝column-oriented」用詞正確；改掉「列式」反而**避免**了與簡中相反慣例混淆的誤讀風險。本審查只判正確性（不評文筆）：判定為正確選擇，無技術缺陷。
> 註：本判定屬語言慣例＋官方英文術語對照，非可 WebFetch 的單一條目；依據為 Parquet 官方「columnar」用字 ＋ 繁中表格「欄/列」通行定義，結論明確。

---

## 整章前後一致性附帶查證（非 8 條主張，但本輪順手核對的鄰近事實）

### [附-A — §5.6 broadcast 用 catalog/ANALYZE 統計 vs AQE 用 runtime 統計] ✅已驗證
[Spark SQL Performance Tuning — Leveraging Statistics](https://spark.apache.org/docs/latest/sql-performance-tuning.html) 逐字：「**Catalog**: Statistics that Spark reads from the catalog, like the Hive Metastore. These statistics are collected or updated whenever you run `ANALYZE TABLE`.」「**Runtime**: Statistics that Spark computes itself as a query is running. This is part of the adaptive query execution framework.」→ 完全對齊稿件 §5.6 第 1/3 點「broadcast 計畫階段估計靠 catalog（ANALYZE）統計、AQE 靠 runtime 統計、兩者互補」。✅ 因果分層正確。

### [附-B — §5.4 NameNode 每物件約 150 bytes、千萬檔約 3GB] ✅已驗證（逐字）
[Cloudera — The Small Files Problem](https://www.cloudera.com/blog/technical/the-small-files-problem.html)（原 blog.cloudera.com，301 轉址至 cloudera.com/blog，仍 Cloudera 官方）逐字：「Every file, directory and block in HDFS is represented as an object in the namenode's memory, each of which occupies **150 bytes**, as a rule of thumb.」「So **10 million files, each using a block, would use about 3 gigabytes of memory**.」→ 稿件 §5.4 L107「一筆約 150 bytes、千萬個檔就吃掉約 3GB」逐字吻合（3GB＝千萬檔×（file＋block）兩物件×150B，非 1.5GB；稿件數字正確、與官方一致）。✅

### [附-C — §5.7 bucketing Hive 不相容 AnalysisException 出處可達] ✅已驗證（逐字）
[Cloudera CDP — Write to Hive bucketed tables](https://docs.cloudera.com/cdp-public-cloud/cloud/cdppvc-data-migration-spark/topics/cdp-one-workload-migration-spark-bucketed.html) 逐字：「Spark currently does NOT populate bucketed output which is compatible with Hive.」錯誤訊息：「Output Hive table ... is bucketed but Spark currently does NOT populate bucketed output which is compatible with Hive.」→ 稿件 §5.7 L256「Spark 目前不會產生與 Hive 相容的 bucketed 輸出」「寫 Hive bucketed 表會丟 `AnalysisException`」對齊。✅（稿件把訊息中譯為示意、未逐字英文，方向正確。）

### [附-D — §5.8 Parquet schema merging 預設關] ✅已驗證
`spark.sql.parquet.mergeSchema` 預設 **false**（[Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)）→ 對齊稿件 §5.8 L281「`spark.sql.parquet.mergeSchema` 預設是關的、要時才開」。✅

### [附-E — §5.3 壓縮 codec 清單 ＋ 預設 snappy] ✅已驗證
`spark.sql.parquet.compression.codec` 預設 **snappy**，可選「none, uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd」（[Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)）→ 與稿件 §5.3 L81 列的 codec 清單逐字一致。✅

### [附-F — §5.2 filterPushdown 預設 true] ✅已驗證
`spark.sql.parquet.filterPushdown` 預設 **true**（同 Parquet Files 頁）→ 對齊稿件 §5.2 L63。✅

---

## 結尾彙整：三級分類

### 真缺陷（必補）
**無。** 本輪 8 條主張（§5.4 locality 更正、§5.5 openCost 打包估算、coalesce vs repartition 平行度、分區表寫出、partitioning hints、§5.6 預設值、§5.8 external/dbt、§5.2 欄式）以及 6 條鄰近一致性事實，全部查證通過、無與官方／權威來源衝突之處。**沒有「建議被寫成硬限制」「因果正負號錯」「引用出處錯」「更正後矯枉過正」的缺陷。**

### 可加強（斟酌；皆非錯誤）
1. **§5.6 預設值 hedge 可鬆綁**（主張 6）：稿件 L245／章末 L332 自承「自動工具未能在公開 Configuration 頁逐字擷取」`cbo.enabled`／`joinReorder.enabled`／`statistics.size.autoUpdate.enabled` 三列。**本輪已直接核對 Spark 3.3.2 `SQLConf.scala` 原始碼，三者 `createWithDefault(false)` 確證。** 若要，可把「以你環境 `SET <key>;` 查證為準」的 caveat 改為「已對 Spark 3.3.2 原始碼確認預設 false」，更篤定（保留「以你環境為準」亦無害）。
2. **§5.4 locality 等級可補 `NO_PREF`**（主張 1）：官方等級為 `PROCESS_LOCAL`／`NODE_LOCAL`／**`NO_PREF`**／`RACK_LOCAL`／`ANY`，稿件列四級略去 `NO_PREF`。對 SQL-first 讀者影響極小（NO_PREF 是「哪都一樣快、無偏好」，非排序中的一階重點），補不補皆可；user 提供的基準事實也只列四級。
3. **§5.5 openCost 累加公式可顯式化**（主張 2，可選）：稿件用「1MB 檔被當約 5MB 算」具體化了 `fileLength + openCost` 累加，已足夠；若想更精確，可一句點明「每檔有效大小＝實際大小＋openCost，再 bin-pack 進約 128MB」。非必要。

### 誤讀（不改或微調）
- **§5.4「小碎檔對 locality 影響間接偏弱、真正傷 locality 的是不可切分大檔」未矯枉過正**——這是本輪重點質疑項，判定為**正確的釐清、非過度更正**：不可 split 大檔跨多 block、單 task 無法對所有 block NODE_LOCAL 故必有遠端讀（因果成立）；小檔仍多能就近排、其主痛是 metadata／排程開銷（已正確導向 §5.5）。稿件用「間接、偏弱」而非「無」，分寸恰當。**不需改。**
- **§5.5 coalesce/repartition 平行度段**——與 RDD.coalesce ScalaDoc「narrow dependency / no shuffle」「drastic coalesce → fewer nodes」「pass shuffle=true → upstream executed in parallel」逐字吻合，是全章最紮實的一段。**不需改。**
- **§5.5 分區表寫出**——機制正確、屬 Spark 標準寫出行為；無單一官方頁逐字（稿件 L208 已 hedge）。**不需改。**
- **§5.2 欄式用語**——繁中脈絡下正確、避免與簡中「列式」相反慣例混淆。**不需改。**

> **總結**：第 05 章第二輪（針對上輪更正後的新增/修改內容）**全數通過技術正確性查證，無真缺陷**。所有更正方向正確、因果正負號正確、引用出處正確、hedge 分寸恰當；唯一可主動處理的是把 §5.6 三個預設值的 hedge 鬆綁（本輪已用 3.3.2 原始碼確證為 false）。其餘為可選微調，不影響正確性。

