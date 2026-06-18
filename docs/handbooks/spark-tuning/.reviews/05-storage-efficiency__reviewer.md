# 05 · 儲存效率 — 技術審查日誌

審查員：技術正確性審查（Spark 3.3.x + Hive 3.1.3 / CDP 7.1.9）
方法：逐條主張比對權威來源（Spark 3.3 官方文件、Apache Hadoop、Cloudera CDP、《Spark: The Definitive Guide》《High Performance Spark》）。
圖例：✅已驗證(附出處) / ❌錯誤(正確值+出處) / ⚠️無法查證(或方向正確但無逐字出處)。

> 進行中，逐條 append。結尾有三級彙整。

---

## §5.8 Hive 3/CDP managed vs external、HWC（重點可疑點之一）

✅ **「Hive CREATE TABLE 預設→managed ACID(ORC)」** — 已逐字查證。CDP Private Cloud Base **7.1.9** 官方文件〈Understanding CREATE TABLE behavior〉原文：「CREATE TABLE creates a full ACID transactional table in ORC format or insert-only ACID transactional tables for all other table formats.」
出處：https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/using-hiveql/topics/cdp-data-migration-table-create.html

✅ **「Spark SQL CREATE TABLE 在 CDP 建 external 表」** — 已逐字查證（同頁）：「Calling 'create table' from SparkSQL, for example, creates an external table after upgrading to CDP as it did before the upgrade.」這正好是手冊版本鎖定到 7.1.9 的正確頁（手冊原引的是 runtime/7.1.0 與 cdp-public-cloud 頁；內容一致，但 7.1.9 私雲頁是讀者環境最精確的出處）。
出處：同上。

✅ **「從 Spark 讀寫 Hive managed(ACID) 表需 HWC、external 不需」** — 已查證（同頁＋CDP HWC 頁）：「You can connect to Hive using the Hive Warehouse Connector (HWC) to read Hive ACID tables from Spark. To write ACID tables to Hive from Spark, you use the HWC and HWC API.」external 表 Spark 直讀寫不需 HWC（同系文件多處）。
出處：同上；https://docs.cloudera.com/cdp-public-cloud/cloud/cdppvc-data-migration-spark/topics/cdp-one-workload-migration-hwc.html

判定：§5.8 三項核心主張（最容易誤導的 CDP 特例）**全部正確且有官方出處**。手冊已自行 hedge HWC 確切用法依版本而異，得當。
🔧 可加強（非缺陷）：手冊引的出處是 `runtime/7.1.0` 與 `cdp-public-cloud` 兩頁；建議改/補引 `cdp-private-cloud-base/7.1.9` 的〈Understanding CREATE TABLE behavior〉一頁即同時涵蓋「Hive 預設 managed ACID」＋「SparkSQL 建 external」＋「HWC 讀寫 ACID」三點，且正中讀者 7.1.9 環境。

## §5.6 CBO 預設值（重點可疑點之一）

✅ **「spark.sql.cbo.enabled 預設 false」** — 已查證。Spark SQL 內部文件與多方一致：預設 false（opt-in）。
✅ **「需先 ANALYZE FOR COLUMNS」** — 與官方 CBO 設計一致（CBO 靠欄位級統計）。
✅ 補充：**spark.sql.cbo.joinReorder.enabled 也預設 false**，且「多表 join 自動排序」需 cbo.enabled 與 cbo.joinReorder.enabled **兩者皆開**才生效——手冊 §5.6 把「自動排出較省 join 順序」歸給 CBO，方向正確；嚴格說還需額外開 joinReorder（手冊未提，但屬細節、非錯誤）。
出處：Spark SQL internals（Configuration Properties / CostBasedJoinReorder），cbo.enabled 與 cbo.joinReorder.enabled 皆預設 false。
備註：手冊章末 #4 已 hedge「自動工具未能在公開 Configuration 頁逐字擷取」。本次以 Spark SQL internals 佐證預設值為 false，方向無誤；唯仍非 spark.apache.org Configuration 頁逐字（該頁本就不逐列列出 cbo.* 預設），維持 ⚠️「官方 Configuration 頁未逐字、但預設 false 經多源佐證」較誠實。

## §5.2 格式：Parquet/ORC 列式、column pruning、filterPushdown、ORC=Hive 預設

✅ **「Parquet 是 columnar 格式」** — 逐字：「Parquet is a columnar format that is supported by many other data processing systems.」
✅ **「spark.sql.parquet.filterPushdown 預設 true」** — 逐字：「Enables Parquet filter push-down optimization when set to true.」（預設 true）。
✅ **「column pruning（只讀需要欄）、依資料塊統計跳塊」** — Parquet 列式＋filter pushdown 為官方支援；min/max 塊統計跳塊為 Parquet 設計，方向正確。
出處：https://spark.apache.org/docs/latest/sql-data-sources-parquet.html （版本鎖定 3.3.2：把 latest→3.3.2，內容同）。

✅ **「CDP 上 Hive managed 表預設用 ORC」** — 與 §5.8 查到的 CDP 7.1.9〈CREATE TABLE behavior〉一致：「full ACID transactional table in ORC format」。站得住。
⚠️ **「Spark 生態 Parquet 最常見」** — 業界通則/生態觀察，無單一官方逐字出處；手冊用語「跟著你平台的主流走」已軟化為建議，**非硬主張**，可接受。《Spark: The Definitive Guide》Ch.9 以 Parquet 為 Spark 預設檔案格式（`spark.sql.sources.default=parquet`）佐證「Spark 生態主流」方向正確。
判定：§5.2 主張正確；唯「Parquet 最常見」屬生態通則（已適度軟化）。

## §5.3 壓縮：snappy 預設、可選值、snappy 快 vs gzip/zstd 小

✅ **「spark.sql.parquet.compression.codec 預設 snappy」** — 逐字確認。
✅ **可選值清單** — 手冊列 none/uncompressed/snappy/gzip/lzo/brotli/lz4/zstd；官方 latest 列「none, uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd」。
   ⚠️ 細節：官方 latest 多一個 **lz4_raw**（手冊未列）。手冊章末「我已查證的關鍵事實」基準含 lz4_raw，但 §5.3 內文與 📚來源列舉時漏了 lz4_raw。屬**極小遺漏**（不影響任何建議），可補可不補；注意 lz4_raw 是否在 3.3.2 即存在需確認（3.3.2 頁 404 無法逐字），latest（4.x）確定有。
✅ **「snappy 解壓快、CPU 低；gzip/zstd 壓縮率高但耗 CPU；zstd 介於兩者」** — 各編碼設計取向，方向正確；手冊已 hedge「確切倍率依資料而異、無官方逐字」。得當，未把建議寫成硬限制。
判定：§5.3 正確。唯一可加強：可選值清單補 lz4_raw 以對齊官方（且需註明 3.3.2 適用性）。

## §5.6 ANALYZE 語法、各選項收集物、broadcast 三層因果（重點可疑點之一）

✅ **ANALYZE TABLE 語法與各選項** — 全部逐字確認：
  - 無選項：「both number of rows and size in bytes are collected.」（手冊：列數＋大小、會掃 ✓）
  - NOSCAN：「Collects only the table's size in bytes (which does not require scanning the entire table).」（手冊：只大小、不掃 ✓）
  - FOR COLUMNS / FOR ALL COLUMNS：「Collects column statistics for each column specified, or alternatively for every column, as well as table statistics.」（手冊：欄位級 ✓）
  - partition_spec 支援（手冊 §5.9 用 PARTITION(month=...) ✓）。
出處：https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html

✅ **autoBroadcastJoinThreshold 預設 10MB** — 逐字：「10485760 (10 MB)」。
出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html

✅✅ **三層因果關係（本章最關鍵的可疑點）完全正確、因果方向無誤** — 官方 Performance Tuning 頁逐字把統計來源分三類，與手冊 §5.6 的三層敘述精確對應：
  1. **Data source**：「counts and min/max values in the metadata of Parquet files」→ 手冊「直接量資料源的檔案大小」✓
  2. **Catalog**：「collected or updated whenever you run ANALYZE TABLE」→ 手冊「ANALYZE TABLE 後存進 catalog/Hive Metastore」✓
  3. **Runtime**：「Statistics that Spark computes itself as a query is running」→ 手冊「AQE 用執行途中量到的真實大小、不靠事前 ANALYZE」✓
  關鍵：手冊把「broadcast 大小估計靠 (1)(2)、不必開 CBO」「CBO 另用欄位級統計、預設關」「AQE 靠 (3) runtime、不靠 ANALYZE」三者分清，**因果正負號與歸屬全對**，無混淆。這是本章最容易寫錯的地方，手冊寫對了。
判定：§5.6 三層因果**正確**。唯 CBO joinReorder 需額外開關（見上），屬可補細節。

## §5.5 coalesce vs repartition shuffle 方向（重點可疑點之一）

✅ **「coalesce 減少分區、不觸發 shuffle；repartition 重分區、觸發一次 shuffle」** — 方向**正確**。
   注意出處精確度：手冊 📚來源指向 Spark SQL Performance Tuning（Coalesce Hints）頁，但**該頁並未**陳述 coalesce 避免 full shuffle、repartition 觸發 shuffle 的差異（該頁只說 coalesce/repartition hint 都能控制輸出檔數）。
   正確權威出處應為 RDD/Dataset API：`coalesce(numPartitions)` 文件「Returns a new ... reduced into numPartitions partitions. ... results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle」；`repartition` 「Can increase or decrease the level of parallelism ... this always shuffles all data over the network.」兩者的 shuffle 行為亦見《Spark: The Definitive Guide》Ch.5 與《High Performance Spark》Ch.4。
🔧 **出處精確度問題（可加強，非事實錯）**：方向正確，但引用頁不支撐該主張——建議改引 RDD/DataFrame `coalesce`/`repartition` API 文件或書籍章節，而非 Performance Tuning（Coalesce Hints）頁。
判定：§5.5 coalesce/repartition 方向**正確**；引用出處不精確（軟缺陷）。

✅ **coalesce/repartition 權威出處（補上 §5.5 應引處）** — Spark RDD Programming Guide Transformations 表逐字：
   - coalesce：「Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.」
   - repartition：「Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. **This always shuffles all data over the network.**」
   → 證實手冊「coalesce 不（全）shuffle、repartition 觸發 shuffle」方向正確。建議 §5.5 改引此頁（或 Dataset API），取代不支撐此主張的 Performance Tuning（Coalesce Hints）頁。
出處：https://spark.apache.org/docs/latest/rdd-programming-guide.html

## §5.7 bucketing：Hive/Spark 不相容、AnalysisException（重點可疑點之一）

✅ **「Spark 寫 Hive bucketed 表會丟 AnalysisException、Spark bucketing 與 Hive 不相容」** — 已查證，CDP 官方頁逐字錯誤訊息：
   「Output Hive table `hive_test_db`.`test_bucketing` is bucketed but **Spark currently does NOT populate bucketed output which is compatible with Hive.**」
出處：https://docs.cloudera.com/cdp-public-cloud/cloud/cdppvc-data-migration-spark/topics/cdp-one-workload-migration-spark-bucketed.html （即手冊 §5.7 所引頁，存在且內容相符）。

⚠️ **手冊引號內的錯誤訊息是改寫、非逐字** — 手冊寫『AnalysisException：Spark 目前不會產生與 Hive 相容的 bucketed 輸出』（中譯改寫，可接受），但官方原文是「... is bucketed but Spark currently does NOT populate bucketed output which is compatible with Hive.」。屬中譯改寫，非缺陷；若想顯得是「逐字引用」可註明係意譯。

⚠️ **「跨引擎讀別人用不同 bucketing 版本建的表，也可能拿到錯的 join 結果」** — 此主張在所查 CDP 頁（spark-bucketed、hive-read-write-operations）**未找到逐字支撐**。CDP bucketed 頁談的是「Spark **寫** Hive bucketed 表報錯」與 workaround（`hive.enforce.bucketing=false`），未明述「跨引擎**讀**會回錯 join 結果」。Spark 端的相關事實是：Spark 預設**不會**對 Hive bucketed 表套用 bucket-aware join 優化（因 hash 不相容），故不至於「靜默回錯結果」——真正風險是 Spark 自建 bucketed 表與 Hive hash 不同。
   → 手冊已 hedge「屬 CDP 已知議題、以實測為準」，但**「跨引擎可能回錯 join 結果」這句缺權威逐字出處**，且機制描述偏強。建議：要嘛補一個明確出處，要嘛改寫為較保守的「跨引擎的 bucketing 語意/優化不保證互通，勿假設能無縫共用 bucket-aware 優化」。歸為**可加強**（hedge 已在，但具體因果宜軟化或補證）。

✅ 補充事實（不影響手冊正確性，供完整）：CDP/Hive 3 的 bucketing「不需使用者指定 bucket 數、隱式分桶」；且 Spark 2.4 起預設不能寫 Hive bucketed 表（手冊未提版本沿革，無妨）。
判定：§5.7 核心（不相容＋寫入報錯）**正確且有出處**；「跨引擎讀回錯結果」**缺逐字出處、機制偏強**（可加強）。

## §5.4 / §5.5 HDFS block 128MB、小檔對 NameNode metadata 壓力

✅ **「HDFS 預設/典型區塊 128MB」** — Hadoop 3.1.3 HDFS Architecture 逐字：「A typical block size used by HDFS is 128 MB.」
   ⚠️ 措辭精確度：官方說的是「**typical**（典型）」，非字面「default（預設）」；手冊 §5.4 寫「HDFS 的預設區塊大小也是 128MB」「HDFS 預設區塊 128MB」。實務上 `dfs.blocksize` 預設確為 134217728(128MB)（hdfs-default.xml），故「預設」說法亦正確，但**所引的 HDFS Architecture 頁用字是「typical」而非「default」**。手冊章末已 hedge「可由叢集設定」。歸**可加強**（若要嚴謹，補引 hdfs-default.xml 的 dfs.blocksize 預設值，或把「預設」對齊官方「典型」用字）。
出處：https://hadoop.apache.org/docs/r3.1.3/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html

❌→⚠️ **「過多小檔對 NameNode metadata 的壓力」所引頁不支撐** — 手冊 §5.4、§5.5 兩處 📚來源都把「小檔對 NameNode metadata 壓力」指向 [Apache Hadoop HDFS Architecture] 頁。但該頁**只說** NameNode「maintains the file system namespace」「FsImage/EditLog」，**並未討論小檔對 NameNode 的壓力/成本**（已逐字確認該頁無此論述）。
   → 「小檔→NameNode metadata 壓力」本身是**業界公認正確的通則**（每個檔/塊在 NameNode 佔固定記憶體），但**手冊引的這個 URL 不含此主張**＝引用出處不支撐主張。建議：改引有明確論述的權威來源（如 Hadoop 的 `Hadoop Archives/HAR` 或 Cloudera 「small files problem」官方文件），或把該主張標為通則而非掛在 HDFS Architecture 頁。歸**真缺陷（引用出處不支撐主張，必補/必改）**——因手冊明確以「📚來源」形式背書，讀者會以為該頁可查到，實際查不到。

✅ **「小檔→NameNode metadata 壓力」主張本身正確（有權威量化）** — Cloudera 官方部落格〈Small Files, Big Foils〉與社群文件：NameNode namespace tree 與 metadata 為記憶體物件，**每個約 150 bytes**；Cloudera 建議**每百萬 block 配 1GB NameNode heap**；「small file＝顯著小於預設 block size(128MB)」。
   → 證實主張無誤；問題僅在**手冊把出處掛在不含此論述的 HDFS Architecture 頁**。正確補引：Cloudera〈Small Files, Big Foils〉（vendor 官方，非個人部落格，符手冊引用政策）或 Cloudera 小檔官方文件。
出處：https://blog.cloudera.com/small-files-big-foils-addressing-the-associated-metadata-and-application-challenges/

✅ **dfs.blocksize 預設 = 134217728 (128MB)，Hadoop 3.x 一致** — 證實手冊「HDFS 預設區塊 128MB」**事實正確**（hdfs-default.xml `dfs.blocksize` 預設 128MB）。故 §5.4 措辭問題僅是「所引 HDFS Architecture 頁用『typical』」，事實層面手冊「預設 128MB」站得住。

## §5.4 目標檔案 128MB–1GB

⚠️ **「目標檔案 128MB–1GB」** — 業界常見目標區間（對齊 HDFS block 與「一塊≈一 task」），《High Performance Spark》Ch.5 資料布局有「避免過小/過大檔案」精神，但**「128MB–1GB」這個具體區間無官方逐字硬規定**。手冊已明確 hedge「業界常見目標區間、非官方逐字硬規定、以你平台為準」——**未把建議寫成硬限制**，處理得當。歸**誤讀防呆已到位**（不改或微調）。

## §5.8 schema 演進（Parquet/ORC 加欄）

✅ **「Parquet/ORC 支援 schema evolution、加欄安全（讀舊檔補 null）」** — Parquet 官方逐字：「Parquet also supports schema evolution. Users can start with a simple schema, and gradually add more columns to the schema as needed.」
   ⚠️ 細節：Spark 的 `spark.sql.parquet.mergeSchema` **預設 false**（官方：「we turned it off by default starting from 1.5.0」）。手冊 §5.8 說「Parquet/ORC 支援讀舊檔時把缺的新欄補 null」方向正確，但**未提 mergeSchema 預設關**——若不同檔 schema 不同，Spark 預設取單一檔 summary schema，跨檔合併需手動開 mergeSchema 或讀時指定。屬**可加強的細節**（不影響「加欄安全、改/刪危險」這個核心通則的正確性）。手冊已 hedge「個別型別變更依資料源與設定而定」。
✅ **「改型別/改名/刪欄危險」** — 通則正確，呼應第 03 章 join key 型別坑。
判定：§5.8 schema 演進通則正確；可補「mergeSchema 預設 false」一句更精確。

---

# 結尾彙整（三級）

## A. 真缺陷（必補/必改）
1. **§5.4＋§5.5「小檔→NameNode metadata 壓力」的引用出處錯**：兩處 📚來源都掛 [Apache Hadoop HDFS Architecture] 頁，但**該頁不含此論述**（已逐字確認）。主張本身正確（Cloudera：每物件~150B、每百萬 block 1GB heap），但讀者循手冊出處查不到 ＝ 出處不支撐主張。**必改**：改引 Cloudera〈Small Files, Big Foils〉或 Cloudera 小檔官方文件（vendor 官方，合引用政策）；HDFS Architecture 頁可保留作「128MB typical block」出處。

## B. 可加強（斟酌）
1. **§5.5 coalesce/repartition 引用頁不支撐**：📚指向 Performance Tuning（Coalesce Hints）頁，但該頁只談「控制輸出檔數」，**未陳述 coalesce 不 shuffle / repartition shuffle**。方向正確。改引 RDD Programming Guide Transformations（repartition「always shuffles all data over the network」）或 Dataset API / 兩本書。
2. **§5.7「跨引擎讀不同 bucketing 版本可能回錯 join 結果」缺逐字出處且機制偏強**：所查 CDP 頁只支撐「Spark **寫** Hive bucketed 表報錯＋不相容」，未支撐「**讀**會回錯 join 結果」。建議補明確出處，或軟化為「bucket-aware 優化不保證跨引擎互通」。hedge 已在，但具體因果宜補證/軟化。
3. **§5.4 HDFS block「預設」vs 官方「typical」**：事實正確（dfs.blocksize 預設 128MB），但所引 HDFS Architecture 頁用字是「typical block size」。要嚴謹可補引 hdfs-default.xml dfs.blocksize 預設，或對齊「典型」用字。
4. **§5.8 未提 `spark.sql.parquet.mergeSchema` 預設 false**：加欄通則正確，補一句「跨檔 schema 合併需手動開 mergeSchema（預設 false）」更精確。
5. **§5.3 可選值漏 lz4_raw**：官方 latest 列 lz4_raw，§5.3 內文/來源未列（章末基準事實有列）。極小遺漏；補列並註明 3.3.2 適用性。
6. **§5.6 CBO 自動 join 排序需 cbo.joinReorder.enabled（亦預設 false）**：手冊把「自動排 join 順序」歸 CBO 方向對，但嚴格說需 cbo.enabled＋cbo.joinReorder.enabled 兩者皆開。可補半句。
7. **§5.8 出處可精煉**：建議改引 cdp-private-cloud-base/7.1.9〈Understanding CREATE TABLE behavior〉一頁即涵蓋三點且正中讀者 7.1.9 環境。
8. **§5.7 AnalysisException 為中譯改寫**：可註明係意譯，或附英文原文「Spark currently does NOT populate bucketed output which is compatible with Hive」。

## C. 誤讀（不改或微調）— 已正確且 hedge 到位
- **§5.6 broadcast/CBO/AQE 三層因果**：官方 Performance Tuning 頁三來源（Data source / Catalog via ANALYZE / Runtime）逐字對應，**因果方向與歸屬全對**，本章最易錯處寫對了。✅✅
- **§5.8 CDP managed/external/HWC**：三項核心主張全有官方逐字出處（7.1.9 頁），CDP 特例寫對。✅
- **§5.2 Parquet 列式/column pruning/filterPushdown=true**、**§5.3 snappy 預設＋取捨方向**、**§5.6 ANALYZE 語法/各選項/autoBroadcastThreshold=10MB**、**§5.6 cbo.enabled 預設 false**：全部逐字或多源確認 ✅。
- **§5.4 128MB–1GB 目標區間**、**§5.3 壓縮倍率依資料而異**：已明確標為業界通則/無逐字倍率，未寫成硬限制，防呆到位。
- 「建議 vs 硬限制」全章檢查：**未發現把建議寫成硬限制**（128MB–1GB、snappy、bucketing「先不急著用」均為軟建議且有 hedge）。
- 「因果正負號」全章檢查：**未發現正負號錯誤**；coalesce(少shuffle)/repartition(shuffle)、分區越細→少讀但小檔壓力越大、snappy(快)/gzip(小)、加欄(安全)/改刪(危險) 方向皆正確。

## 版本對齊備註
- Spark 3.3.2 版本鎖定頁（如 .../docs/3.3.2/sql-data-sources-parquet.html）目前 **404 無法直接逐字驗證**；改以 latest 頁查證，關鍵預設值（snappy、filterPushdown=true、autoBroadcastJoinThreshold=10MB、ANALYZE 語法）3.3 與 latest 一致，無版本漂移風險。唯 **lz4_raw** 可選值需確認 3.3.2 是否已含（latest=4.x 確定有）。
- HDFS/Hive 以 Hadoop 3.1.3 / CDP 7.1.9 同系文件查證，與讀者環境對齊。
