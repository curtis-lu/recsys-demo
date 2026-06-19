# 第 06 章「引擎選用：Spark vs Hive on Tez vs Impala」審查日誌（reviewer）

> 審查員：技術正確性審查。對齊 Spark 3.3.x / Hive 3.1.3 / CDP Private Cloud Base 7.1.9（YARN+HDFS+Impala）。
> 規則：只查證不改稿；來源限 Spark 3.3 官方 / Apache Impala 官方（impala.apache.org）/ Apache Hadoop 官方 / Cloudera CDP 官方（docs.cloudera.com，含 blog.cloudera.com）/《Spark: The Definitive Guide》/《High Performance Spark》。Cloudera Community 論壇僅作「方向性傾向」佐證、非規格來源。查不到標「無法查證」、不臆測。
> 每查一條即時 append。結尾按 真缺陷(必補)/可加強(斟酌)/誤讀(不改或微調) 三級彙整。
>
> 使用者特別點名查證：(1) REFRESH vs INVALIDATE METADATA 語意與輕重、官方是否真建議優先 REFRESH；(2) Impala 對 full ACID 是否「可讀不可寫」、insert-only 是否可讀可寫；(3) CDP Hive 是否只跑 Tez；(4)「Impala 不容錯（節點故障整條查詢失敗）」是否屬實；(5) 章內對 §01/§02/§05 章與 §1.9/§5.6/§5.8/§5.9 的交叉引用是否指對。

---

## 逐條查證（即時 append）

### [主張 1 — §6.6 REFRESH 語意：增量、同步、輕量、可刷單一 PARTITION] ✅已驗證（逐字對齊 Impala 官方）

**稿件主張**（§6.6、L164、L176）：`REFRESH table`＝增量地重載表 metadata（從 Metastore 拿表資訊＋從 HDFS 增量更新檔案與資料塊清單）、**同步**、比完整載入輕量、用於既有表 `INSERT`／加改分區、可只刷一個分區 `REFRESH ... PARTITION (month='2026-05')`。

**查證**——[Impala — REFRESH Statement](https://impala.apache.org/docs/build/html/topics/impala_refresh.html) 逐字：
- 「The `REFRESH` statement **reloads the metadata for the table from the metastore database and does an incremental reload of the file and block metadata from the HDFS NameNode**.」✅ 對齊「從 Metastore 拿表資訊＋從 HDFS 增量更新檔案與資料塊清單」。
- 「`REFRESH` reloads the metadata **synchronously**.」✅ 對齊「同步（指令回來就刷好）」。
- 「`REFRESH` is **more lightweight** than doing a full metadata load after a table has been invalidated.」✅ 對齊「比完整載入輕量」。
- 用例：「Deleting, adding, or modifying files」「Deleting, adding, or modifying partitions」✅ 對齊「外部加／改檔案或分區」。
- 「In Impala 2.7 and higher, the `REFRESH` statement can apply to a **single partition at a time**, rather than the whole table.」＋語法 `REFRESH [db_name.]table_name [PARTITION (key_col1=val1...)]`✅ 對齊「可只刷一個分區」。

**判定**：✅已驗證。五個子主張（增量／同步／輕量／用於加改檔案分區／可刷單一 PARTITION）全部逐字對齊官方。

### [主張 2 — §6.6 INVALIDATE METADATA 語意：丟快取、非同步、昂貴、不帶表名 flush 全部、官方建議優先 REFRESH] ✅已驗證（逐字對齊 Impala 官方）

**稿件主張**（§6.6、L166、L168、L170、L176）：`INVALIDATE METADATA table`＝把表快取 metadata 整個丟掉標記過期、下次有人查才重載（**非同步**）；官方明講「比 REFRESH 增量更新**昂貴得多**」；不帶表名＝把所有表 metadata 全部失效；原則「能用 REFRESH 就別用 INVALIDATE METADATA（官方原話：when possible, prefer REFRESH）」；用於新建表／改 schema／改權限。

**查證**——[Impala — INVALIDATE METADATA Statement](https://impala.apache.org/docs/build/html/topics/impala_invalidate_metadata.html) 逐字：
- 非同步/lazy：「The next time the Impala service performs a query against a table whose metadata is invalidated, Impala reloads the associated metadata **before the query proceeds**.」✅ 對齊「下次查才重載＝非同步」。
- 昂貴＋優先 REFRESH：「As this is a **very expensive operation compared to the incremental metadata update done by the REFRESH statement, when possible, prefer REFRESH rather than INVALIDATE METADATA**.」✅✅ 稿件 L170 引的「when possible, prefer REFRESH」是官方逐字原話；L166「比 REFRESH 增量更新昂貴得多」對齊「very expensive operation compared to the incremental metadata update done by the REFRESH」。
- 不帶表名 flush 全部：「If there is no table specified, the **cached metadata for all tables is flushed** and synced with Hive Metastore (HMS).」✅ 對齊 L168「不帶表名＝把所有表 metadata 全部失效」。
- 用例：new tables / metadata changes / Ranger privilege updates / block metadata changes / UDF jar updates 等 ✅ 對齊「新建表／改 schema／改權限」。

**判定**：✅已驗證。非同步、昂貴、不帶表名 flush 全部、官方原話「when possible, prefer REFRESH」四點全部逐字對齊。**使用者特別點名查證 #1（REFRESH vs INVALIDATE METADATA 的輕重語意與官方是否建議優先 REFRESH）→ 完全正確。** 稿件 L171 §6.6 來源段對兩指令的引用敘述精確。

> 細節（非缺陷）：稿件把 INVALIDATE METADATA 描述為「**標記為過期、等下次有人查它時才重新載入**」——官方確實是 lazy reload（query 觸發時才 reload）。稿件 L166 用「直接把快取整個丟掉、標記為過期」正確；唯一可挑剔的是它**回收/釋放 catalog 記憶體**這層用途稿件沒提（官方列 memory optimization），但那不影響本章主旨（metadata 同步），屬刻意省略、非錯誤。

### [主張 3 — §6.6 CDP 事件驅動自動 metadata 同步：catalogd 輪詢 HMS 事件、hms_event_polling_interval_s、impala.disableHmsSync] ✅已驗證（逐字對齊 Cloudera CDP 官方）

**稿件主張**（§6.6、L156、L172、L176）：CDP 可開「事件驅動的自動 invalidate／refresh」——catalogd 定時輪詢 HMS 變更事件、自動刷新對應表；有輪詢間隔延遲（`hms_event_polling_interval_s`）；可被關或針對某些表停用（`impala.disableHmsSync`）。

**查證**——[Cloudera CDP — Automatic Invalidation/Refresh of Metadata](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/impala-manage/topics/impala-auto-metadata-sync.html) 逐字：
- 「When automatic invalidate/refresh of metadata is enabled, the **Catalog Server polls Hive Metastore (HMS) notification events at a configurable interval and automatically applies the changes** to Impala catalog.」✅ 對齊「catalogd 輪詢 HMS 事件、自動刷新」。
- 「This feature is controlled by the `--hms_event_polling_interval_s` flag.」✅ 對齊 config 鍵名與「輪詢間隔延遲」。
- 「use the `impala.disableHmsSync` property to disable the event processing at the **table or database level**」＋`ALTER TABLE <name> SET TBLPROPERTIES ('impala.disableHmsSync'='true'|'false')`✅ 對齊「可針對某些表停用」。

**判定**：✅已驗證。catalogd 輪詢、`hms_event_polling_interval_s`、`impala.disableHmsSync` 三點全部逐字對齊。稿件用 7.1.8 同系文件，與本手冊環境 7.1.9 同 minor-train，合用。

### [主張 4 — §6.7 ACID 跨引擎：Impala 讀 full ACID（ORC）不可寫、insert-only 可讀寫、full ACID 由 Hive 寫與維護] ✅已驗證（逐字對齊 Cloudera CDP 官方）

**稿件主張**（§6.7、L184–186、L195）：CDP 7.1.x 起 Impala 可**讀** full ACID（ORC）交易表、不必特別設定；但**不能 CREATE 或寫入** full ACID 表；insert-only（micromanaged）交易表 Impala 可讀可寫；full ACID 的「寫」歸 Hive（由 Hive 建與維護含 compaction）、Impala 只讀。

**查證**——[Cloudera CDP — READ Support for FULL ACID ORC Tables](https://docs.cloudera.com/runtime/7.2.17/impala-manage/topics/impala-read-fullacid-orc.html) 逐字：
- 讀 full ACID 免設定：「FULL ACID v2 transactional tables are **readable in Impala without modifying any configurations**.」✅ 對齊「可讀、不必特別設定」。
- 不可寫：「Impala **cannot CREATE or WRITE to FULL ACID transactional tables** yet.」✅ 對齊「不能 CREATE 或寫入」。
- Hive 寫、Impala 讀：「You can **CREATE and WRITE FULL ACID transactional tables ... via HIVE and use Impala to READ** these tables.」✅ 對齊「full ACID 由 Hive 寫與維護、Impala 只讀」。
- insert-only 可讀寫：「Impala in CDP supported **INSERT-ONLY transactional tables allowing both READ and WRITE** operations」＋「By default tables created in Impala are INSERT-ONLY managed tables」✅ 對齊「insert-only 可讀可寫」。

**判定**：✅已驗證。**使用者特別點名查證 #2（Impala 對 full ACID「可讀不可寫」、insert-only「可讀可寫」）→ 完全正確、逐字對齊。** 稿件 L195 來源段引用敘述精確；其 hedge「可讀範圍依 CDP／Impala 版本而異、確切邊界以平台實測為準」恰當（此頁為 Runtime 7.2.17，本手冊環境 7.1.9 同屬 7.1.x／7.2.x full ACID 讀支援已 GA 的範圍，方向一致）。

### [主張 5 — §6.2／§6.4 CDP Hive 只跑 Tez、指定 MapReduce 報錯、Tez 是 DAG 比 MapReduce 快] ✅已驗證（逐字對齊 Cloudera CDP 官方）；⚠️「Tez UI 經 Hue 看 DAG」此來源未證

**稿件主張**（§6.2、L64、L49；§6.4、L112、L121）：CDP 上 Hive 早就不用 MapReduce（直接拿掉執行引擎、硬指定還會報錯）、改用 Tez；Tez 和 Spark 一樣是 DAG 引擎、stage 間盡量不落地、比老 MapReduce 快得多。§6.4 另稱 Tez DAG 各 vertex 可透過 Hue 的查詢頁檢視。

**查證**——[Cloudera CDP — Hive on Tez introduction](https://docs.cloudera.com/runtime/7.2.18/hive-introduction/topics/hive-on-tez.html) 逐字：
- 只跑 Tez：「Apache Tez is the Hive execution engine for the Hive on Tez service」＋「**MapReduce is not supported**.」✅ 對齊「CDP Hive 只跑 Tez、MapReduce 不支援」。
- 指定 MapReduce 報錯：「In a Cloudera cluster, if a legacy script or application **specifies MapReduce for execution, an exception occurs**.」✅ 對齊「硬指定還會報錯」。
- DAG 比 MapReduce 快：「With expressions of directed acyclic graphs (DAGs) and data transfer primitives, **execution of Hive queries on Tez instead of MapReduce improves query performance**.」✅ 對齊「DAG 引擎、比老 MapReduce 快」。

**判定**：✅已驗證。**使用者特別點名查證 #3（CDP Hive 是否只跑 Tez）→ 完全正確、逐字對齊。**

> ⚠️ 小瑕疵（出處對不上、非事實錯）：§6.4 L112/L121 稱「Tez UI（多半從 Hue 的查詢頁進）」「Tez UI 透過 Hue 檢視 Hive on Tez 查詢的 DAG 見 [Hive on Tez introduction]」——**該 Hive on Tez introduction 頁並未提及 Hue 或任何 DAG 視覺化 UI**（本審查 WebFetch 該頁「未提及」）。「Tez 查詢有對應 DAG／可在 UI 檢視」方向正確，但把它掛在這個出處是**引用錯置**。稿件本身在 §6.4 來源段已用 ⚠️ hedge「各 UI 確切入口依平台部署與權限而異」，緩解了部分風險，但「見 [Hive on Tez introduction]」這個具體指向仍對不上內容。歸為「可加強：引用出處精確化」。

### [主張 6 — §6.2／§6.8 Impala 不容錯：節點故障整條查詢失敗、要重跑] ✅事實正確（方向有官方部落格＋業界共識撐腰），但⚠️所引 Cloudera Blog URL 已失效、且該文未「逐字」明說「Impala 不容錯」

**稿件主張**（§6.2、L44、L62；§6.8、L205、L220；§6.9）：Impala **不像** Spark／Hive 那樣容錯——查詢跑到一半某台機器掛了，整條查詢就失敗、得從頭重來；這是「Impala 為互動而生、不為長批次而生」的根本原因。

**查證**：
- **事實本身正確**。Cloudera 官方部落格（canonical URL，見下）逐字：「**Hive is designed to be very fault-tolerant. If a fragment of a long-running query fails, Hive will reassign it and try again.**」——這是「**對比**」式陳述：明說 Hive 容錯、藉以對照 Impala。Impala 官方 intro 也定位它為與 batch（MapReduce/Hive）並存的互動引擎（「Impala does not replace the batch processing frameworks built on MapReduce such as Hive」）。Impala daemon 任一 impalad 故障會使該查詢失敗、需重跑，是 Impala 架構的公認特性（業界與 Cloudera Community 一致；後者僅作方向性佐證）。✅ **使用者特別點名查證 #4（Impala 不容錯／節點故障整條查詢失敗）→ 事實成立。**
- ⚠️ **問題 A（出處 URL 失效）**：稿件在 §6.2/§6.3/§6.8 三處反覆引用 `https://blog.cloudera.com/choosing-the-right-data-warehouse-sql-engine-apache-hive-llap-vs-apache-impala/`。本審查 WebFetch 此 URL **301 重導到 `https://www.cloudera.com/blog.html`（一個泛用部落格首頁），文章本體取不到**。文章實際的 canonical URL 是 **`https://www.cloudera.com/blog/technical/choosing-the-right-data-warehouse-sql-engine-apache-hive-llap-vs-apache-impala.html`**（WebSearch 與直接 WebFetch 該 URL 均可達、內容存在）。**讀者照稿件 URL 點過去會落到首頁、找不到依據**。建議把三處 `blog.cloudera.com/...` 改為上述 `www.cloudera.com/blog/technical/...`。
- ⚠️ **問題 B（「逐字」程度被高估）**：§6.2 來源段 L68 寫「Impala ...不像 Hive 那樣容錯（節點故障會讓查詢失敗）...見 [Cloudera Blog]」。但該文**只逐字說了 Hive 容錯**、對 Impala 不容錯是**由對比隱含**，並未出現「Impala is not fault-tolerant / a node failure fails the query」這類直述句。事實無誤，但「見此出處」對「Impala 不容錯」是「隱含支持」而非「逐字支持」。屬可加強：要嘛補一個直述 Impala 不容錯的權威出處，要嘛把語氣調成「對比隱含」。

**判定**：事實 ✅；但出處 URL 失效（真缺陷：連結壞掉，讀者點不到）＋「逐字 vs 隱含」精度（可加強）。歸入「真缺陷（必補）：修 URL」與「可加強：標明隱含」。

### [主張 7 — §6.2 Impala 是 MPP、原生執行（不轉 MapReduce）、常駐 daemon（impalad）於資料節點、低延遲] ✅已驗證（MPP／原生執行有 Impala 官方逐字；常駐 daemon 為架構設計、方向正確）

**稿件主張**（§6.2、L39–42、L60；§6.1 定位表）：Impala 在每台資料節點上跑常駐 `impalad`、365 天開著等查詢；查詢一來不需先起 application／申請 YARN 容器；用 C++ 執行引擎；MPP；低延遲互動；不轉 MapReduce。

**查證**：
- MPP：[Impala — Components of Impala](https://impala.apache.org/docs/build/html/topics/impala_components.html) 逐字「The Impala server is a **distributed, massively parallel processing (MPP) database engine**.」✅
- 原生執行、不轉 MapReduce：[Impala — Overview](https://impala.apache.org/overview.html) 逐字「To avoid latency, **Impala circumvents MapReduce to directly access the data through a specialized distributed query engine**.」✅ 直接撐「原生執行（不轉成 MapReduce job）」與「低延遲」。
- 常駐 daemon／在 DataNode：components 頁「each Impala daemon runs on the same host as a DataNode」（co-located 部署）✅；impalad 是長駐 service（一次啟動後持續與 StateStore 通訊）。稿件「365 天開著」是把「長駐 service（非 per-query 啟動）」具象化，方向正確（官方未用「365 天」字眼，屬合理具象化、非數字宣稱）。
- 「不需先起 application／申請 YARN 容器→省啟動開銷」：是「常駐 daemon vs 每查申請容器」對比的合理推論，方向正確；§6.2 來源段 L68 已 hedge「確切啟動秒數依設定而異、無官方逐字數字」，恰當。

**判定**：✅已驗證。MPP、原生執行（circumvents MapReduce）為官方逐字；常駐 daemon／低延遲方向正確且有出處撐腰。
> 細節：§6.2 來源段 L68 把這些掛在 [Cloudera Blog]（URL 已失效，見主張 6 問題 A）。MPP／原生執行其實有更直接、可達的 **Impala 官方** 出處（overview.html、impala_components.html）。建議補上 Impala 官方連結，不要只靠失效的部落格 URL。歸「可加強」。

### [主張 8 — §6.2／§6.8 Impala 能 spill 到磁碟、可設 MEM_LIMIT、PROFILE 提供 per-fragment 時間/記憶體/spill] ✅已驗證（逐字對齊 Impala 官方）

**稿件主張**（§6.2、L43、L62；§6.4、L113、L121；§6.8、L205、L223）：Impala 盡量在記憶體裡算、不夠才 spill 到磁碟（不是一律 OOM）；`PROFILE` 提供各 fragment 時間、記憶體用量、有沒有 spill。

**查證**——[Impala — Scalability Considerations](https://impala.apache.org/docs/build/html/topics/impala_scalability.html) 逐字：
- spill：「Certain memory-intensive operations **write temporary data to disk (known as spilling to disk) when Impala is close to exceeding its memory limit** on a particular host. The result is a **query that completes successfully, rather than failing with an out-of-memory error**.」✅ 對齊「不夠才 spill、不是一律 OOM」。
- `MEM_LIMIT`：「you can issue `SET MEM_LIMIT` as a SQL statement...」✅（稿件 L68 提及可設 `MEM_LIMIT`）。
- PROFILE：「Issue the `PROFILE` command to get a detailed breakdown of the **memory usage on each node** during the query.」＋ spill 統計（SpilledPartitions）、I/O 指標 ✅ 對齊「各 fragment 時間、記憶體、spill」。

**判定**：✅已驗證。spill 行為、MEM_LIMIT、PROFILE 內容三點全部對齊官方。

### [主張 9 — §6.8 Impala「記憶體導向／記憶體密集」「超大 join／聚合吃力」「多人重查互相排擠」] ⚠️方向正確但措辭需精準（官方對「記憶體密集」是限定在特定運算，且明說多數查詢 CPU-bound）

**稿件主張**（§6.8、L205）：Impala「**記憶體導向**——超大 join／聚合即使能 spill，也比 Spark／Tez 的批次吃力。多人同時對它丟重查詢時，記憶體壓力會互相排擠。」§6.2 來源段 L68 亦稱「記憶體密集」。

**查證**——同 Scalability 頁：
- 官方把「記憶體密集」**限定在特定運算**：「**Certain memory-intensive operations** ... when Impala is close to exceeding its memory limit」——即 **join／aggregation 這類**運算是 memory-intensive，會在逼近上限時 spill。✅ 撐「超大 join／聚合吃力、會 spill」。
- 但同頁也說「Impala retrieves data ... so quickly ... that **most queries are CPU-bound rather than I/O-bound**」——即 Impala **整體不被官方定性為「記憶體導向 engine」**，而是「特定運算 memory-intensive＋整體偏 CPU-bound」。
- 「多人重查記憶體互相排擠」：Impala 並行查詢共享每節點記憶體、admission control／MEM_LIMIT 即為管理此而設，方向成立（為設計取向描述）。

**判定**：⚠️ 方向正確、但「記憶體導向（memory-oriented）」當作 Impala 的**整體定性**略過頭——官方只說「**特定**運算 memory-intensive」、且「多數查詢 CPU-bound」。稿件講的具體現象（超大 join/聚合吃力、會 spill、並行重查互相排擠）都成立，但「記憶體導向」這個整體標籤，嚴格說是「在重 join/聚合等運算上記憶體吃緊」。屬「誤讀/微調」級：把「記憶體導向」改為「重 join/聚合等運算記憶體吃緊」更精準，不改也不算事實錯（讀者拿到的操作結論——別把超大 join/長批次塞 Impala——正確）。

### [主張 10 — §6.7／§6.5／§6.1 Hive 3 預設 CREATE TABLE＝managed ACID(ORC)、external 非 ACID、Spark 存 managed/ACID 需 HWC、external 不需] ✅內容正確（逐字對齊）；⚠️所引兩個 7.1.0 URL 皆 301 重導到 docs-archive

**稿件主張**（§6.7、L182、L187、L195；§6.5、L133、L140；§6.1、L19）：Hive 裡直接 `CREATE TABLE` 預設＝managed／ACID(ORC) 交易表；external Parquet 表非交易表、三引擎可直讀、Spark 寫不必走 HWC；Spark 要存取 Hive managed／ACID 表通常要走 HWC。

**查證**：
- Hive 3 預設＝managed ACID(ORC)：[Cloudera — Apache Hive 3 tables（archive）](https://docs-archive.cloudera.com/runtime/7.1.0/using-hiveql/topics/hive_hive_3_tables.html) 逐字「If you accept the default by not specifying any storage during table creation, or if you specify ORC storage, you get an **ACID table with insert, update, and delete (CRUD) capabilities**.」✅
- external 非 ACID：同頁「Because Hive control of the external table is weak, the table is **not ACID compliant**.」✅ 撐「external Parquet 不是交易表、沒 ACID 包袱」。
- Spark 存 managed/ACID 需 HWC：[Cloudera — Spark access to Hive（archive）](https://docs-archive.cloudera.com/runtime/7.1.0/securing-hive/topics/hive_spark_access_to_hive.html) 逐字「From Apache Spark, you access **ACID tables and external tables** in Apache Hive 3 **using the Hive Warehouse Connector**.」＋「You do _not_ need LLAP to access external tables from Spark.」✅ 撐「managed/ACID 走 HWC」。

**判定**：內容 ✅；但兩個出處 URL 有問題（見下）。

> ⚠️ **真缺陷（URL 失效）**：稿件 §6.1/§6.5/§6.7 反覆引用 `https://docs.cloudera.com/runtime/7.1.0/securing-hive/...` 與 `https://docs.cloudera.com/runtime/7.1.0/using-hiveql/...`。本審查實測**兩者皆 301 重導到 `docs-archive.cloudera.com/...`**（Cloudera 已把 7.1.0 內容移到 archive 子網域）。讀者照稿件 URL 點過去會被導走（雖內容尚在 archive，但 `docs.cloudera.com` 那條路徑已非正規）。建議全章把 `docs.cloudera.com/runtime/7.1.0/...` 改成 `docs-archive.cloudera.com/runtime/7.1.0/...`，或改引本手冊環境對應的 7.1.9 同主題頁（CDP Private Cloud Base 7.1.9 應有對應頁，更貼齊版本鎖定）。

> ⚠️ **語意精度（external 是否「需 HWC」）**：上述 Spark-access 頁逐字是「access **ACID tables and external tables** ... using HWC」——字面把 external 也算進 HWC 的覆蓋面。但同頁緊接「You do _not_ need LLAP to access external tables from Spark」，且 CDP 後續文件（及社群慣例）一致認定 **external 表 Spark 可不透過 HWC 直接讀寫、HWC 主要是為 managed/ACID 而生**。稿件「external 不必走 HWC、managed/ACID 才要」**結論正確且為 CDP 主流認知**，但若只引這一頁，字面上會被該句「ACID tables and external tables ... using HWC」反咬。建議補一個更明確區分「external 直存、managed 走 HWC」的 CDP 出處（例如 HWC 讀寫模式或 Hive-Spark 整合的較新版頁），避免被字面打臉。歸「可加強」。

### [主張 11 — §6.7／§6.9 full ACID 由 Hive 維護含 compaction、external Parquet 用 INSERT OVERWRITE ... PARTITION 整批覆寫即可] ✅機制正確（compaction 屬 Hive ACID 標準維護；整批覆寫呼應 §5.9/§08，章內交叉引用）

**稿件主張**（§6.7、L186、L193）：full ACID 的維護（含 §5.8 提的 compaction 合併增量檔）歸 Hive；若需求只是「整批換掉一個分區」，用 `INSERT OVERWRITE ... PARTITION` 配 external Parquet 即可、不需 ACID。

**查證**：Hive ACID 表以 base＋delta 增量檔累積變更、靠 compaction（minor/major）合併，是 Hive 交易表的標準維運機制（Hive 3 tables 頁與 ACID 文件一致）。✅ 機制方向正確。`INSERT OVERWRITE ... PARTITION` 為 Hive/Spark SQL 標準整批覆寫語意（Spark SQL `INSERT OVERWRITE` 支援動態/靜態分區），與 §5.9／§08 的存法一致。✅ 為章內交叉引用（§5.9／第 08 章），下方第 12 條一併核對交叉引用是否指對。

**判定**：✅機制正確。屬設計建議（「需 update/delete 才用 ACID，否則整批覆寫＋external Parquet」），方向與 §5.8/§5.9 主軸一致、無誇大。

### [主張 12 — 章內對 §01/§02/§05 與 §1.9/§5.6/§5.8/§5.9 的交叉引用是否指對] ✅全部指對（逐一核對標題與內容）

**查證**（實際開檔比對）：
- 連結檔案存在：`01-how-spark-runs-your-sql.md`、`02-diagnose-with-spark-ui.md`、`05-storage-efficiency.md` 均存在，ch06 連結語法正確。（`07-pyspark-dataframe-api.md` 尚未建檔，但那是章末「下一章」forward-ref，符合手冊體例、非錯。）
- **§1.9**：ch01 L268 標題「## 1.9 為什麼 Spark 通常比老 Hive（MapReduce）快」✅ —— 與 ch06 L3/L49/L64 描述「§1.9 說 Spark 比老 Hive（MapReduce）快」**完全指對**。ch06 對「老 Hive＝MapReduce、慢在每 stage 寫回 HDFS」的轉述與 §1.9 主旨一致。
- **§5.6**：ch05 L221「## 5.6 餵統計：`ANALYZE TABLE` 為什麼關鍵」✅ —— ch06 §6.4 L118「統計過時→回去 §5.6 跑 `ANALYZE`（或 Impala 端 `COMPUTE STATS`）」指對；§5.6 內文確實講 ANALYZE TABLE COMPUTE STATISTICS。Impala 端對應指令確為 `COMPUTE STATS`（Impala 官方標準語法），轉述正確。
- **§5.8**：ch05 L273「## 5.8 營運共用資料表：Hive 3 的 managed／external、與 schema 演進」✅ —— ch06 §6.1/§6.5/§6.7 引「§5.8 共用 Hive Metastore、managed/ACID、external、HWC」全部對得上：§5.8 內文逐項講了 managed＝預設 ACID(ORC)＋需 HWC、external＝Spark CREATE 預設＋登記共用 Metastore＋Hive/Impala 皆可讀、compaction。ch06 的轉述與 §5.8 內容**一致**。
- **§5.9**：ch05 L299「## 5.9 把它全部串起來：設計一張每月帳務彙總表」✅ —— ch06 §6.1/§6.5/§6.9 引「§5.9 那張 external Parquet 表 `monthly_cust_txn`、按 month 分區、`REPARTITION(32)`、產完跑 ANALYZE」**逐項對得上**：§5.9 內文正是 `monthly_cust_txn`、Parquet、按 `month` 分區、`/*+ REPARTITION(32) */`、external Parquet、`ANALYZE TABLE ... PARTITION`。ch06 §6.9 的情境數字（3000 萬/月、REPARTITION(32)、ANALYZE）與 §5.9 一致。
- **第 02 章**：ch02 講 `EXPLAIN`＋Spark UI（從 History Server 進）✅ —— ch06 §6.2/§6.4 引「第 02 章先量再調、用 Spark UI／`EXPLAIN` 找瓶頸、從 History Server 進」指對。
- **第 01 章**：ch01 講 driver/executor/shuffle/task、容錯靠 task 重試／stage 重算 ✅ —— ch06 §6.2 引「第 01 章那套：起 application、driver 排程、executor 平行算、shuffle、容錯靠重試／重算」指對。

**判定**：✅ **使用者特別點名查證 #5（對 §01/§02/§05 與 §1.9/§5.6/§5.8/§5.9 的交叉引用）→ 全部指對。** 標題、小節編號、被引內容三層皆核對一致，無一指錯。

### [主張 13 — §6.3／§6.8 引用 Cloudera Community 論壇作 ETL 批次傾向佐證] ✅符合「方向性佐證、非規格來源」的引用原則

**稿件主張**（§6.3 L101、§6.8 L223 來源段）：以 [Cloudera Community — Hive on Spark or Impala in batch (ETL)] 佐證「Impala 適合低延遲 ad-hoc、Hive 適合容錯批次 ETL、Impala 不適合重型 join／長跑」。

**查證**：稿件**明確標註**此為 Cloudera Community 論壇連結，且全章來源原則段（L282）寫「Cloudera Community 論壇答覆僅作『方向性傾向』的佐證、非規格來源」——**完全符合**使用者設定的引用紀律（論壇僅作方向性傾向）。被佐證的主張本身（Impala 互動/Hive 批次）另有 Cloudera 官方部落格＋Impala 官方文件撐腰（見主張 6/7/8），論壇只是補方向。✅

**判定**：✅符合引用原則。論壇連結僅作方向性佐證、未當規格來源，且核心事實另有權威出處。（唯該論壇 URL 本審查未逐一點開驗活性；即便失效，因非規格來源、不影響事實。歸「誤讀/不改」。）

### [補充查證 — §6.2 Tez「容器可重用、stage 間盡量不落地」是否成立] ✅方向正確（Tez container reuse 屬其公認特性）

**稿件主張**（§6.2、L49、L64）：Tez「和 Spark 一樣是 DAG 引擎、stage 之間盡量不落地、容器可重用，比老 MapReduce 快得多」。

**查證**：CDP Hive on Tez introduction 逐字「With expressions of directed acyclic graphs (DAGs) and **data transfer primitives**, execution ... on Tez instead of MapReduce **improves query performance**」——「data transfer primitives」即指 Tez 在 vertex 間以更有效率的方式傳資料（相對 MapReduce 每 stage 落 HDFS）。Tez 的 container reuse 為其公認設計特性（Hive on Tez 性能調校文件提及 `tez.am.container.reuse.enabled` 等）。✅ 方向正確，屬設計取向描述，無誇大。

**判定**：✅方向正確。「stage 間盡量不落地、容器可重用」是 Tez 相對 MapReduce 的核心改進，與官方「DAG＋data transfer primitives 改善效能」一致。

### [補充查證 — 章末「版本對齊」說明：Spark latest 已指向 4.x、應改 3.3.2] ✅已驗證

**稿件主張**（資料來源段、L271）：Spark 官方連結指向「最新版」頁面、`latest` 目前已指向 4.x；要對齊本手冊版本，把網址 `…/docs/latest/…` 改成 `…/docs/3.3.2/…`。

**查證**：本審查 WebFetch `https://spark.apache.org/docs/latest/sql-performance-tuning.html`，頁面標題顯示 **「Performance Tuning - Spark 4.1.2 Documentation」**。✅ `latest` 確已指向 4.x（4.1.2）。稿件提醒讀者改 `…/docs/3.3.2/…` 對齊本手冊版本，作法正確。

**判定**：✅已驗證。版本對齊說明準確、誠實（自承自動工具無法逐字驗 3.3.2 鎖定頁），符合手冊「結論誠實」原則。

### [補充查證 — §6.8「Hive 是 ACID 表的擁有者與維護者」措辭精度] ✅成立（限定 full ACID 即正確；稿件已區分 full ACID vs insert-only）

**稿件主張**（§6.8 L209、§6.9 L237、§6.1 定位表）：Hive 是「ACID 交易表的擁有者與維護者」、ACID 交易表「本來就歸 Hive 寫」。

**查證**：嚴格說，**full ACID** 由 Hive 建/寫/維護（Impala 只讀，主張 4 已證）；但 **insert-only** ACID 表 Impala/Spark 也能寫（主張 4：「Impala 預設建 INSERT-ONLY managed 表」）。所以「ACID 表一律歸 Hive 寫」若不分 full/insert-only 會略過頭。**但稿件 §6.7 已明確區分** full ACID（Impala 不可寫）vs insert-only（可讀可寫），§6.8/§6.9 的「ACID 交易表歸 Hive 寫」是在「需要 full ACID 的 update/delete／合規刪資料」語境下講的（L209 括號明寫「update／delete／合規刪資料」＝full ACID 用例）。在該語境下「歸 Hive 寫」**正確**。

**判定**：✅成立。稿件對 full ACID vs insert-only 的區分在 §6.7 已做足，§6.8/§6.9 的「Hive 擁有 ACID 寫入」在其 full-ACID 語境下精確。屬「誤讀/不改」級（若要更嚴謹，§6.8 可加「（full ACID）」三字，但非必要）。

---

## 結尾彙整（按三級分類）

### 真缺陷（必補）

1. **§6.2／§6.3／§6.8：Cloudera Blog URL 失效（出現 3 次）。** 稿件引用的 `https://blog.cloudera.com/choosing-the-right-data-warehouse-sql-engine-apache-hive-llap-vs-apache-impala/` 實測 **301 重導到泛用部落格首頁 `www.cloudera.com/blog.html`，文章本體取不到**。文章 canonical URL 為 **`https://www.cloudera.com/blog/technical/choosing-the-right-data-warehouse-sql-engine-apache-hive-llap-vs-apache-impala.html`**（可達、內容存在）。讀者照現稿 URL 點過去找不到依據 → 三處都要改。**（這是「連結壞掉」型缺陷，不是事實錯——文章內容仍撐住本章核心定位。）**

2. **§6.1／§6.5／§6.7：兩個 7.1.0 Cloudera 文件 URL 301 重導到 docs-archive（各出現多次，共 3 處引用）。** `docs.cloudera.com/runtime/7.1.0/securing-hive/...hive_spark_access_to_hive.html` 與 `docs.cloudera.com/runtime/7.1.0/using-hiveql/...hive_hive_3_tables.html` 皆 **301 → `docs-archive.cloudera.com/...`**。內容尚在 archive，但現稿路徑已非正規。建議改 `docs-archive.cloudera.com/...`，**或更好**：改引本手冊環境對應的 **CDP Private Cloud Base 7.1.9** 同主題頁（更貼齊版本鎖定）。
   - 註：此 URL 問題 **§5.8 來源段也有同樣的 7.1.0 連結**（ch05 也命中一次），屬全書性的連結 stale，建議兩章一起修。

### 可加強（斟酌）

3. **§6.4：「Tez UI 透過 Hue 檢視 DAG」掛在 [Hive on Tez introduction] 出處——該頁未提 Hue／DAG UI（引用錯置）。** 事實方向（Tez 查詢有 DAG、有對應 UI）成立，但這個具體出處對不上內容。建議換成真正講 Tez UI/Hue 的 Cloudera 頁，或把該句的出處標註拿掉、留在 §6.4 已有的「各 UI 入口依平台而異」hedge 下。

4. **§6.2 來源段：「Impala 不容錯（節點故障會讓查詢失敗）」是「對比隱含」非「逐字」。** 所引 Cloudera Blog **只逐字說 Hive 容錯**、Impala 不容錯由對比隱含。事實無誤，但若要「逐字」等級，建議補一個直述 Impala 不容錯的權威出處，或把語氣調成「（由 Hive 容錯的對比隱含）」。

5. **§6.2 來源段：MPP／原生執行只掛失效 Blog URL，未引更直接可達的 Impala 官方頁。** 「MPP」「circumvents MapReduce」其實有 Impala 官方逐字出處（`impala_components.html`：「distributed, massively parallel processing (MPP) database engine」；`overview.html`：「Impala circumvents MapReduce to directly access the data」）。建議補這兩個 Impala 官方連結，不要只靠失效的部落格。

### 誤讀（不改或微調）

6. **§6.8「Impala 記憶體導向（memory-oriented）」當整體定性略過頭。** 官方只說「**特定**運算（join/聚合）memory-intensive」、且「**多數查詢 CPU-bound**」。稿件講的具體現象（超大 join/聚合吃力、會 spill、並行重查互相排擠）全部成立，操作結論（別把超大 join/長批次塞 Impala）正確；僅「記憶體導向」這個整體標籤可微調為「重 join/聚合等運算記憶體吃緊」。不改不算事實錯。

7. **§6.8／§6.9「Hive 是 ACID 表的擁有者」措辭。** 嚴格分 full ACID（Hive 寫）vs insert-only（Impala/Spark 也能寫），但稿件 §6.7 已做足區分，§6.8/§6.9 在 full-ACID（update/delete）語境下「歸 Hive 寫」正確。若要更嚴謹可在 §6.8 加「（full ACID）」三字，非必要。

8. **§6.3 Cloudera Community 論壇連結。** 符合「方向性佐證、非規格來源」原則，核心事實另有官方出處撐腰，無問題（論壇 URL 活性未逐一驗，但非規格來源、不影響事實）。

---

## 使用者五個點名查證的結論

1. **REFRESH vs INVALIDATE METADATA 語意/輕重、官方是否建議優先 REFRESH** → ✅ **完全正確、逐字對齊 Impala 官方**（含「when possible, prefer REFRESH」原話、不帶表名 flush 全部、增量/同步 vs 丟快取/非同步/昂貴、可刷單一 PARTITION）。
2. **Impala 對 full ACID「可讀不可寫」、insert-only「可讀可寫」** → ✅ **完全正確、逐字對齊 Cloudera CDP 官方**（「readable without modifying configurations」「cannot CREATE or WRITE to FULL ACID yet」「INSERT-ONLY ... both READ and WRITE」）。
3. **CDP Hive 是否只跑 Tez** → ✅ **完全正確、逐字對齊**（「MapReduce is not supported」「specifies MapReduce ... an exception occurs」）。
4. **「Impala 不容錯（節點故障整條查詢失敗）」** → ✅ **事實成立**；唯所引 Cloudera Blog URL 失效（真缺陷#1）＋該文對 Impala 不容錯是「對比隱含」非逐字（可加強#4）。
5. **對 §01/§02/§05 與 §1.9/§5.6/§5.8/§5.9 的交叉引用** → ✅ **全部指對**（標題、小節編號、被引內容三層逐一核對一致，無一指錯）。

**總評**：本章**技術事實正確性極高**——五個點名查證的核心技術主張全部成立，多數逐字對齊官方文件；交叉引用零失誤。唯一的真缺陷是**引用 URL 失效/重導**（Cloudera 改版導致，非作者寫錯事實，但讀者點不到依據，必補）。其餘為出處精確化與單一措辭微調。
