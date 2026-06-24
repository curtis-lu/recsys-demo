# 第 11 章 速查與名詞表

> **本章前提**：這是一頁式的**速查**：取捨、常用設定預設值、症狀對策、中英名詞對照。內容都是前面各章的彙整，每項附「去哪章看細節」。預設值對齊 **Spark 3.3 / CDP 7.1.9**，請以你平台實際設定為準。

---

## 本章目錄

- [11.1 取捨速查](#111-取捨速查)
- [11.2 常用設定速查（Spark 3.3 預設）](#112-常用設定速查spark-33-預設)
- [11.3 症狀 → 對策 → 章](#113-症狀--對策--章)
- [11.4 中英名詞對照](#114-中英名詞對照)
- [11.5 一句話帶走](#115-一句話帶走)

---

## 11.1 取捨速查

效率不是單一維度，這本手冊講的取捨整理在這：

| 你想要 | 代價／要小心 | 看 |
|---|---|---|
| 更高平行度（更快） | 每個 task 分到的記憶體變少 → 容易 spill | §1.7、§4.5 |
| broadcast 小表（省 shuffle） | 表太大會撐爆 **driver** 記憶體 | §3.5、§4.4 |
| 分區切細（掃描省） | 小檔變多、NameNode metadata 壓力、metadata 爆 | §5.4、§5.5 |
| 壓縮率高（省儲存） | 耗 CPU；gzip 不可切分 | §5.3 |
| 寫外部 DB 加平行度 | **打垮對方 OLTP**，`numPartitions` 要保守 | §9.2 |
| 改用 PySpark DataFrame API | 可測試/可重用，但**不會更快**、團隊要會 Python | §10.2 |
| 用 ACID 表（能改既有列） | compaction 維運成本、跨引擎讀寫限制 | §6.7 |

**通則**：對 SQL-first 的人，**改 SQL 寫法（§03）＋ 餵對統計（§5.6）** 多半比硬調 config（§04）更有效。

---

## 11.2 常用設定速查（Spark 3.3 預設）

| 設定 | 預設 | 作用 | 看 |
|---|---|---|---|
| `spark.sql.adaptive.enabled` | **true**（3.2+） | AQE：合併小分區、動態 broadcast、處理 skew | §4.2 |
| `spark.sql.shuffle.partitions` | **200** | shuffle 後的分區數（AQE 會再合併過小的） | §1.6、§4.4 |
| `spark.sql.autoBroadcastJoinThreshold` | **10MB** | 小於它的表才會被自動 broadcast | §3.5、§4.4 |
| `spark.sql.files.maxPartitionBytes` | **128MB** | 讀檔時每個 partition 的目標大小 | §1.2、§4.4 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | **64MB** | AQE 合併分區時的目標大小 | §4.4 |
| `spark.memory.fraction` | **0.6** | （heap−300MB）中給 execution+storage 的比例 | §4.5 |
| `spark.memory.storageFraction` | **0.5** | 上面那塊裡保給 storage（cache）的比例 | §4.5 |
| `spark.executor.memoryOverheadFactor` | **0.10** | executor 額外要的非 heap 記憶體比例 | §4.5、§4.6 |
| `spark.driver.memory` | **1g** | driver 的記憶體；大量 `collect()`/broadcast 要加大 | §4.5 |
| `spark.dynamicAllocation.enabled` | 開源 **false**／**CDP 預設 true** | executor 台數隨負載伸縮 | §4.7 |
| `spark.yarn.queue` | **default** | 作業跑在哪個 YARN queue（多租戶的隔離） | §4.7 |
| `spark.sql.sources.partitionOverwriteMode` | **static** | `INSERT OVERWRITE` 覆寫範圍：static 全清/dynamic 只清命中分區 | §7.2 |
| `spark.sql.parquet.compression.codec` | **snappy** | Parquet 寫出壓縮 | §5.3 |
| `spark.sql.cbo.enabled` | **false** | 成本優化（CBO）；要開且要先 `ANALYZE` 才有用 | §5.6 |
| HDFS block size／副本 | **128MB／3** | 底層儲存單位與副本數 | §5.4 |

> ⚠️ 預設值會隨版本變動；上表對齊 Spark 3.3、部分（dynamicAllocation）為 CDP 平台預設。要對齊精確版本，查 [Spark 3.3.2 Configuration](https://spark.apache.org/docs/3.3.2/configuration.html)。

---

## 11.3 症狀 → 對策 → 章

| 症狀（在 Spark UI 看到的） | 多半是 | 去哪章 |
|---|---|---|
| 少數 task 特別久、其餘早就跑完 | 資料傾斜（skew） | §3.10 |
| Stages 頁 spill (memory/disk) 非零、很大 | 記憶體不夠 → 溢寫磁碟 | §4.5、§3 改寫法 |
| 該 broadcast 卻是 `SortMergeJoin` | 統計缺/表被當大表/門檻 | §3.5、§5.6 |
| Scan 沒有 `PartitionFilters`、整表掃 | partition 沒裁到 | §3.2、§3.4 |
| 一個 stage 幾萬個 task、每個 Input 很小 | 小碎檔 | §5.5 |
| 結果筆數爆量、join 後列數暴增 | 一對多/笛卡兒積 join | §3.5、§3.7 |
| 作業卡在 ACCEPTED 不動 | executor 超過 YARN/queue 上限 | §4.6、§4.7 |
| 排程「每週都比上週慢」 | 漸進退化（資料長大/skew 惡化） | §2.11 |

完整的「怎麼從畫面讀出這些」在 [第 02 章](02-diagnose-with-spark-ui.md)。

---

## 11.4 中英名詞對照

| 英文 | 中文／白話 | 出處 |
|---|---|---|
| partition | 分區——資料被切成的一塊，平行處理的單位 | §1.2 |
| shuffle | 跨機器重新分配資料（最貴的操作） | §1.5、§1.6 |
| narrow / wide dependency | 窄／寬依賴——不搬／要搬資料；寬依賴＝shuffle | §1.5 |
| executor / driver | 執行器（做事的工人）／驅動器（總指揮那台） | §1.1、§1.4 |
| application → job → stage → task | 一次連線 → 每個 action → 每次 shuffle 切一刀 → 一個 partition | §1.4 |
| lazy evaluation | 惰性求值——transformation 先累積、action 才真的跑 | §1.3 |
| Catalyst | Spark 的查詢優化器（SQL 與 DataFrame 共用） | §1.3、§10.2 |
| AQE | Adaptive Query Execution，跑的途中自動調整計畫 | §4.2 |
| skew | 資料傾斜——某些 key 特別大、卡住少數 task | §3.10 |
| spill | 溢寫——記憶體放不下，暫存到磁碟（慢） | §1.6、§4.5 |
| broadcast join | 把小表複製到每台、免 join 的 shuffle | §3.5 |
| predicate pushdown / partition pruning | 謂詞下推（提早篩）／分區裁剪（只讀命中分區） | §3.2、§3.4 |
| Metastore | 記「有哪些表、欄位、分區在哪」的目錄服務 | §1.1、§5.8 |
| external / managed table | external＝只登記、檔案你管；managed＝Hive 全包（CDP 預設 ACID） | §5.8、§6.7 |
| ACID / compaction | 能可靠改既有列／把增量 delta 檔併回大檔 | §6.7 |
| idempotent | 冪等——重跑同一批，結果一樣（不變兩份） | §7.2 |
| backfill | 回填——補算過去某些期的資料 | §7.4 |
| feature / label | 特徵（拿來預測的資訊）／label（要預測的答案） | §8.3 |
| snapshot / grain | 快照（某時間點的狀態）／粒度（一列代表什麼） | §8.3 |
| data leakage | 特徵洩漏——用了預測當下還拿不到的資訊 | §8.3 |
| training–serving skew | 訓練與推論用的特徵算法不一致 | §8.3、§10.5 |
| reverse ETL | 把倉庫裡算好的資料推回業務系統 | §9.1 |
| upsert | 有則更新、無則新增（`MERGE`/`ON CONFLICT`） | §9.3 |
| WAP | Write-Audit-Publish：先寫、驗過、才發佈 | §8.6、§9.4 |
| PII | 個人可識別資訊（客戶 ID、姓名、聯絡方式） | §9.5 |
| OLTP | 線上交易處理資料庫（業務系統那台） | §9.2 |

---

## 11.5 一句話帶走

> **這頁是「我記得有這回事，但忘了細節」時用的。真正的脈絡與取捨都在它指向的章，速查表給你關鍵字，章節給你為什麼。**

---

*← 上一章：[第 10 章（進階）PySpark DataFrame API](10-pyspark-dataframe-api.md)　｜　回 [手冊首頁](index.md)*
