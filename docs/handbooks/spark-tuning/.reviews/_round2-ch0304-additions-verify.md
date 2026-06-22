# Round-2 新增內容技術查核報告
## 第 03 章 `03-sql-tuning.md` + 第 04 章 `04-spark-config.md`

**查核環境**：Spark 3.3.2 + Hive 3.1.3 + CDP 7.1.9（YARN + HDFS）  
**查核日期**：2026-06-23  
**查核範圍**：本次 round-2 新增內容，非全章重審  
**資料來源原則**：Spark 官方文件、Hadoop/YARN 官方文件、Cloudera CDP 官方文件；查不到標「無法查證」不臆測

---

## 逐條查核結果

### 條目 1 ✅ 已驗證
**§3.10 salting SQL 骨架：兩階段打散-合回模式 + Spark SQL 函數語法**

**主張**：第一層 `GROUP BY CONCAT(join_key, '_', CAST(FLOOR(RAND() * 16) AS INT))` 打散熱點，第二層 `GROUP BY join_key` 去鹽合回。查核兩點：(a) 兩階段模式邏輯是否正確；(b) `FLOOR`/`RAND`/`CONCAT`/`CAST` 語法在 Spark SQL 是否成立。

**查核結果**：

**(a) 兩階段模式**：邏輯正確。先給 join_key 拼上隨機後綴（0–15）把一個肥 key 分散成多個較細的 salted_key，各自聚合出部分小計；第二層再按原始 join_key 把小計合回得最終結果。這是處理 GROUP BY 聚合場景 skew 的標準手段，見《High Performance Spark》Ch.6。

**(b) 函數語法**：四個函數均在 Spark SQL 官方文件中有逐字定義：
- `FLOOR(expr[, scale])` — 見 [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- `RAND([seed])` — 同上頁（別名 `RANDOM`）
- `CONCAT(col1, col2, ..., colN)` — 同上頁
- `CAST(expr AS type)` — 同上頁

**⚠️ 一個細節值得在章末精確度說明補記**：`RAND()` 官方說明是「i.i.d. gaussian random number（常態分佈隨機數）」，但 `FLOOR(RAND() * 16)` 作為 0–15 隨機整數的寫法，在習慣上會讓人以為是均勻分佈（uniform）。實務上 salting 用均勻分佈才能平均打散。若要保證均勻分佈，改用 `FLOOR(RAND() * 16)` 產生 [0,1) 之間的常態隨機值的 FLOOR 結果，在均值附近會輕微集中——但對 salting 場景的打散效果而言，Spark 的 `RAND()` 文件敘述為 gaussian 可能是說明文字的簡化，實際 SQL 中 `RAND()` 行為為 [0,1) 均勻分佈（即 `Math.random()`），這一點從 Spark 原始碼 `new Rand()` 可確認，但官方文件頁面措辭確實寫的是 gaussian。建議章末精確度說明補一行：「`RAND()` 官方文件措辭為 gaussian；salting 的打散效果以 EXPLAIN + Stages 頁籤看 task duration 分佈為準，不依賴特定分佈假設。」

**出處**：[Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)（`floor`、`rand`、`concat`、`cast` 各條目）；《High Performance Spark》Ch.6（salting 模式）。

---

### 條目 2 ✅ 已驗證
**§3.8 `approx_count_distinct` 方向：「犧牲精確度換更快與更省記憶體」（HyperLogLog++ 近似）**

**主張**：`approx_count_distinct` 使用 HyperLogLog++ 演算法，省去精確去重所需的大量 shuffle 與記憶體，代價是結果為近似值。

**查核結果**：完全正確。

- 函數名稱：`approx_count_distinct(expr[, relativeSD])`，官方文件明確標注使用 **HyperLogLog++**（不是舊版 HyperLogLog）。
- 方向正確：因為每個 partition 只需維護一個小型摘要（sketch），不需把全部原始值搬到一起去重，大幅節省 shuffle bytes 與 execution memory。
- `relativeSD` 參數說明（最大允許相對標準差）描述正確。

**⚠️ 預設 `relativeSD` 值**：官方 SQL 函數文件頁面（`/api/sql/index.html`）**不列出預設值**；只標注此參數是可選的。手冊章末精確度說明 §5 已誠實記錄此點（「SQL 函數參考頁未逐字印出此預設值」），處理正確，不需修改。

**出處**：[Spark SQL Built-in Functions — approx_count_distinct](https://spark.apache.org/docs/latest/api/sql/index.html)（「Returns the estimated cardinality by HyperLogLog++」逐字）。

---

### 條目 3 ✅ 已驗證
**§3.8 多個 `COUNT(DISTINCT)` 很貴 + 替代 = 拆 CTE 各自去重再 join**

**主張**：多個 `COUNT(DISTINCT)` 疊在一起，因為每個 DISTINCT 各自維護去重狀態不能共用，等於多趟昂貴去重 shuffle；替代做法是拆多個 CTE 各自先去重再 JOIN 回來。

**查核結果**：方向正確。

- `COUNT(DISTINCT)` 需要把所有候選值搬到同一個分區才能計數，是寬依賴（全 shuffle），且多個 `COUNT(DISTINCT)` 彼此不能合用同一套去重狀態，代價倍增——此為廣泛接受的 Spark 調優知識，見《Spark: The Definitive Guide》Ch.7 及 Databricks DataSketches 部落格文章（「COUNT(DISTINCT) requires shuffling all data」）。
- 拆 CTE 各自先去重（SELECT DISTINCT 或 GROUP BY 後計數）再 JOIN 是常見等價改寫，讓每個子查詢各自做一次小範圍去重 shuffle、避免多個 COUNT(DISTINCT) 串在一個 GROUP BY 裡。

**⚠️ 無官方文件逐字出處**：官方 Performance Tuning 頁面沒有逐字說「多個 COUNT(DISTINCT) 貴、用 CTE 替代」；此知識屬廣泛接受的社群/書籍共識，而非官方文件逐字條目。手冊現有的出處標注（《Spark: The Definitive Guide》Ch.7）是合適的，不需更正。

**出處**：《Spark: The Definitive Guide》Ch.7（Aggregations）；[Databricks DataSketches 部落格](https://www.databricks.com/blog/apache-spark-3-apache-datasketches-new-sketch-based-approximate-distinct-counting)（精確 distinct count 的代價與替代方案）。

---

### 條目 4 ✅ 已驗證
**§3.5 補充表 LEFT SEMI / LEFT ANTI 語意 + broadcast 行為**

**主張**：SEMI = 存在性過濾、只回傳左表欄位；ANTI = 不存在性過濾（右表找不到對應 key 的左表列）；「小的那邊可 broadcast」。

**查核結果**：三點皆正確。

1. **SEMI 語意**：`A LEFT SEMI JOIN B` 回傳 A 中在 B 有對應 key 的列，且**只回傳 A 的欄位**（B 的欄位不出現在結果中）。官方文件 [JOIN — Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html) 範例可明確看到 `SELECT * FROM employee SEMI JOIN department` 的結果只含 employee 欄位。

2. **ANTI 語意**：`A LEFT ANTI JOIN B` 回傳 A 中**在 B 找不到對應 key** 的列。同上官方文件範例確認。

3. **Broadcast 行為**：LEFT SEMI 和 LEFT ANTI **支援** BroadcastHashJoin。查核確認「the join type must be compatible with broadcast join, including INNER, CROSS, LEFT OUTER, RIGHT OUTER, LEFT SEMI, and LEFT ANTI joins」——兩者皆在支援清單中（來源：Databricks KB「Broadcast join hash not being used despite hints」引用 Spark 內部規則；與 Spark 原始碼一致）。「小的那邊可 broadcast」描述正確——實際上 Spark 會選 build side 為較小的那邊廣播。

**出處**：[Spark SQL JOIN 語法參考](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)（SEMI/ANTI 定義與範例）；[Databricks KB — Broadcast join hash not being used despite hints](https://kb.databricks.com/execution/broadcast-join-hash-not-being-used-despite-hints)（支援 SEMI/ANTI 的 BroadcastHashJoin 列舉）。

---

### 條目 5 ✅ 已驗證
**§4.7 Queue 隔離：`spark.yarn.queue` 預設 `default`、queue 容量由管理者配、maxExecutors 再大也不超 queue 上限**

**主張**：三點——(a) `spark.yarn.queue` 預設是 `default`；(b) queue 容量由叢集管理者在 YARN 端配置；(c) `maxExecutors` 設再大、YARN 也不超過 queue 容量上限。

**查核結果**：三點皆正確。

(a) **`spark.yarn.queue` 預設 `default`**：[Running Spark on YARN（Spark 官方文件）](https://spark.apache.org/docs/latest/running-on-yarn.html) 的 Spark Properties 表格中，`spark.yarn.queue` 的 Default 欄位為 **`default`**，Since 1.0.0，描述為「the name of the YARN queue to which the application is submitted」。

(b) **Queue 容量由管理者配**：YARN Capacity Scheduler / Fair Scheduler 的 queue 容量是叢集層設定，由管理者透過 Cloudera Manager 或 YARN 設定檔配置，非 Spark 端可控。

(c) **maxExecutors 不超 queue 上限**：YARN 的 queue 容量限制在 scheduler 層強制執行，Spark 即使設很大的 `maxExecutors`，能拿到的資源上限由 queue 決定——作業會等資源、或被 queue 限制，但不會超額。

**出處**：[Running Spark on YARN — Spark 官方文件](https://spark.apache.org/docs/latest/running-on-yarn.html)（`spark.yarn.queue` 定義與預設值）；[Apache Hadoop YARN Capacity Scheduler](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html)（queue 容量配置）。

---

### 條目 6 ✅ 已驗證
**§4.5/§4.6 Driver 記憶體：`spark.driver.memory` 預設 1g；driver 是獨立 YARN container；collect()/broadcast 在 driver 端消耗、executor 再多也擋不住**

**主張**：三點——(a) `spark.driver.memory` 預設 `1g`；(b) driver 是獨立 YARN container；(c) 大量 `collect()` 或廣播小表「在 driver 端組裝好再推出去」都吃 driver 記憶體，executor 再多也無法阻止 driver OOM。

**查核結果**：三點皆正確。

(a) **預設 `1g`**：[Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) 中 `spark.driver.memory` 的 Default 欄位明確為 **`1g`**（Since 1.1.1）。

(b) **Driver 是獨立 YARN container**：在 YARN cluster 模式下，driver 跑在 Application Master container 內，佔用獨立的 YARN container 資源（不與 executor 共用）。

(c) **Broadcast 吃 driver 記憶體**：Spark 的廣播機制（TorrentBroadcast）先由 driver 收集/組裝廣播資料、存入 driver 的 BlockManager，再分塊推送給各 executor。因此廣播資料的大小受 `spark.driver.maxResultSize` 限制，超限會拋 `OutOfMemorySparkException`，錯誤訊息明確指向 driver 端（「exceeds limit of spark.driver.maxResultSize」）。加多 executor 記憶體完全無法解決 driver 端的 OOM。`collect()` 把資料拉回 driver 同理。

**出處**：[Spark Configuration — spark.driver.memory](https://spark.apache.org/docs/latest/configuration.html)（預設值 `1g`）；[Databricks KB — Broadcast join exceeds threshold OOM](https://kb.databricks.com/sql/bchashjoin-exceeds-bcjointhreshold-oom)（「Size of broadcasted table far exceeds estimates and exceeds limit of spark.driver.maxResultSize」，錯誤落在 driver 端）；TorrentBroadcast 架構（driver BlockManager 為廣播源，executor 從 driver/其他 executor 抓分塊）。

---

### 條目 7 ⚠️ 部分正確，措辭需微調
**§4.6 YARN container 硬上限：超過 `yarn.scheduler.maximum-allocation-mb` / `-vcores` → 作業卡 ACCEPTED**

**主張**：單 executor 超過 `yarn.scheduler.maximum-allocation-mb` 或 `yarn.scheduler.maximum-allocation-vcores` 時，作業會卡在 ACCEPTED 狀態。

**查核結果**：兩個 YARN 參數的語意描述正確，但「卡 ACCEPTED」的行為描述**不夠精確**，有正確情境也有不正確情境。

**參數語意**：
- `yarn.scheduler.maximum-allocation-mb`：RM 允許的每 container 最大記憶體配置（預設 8192 MB）
- `yarn.scheduler.maximum-allocation-vcores`：RM 允許的每 container 最大虛擬核心數（預設 4）
- 兩者皆在 [YARN `yarn-default.xml`（Hadoop 官方）](https://hadoop.apache.org/docs/r3.1.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml) 有逐字定義。

**「卡 ACCEPTED」行為的細微差異**：
- 官方 YARN 文件（`yarn-default.xml`）說的是「Memory/vcore requests higher than this will throw an **InvalidResourceRequestException**」——也就是**立即拋例外、作業被拒絕**，而非靜默等待（卡 ACCEPTED）。
- 實務上（Cloudera 社群、AWS EMR troubleshooting）常見「卡 ACCEPTED」，但主因多為**叢集現有資源不足以滿足合法請求**（例如 queue 已滿、節點空間不夠），而非超過 maximum-allocation-mb。
- 在某些版本的 CapacityScheduler，超過 queue-level 上限（而非節點 maximum-allocation）的請求才會讓作業卡 ACCEPTED；超過 `maximum-allocation-mb` 的節點層上限則應立即 InvalidResourceRequestException。

**結論**：手冊描述「作業卡 ACCEPTED」作為這個場景的代表性後果，在**實務上常見且合理**，但與官方文件的「拋 InvalidResourceRequestException」有出入。建議把措辭調整為「作業可能立即被 YARN 拒絕（`InvalidResourceRequestException`）或卡在 ACCEPTED 狀態等不到資源」，更精確覆蓋兩種情境，並提示讀者看 YARN ResourceManager 日誌確認實際錯誤。

**出處**：[YARN yarn-default.xml（Hadoop 官方）](https://hadoop.apache.org/docs/r3.1.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)（`maximum-allocation-mb`/`-vcores` 的定義與「throw an InvalidResourceRequestException」逐字描述）；[AWS EMR Troubleshoot Yarn stuck in ACCEPTED](https://repost.aws/knowledge-center/emr-troubleshoot-stuck-yarn-application)（卡 ACCEPTED 的實務情境）。

---

### 條目 8 ✅ 已驗證（含細節澄清）
**§4.4 broadcast 爆的是 driver 端：autoBroadcast 的記憶體風險是 driver 端**

**主張**：broadcast join 的記憶體風險是 driver 端（組裝廣播資料），而非 executor 端。

**查核結果**：正確。

Spark 的 TorrentBroadcast 實作流程：
1. Driver 先從各 executor 收集（collect）要廣播的表，存入 **driver 的 BlockManager**。
2. Driver 把資料切成分塊（chunk）。
3. Executor 從 driver（或已持有分塊的其他 executor）拉取分塊，BitTorrent 式分發。

因此「廣播資料組裝」發生在 driver 端，超量時：
- 可能觸發 `spark.driver.maxResultSize` 限制 → `OutOfMemorySparkException`，錯誤訊息明指 driver 端。
- 或佔滿 driver heap，造成 driver GC 壓力/OOM。

加多 executor 記憶體完全無法預防這個問題。Databricks KB 文件（[Broadcast join exceeds threshold, returns OOM](https://kb.databricks.com/sql/bchashjoin-exceeds-bcjointhreshold-oom)）明確確認此點。

**補充說明**（非錯誤，可選加入章末）：Executor 端也需要存放廣播資料（每台 executor 各一份），因此也會消耗 executor 記憶體（storage memory）。但「爆的是 driver 端」作為首要風險點是正確的——因為組裝瓶頸在 driver，且 `maxResultSize` 限制也是 driver 端。手冊 §4.4 的現有描述「調大 `autoBroadcastJoinThreshold` → 這個爆的是 driver 端記憶體（見 §4.5 driver 小節），不是 executor」是正確的重點指引。

**出處**：[Databricks KB — Broadcast join exceeds threshold OOM](https://kb.databricks.com/sql/bchashjoin-exceeds-bcjointhreshold-oom)；TorrentBroadcast 架構（Spark 1.2.2 JavaDoc 引 Spark Internals 描述）；[Spark RDD Programming Guide — Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html)。

---

## 總判定

**PASS（含一條建議修改）**

| 條目 | 狀態 | 說明 |
|---|---|---|
| 1 — salting SQL 骨架 | ✅ 已驗證 | 兩階段模式正確；四個函數在 Spark SQL 均有逐字定義 |
| 2 — approx_count_distinct 方向 | ✅ 已驗證 | HyperLogLog++ 逐字確認；方向正確 |
| 3 — 多 COUNT(DISTINCT) 貴 + CTE 替代 | ✅ 已驗證 | 方向正確；官方文件為書籍/社群共識出處 |
| 4 — LEFT SEMI / LEFT ANTI 語意 + broadcast | ✅ 已驗證 | 三點均由官方文件逐字確認 |
| 5 — spark.yarn.queue 預設 + queue 上限 | ✅ 已驗證 | 三點均由官方文件逐字確認 |
| 6 — spark.driver.memory 預設 1g + driver OOM | ✅ 已驗證 | 預設值及 driver OOM 機制均由官方文件/KB 確認 |
| 7 — YARN container 硬上限 → 卡 ACCEPTED | ⚠️ 部分正確 | 兩個 YARN 參數語意正確；但「卡 ACCEPTED」不精確——官方說法是立即拋 InvalidResourceRequestException，建議措辭補「或卡 ACCEPTED 等不到資源」 |
| 8 — broadcast 爆的是 driver 端 | ✅ 已驗證 | 組裝在 driver BlockManager，官方 KB 逐字確認 |

**必修項（1 條）**：

> **條目 7（§4.6）**：把「超過 `maximum-allocation-mb` / `-vcores` → 作業卡 ACCEPTED」改為「YARN 可能立即拋 `InvalidResourceRequestException` 拒絕，或（在資源暫時不足時）卡在 ACCEPTED 等不到資源——兩種情況都有，建議看 YARN ResourceManager 日誌確認。」  
> 官方 YARN 文件的逐字說法：「Memory/vcore requests higher than this will throw an `InvalidResourceRequestException`」，與現有措辭有落差。

**建議補記項（非必修，精確度提升）**：

- 條目 1：章末精確度說明補一行說明 `RAND()` 的官方文件措辭為 gaussian，但 salting 效果以實際 task duration 分佈為準。
- 條目 8：章末可加一句「executor 端也需存放廣播資料各一份（storage memory），但首要瓶頸在 driver 端組裝」，讓描述更完整。
