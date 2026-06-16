# 第 03 章技術審查記錄：SQL 寫法優化

審查員：技術審查 subagent（Opus）
日期：2026-06-16
對齊版本：Spark 3.3.x（AQE 預設開）+ Hive 3.1.3 / CDP Private Cloud Base 7.1.9（YARN+HDFS，另有 Impala）
審查素材：`docs/handbooks/spark-tuning/03-sql-tuning.md`
權威來源限：Spark 3.3 官方文件（SQL Performance Tuning、SQL ref：hints / functions、Data Sources：Parquet）、《Spark: The Definitive Guide》《High Performance Spark》、Cloudera CDP 官方文件。

> 規則：每查一條 append 一條；判定附出處；查不到標「無法查證」、不臆測。
> 自動工具對 spark.apache.org 的 3.3.2 版頁常回 404，改用 docs/latest 查證（行為自 3.x 穩定）。
> 主筆已查證、可直接採信的關鍵事實（避免重複 WebFetch）：
> - autoBroadcastJoinThreshold 預設 10485760 (10MB)
> - join hints：BROADCAST/MERGE/SHUFFLE_HASH/SHUFFLE_REPLICATE_NL；別名 BROADCASTJOIN/MAPJOIN；hint 是建議不保證
> - AQE skew：enabled=true、factor=5.0、threshold=256MB；判定＝同時 >factor×median 且 >threshold
> - AQE：adaptive.enabled=true；adaptive.autoBroadcastJoinThreshold=none(未設=靜態門檻)；coalescePartitions.enabled=true、advisoryPartitionSizeInBytes=64MB
> - approx_count_distinct=HLL++；rsd 預設 0.05 出自 DataFrame API（SQL 函數頁未逐字印）
> - spark.sql.parquet.filterPushdown 預設 true

---

（以下逐條 append）

---

## 逐條查證（Opus，2026-06-16）

### §3.1 本章地圖

**主張 1**「一條查詢成本主要來自讀資料與搬資料（shuffle）；改 SQL 變快只有少讀/少搬兩方向」。
✅ 已驗證（概念框架，非可逐字核的 config）。這是第 01 章的主軸延伸，分類合理；shuffle＝寬依賴跨機器重分配、掃描＝讀 partition，均為標準 Spark 模型。出處：sql-performance-tuning.html（shuffle/AQE 機制）、《Spark: The Definitive Guide》Ch.8/15。無正負號或預設值問題。

---

### §3.2 partition 裁剪（partition pruning）

**主張 2**「分區大表磁碟上不同分區值放不同目錄；WHERE 過濾分區欄位 → Spark 只讀命中目錄，其他不碰（partition pruning）」。
✅ 已驗證。Parquet Partition Discovery 原文：「In a partitioned table, data are usually stored in different directories, with partitioning column values encoded in the path of each partition directory. All built-in file sources (...Parquet) are able to discover and infer partitioning information automatically.」WHERE 命中分區欄位 → 只讀對應目錄為 partition pruning 標準行為。出處：docs/latest/sql-data-sources-parquet.html。因果方向（過濾分區欄位→少讀目錄）正確。

**主張 3**「EXPLAIN 的 Scan 那行會出現 `PartitionFilters: [month = '2026-05']`；沒裁到則無 month」。
✅ 已驗證（第 02 章基準已確認）。`PartitionFilters` 為 FileScan 節點實際欄位名，predicate 用於分區裁剪時列於此，缺席＝未裁。本章已自附「以 EXPLAIN 為準」。負號正確。

**主張 4**「partition 裁剪幾乎沒壞處，但前提 (1) 表得真按該欄位分區 (2) 別在分區欄位上包函數（substr 會失效）」。
✅ 已驗證（方向正確）。(1) 無分區則 WHERE 省不了讀檔，屬實。(2)「函數包住分區欄位 → 裁剪失效」與 §3.4 同一機制，方向正確；本章已標「以 EXPLAIN 為準」。屬安全建議，非過度宣稱。

**主張 5**「36 個月差 36 倍」。
✅ 自附 ⚠️ 已誠實標為「各月筆數視為相近的算術示意」。非缺陷。

**出處引用核**：§3.2 來源列 Parquet Partition Discovery（pruning）＋ sql-performance-tuning（PartitionFilters 呈現）。⚠️ 注意：`PartitionFilters` 這個欄位名本身在 sql-performance-tuning.html 並無逐字定義（該頁談 AQE/hint/coalesce），其實際出處是 EXPLAIN FORMATTED 的 FileScan 輸出（第 02 章 §2.3 已驗）。本章把它掛在 sql-performance-tuning 略不精確，但第 02 章交叉引用已補；屬輕微。

---

### §3.3 column pruning（別 SELECT *）

**主張 6**「Parquet/ORC 列式格式：要哪幾欄就只讀哪幾欄，用不到的欄位資料塊不從磁碟讀（column pruning）」。
✅ 已驗證（概念正確，但官方 Parquet 頁未逐字寫此句）。列式格式的 columnar projection / column pruning 是 Parquet 核心特性與 Spark FileScan 的 `ReadSchema` 機制；《Spark: The Definitive Guide》Ch.8 有述。⚠️ docs/latest/sql-data-sources-parquet.html **並未**明文出現「column pruning / 只讀用到欄位」字句（本次 WebFetch 確認該頁無此段）。本章 §3.3 來源同時引《Definitive Guide》Ch.8，書才是 column pruning 的真正出處；單引 Parquet 官方頁支撐「column pruning」字面略弱，但結論正確。建議：來源敘述以《Definitive Guide》Ch.8 為主、Parquet 官方頁為輔。

**主張 7**「ReadSchema 只列你要的欄位；SELECT * 列全部」。
✅ 已驗證（概念）。`ReadSchema` 為 FileScan 節點實際欄位名（第 02 章已驗 EXPLAIN 機制）。正確。

**主張 8**「column pruning 純賺；SELECT * 後再 JOIN 或包 view 用 * 會讓它失效」。
✅ 合理且方向正確。屬安全工程建議，非可逐字核的官方主張。無正負號問題。

**主張 9**「1000 欄差幾十倍」。✅ 已自附 ⚠️ 量級示意。非缺陷。

---

### §3.4 predicate pushdown 與失效

**主張 10**「`spark.sql.parquet.filterPushdown` 預設 true」。
✅ 已驗證。Parquet 頁原文：Property `spark.sql.parquet.filterPushdown`、Default `true`、"Enables Parquet filter push-down optimization when set to true."、Since 1.2.0。出處：docs/latest/sql-data-sources-parquet.html。

**主張 11**「Parquet 內部存每個資料塊 min/max 統計；下推後整個不可能命中的資料塊被跳過、不解壓縮（row-group skipping）」。
✅ 已驗證（機制正確，官方 Parquet 頁未逐字詳述 min/max row-group skipping）。row-group statistics（min/max）跳塊是 Parquet 格式規格與 filter pushdown 的標準運作；Spark 官方 Parquet 頁確認 filterPushdown=true 啟用「filter push-down optimization」但**未逐字**寫「依 min/max 跳 row group」（本次 WebFetch 確認該頁僅提 `recordLevelFilter.enabled` 暗示用到統計）。本章已自附 ⚠️「以 EXPLAIN 看 PushedFilters 為準」。結論正確、方向正確；逐字數字無誤用。

**主張 12（重點：因果正負號）**「用函數包住欄位（substr(month,1,4)、year(txn_date)）→ Spark 無法用欄位統計跳塊 → 下推/裁剪失效、整批讀進來再算；裸欄位 vs 常數比較才生效」。
✅ 方向正確。對非分區欄位包函數使 Spark 無法把謂詞轉成資料源可用的 filter（min/max 統計無法套在 f(col) 上），下推失效；對分區欄位包函數則裁剪失效。本章已誠實自附 ⚠️「個別函數是否仍能部分下推依版本/資料源而異，以 EXPLAIN 為準」。⚠️ 補強（非錯）：Spark 對某些「裸函數＝常數」的情形仍可能下推（如 `CAST(col)` 在特定型別、或 `to_date` 等），但「裸欄位最安全」這建議永遠成立，本章措辭未寫成硬限制（用「最好」「通常」），不算過度宣稱。
　- 細查 `WHERE substr(month,1,4)='2026'` 失效是否絕對：對**分區**欄位，substr 屬非單調的字串切片，Catalyst 一般無法轉成 partition filter → 裁剪失效，方向對。✅

**主張 13**「partition 裁剪跳整個目錄、pushdown 跳檔案內資料塊；一個在目錄層一個在檔案層」。
✅ 已驗證（區分正確）。partition pruning＝目錄層（分區欄位）、predicate pushdown＝檔案內 row-group（一般欄位），兩者層級區分與 Spark 行為一致。出處：Parquet Partition Discovery + filterPushdown。

**出處引用核**：§3.4 來源引 Parquet 頁（filterPushdown 預設 true）✅ 正確。

---

### §3.5 broadcast vs sort-merge join

**主張 14**「Sort-Merge Join：兩張表都按 key shuffle、各自排序後合併，兩次 shuffle」。
✅ 已驗證。SMJ 需兩側按 join key 重分配（各一次 shuffle）後排序合併，為標準物理 join；《Spark: The Definitive Guide》Ch.8（Joins）。「兩次 shuffle」指兩側各一次 Exchange，正確。

**主張 15**「Broadcast Hash Join：小表整份複製到每台 executor、大表留原地就地比對，完全不為此 join 做 shuffle」。
✅ 已驗證。BHJ 把小表 broadcast 至各 executor、大表免 shuffle 就地 hash 比對；《Spark: The Definitive Guide》Ch.8。方向正確。

**主張 16**「Spark 估計一邊 < `autoBroadcastJoinThreshold`（預設 10MB）→ 自動走 broadcast」。
✅ 已驗證。sql-performance-tuning.html：`spark.sql.autoBroadcastJoinThreshold` 預設 10485760 (10 MB)，"Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join."。10MB 正確。

**主張 17（AQE runtime 轉 broadcast）**「Spark 3.3 AQE：就算計畫是 SortMergeJoin，執行途中發現某邊 shuffle 後實際很小，AQE 動態改 broadcast」。
✅ 已驗證。sql-performance-tuning.html「Converting sort-merge join to broadcast join」原文：「AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side are smaller than the adaptive broadcast hash join threshold.」方向、版本（3.3 AQE 預設開）正確。

**主張 18**「broadcast 省 shuffle 但付記憶體：小表在 driver 收集再複製到每台 executor，硬廣播大表會 OOM」。
✅ 已驗證（機制正確）。broadcast 經 driver collect → 各 executor 一份副本，過大會撐爆記憶體；這正是 threshold 存在的理由。《Definitive Guide》Ch.8 與 autoBroadcastJoinThreshold 說明一致。方向正確。

**出處引用核**：§3.5 來源引 sql-performance-tuning（10MB、AQE 轉 broadcast）＋《Definitive Guide》Ch.8（兩種物理 join）✅ 正確。

---

### §3.6 手動 BROADCAST hint

**主張 19**「hint 寫在 SELECT 後 `/*+ ... */`，括號放要被廣播的表（別名 b）」。
✅ 已驗證。hints 頁語法 `SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;`。用別名亦可。正確。

**主張 20**「`BROADCAST` 可寫成別名 `BROADCASTJOIN` 或 `MAPJOIN`，效果相同」。
✅ 已驗證。hints 頁原文：「The aliases for `BROADCAST` are `BROADCASTJOIN` and `MAPJOIN`.」完全正確。

**主張 21（取捨：hint 會蓋過門檻）**「hint 會蓋過 autoBroadcastJoinThreshold；對很大的表下 BROADCAST，Spark 會照辦然後 OOM」。
✅ 方向正確。hint 是強指示、繞過大小門檻（這正是它的用途），故對大表下 hint 會嘗試 broadcast 而 OOM。與 hint 語意一致。

**主張 22（hint 是建議不保證）**「hint 是建議不是命令保證；某些 join 型別（如某些 outer join）不支援 broadcast 某一邊時，Spark 會忽略它」。
✅ 已驗證。hints 頁原文：「Since a given strategy may not support all join types, Spark is not guaranteed to use the join strategy suggested by the hint.」本章舉「某些 outer join 不支援 broadcast 某一邊」為具體例 —— 此為正確實例：broadcast 需廣播被 join 的那側，left outer join 無法 broadcast 左（streamed）側、right outer 無法 broadcast 右側，故會被忽略。方向、實例皆正確。

**主張 23**「沒統計 → 被當大表 → SortMergeJoin」。
✅ 已自附 ⚠️「常見因果方向，實際由估計大小與門檻共同決定，以 EXPLAIN 為準」。誠實，非缺陷。

**出處引用核**：§3.6 來源引 hints 頁（hint 語法/別名/不保證）＋ sql-performance-tuning（10MB/AQE）✅ 正確。

---

### §3.7 join 隱藏陷阱（型別不符、爆量）

**主張 24（陷阱一）**「join key 型別不同（bigint vs string）→ Spark 不報錯、自動插入隱式 cast 包在 key 上 → 可能讓 broadcast/pushdown 打折、key 比對多繞一層、靜默變慢」。
⚠️ 方向大致正確、但「broadcast 失效」這一條本章已自我降級（自附 ⚠️「『型別不符一定讓 broadcast 失效』說法過強，主要後果是多一層 cast」）。查證：型別不符確實插入隱式 cast（Catalyst type coercion），cast 包在 join key 上會妨礙 partition pruning / 部分 pushdown（同 §3.4 機制）；但**對 broadcast 本身的影響有限**——broadcast 看的是表大小估計，cast 不直接擋 broadcast。本章正文措辭已用「可能讓 broadcast／pushdown 的相關優化打折」（「可能」「相關」皆為 hedge），未寫成硬限制，且 footnote 明確修正「型別不符一定讓 broadcast 失效＝說法過強」。判定：**處理得當，非缺陷**。注意正文 §3.7 一處仍說「可能擋掉優化」（before 範例註解 ②），與 footnote 一致（用「可能」）。
　- ⚠️ 一個值得提的細節：實務上若 string 欄位含非數字，`CAST(string AS BIGINT)` 會產生 NULL → join 結果**變了**（不只是變慢）。本章把型別不符純粹當「變慢、結果是對的」陳述（§3.7「你的查詢結果是對的，只是慢」）——這在「兩邊都是數字內容、只是宣告型別不同」時成立，但若一邊真是非數字字串，隱式 cast 可能改變結果。屬「可加強」：可補一句「前提是兩邊內容本可互轉；若 string 含非數字，cast 還會改變結果」。

**主張 25（陷阱一治本）**「最糟的是它完全靜默、結果是對的只是慢；治本是建表起同型別，CAST 只是補救」。
✅ 方向正確（在內容可互轉的前提下，見上）。「對齊型別永遠安全」與 footnote 一致。

**主張 26（陷阱二）**「join key 在某邊不唯一（一對多）→ 輸出列數是乘積非相加；條件寫漏 → 笛卡兒積把叢集塞爆」。
✅ 已驗證（關聯代數常識）。一對多 join 列數放大、缺 join 條件→ cross join，為標準 SQL/關聯代數行為。無版本相依、無需官方頁。方向正確。

**主張 27（偵測）**「SQL 頁籤看 Join 算子 `number of output rows`；輸入 3000 萬輸出 3 億＝一對多或近笛卡兒」。
✅ 已驗證（第 02 章 §2.5 基準）。`number of output rows` 為 SQL UI 實際 metric。正確。

**主張 28（解法）**「先過濾、先聚合再 join，把兩邊縮小再碰頭」。
✅ 標準優化建議（filter/aggregate pushdown 精神），方向正確。

**出處引用核**：§3.7 來源引 sql-performance-tuning + 《Definitive Guide》Ch.8 + 第 02 章 §2.5。⚠️ 「型別不符會插入隱式 cast、影響計畫」這句掛在 sql-performance-tuning.html 略不精確（該頁不專講 type coercion；隱式 cast 屬 Catalyst type coercion / SQL ref，非 performance-tuning 頁主題）。但有 footnote 自我修正，且《Definitive Guide》Ch.8 為輔，結論不誤。屬輕微出處掛靠不夠精準。

---

### §3.8 聚合成本（GROUP BY / DISTINCT / COUNT DISTINCT）

**主張 29**「GROUP BY、DISTINCT 都是寬依賴、每次一次 shuffle；GROUP BY+SUM/COUNT/AVG 可先本地（partial/map-side）聚合縮小再 shuffle，較便宜」。
✅ 已驗證。map-side/partial aggregation 把各 partition 先聚成小計再 shuffle，為 Spark HashAggregate 的 partial+final 兩階段標準行為；《Spark: The Definitive Guide》Ch.7（Aggregations）。Databricks 確認 HLL「map + combine、不需 shuffle」、一般 aggregate 可 map-side combine。方向正確。

**主張 30**「COUNT(DISTINCT) 要去重 → 得先把所有不同值搬到一起才能數，本地能先做的有限，高基數很貴；多個 COUNT(DISTINCT) 疊更糟」。
✅ 已驗證（方向正確）。精確 distinct 需把（key, distinct-value）分佈搬到一起去重，map-side 縮減有限；多個 distinct 需 expand/多路 → 更貴。《Definitive Guide》Ch.7。⚠️ 細節精確度：「本地能先做的有限」對 `COUNT(DISTINCT cust_id) GROUP BY month` 而言，partial 階段仍可在各 partition 先局部去重（partial distinct），並非完全不能 map-side；但相對 SUM 類，distinct 的 shuffle 量級確實大得多。本章用「有限」（非「完全不能」），措辭分寸正確，非缺陷。

**主張 31**「`approx_count_distinct` 用 HyperLogLog++ 估計、不需把所有值搬到一起精確去重 → 快、省記憶體；代價是近似值（預設最大相對誤差約 5%）；可傳第二參數調誤差（更小→更多資源）」。
✅ 已驗證。SQL 函數頁：`approx_count_distinct(expr[, relativeSD])`「Returns the estimated cardinality by HyperLogLog++. `relativeSD` defines the maximum relative standard deviation allowed.」HLL 不需 full de-dup shuffle（Databricks 確認 map+combine）。預設 rsd=0.05 出自 DataFrame/Scala API（已查證：PySpark/Scala functions 預設 rsd=0.05，"maximum estimation error allowed (default = 0.05)"）。「更小誤差→更多資源」方向正確（Databricks：rsd<0.01 不如直接用 count_distinct）。

**主張 32（§3.8 footnote 的關鍵標註）**「預設 relativeSD≈0.05（5%）出自 DataFrame API；SQL 函數參考頁未逐字印此預設值」。
✅ 已驗證、標註正確（這正是主筆要我確認的點）。本次 WebFetch docs/latest/api/sql/index.html 確認：SQL 函數頁**只**寫「`relativeSD` defines the maximum relative standard deviation allowed」、**未**印任何預設數字（0.05/5% 皆無）。而 PySpark/Scala API 確為 default=0.05。本章把 5% 歸給 DataFrame API、未誤稱為「SQL 官方文件逐字數字」——**標註完全正確，無過度宣稱**。

**主張 33（取捨）**「對帳/稽核/財報用精確 COUNT(DISTINCT)；看趨勢/量級用 approx」。
✅ 合理的工程取捨，無正負號問題。

**出處引用核**：§3.8 來源引《Definitive Guide》Ch.7（Aggregations）✅ 正確（Ch.7 確為 Aggregations，且 partial aggregation 屬該章）；approx_count_distinct 引 SQL Functions 頁 ✅ 正確。

---

### §3.9 window function（PARTITION BY = 一次 shuffle）

**主張 34**「window function 的 `OVER (PARTITION BY x ...)` 按 x 做一次 shuffle，與 GROUP BY x 同等量級」。
✅ 已驗證（機制正確）。window 需把同 PARTITION BY key 的列重分配到一起才能在窗內排序/累加，計畫上是 Window 算子前置一個 Exchange（=shuffle）。《Definitive Guide》Ch.7（Window Functions 屬 Ch.7，已查證 TOC）。方向正確。

**主張 35**「不同 PARTITION BY key = 不同 shuffle；多個不同 key 的 window = 多次 shuffle 疊；同一 PARTITION BY 可共用一次 shuffle」。
✅ 方向正確。Catalyst 對相同 distribution 需求可避免重複 Exchange（EnsureRequirements 不會為已滿足的分佈再加 Exchange）；不同 key 則各需一次。本章已自附 ⚠️「實際是否合併以 EXPLAIN 的 Exchange 個數為準」。誠實，非缺陷。

**主張 36**「每個 Window 算子前面有一個 Exchange；數 Exchange 即知付幾次 shuffle」。
✅ 已驗證（概念，第 02 章 §2.3 Exchange 基準）。同 PARTITION BY 的相鄰 Window 可共用前置 Exchange，故「每個 Window 前必有一個 Exchange」嚴格說在共用情形下不是「各一個」——但本章下一句即說「同一 PARTITION BY 可共用」，前後文一致，不矛盾。輕微：單看主張 36 那句像「逐一對應」，靠上下文修正。

**主張 37**「只要每組取第一筆這類需求，有時 GROUP BY+聚合更便宜，不一定要 window」。
✅ 合理建議，方向正確。

**出處引用核**：§3.9 來源引《Definitive Guide》Ch.7（Window Functions）✅ 正確（Window Functions 確在 Ch.7）＋ sql-performance-tuning（shuffle 機制，泛指）。

---

### §3.10 處理 skew

**主張 38（第一層 AQE）**「Spark 3.3 AQE 內建 skew join 處理（預設開）：偵測異常大 partition、自動切成幾小塊並行；判定＝某 partition 同時 > median×factor(預設5) 且 > threshold(預設256MB)」。
✅ 已驗證。sql-performance-tuning.html：`spark.sql.adaptive.skewJoin.enabled`=true「dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed partitions」；factor=5.0、threshold=256MB，判定為兩條件同時成立（"larger than this factor multiplying the median ... and also larger than ...threshold"）。預設值、判定邏輯、方向全部正確。

**主張 39（第二層 salting）**「AQE 沒搞定（極端熱點如九成集中一 key）時手動 salting：給 key 加隨機後綴打散成 N 個小 key、算完再合併」。
✅ 已驗證（通用手段）。salting（加鹽打散熱點 key）為處理 skew 的標準技法；《High Performance Spark》（Karau & Warren）論及 key skew。本章已自附 ⚠️「具體寫法依 join/group 而異、給概念、九成為示意」。誠實。⚠️ 出處標「Ch.6（Key skew）」——見下方出處核。

**主張 40（第三層 熱點分流）**「skew 來自無意義值（大量 NULL/預設值）時，最乾淨是分流：WHERE key IS NOT NULL 濾掉、或單獨處理不參與主 join」。
✅ 合理且正確（NULL key 在 inner join 本就不匹配、可安全濾；分流是標準做法）。方向正確。取捨已提醒「先確認業務上真可不要」。

**主張 41（順序）**「從最省事先試：AQE → salting → 分流」。
✅ 合理的成本排序，無問題。

**出處引用核（注意）**：§3.10 來源把 salting/熱點分流引《High Performance Spark》**Ch.6（Key skew）**。
⚠️→✅ **章節號已查證、屬合理（非錯）**。經 O'Reilly 官方 TOC 核對《High Performance Spark》：**Ch.6＝「Working with Key/Value Data」**（含 partitioner / key distribution / 管理資料分佈，即 key skew 處理的所在章），**Ch.4＝「Joins (SQL and Core)」**。
　- 出處：oreilly.com/library/view/high-performance-spark/9781491943199/（官方 TOC）。
　- 判定：本章標「Ch.6（Key skew）」中的「Key skew」是**主題描述**而非該章逐字標題（章名其實是「Working with Key/Value Data」），但 Ch.6 確為 key/value data 與分佈處理章、skew/salting 內容歸屬於此，**章節號正確**。skew join 與 salting 在 Ch.4（Joins）亦有觸及。屬「可微調」：若要更精準可標「Ch.6（Working with Key/Value Data）」或併引 Ch.4（Joins）。**不是缺陷。**
　- AQE skew join 的 config/門檻部分（引 sql-performance-tuning）✅ 完全正確。

---

### §3.11 改寫範例

**主張 42**「四問題：① substr 包分區欄位→無 PartitionFilters 全掃；② CAST(cust_id AS STRING) 型別不符擋優化；③ COUNT(DISTINCT) 高基數貴；④ 確認只讀三欄」。
✅ 與 §3.2/§3.4/§3.7/§3.8/§3.3 一致，範例整合正確。改寫後拿掉 substr→裸欄位比較、對齊型別、approx_count_distinct，方向皆對。
⚠️ 小一致性：改寫後 join 條件 `t.cust_id = c.cust_id` 並註「兩邊都 bigint」；但 before 的問題②是「CAST(t.cust_id AS STRING)=c.cust_id」即 c.cust_id 本是 string。若 c.cust_id 真是 string，改寫後直接 `t.cust_id(bigint)=c.cust_id(string)` 仍型別不符。範例註解假設「對齊型別（兩邊都 bigint）」隱含 dim_customer 端也已改成 bigint（治本＝改 schema），與 §3.7「治本是建表起同型別」一致。屬範例的隱含前提，正文 §3.7 已交代治本方向，不算矛盾；但 before→after 之間「c.cust_id 從 string 變 bigint」這步是預設了 schema 已對齊，讀者可能略困惑。屬「可加強」：可加半句「（前提：dim_customer.cust_id 已對齊為 bigint）」。

**主張 43**「改寫後 active_custs 改 approx_count_distinct（已跟需求方確認容許約 5% 誤差）」。
✅ 與 §3.8 取捨一致（精確 vs 近似看用途）。誠實標「已確認需求方接受」。

**主張 44**「AQE 還可能 runtime 改 BroadcastHashJoin（若過濾後 dim_customer 夠小）」。
✅ 與 §3.5 主張 17（AQE 動態轉 broadcast）一致、方向正確、用「可能」hedge。

---

### §3.12 收尾

**主張 45**「先少讀（裁剪/別 SELECT */下推）再少搬（broadcast/避免爆量·型別不符 join/節制 DISTINCT·window/skew），每招回 UI 驗證」。
✅ 與全章一致的操作原則，無新主張。

**主張 46（接下章節指引）**「第 04 章＝Spark 設定/AQE；第 05 章＝資料怎麼存（分區/格式/ANALYZE TABLE）；第 09 章＝工作流程」。
⚠️ 跨章引用未核：本審查只看 §03，未核第 04/05/09 章標題與內容是否如此。屬手冊內部導覽，需主筆自行確認章節對應（§3.2/§3.6 也多次「見第 05 章 ANALYZE TABLE」「第 08 章 schema 設計」，同樣未核）。非技術正確性缺陷，列為提醒。

---

### 版本對齊與精確度說明（章末）核查

**主張 47**「關鍵預設值 autoBroadcastJoinThreshold 10MB、skewedPartitionFactor 5.0、skewedPartitionThresholdInBytes 256MB、adaptive.enabled true 皆對 Spark 3.3 Performance Tuning 核對」。
✅ 全部已驗證（見主張 16/38）。10MB ✅、5.0 ✅、256MB ✅、adaptive.enabled=true ✅。本次以 docs/latest 核對，數值自 3.3 起穩定。

**主張 48（精確度說明 #4）**「approx_count_distinct 預設最大相對誤差約 5%（rsd≈0.05）出自 DataFrame API；SQL 函數頁只說明 relativeSD 為最大相對標準差、未逐字印預設值」。
✅ 完全正確（見主張 32）。標註是本章最謹慎、最正確的一處。

**主張 49（版本對齊說明）**「官方連結指向 latest；對齊本手冊把 /docs/latest/ 改 /docs/3.3.2/」。
✅ 機制正確（Spark 版本化文件 URL 規則如此）。本審查亦因 3.3.2 頁常 404 而用 latest，行為自 3.x 穩定。

---

## 出處引用錯誤彙整（書本章節號）

**關鍵發現（出處錯）**：§3.3 來源寫「《Spark: The Definitive Guide》Ch.8（Data Sources）」。
❌ **章節號錯誤**。經 O'Reilly 官方 TOC 核對：《Spark: The Definitive Guide》**Ch.8＝Joins**、**Ch.9＝Data Sources**（Ch.7＝Aggregations，含 Window Functions）。
　- 出處：oreilly.com/library/view/spark-the-definitive/9781491912201/（官方 TOC：Ch.7 Aggregations / Ch.8 Joins / Ch.9 Data Sources）。
　- 影響：§3.3 講 column pruning 引「Ch.8（Data Sources）」→ 應為 **Ch.9（Data Sources）**。Ch.8 其實是 Joins。**這是真缺陷（引用出處錯，須改）**。
　- 對照：§3.5、§3.7 引「Ch.8（Joins）」✅ 正確；§3.8 引「Ch.7（Aggregations）」✅ 正確；§3.9 引「Ch.7（Window Functions）」✅ 正確（Window Functions 確在 Ch.7）。
　- 故全章書本章節號**只有 §3.3 的「Ch.8（Data Sources）」錯**，其餘正確。

---

## 三級彙整

### A. 真缺陷（必補）

1. **§3.3 出處章節號錯**：「《Spark: The Definitive Guide》Ch.8（Data Sources）」應改為 **Ch.9（Data Sources）**。經 O'Reilly 官方 TOC：Ch.7 Aggregations / Ch.8 Joins / Ch.9 Data Sources。目前寫的 Ch.8 其實是 Joins，與「Data Sources」不符。這是唯一一處硬錯誤，須改章號。（出處：oreilly.com/library/view/spark-the-definitive/9781491912201/）

### B. 可加強（斟酌）

2. **§3.7 陷阱一的「結果是對的、只是慢」前提不完整**：此說僅在「兩邊內容本可互轉、只是宣告型別不同」時成立。若 string 欄位含非數字，隱式 `CAST(string AS BIGINT)` 會產生 NULL → **join 結果改變**（不只是變慢）。建議補半句限定前提，否則對「型別不符＝純效能問題」的論斷在邊界情形過強。
3. **§3.11 範例 before→after 的型別對齊隱含假設**：before 的 `c.cust_id` 是 string，after 卻註「兩邊都 bigint」，中間隱含「dim_customer.cust_id 已改建為 bigint」這一步未明說。與 §3.7「治本＝建表起同型別」一致，但讀者可能困惑。建議加半句點明前提（「dim_customer.cust_id 已對齊為 bigint」）。
4. **出處掛靠精準度（輕微）**：(a) §3.2「PartitionFilters 呈現」掛 sql-performance-tuning，實際出處是 EXPLAIN FORMATTED 的 FileScan 輸出（第 02 章已補）；(b) §3.3「column pruning」單引 Parquet 官方頁字面略弱（該頁無此字句），真正出處是《Definitive Guide》Ch.9；(c) §3.7「型別不符插入隱式 cast」掛 sql-performance-tuning 不夠精準（屬 Catalyst type coercion / SQL ref）。皆有書本或 footnote 為輔，結論不誤，但可微調出處措辭。
5. **§3.10 書本章號可更精準（非錯）**：「Ch.6（Key skew）」中「Key skew」是主題描述，章名實為「Working with Key/Value Data」；可標「Ch.6（Working with Key/Value Data）」或併引 Ch.4（Joins）。
6. **跨章導覽未核（§3.12 及全章「見第 04/05/08/09 章」）**：本審查只看 §03，未核第 04/05/08/09 章標題與內容是否如所述。需主筆自行確認手冊內部章節對應正確（屬導覽，非技術正確性）。

### C. 誤讀／不改或微調（已處理得當）

7. **§3.4 / §3.7 因果方向（函數包欄位→下推失效、型別不符→擋優化）**：✅ 方向正確，且本章已用「最好/通常/可能/相關」hedge、並附 ⚠️ footnote 明確降級「型別不符一定讓 broadcast 失效＝說法過強」「以 EXPLAIN 為準」。**未寫成硬限制，處理得當。** 「建議被寫成硬限制」的風險在本章不成立——broadcast threshold(10MB)、AQE 門檻(5.0/256MB)等被正確標為「預設值/估計/動態調整」，hint 被正確標為「建議不保證」（引用逐字相符）。
8. **§3.8 footnote（rsd≈5% 出自 DataFrame API、SQL 頁未逐字印）**：✅ **本章最謹慎、最正確的一處**。已查證 SQL 函數頁確實只寫「relativeSD defines the maximum relative standard deviation allowed」、無預設數字；5% 確出自 PySpark/Scala API（default=0.05）。無把 5% 誤稱為官方 SQL 文件逐字數字。
9. **所有預設值**：autoBroadcastJoinThreshold 10MB ✅、filterPushdown true ✅、AQE skewJoin enabled/factor 5.0/threshold 256MB ✅、adaptive.enabled true ✅、hint 別名 BROADCASTJOIN/MAPJOIN ✅、hint「不保證採用」逐字 ✅ —— 全部與 Spark 3.3/latest 官方文件相符。
10. **量級示意（36 倍、1000 欄幾十倍、九成集中一 key、5%）**：✅ 均已自附 ⚠️ 標為示意，非缺陷。

---

## 一句話結論

全章技術主張**因果方向全部正確、預設值全部正確、hint「建議非命令」處理正確、approx_count_distinct 預設值標註是模範**。唯一硬缺陷是 **§3.3 把《Definitive Guide》「Data Sources」章標成 Ch.8（應為 Ch.9）**；其餘為可斟酌的前提補強與出處措辭微調。本章對「建議 vs 硬限制」「因果正負號」的自律（大量 ⚠️ footnote + hedge 用詞）做得比一般技術文件嚴謹。



---
---

# 【補充小節追加審查】§3.5「### 補充：Spark 一共有哪幾種 join？一張表看懂」

審查日期：2026-06-16。範圍限定：§3.5 內標題為「### 補充：Spark 一共有哪幾種 join？一張表看懂」的小節（含 5 列對照表、其後 BroadcastNestedLoopJoin / CartesianProduct 段落、該小節 📚 來源 footer），外加章末「資料來源與精確度說明」第 4 項與本小節一致性核對。對齊 Spark 3.3.x。其餘小節不在本輪範圍。

## 逐條主張查證

### 物理算子名稱（EXPLAIN 顯示名）

- **C1. 表頭與內文用 `BroadcastHashJoin` / `SortMergeJoin` / `ShuffledHashJoin` / `BroadcastNestedLoopJoin` / `CartesianProduct` 五個名字** — ✅已驗證。Spark SQL 5 個 join 物理算子為 `BroadcastHashJoinExec` / `ShuffledHashJoinExec` / `SortMergeJoinExec` / `CartesianProductExec` / `BroadcastNestedLoopJoinExec`，EXPLAIN 顯示名即去掉 `Exec` 後綴。出處：dataninjago「Spark SQL Query Engine Deep Dive (11) – Join Strategies」逐條列出五個 Exec 算子（https://dataninjago.com/2022/01/11/spark-sql-query-engine-deep-dive-11-join-strategies/）；官方 Join Hints 頁的四 hint 映射亦間接佐證算子名（見 C7）。
  - ⚠️ 細節（不改）：原始碼 case class 名是 `ShuffledHashJoinExec`（過去分詞 Shuffled），EXPLAIN 顯示亦為 `ShuffledHashJoin`；表中用 `ShuffledHashJoin` 正確。官方 hint 文案則用「shuffle hash join」（hint 名 `SHUFFLE_HASH`）——是 hint 措辭，非算子名，兩者並存不衝突，本表用算子名是對的。

### 對照表 5 列「Spark 何時選它 / 要不要 shuffle」

- **C2.（第 1 列）`BroadcastHashJoin`：等值 join + 一邊夠小（<10MB 門檻或 BROADCAST hint）；不 shuffle（廣播小表）** — ✅已驗證。等值 join 選擇順序中 BHJ 條件為「至少一邊小到可收集到 driver 再廣播到各 executor」；門檻 `spark.sql.autoBroadcastJoinThreshold` 預設 10MB；廣播取代 join 的 shuffle。出處：dataninjago deep-dive(11)（equi-join 選擇順序：Hint→BHJ→SHJ→SMJ）；官方 SQL Performance Tuning（autoBroadcastJoinThreshold 預設 10485760、broadcast 免 join shuffle，https://spark.apache.org/docs/latest/sql-performance-tuning.html）。
- **C3.（第 2 列）`SortMergeJoin`：等值 join、兩邊都大；要 shuffle（兩邊都 shuffle＋排序）** — ✅已驗證。SMJ 是等值 join 在 BHJ/SHJ 都不適用時的 fallback（join key 可排序），兩邊各按 key shuffle 後排序合併。出處：dataninjago deep-dive(11)（SMJ 為 sortable key 的 fallback）；官方 SQL Performance Tuning（MERGE hint = shuffle sort merge join）。
- **C4.（第 3 列）`ShuffledHashJoin`：等值 join、一邊夠小可建記憶體 hash 但沒小到能廣播；要 shuffle（兩邊都 shuffle，但不排序）；較少見、Spark 多半偏好 sort-merge、通常要靠 SHUFFLE_HASH hint** — ✅已驗證（含「較少見/偏好 SMJ」的因果）。SHJ 條件：一邊 size ≤ broadcast 門檻 × shuffle 分區數、且至少比另一邊小 3 倍；兩邊 shuffle 但 build side 建記憶體 hash、不排序。**但預設 `spark.sql.join.preferSortMergeJoin=true` 使 Spark 在兩者皆可時選 SMJ**，故 SHJ 罕見、實務多靠 `SHUFFLE_HASH` hint（或 preferSortMergeJoin=false、或 AQE runtime 由 SMJ 轉 SHJ）才走它；偏好 SMJ 的理由是它能 spill 到磁碟、對 OOM 較 robust。出處：dataninjago deep-dive(11)（SHJ「至少比大表小 3 倍」、size ≤ 門檻×分區數）；多篇技術文一致敘述 preferSortMergeJoin 預設 true→偏好 SMJ、SHJ 需 hint（如 medium @manishankarmechanical、bigdatainrealworld「force Spark to use Shuffle Hash Join」）。✅此列因果方向（偏好 SMJ 所以 SHJ 少見）正確，且本表只說「通常要靠 hint」未寫成「一定」，措辭得當。
- **C5.（第 4 列）`BroadcastNestedLoopJoin`：非等值 join（沒有 `=`）、且一邊可廣播；不 shuffle（廣播一邊）** — ✅已驗證。非等值 join 只支援 BNLJ 與 CartesianProduct 兩種；BNLJ 在「一邊小到可廣播」時選用，廣播一邊、不為 join 做 key-based shuffle。出處：dataninjago deep-dive(11)（Non-Equi-Join 僅 BNLJ／CartesianProduct，BNLJ＝relation 小到可廣播）；官方 SQL Performance Tuning（BROADCAST hint 在「無 equi-join key」時走 broadcast nested loop join，見 C8）。
- **C6.（第 5 列）`CartesianProduct`：cross join／完全沒有 join 條件；不 shuffle，但兩邊每列互配** — ✅已驗證。CartesianProductExec 用於「兩 relation 間沒有涉及雙方欄位的 join 條件」（cross join），且 join type 為 InnerLike；每列互配（笛卡兒積）。出處：dataninjago deep-dive(11)（CartesianProduct＝join type InnerLike、無 equi 條件）；SPARK-17298（無顯式 CROSS 的笛卡兒積預設由 `spark.sql.crossJoin.enabled=false` 擋下並拋例外）。
  - ⚠️ 補充（不改，措辭已安全）：嚴格說 CartesianProduct 也可承接「非等值且不可廣播」的 InnerLike join（不只「完全沒條件」）；但本表針對 SQL-first 讀者把它定位成「cross join／完全沒 join 條件」是最常見、最該警覺的情形，且內文另有「呼應 §3.7 條件寫漏」的引導，無誤導。歸為「常見情形」而非「唯一情形」，但表格語境清楚，列「可加強」備忘即可。

### Join Hints footer 映射（小節 📚 來源）

- **C7. footer：四 hint 映射 BROADCAST→broadcast hash／MERGE→shuffle sort merge／SHUFFLE_HASH→shuffle hash／SHUFFLE_REPLICATE_NL→shuffle-and-replicate nested loop** — ✅已驗證，逐字相符。官方 Join Hints 頁：BROADCAST=broadcast join、MERGE=shuffle sort merge join、SHUFFLE_HASH=shuffle hash join、SHUFFLE_REPLICATE_NL=shuffle-and-replicate nested loop join。出處：https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html（已 WebFetch 逐字確認）。
- **C8. footer：「用 BROADCAST hint 時，依有無 equi-join key 決定走 broadcast hash join 或 broadcast nested loop join」** — ✅已驗證，逐字相符。官方 SQL Performance Tuning（Join Strategy Hints）原文：「broadcast join (either broadcast hash join or broadcast nested loop join depending on whether there is any equi-join key)」。出處：https://spark.apache.org/docs/latest/sql-performance-tuning.html（已 WebFetch 逐字確認）。

### BroadcastNestedLoopJoin / CartesianProduct 內文段落

- **C9. 觸發條件：非等值 join——範圍 `BETWEEN`、不等式 `<`、或 join 條件夾 `OR`／`CASE WHEN`——Spark 無相等 key 可 hash/排序對齊，退化成巢狀迴圈** — ✅已驗證。非等值（含 `<`/`>`/`BETWEEN`）只支援 BNLJ/CartesianProduct；含 `OR`／複合條件的 join（如 `t1.a=t2.x OR t1.a=t2.y`）會觸發 BNLJ。出處：dataninjago deep-dive(11)（Non-Equi-Join 僅 BNLJ/Cartesian）；Databricks/community「OR 條件 join 觸發 BNLJ、建議拆成 UNION 或改 equi-join」（搜尋結果一致，見下 C12 出處說明）。
- **C10. 成本 O(n×m)（兩邊列數相乘）、只要兩邊不夠小就慢到失控甚至跑不完** — ✅已驗證。BNLJ「each row from one dataset is compared with every row from the other」＝每列互比＝O(n×m)。出處：dataninjago deep-dive(11)（nested loop 對大資料因比較次數爆量而差）；dezimaldata / community 多篇一致敘述「row-by-row、computationally expensive」。
- **C11. CartesianProduct 是更極端版本、完全沒可用 join 條件時出現、通常代表條件寫漏（呼應 §3.7 陷阱二）** — ✅已驗證（方向正確、措辭安全）。無 join 條件→CartesianProductExec；預設 `crossJoin.enabled=false` 會擋下未顯式 CROSS 的笛卡兒積，正佐證「通常是寫漏了」。出處：SPARK-17298（要求顯式 CROSS join）。

- **C12.（出處不符 — 真缺陷）小節 footer 把「非等值／OR／CASE WHEN→BNLJ、成本相乘、建議改 equi-join」整包掛到 `https://kb.databricks.com/sql/disable-broadcast-when-broadcastnestedloopjoin`** — ❌出處不符。實際抓取該 Databricks KB 頁，其主題是 **`NOT IN` 子查詢的 null 語義**為何 fallback 到 BNLJ、解法是改寫 `NOT EXISTS`；該頁**未**論及「非等值／OR／CASE WHEN 觸發 BNLJ」「成本 O(n×m)／每列互比」「建議改寫成 equi-join」這三項本小節實際宣稱的內容。
  - 主張本身（C9/C10/C11）技術上正確（有 dataninjago deep-dive(11) + Databricks/community OR-join→BNLJ→改 equi-join/UNION 佐證），**但所掛的這個 URL 撐不起這些字面宣稱**＝典型「出處與宣稱不符」。
  - 正確值/修法：把該 footer 的 Databricks 連結換成真正論及這些點的權威頁。建議改掛 (a) 官方 SQL Performance Tuning「BROADCAST…depending on whether there is any equi-join key」（已在同 footer，撐 BNLJ 觸發於無 equi key）＋ (b) Spark 原始碼/開發者級整理（如 dataninjago「Join Strategies」deep-dive，明列非等值僅 BNLJ/Cartesian、選擇順序、3× 條件）作為「成本與選擇規則」出處；「建議改 equi-join／拆 OR 為 UNION」可掛 Databricks community/KB 中實際討論 OR-join 改寫者。若要保留 Databricks 域名，需換成內容真的對應的 KB 頁，不能續用 `disable-broadcast-when-broadcastnestedloopjoin`（其情境是 NOT IN+null，與本段宣稱不符）。
  - ⚠️ 查證限制：截至本輪，未能定位一個「Databricks 官方且逐字同時涵蓋非等值/OR/CASE WHEN→BNLJ＋O(n×m)＋建議 equi-join」的單一 KB URL。OR→BNLJ→改寫的佐證來自 Databricks community 討論串與多篇技術整理（非單一官方 KB 逐字）。故建議改用「官方 Perf Tuning + 原始碼級 deep-dive」組合，避免再次出處單掛而不符。

### preferSortMergeJoin 的處理（小節 footer ⚠️ + 章末第 4 項）

- **C13. footer ⚠️：「偏好 sort-merge 勝過 shuffle hash 由 `spark.sql.join.preferSortMergeJoin`（預設 true）決定，該設定不在公開 Configuration 頁；最終以 EXPLAIN 算子為準」** — ✅已驗證，且 hedge 誠實、無過度宣稱。
  - 「預設 true」：✅ 多源一致（medium/bigdatainrealworld 等技術文；行為與 dataninjago 選擇順序自洽）。
  - 「不在公開 Configuration 頁」：✅ 實際抓取 https://spark.apache.org/docs/latest/configuration.html 全頁搜尋 `preferSortMergeJoin` / `spark.sql.join`＝**無任何條目**，屬內部設定。手冊**未**在正文報它的預設、只在 footer/章末標「以 EXPLAIN 為準」＝處理正確、未過度宣稱（沒把「預設 true」假裝成官方 Configuration 頁逐字數字）。
- **C14. 章末「資料來源與精確度說明」第 4 項與本小節一致性** — ✅一致、無過度宣稱。第 4 項措辭：「Spark 自動在 5 種 join 物理模式間的選擇規則（含『非等值→BNLJ／CartesianProduct』『偏好 sort-merge 勝過 shuffle hash』）方向正確、有官方／Databricks 出處，但完整選擇演算法散在原始碼、官方公開文件未逐字完整載明；preferSortMergeJoin（預設 true）屬內部設定、不在公開 Configuration 頁。實際走哪種一律以 EXPLAIN 算子名稱為準。」——此段把「選擇規則方向正確但非官方逐字完整」「preferSortMergeJoin 屬內部設定」「以 EXPLAIN 為準」三點都誠實標註，**與小節正文/footer 完全一致，未過度宣稱**。唯一可連帶修正：第 4 項也概括性地說「有官方／Databricks 出處」——既然 C12 指出小節實際掛的那個 Databricks URL 與宣稱不符，修 C12 出處時，第 4 項這句的「Databricks 出處」也應指向換新後真正對應的來源（連帶一致即可，本句本身不算錯）。

## 三級彙整（本補充小節）

### A. 真缺陷（必補）

1. **【出處不符】小節 📚 footer 的 Databricks KB 連結（`kb.databricks.com/sql/disable-broadcast-when-broadcastnestedloopjoin`）撐不起其字面宣稱**（見 C12）。該頁實為 `NOT IN`+null 語義 → BNLJ → 改 `NOT EXISTS`，**未**談「非等值／OR／CASE WHEN 觸發 BNLJ」「O(n×m)／每列互比」「建議改 equi-join」。主張本身正確，但須把出處換成真正對應者：建議改掛「官方 SQL Performance Tuning（BNLJ 觸發於無 equi-join key，已在同 footer）＋ Spark 原始碼級 deep-dive（非等值僅 BNLJ/Cartesian、選擇順序、3× 條件、O(n×m)）」組合；保留 Databricks 域名則需換成內容真的對應 OR-join→BNLJ→改寫的 KB/community 頁。連帶把章末第 4 項「Databricks 出處」一語對齊到換新後來源（C14）。

### B. 可加強（斟酌）

2. **CartesianProduct 第 5 列定義範圍偏窄**（C6）：嚴格說它也承接「非等值且不可廣播」的 InnerLike join，不只「完全沒 join 條件」。本表針對 SQL-first 讀者定位成「cross join／沒條件」是最該警覺的常見情形、且有「呼應條件寫漏」引導，不誤導；如要更嚴謹可加半句「（或非等值又無法廣播時的退路）」。屬「常見情形」收斂，非錯。

### C. 誤讀／不改（已處理得當）

3. **5 算子名稱、各列「何時選/要不要 shuffle」、equi-join 三策略選擇順序、SHJ 罕見且偏好 SMJ 的因果、BNLJ 觸發於非等值/O(n×m)、CartesianProduct 無條件、四 hint 映射、BROADCAST 依 equi-key 走 hash 或 NL** — 全部 ✅已驗證、與 Spark 3.3/latest 官方文件＋原始碼級 deep-dive 相符（C1–C11、C13）。
4. **preferSortMergeJoin 的 hedge**（C13/C14）— ✅**模範處理**：實測該設定確不在公開 Configuration 頁；手冊未在正文報其預設、只標「內部設定、以 EXPLAIN 為準」，無過度宣稱；章末第 4 項與小節完全一致。
5. **因果方向**：「preferSortMergeJoin 預設 true → SHJ 罕見、需 hint」「非等值無相等 key → 退化巢狀迴圈」「無 join 條件 → 笛卡兒積、多半寫漏」— 因果方向皆 ✅正確，且用「多半／通常／較少見」hedge，未把常見情形寫成保證。

## 一句話結論（本補充小節）

技術主張**全部正確**（5 算子名、各列選擇規則與 shuffle 與否、BNLJ 非等值觸發＋O(n×m)、CartesianProduct、四 hint 映射、BROADCAST 依 equi-key 分流）；preferSortMergeJoin 的「內部設定、不在公開 Configuration 頁、以 EXPLAIN 為準」hedge 經實測屬實、**無過度宣稱**，章末第 4 項與小節一致。**唯一必補的是出處不符**：BNLJ 段掛的那個 Databricks KB URL（NOT IN+null 情境）對應不到它實際的三項宣稱，須換成真正涵蓋「非等值/OR/CASE WHEN→BNLJ、O(n×m)、改 equi-join」的權威來源。
