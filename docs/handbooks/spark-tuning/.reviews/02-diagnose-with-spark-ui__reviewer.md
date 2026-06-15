# 第 02 章技術審查記錄：用 Spark UI 與 EXPLAIN 找瓶頸

審查員：技術審查 subagent
日期：2026-06-15
對齊版本：Spark 3.3.x（AQE 預設開）+ Hive 3.1.3 / CDP Private Cloud Base 7.1.9
審查素材：`docs/handbooks/spark-tuning/02-diagnose-with-spark-ui.md`
權威來源限：Spark 官方文件（web-ui / monitoring / sql-ref EXPLAIN / sql-performance-tuning / configuration）、Cloudera CDP 官方文件、《Spark: The Definitive Guide》《Learning Spark 2nd》《High Performance Spark》。

> 規則：每查一條 append 一條；判定附出處；查不到標「無法查證」、不臆測。
> 自動工具對 spark.apache.org 的 3.3.2 版頁常回 404，改用 docs/latest 查證（行為自 3.x 穩定）。

---

（以下逐條 append）

---

## 第二輪審查（Opus，2026-06-15）：逐條技術主張查證

> 方法：以主筆已查證的事實為基準「確認或挑戰」；只對真正懷疑的點上網（WebFetch / WebSearch），來源限 Spark 官方 / Cloudera CDP 官方 / ASF JIRA / Databricks（核心開發者）。

### §2.1 先量再調

**主張 A**「`EXPLAIN` 不真的執行、只印計畫」「Spark UI 看實際執行數字」兩工具定位。
✅ 已驗證。EXPLAIN 語法頁定義其為印查詢計畫；Monitoring 頁定義 Web UI 監看執行。出處 docs/latest/sql-ref-syntax-qry-explain.html、docs/latest/monitoring.html（主筆基準）。定位正確。

---

### §2.2 怎麼打開 Spark UI（CDP）— 高風險點 1 & 2

**主張 B**「driver 開 live UI，預設 port 4040；同台多個順延 4041、4042…；只在 application 活著時存在」。
✅ 已驗證。monitoring.html 原文：「Every SparkContext launches a Web UI, by default on port 4040」「If multiple SparkContexts are running on the same host, they will bind to successive ports beginning with 4040 (4041, 4042, etc)」「this information is only available for the duration of the application by default」。出處 docs/latest/monitoring.html（主筆基準，逐字符合）。

**主張 C（高風險點 2）**「在 CDP／YARN 上你**通常不會、也不該直接去連 driver 那台機器的 4040**；而是從 YARN ResourceManager 網頁找到 application、點 ApplicationMaster 連結進 Spark UI」。
❌ **錯誤（方向性，須修）**。查 CDP 7.1.9 官方頁「Accessing the Web UI of a **Running** Spark Application」，其唯一指示是：**「open `http://spark_driver_host:4040` in a web browser」**——即官方對 running app 的建議**正是直連 driver 的 4040**，且全頁**未**提及「ResourceManager → 點 ApplicationMaster 進 Spark UI」這條路徑（該頁其餘段落只講 Knox 配置下 Tracking URL 顯示錯誤的修法）。出處：docs.cloudera.com/cdp-private-cloud-base/7.1.9/monitoring-and-diagnostics/topics/cm-accessing-the-web-ui-of-a-running-spark-application.html。
　- 補充：YARN 確有 proxy 機制（ResourceManager 用 tracking URL 連到 running app，`YarnProxyRedirectFilter`）——見 docs/latest/running-on-yarn.html，但該頁主述的是「app UI 關閉時改用 History Server 當 tracking URL」，**不**等於「不該連 4040」。
　- **判定**：本章把「YARN proxy 可進入」（屬實、但官方文件主述為 History Server fallback）寫成了「**不該**直連 4040」（與 CDP running-app 官方頁直接牴觸）。這是把一個可選實務寫成了硬性禁令，且引用方向相反。**建議改寫**：CDP running app 官方就是直連 `http://<driver_host>:4040`；ApplicationMaster / RM proxy 是「driver host 不可直達時」的替代路徑，措辭從「不該」降為「也可」。

**主張 D（高風險點 1）**「CDP History Server 進入方式：Cloudera Manager → Spark 服務 → History Server Web UI；或 `http://<history-server-host>:18088`；清單點 App ID」。
✅ 已驗證。Cloudera CDP 7.1.9「Accessing the Web UI of a Completed Spark Application」原文即此路徑與 port **18088**。出處同主筆基準。

**主張 E（高風險點 1）— 精確度#1**「CDP 用 18088、上游 Apache Spark History Server 預設 18080，兩者各自情境都對」。
✅ 已驗證、區分正確。monitoring.html 原文 History Server 預設「http://<server-url>:18080」；CDP 文件為 18088。本章在正文用 18088、在精確度說明#1 明確並陳兩個 port 與其情境，**區分無誤**（此為高風險點 1 的要求，通過）。

**主張 F**「event log 需 `spark.eventLog.enabled`；CDP 預設開著」。
✅ 部分驗證。「History Server 重建 UI 需 spark.eventLog.enabled」逐字見 monitoring.html（主筆基準）。「CDP 預設開著」一句屬平台預設陳述：CDP 由 Cloudera Manager 部署 Spark 時 event log 通常預設啟用，但本章未逐字引 CDP 頁佐證此「預設開」。
⚠️ 輕度：「CDP 預設開著」未附逐字出處，屬合理但未引述的平台陳述。建議加一句「依平台配置，多數 CDP 部署預設啟用」或標為實務經驗，不影響主結論。

---

### §2.3 EXPLAIN — 高風險點 3（EXPLAIN 看到調整前計畫）

**主張 G**「預設（不加字）只印 physical plan；`FORMATTED` 把每步驟細節分區整理；另有 `EXTENDED`/`COST`/`CODEGEN`」。
✅ 已驗證。sql-ref EXPLAIN 頁：語法「EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement」，預設(SIMPLE)只印 physical plan；FORMATTED 印「physical plan outline + node details」兩段；EXTENDED 印 parsed/analyzed/optimized logical + physical；COST 印 logical+statistics；CODEGEN 印程式碼+physical。出處 docs/latest/sql-ref-syntax-qry-explain.html（主筆基準，逐字符合）。本章對 FORMATTED 的「分區整理、最好讀」描述與「physical plan outline + node details」一致。

**主張 H**「讀計畫找三關鍵字：`Exchange`＝shuffle；`BroadcastHashJoin` vs `SortMergeJoin`；Scan 的 `PartitionFilters`/`PushedFilters`」。
✅ 已驗證（概念）。`Exchange`＝shuffle 算子、broadcast vs sort-merge join、partition pruning / pushed filters 為 Spark physical plan 標準算子/欄位，見 sql-performance-tuning.html 與《Spark: The Definitive Guide》Ch.8/Ch.15（主筆基準）。`PartitionFilters`/`PushedFilters` 為 Parquet/FileScan 節點實際欄位名，與 EXPLAIN FORMATTED 輸出一致。本節已自附 ⚠️「未貼逐字輸出，欄位排版以你環境為準」，誠實。

**主張 I（與高風險點 3 相關）**「`WHERE month='2026-05'` 沒出現在 PartitionFilters，代表沒裁到、整表掃」。
✅ 因果方向正確。partition column 上的 predicate 若被用於 partition pruning 會列在 `PartitionFilters`；缺席即未裁剪 → 掃所有分區目錄。與 §1.8 引的 Parquet Partition Discovery 一致。負號正確。

---

### §2.4 頁籤地圖

**主張 J**「主要六個頁籤：SQL / Stages / Jobs / Executors / Storage / Environment」與各自用途；「另有 streaming／JDBC 等」。
✅ 已驗證。web-ui.html 頁籤：Jobs / Stages / Storage / Environment / Executors / SQL（另有 Structured Streaming / Streaming / JDBC-ODBC Server）。出處 docs/latest/web-ui.html（主筆基準）。表格各頁用途（SQL=查詢步驟最貴、Stages=task 分佈、Executors=資源/記憶體/shuffle、Storage=cache、Environment=設定值）與官方頁描述一致。
⚠️ 極輕：表格把 SQL 頁籤排第一（「最常用」）是教學取向的排序，與官方頁籤順序（Jobs 在前）不同——非錯誤，僅呈現順序差異，無須改。

---

### §2.5 SQL 頁籤 — 高風險點 3 & 5（爆量 join）

**主張 K**「SQL 頁籤有 query execution DAG，算子掛實際數字；`number of output rows`；Exchange 的 `shuffle bytes written total`」。
✅ 已驗證。web-ui.html SQL 頁：query details 顯示 query execution time/duration/關聯 jobs/「query execution DAG」；per-operator metrics 舉例「number of output rows」與 Exchange 的「shuffle bytes written total」。出處 docs/latest/web-ui.html（主筆基準，metric 名稱逐字符合）。

**主張 L（高風險點 5）**「某個 `Join` 之後列數暴增（比輸入還多很多）＝一對多甚至接近笛卡兒積的爆量 join」。
✅ 因果方向正確。join output rows ≫ 兩側輸入 rows 確為 fan-out（一對多 / 重複 key / 缺條件笛卡兒積）的訊號。負號正確；屬通用 join 語意（與 aligning-on-table-joins 概念一致）。

**主張 M（高風險點 3）**「`EXPLAIN`（跑之前）看到的是**還沒被 AQE 改過**的計畫；真正最終計畫要在 SQL 頁籤（跑完）才看得到；兩邊對不起來正常」。
✅ 已驗證、行為正確。
　- AQE「在執行途中 re-optimize query plan」「自 Spark 3.2 起預設開（spark.sql.adaptive.enabled=true）」：docs/latest/sql-performance-tuning.html 原文「re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics」。
　- EXPLAIN 看到的是調整前計畫：SPARK-33850（ASF JIRA）EXPLAIN FORMATTED 輸出頂層為「AdaptiveSparkPlan (3)」且「Arguments: **isFinalPlan=false**」＝執行前/中的初始計畫（AQE 套用前）。Databricks AQE 文件亦述：執行前/中 `isFinalPlan=false`、完成後變 `true`，AdaptiveSparkPlan 節點下同時保留 initial plan 與 current/final plan。出處 issues.apache.org/jira/browse/SPARK-30331、SPARK-33850。
　- **本章行為描述正確**。但見下一條對「字樣未能查證」的更正。

**主張 N（精確度#4）**「SQL 頁籤呈現 AQE 最終計畫常見以 `AdaptiveSparkPlan` 標示這點，工具未能逐字查證該字串」。
⚠️ **此自承過度保守，可更正/升級**。`AdaptiveSparkPlan` 字串實際**可查證**為 Spark 真實節點名：SPARK-33850 的 EXPLAIN FORMATTED 輸出明列「AdaptiveSparkPlan (3)」「isFinalPlan=false」（ASF JIRA），SPARK-30331 標題即「The final AdaptiveSparkPlan event…isFinalPlan=true」。
　- 一點精確化：`AdaptiveSparkPlan` 是「**EXPLAIN 與 SQL 頁籤都會出現的計畫根節點**」（在 EXPLAIN 中 `isFinalPlan=false`；執行完成後在 SQL 頁籤 / History Server 事件中可見 `isFinalPlan=true` 的最終形）。章內把它說成「SQL 頁籤呈現最終計畫的標籤字樣」方向沒錯，但更精準的說法是「同一個 `AdaptiveSparkPlan` 根節點，EXPLAIN 時 `isFinalPlan=false`、跑完看到 `isFinalPlan=true`」。
　- **建議**：精確度#4 從「工具未能逐字查證該字串」改為「`AdaptiveSparkPlan` 為實際節點名（見 SPARK-33850 / SPARK-30331），EXPLAIN 顯示 isFinalPlan=false 之初始計畫、執行後為 isFinalPlan=true 之最終計畫」。屬「可加強」，非缺陷。

---

### §2.6 Stages 頁籤 — 高風險點 4 & 5（skew/spill 因果）

**主張 O（高風險點 4）**「Summary Metrics 以分位數呈現：Min / 25th percentile / Median / 75th percentile / Max」。
⚠️ **無法逐字查證（與主筆自承一致）**。web-ui.html 提及「Summary metrics for all tasks are represented in a table and in a timeline」，但**未逐字列出** Min/25th/Median/75th/Max 這幾個列標字樣。WebFetch 確認該頁無此逐字字串。
　- 補充佐證（非官方逐字、但屬權威次級）：這五分位（min, 25th percentile, median, 75th percentile, max）確為 Spark UI Stage 頁 Summary Metrics 的標準呈現，廣見於《Spark: The Definitive Guide》《Learning Spark 2nd》對 UI 的描述與實機。本章已在 §2.6 末與精確度#5 明確標 ⚠️「分位數列標未能逐字擷取，屬標準呈現」——**如實、無遺漏**。判定：保留 ⚠️ 標註即可，屬「行為已知、官方頁未逐字」。

**主張 P（高風險點 5：skew）**「`Duration` 的 Max ≫ Median ＝ skew 招牌訊號；同樣差距也見於 `Shuffle Read Size / Records`」。
✅ 因果方向正確。skew＝少數 key 過肥 → 少數 task 的 duration / shuffle-read 遠大於中位數 → stage 卡在最慢 task（stage barrier，§1.6）。負號正確。`Shuffle Read Size / Records` 定義「Total shuffle bytes read, includes both data read locally and data read from remote executors」逐字見 web-ui.html（主筆基準）。

**主張 Q（高風險點 5：spill）**「`Shuffle spill (memory)`＝shuffled data 記憶體中反序列化形式大小；`Shuffle spill (disk)`＝磁碟上序列化形式大小；只要非零＝記憶體不夠、溢寫磁碟」。
✅ 已驗證。web-ui.html 原文：「Shuffle spill (memory) is the size of the deserialized form of the shuffled data in memory」「Shuffle spill (disk) is the size of the serialized form of the data on disk」（主筆基準，逐字符合）。
　- 因果方向正確：spill 非零＝執行期記憶體（execution memory）不足以容納排序/聚合暫存 → 溢寫磁碟。與 §1.6「spill 是 shuffle write 本來就會寫磁碟之外的**額外**一次 I/O」一致。負號正確。
　- 一點精確化（可加強，非錯）：嚴格說 spill 反映的是「**execution memory 相對該 stage 暫存量不足**」，常見成因含 partition 太大/太少、skew、join/agg 暫存爆量，不必然等同「整體 executor 記憶體配太小」。本章 §2.6 結論「記憶體相對資料量太小」方向對，且已把解法分流到「減量(第3章)/分區/資源(第4章)」，涵蓋了正確選項，無須改。

---

### §2.7 / §2.8 症狀對照表 — 高風險點 5（逐一確認正負號）

**主張 R**「shuffle 過大：Exchange 的 shuffle bytes written total 最大者 / EXPLAIN 中 Exchange 個數多」。
✅ 正確。bytes written 大＝該 shuffle 搬最多；Exchange 個數＝shuffle 次數，二者皆為成本指標，方向對。

**主張 S**「skew：Duration 或 Shuffle Read Size 的 Max ≫ Median」。
✅ 正確（同主張 P）。

**主張 T**「spill：Shuffle spill (memory/disk) 非零；Executors 頁籤也看得到誰在 spill」。
✅ 正確。web-ui.html Executors 頁顯示各 executor 的 task / shuffle / 記憶體用量；spill 量可在此對到 executor。方向對。

**主張 U**「掃太多/小檔：EXPLAIN 的 Scan 缺 PartitionFilters ＝沒裁到分區、整表掃；或讀檔 stage task 數異常多＝小檔多」。
✅ 因果方向正確。缺 PartitionFilters → 未裁剪 → 掃全表分區（同主張 I）。讀檔 task 數＝讀檔 partition 數，小檔過多會使 partition/task 數暴增（§1.2 開檔成本），方向對。
　- 一點精確化（可加強，非錯）：`spark.sql.files.maxPartitionBytes` 預設下，Spark 會把多個小檔「打包」進同一 partition（minPartitionNum / openCostInBytes 邏輯），所以「小檔多 → task 數爆多」在**未特別設定**時不總是成立（packing 會緩解）；但在「大量目錄/分區 + 每分區小檔」或關閉打包時確會 task 數爆增。本章把它列為**症狀訊號**（task 數異常多 → 懷疑小檔）方向沒錯，僅嚴謹度上可注記「視 file packing 設定」。不影響速查表用途。

**主張 V**「該 broadcast 卻沒有：EXPLAIN 小表處是 SortMergeJoin 而非 BroadcastHashJoin」。
✅ 正確。預期小表走 broadcast 但計畫顯示 SortMergeJoin，確為「未走 broadcast」訊號（成因：超過 autoBroadcastJoinThreshold、統計缺失、或被停用）。方向對，第03/04章接手合理。

---

### §2.9 收尾 + 精確度說明（高風險點 6：自承 4 項是否如實/有無遺漏）

逐項核對章末「資料來源與精確度說明」4（實為 5）條：
- **#1 History Server port 18088 vs 18080**：✅ 如實、區分正確（見主張 E）。
- **#2 live UI 經 YARN RM/AM 進入「未能逐字擷取」**：❌ **此自承與真相不符的方向相反**。問題不在「未能查證」，而在**正文寫了一個與 CDP 官方相反的硬性建議**（「不該直連 4040」）。CDP running-app 官方頁恰恰要你直連 4040（見主張 C）。→ 這條**不是「無法查證」，是「正文主張須修」**；精確度說明的措辭也需同步更正。**真缺陷。**
- **#3 EXPLAIN FORMATTED 未貼逐字輸出**：✅ 如實，合理保留。
- **#4 AQE 最終計畫標籤字樣 `AdaptiveSparkPlan` 未能逐字查證**：⚠️ **過度保守**，實際可查證（SPARK-33850 / SPARK-30331）。可升級為已驗證（見主張 N）。
- **#5 Summary Metrics 分位數列標未能逐字擷取**：✅ 如實（見主張 O），官方頁確無逐字字串，標 ⚠️ 恰當。

**遺漏該標而未標者**：精確度說明#2 把「live UI 經 YARN/AM」僅標為「未能逐字查證」，**未揭露它其實與 CDP 官方對 running app 的指引（直連 4040）相牴觸**——這是該標為「正文須修正」卻被弱化成「查證不足」的一處，屬遺漏。

---

## 結尾彙整（依三級）

### A. 真缺陷（必補）

1. **§2.2 主張 C — live UI 入口的因果/實務方向相反**：正文寫「CDP/YARN 上你**通常不會、也不該**直接連 driver 的 4040，而是經 ResourceManager → ApplicationMaster」。但 CDP 7.1.9 官方「Accessing the Web UI of a **Running** Spark Application」明確指示**直連 `http://spark_driver_host:4040`**，且未述 RM→AM 路徑。這是把「可選的 YARN proxy 路徑」誤寫成「硬性禁令」、且引用方向相反。
　- **修法**：改為「running app 官方建議直連 `http://<driver_host>:4040`；若 driver host 不可直達，可從 YARN ResourceManager UI 經 ApplicationMaster / proxy 進入」。出處改引 CDP「running application」頁（cm-accessing-the-web-ui-of-a-running-spark-application.html）。
　- 連帶：精確度說明#2 同步更正（不是「未能查證」，是「正文主張修正」）。

### B. 可加強（斟酌）

2. **§2.5 / 精確度#4 — `AdaptiveSparkPlan` 字串實際可查證**：可從「工具未能逐字查證」升級為已驗證，並精確化為「同一 `AdaptiveSparkPlan` 根節點，EXPLAIN 時 `isFinalPlan=false`（初始計畫）、執行完成後為 `isFinalPlan=true`（最終計畫）」。出處 ASF JIRA SPARK-33850 / SPARK-30331。
3. **§2.6 spill 成因措辭**：spill 嚴格反映「execution memory 相對暫存量不足」（成因含 partition 太大/skew/agg 暫存爆量），非單純「executor 記憶體配太小」。現有解法分流已正確涵蓋，僅措辭可更精準。
4. **§2.7/§2.8 主張 U — 小檔→task 數**：可注記「視 file packing（maxPartitionBytes/openCostInBytes）設定」，因 Spark 預設會打包小檔；作為症狀訊號方向無誤。
5. **§2.2 主張 F「CDP event log 預設開」**：未附逐字出處，建議標為平台實務或加 CDP 佐證。

### C. 誤讀 / 不改或微調

6. **§2.6 主張 O & 精確度#5（分位數列標 Min/25th/Median/75th/Max）**：官方 web-ui.html 確無逐字字串，本章已如實標 ⚠️、屬 Spark UI 標準呈現——**保留現狀正確**。
7. 其餘 metric 名稱（`shuffle bytes written total`、`Shuffle Read Size / Records`、`Shuffle spill (memory/disk)`、`number of output rows`）、頁籤組成、EXPLAIN 模式、port 18088/18080、skew/spill/爆量 join 的因果正負號——**全部逐字/方向正確**，無須改。

> 本輪新增 WebFetch/WebSearch 來源：
> - docs.cloudera.com/.../cm-accessing-the-web-ui-of-a-running-spark-application.html（CDP running app 直連 4040）
> - spark.apache.org/docs/latest/running-on-yarn.html（YARN tracking URL / proxy / History Server fallback）
> - issues.apache.org/jira/browse/SPARK-33850、SPARK-30331（AdaptiveSparkPlan / isFinalPlan）
> - docs/latest/web-ui.html、docs/latest/sql-performance-tuning.html（覆核 Summary Metrics 無逐字分位數列標、AQE 預設開且執行期 re-optimize）
