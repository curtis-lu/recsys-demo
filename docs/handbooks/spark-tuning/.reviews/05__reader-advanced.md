# 第 05 章審稿 — 進階 Analytics Engineer 視角
> 人設：會 SQL 也會一點 Python，營運共用特徵庫供訓練、做 reverse ETL 回業務系統。關心：共用表的儲存設計、schema 演進、統計維護對長期營運的深度。
> 聚焦確認深度：§5.6 ANALYZE/CBO、§5.7 bucketing、§5.8 Hive3 managed/external/schema 只加不改。
> 審稿重點：對「營運多人共用特徵庫」這條線深度夠不夠、有沒有缺一個你實際會碰到的考量。不查技術對錯、不改稿。

---

## 逐節深度評

### §5.1 本章地圖
**評：夠，快速定位**

「本錢存進去 vs 花用本錢」的分界把第 03 章的前提曝光，對我有效——我常碰到的情況正是「SQL 寫對了、但表存爛了，所以再怎麼寫法都快不起來」。無缺口。

---

### §5.2 用對格式：Parquet/ORC
**評：夠深，進階子節對特徵庫產表很有用**

Row group → column chunk → page 的三層結構說清楚了，「排序讓 row group min/max 範圍窄→跳更多」這個因果是我會拿去設計特徵庫產表 pipeline 的直接依據（把最常被下游過濾的欄——例如時間維度或 segment——在寫出前 `SORT BY`）。dictionary encoding 對低基數欄的雙重好處（省空間＋跳塊）也是我的特徵庫工作裡很實用的知識。

**缺口（中等，直接影響共用特徵庫的 reverse ETL 設計）：分區欄選擇對 reverse ETL 的影響沒出現。**

特徵庫的下游不只有 Spark 查詢，還有 reverse ETL——定期把特徵數據以「單一 entity（客戶）為單位」抽出去，丟給業務系統（CRM、行銷平台）。這個場景的過濾欄不是時間，而是 `cust_id` 或業務系統定義的細分群（segment）。§5.2 進階子節說「把最常篩的欄排好再寫出」，但沒有特別說明：**當 reverse ETL 的 key（entity ID）和產表的排序鍵不一致時，row group 統計幾乎幫不上忙**，反而應考慮是否要為 reverse ETL 另維護一張按 entity ID 排序（或按 entity 分桶）的衍生表，而非硬把共用特徵庫按 entity 排序。這個決策缺口在 §5.4 和 §5.7 裡也都沒有接起來。

---

### §5.3 壓縮取捨
**評：夠，無需更深**

熱表 snappy / 冷表 zstd 的取捨對我來說已是常識，但放在手冊裡的寫法乾淨正確、無冗餘。無缺口。

---

### §5.4 設計 partition
**評：核心取捨對，但缺一個共用特徵庫常碰的決策情境**

「查得到又不過度分割」的原則對。月份／日期是安全牌的說明也具體。

**缺口（對多用途共用表是真實困境）：雙重分區欄位的選擇衝突。**

特徵庫通常有兩類消費者：
1. 訓練 pipeline：`WHERE snap_date BETWEEN ... AND ...`（時間範圍過濾）。
2. 推論 pipeline（也包含 reverse ETL）：`WHERE entity_id IN (...)` 或 `WHERE segment_type = 'X'`（entity 維度或 segment 過濾）。

「按 `snap_date` 分區」對訓練友善，但推論/reverse ETL 幾乎每次都要全時間範圍掃，分區裁剪沒用。這個雙重用途衝突下的選擇邏輯（例如：是否用雙層分區 `snap_date / segment`？還是接受推論端全掃、靠 row group 排序優化？還是維護兩張衍生表？）在本章完全缺席，是特徵庫維運者最需要決策依據的點。

---

### §5.5 管好檔案大小
**評：夠完整，四條成因串得好**

成因二（shuffle 分區）和成因四（窄依賴直通）對我最有用，因為這兩條最難從「看目錄」一眼看出原因。openCostInBytes 的計算示意能讓我在碰到「為什麼 task 數遠超資料量應有的」時有具體心算依據。

`REPARTITION(dt)` vs 裸 `REPARTITION(n)` 在動態分區場景的分析切到了真正的雷，好。無重大缺口。

---

### §5.6 ANALYZE/CBO
**評：基礎對，但統計過期對查詢計畫的影響輕描淡寫**

「產完表後跑一次」的節奏說了，`NOSCAN` 便宜夠用的說明也對。AQE 與 `ANALYZE` 互補、不衝突的澄清很清楚——這是我確實碰過同事問的問題。

**缺口一（重要）：統計過期（stale statistics）的查詢計畫影響沒說清楚。**

特徵庫每週 append 新快照。如果我忘記（或排程漏跑）`ANALYZE`，Metastore 裡的統計停留在上週的「列數＋大小」，而這週資料量可能是上週的 1.1 倍。在有 CBO 的環境，stale statistics 會讓 join 順序決策建立在舊的基數估計上——這個「統計過期讓優化器選錯計畫」的後果是什麼樣子、徵兆是什麼（例如：明明昨天還快、今天突然慢、查 Spark UI 發現 broadcast 消失了），完全沒說。對長期排程維運者，**統計過期**是比「忘記跑 `ANALYZE`」更難察覺的問題。

**缺口二（次要）：`FOR COLUMNS` 在特徵庫場景的選欄策略。**

一張 1500 欄的特徵表，跑 `ANALYZE … FOR COLUMNS` 如果收所有欄，代價非常高。哪些欄的欄位級統計對 CBO 最有價值（join key、常用過濾欄）？還是說對特徵庫這個場景，`FOR COLUMNS` 幾乎不值得、`NOSCAN` 就夠？這個決策邏輯缺席。

---

### §5.7 bucketing
**評：有點到最重要的雷（Hive/Spark 不相容），但缺一個特徵庫常見的 failed adoption 場景。**

「先不用急著自己設 bucketing」的務實立場我認同。Spark/Hive hash 不相容的警告對 CDP 環境很關鍵——這是我確實需要告訴同事的事。

**缺口（有時間才考慮）：「bucketing 後 shuffle 確實省了嗎？」的驗證方法缺失。**

手冊建議「有特定反覆 join 場景、且確認過相容性後才上」，但沒說**怎麼確認 bucket-aware 優化真的生效**（即 `EXPLAIN` 或 Spark UI 裡出現 `SortMergeJoin` 改為 `BucketJoin` / shuffle 消失）。在我的特徵庫工作裡，若要評估「是否值得為 entity-key 建 bucketing」，需要有辦法在實驗後驗證收益。這個驗證步驟不給，「先實測驗證」落不了地。

---

### §5.8 Hive3 managed/external 與 schema 演進
**評：框架對，但共用特徵庫的兩個核心營運問題深度不足。**

managed vs external 的分野說清楚了，「Spark SQL `CREATE TABLE` → external」的結論對 CDP 環境很實用。HWC 的存在及「讀不到時去問平台」的指引也對。

**缺口一（A 級，是特徵庫維運最常碰的問題）：external 表給多引擎共用（Impala/Spark/dbt）時，schema 的可見性與快取問題完全缺席。**

§5.8 說 external 表「登記在共用 Metastore 供 Hive/Impala 查」，但這件事在多引擎環境裡有個實際陷阱：**Impala 不會自動感知 Metastore 的變化，必須手動 `INVALIDATE METADATA <table>` 或 `REFRESH <table>` 才能看到新分區或新欄**。這是共用特徵庫維運最常碰的跨引擎同步問題——我的 Spark 排程跑完、寫進新分區，但下游 Impala/Hue 查詢的同事看不到新資料（或讀到舊 schema），要去跑 `REFRESH`。這個操作缺席整章，讀者以為 Metastore 登記了就全平台可見，實際上不是。

**缺口二（A 級）：schema 演進的「只加不改」細化至版本管理策略嚴重不足。**

「加欄安全、改/刪危險」這個通則說了，但特徵庫的實際演進遠比「加一欄」複雜：

- **特徵版本化**：同一個 entity，不同時期的 feature schema 不一樣（A/B 實驗、特徵淘汰），要怎麼管？是加 `feature_build_version` tag（CLAUDE.md 裡提到的 deferred 路徑）？還是新開表？手冊完全沒有指引方向。
- **加欄後的下游通知機制**：「只加不改」只保護了「舊查詢不壞」，但下游的 training pipeline 如果有 `SELECT *` 拉特徵，加了新欄它不知道；如果是明列欄名，新欄它用不到。特徵庫的 schema 演進需要配套的變更通知機制——這章完全沒觸及。
- **backfill 的 schema 一致性**：回填歷史分區時，舊分區的 schema（欄數、型別）和新分區不同，`mergeSchema=false`（預設）下 Spark 讀跨分區資料時欄數不符的行為——這正是 `mergeSchema` 選項存在的原因，但 §5.8 只說「`mergeSchema` 預設關、要時才開」，沒有說在什麼情境下你**必須**開（即：歷史回填後欄位數跨分區不一致時）。

---

### §5.9 串起來的設計示例
**評：對，教學價值高**

六步驟把全章收攏的方式清楚，`ANALYZE PARTITION(month=...)` 的具體語法是我需要的細節（很多人只記得表層 `ANALYZE`、不知道可以指定分區）。無缺口。

---

## 三級彙整

### A 級缺口（共用特徵庫維運的根本資訊空白，應補）

1. **Impala `INVALIDATE METADATA / REFRESH`（§5.8 缺）**：external 表寫完新分區，Impala/Hue 不會自動感知；`REFRESH` 是共用特徵庫維運最常被遺忘的跨引擎同步步驟。現在讀者以為 Metastore 登記即全平台可見，這個誤解在多引擎環境會直接造成「同事說看不到新資料」的支援負擔。

2. **統計過期（stale statistics）的可觀測性（§5.6 缺）**：「跑完 `ANALYZE`」只說了時機，沒說統計過期時的徵兆（Spark UI 的計畫變化、broadcast 消失、AQE 行為改變）。對長期排程維運者，**察覺問題比記得操作更難**，兩者的重要性不亞於。

3. **schema 演進細化——backfill 跨分區 schema 不一致的 `mergeSchema` 使用判斷（§5.8 缺）**：§5.8 提了 `mergeSchema` 預設關，但沒說「何時你必須開」，以及開了之後的效能代價（每次讀都要掃所有分區的 footer 合併 schema）。歷史回填是特徵庫最常觸發這個問題的操作。

### B 級缺口（讓「特徵庫維運」更落地，建議補）

4. **分區欄選擇對 reverse ETL 的影響（§5.4 / §5.2 缺）**：訓練 vs 推論/reverse ETL 的過濾欄不同，共用表按 `snap_date` 分區是訓練友善但推論不友善的選擇；「是否維護兩張衍生表」的決策邏輯缺席，是特徵庫維運者最需要的設計判斷。

5. **`ANALYZE FOR COLUMNS` 的選欄策略（§5.6 缺）**：1500 欄特徵表全收欄位統計代價高，哪些欄的 column statistics 對 CBO 最有效？還是在特徵庫場景 `NOSCAN` 就夠？沒有決策依據。

6. **bucketing 生效的驗證方法（§5.7 缺）**：「先實測驗證」的建議落不了地，因為沒說怎麼在 `EXPLAIN` 或 Spark UI 裡確認 bucket-aware join 確實省了 shuffle。

### C 級缺口（可選，加了更完整）

7. **特徵版本化與 schema 演進的版本管理策略方向（§5.8 缺）**：`feature_build_version` tag vs 新開版本表的取捨方向，即使是一句「方案見第 07–08 章」的指引也比完全缺席好，現在讀者面對這個決策完全沒有錨點。

8. **下游 training pipeline 對新欄的感知問題（§5.8 缺）**：「只加不改保護舊查詢」但不保護「下游 `SELECT *` 是否自動用到新欄」，schema 演進需要配套通知機制，哪怕一句提醒也比完全沒說好。
