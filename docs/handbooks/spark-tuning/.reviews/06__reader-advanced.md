# 第 06 章 進階讀者審查日誌（reader-advanced）

**讀者人設**：進階 analytics engineer。
- 會 SQL 也會 Python，在資料平台上維運**特徵庫**（共用特徵表，供模型訓練 pipeline 拉取）。
- 日常工作：用 Spark 跑特徵計算 ETL→寫回 Hive 表→供 Impala/Hue 查詢做 ad-hoc 驗特徵→另一個 reverse ETL 把結果推回業務系統。
- 關心：**多引擎共用同一份資料的營運課題**——誰能讀寫、metadata 何時同步、ACID 表跨引擎限制的實際邊界、表的 ownership（誰管 compaction、誰管 REFRESH、誰負責 schema 對齊）。
- 不關心：「Spark 比 MapReduce 快」這種基礎定位——那我都懂，篇幅佔到影響深度才要標。

**聚焦節**：§6.5（共用表威力與陷阱）、§6.6（REFRESH vs INVALIDATE METADATA + CDP 自動同步）、§6.7（ACID 跨引擎限制矩陣）。

**評估維度**：深度夠不夠、有沒有缺一個實際會碰到的坑。不查單點技術對錯，不改稿。

---

## §6.5 共用表威力與陷阱

### 正面
- 「同一份資料、三引擎各取所長」這個核心訊息（L131–139）對我有價值，快速確認了「external Parquet 是最大公約數、不需 HWC」。
- §6.5 把後兩節的兩大陷阱編號預告（L141–144），讀者知道往下要花力氣讀什麼，導引清楚。

### 深度缺口

**缺口 1：「誰能寫」的雙向流沒有講清楚——reverse ETL 場景在哪？**

§6.5 只描述「Spark ETL 產表 → Impala 互動讀」這個**單向流**（從特徵工程角度）。對我這種也要把結果**推回業務系統**的 analytics engineer，更重要的問題是：「downstream 的 reverse ETL（可能是另一個 Spark job 或業務端的 Hive pipeline）要把資料寫回去時，誰可以寫、誰寫會出事？」

§6.5 完全沒觸及「寫端」的多引擎問題，只說「Spark 寫、Impala/Hive 讀」是理所當然的分工。但實務上特徵庫會有：Spark 主 pipeline 寫、補數腳本（業務端可能用 Hive）也寫、甚至 BI 工具（Impala）做 INSERT 修正——多寫端衝突沒有任何提示。

**評分**：§6.5 作為「共用讀的指引」夠，作為「多引擎共用寫的指引」深度缺失。

---

**缺口 2：external 表的「誰管 schema 演化」沒提**

§6.5 說 external Parquet 是三引擎最大公約數，卻沒點出 external 表的一個陷阱：**schema 在 Metastore 和 HDFS 物理檔案可能不一致**（例如 Spark 寫了新欄位、但 Metastore 的表定義還是舊的；或反過來 Hive ALTER TABLE ADD COLUMN 後 Spark 讀到的 schema 和 Parquet 物理欄位不對）。

這個「schema drift」對特徵庫場景是頻繁發生的坑（加特徵→Spark 重跑→Metastore 未更新→Impala 查不到新欄位），卻完全缺席。

---

## §6.6 REFRESH vs INVALIDATE METADATA + CDP 自動同步

### 正面
- 「Spark 寫完接一個 REFRESH、別賭自動同步」（L174）這條落地建議我會直接用，很值。
- `REFRESH partition`（L164，只刷一個分區）提到了，對大表很重要。
- ⚠️ 框「INVALIDATE METADATA 不帶表名 = 全部失效、非常貴」（L168）是真實雷，有單獨框出很好。
- 「自動同步有輪詢延遲、可能被關」（L172）的兩個條件誠實。

### 深度缺口

**缺口 3：Spark 寫完到 Impala 看到，實際的 metadata 同步延遲是多少？**

§6.6 說「CDP 事件驅動自動同步，catalogd 定時輪詢 HMS 事件」（L171），但沒給**輪詢間隔的量級**（`hms_event_polling_interval_s` 預設值）。對我這種要在排程裡決定「要不要手動 REFRESH」的人，「有延遲」是不夠的——我需要知道「大概幾秒/幾分鐘後會自動同步，超過這個就要手動」。

目前章節給的訊息是「有延遲 + 可能被關」，讓我永遠「別賭自動同步」，但沒幫我做判斷：如果我的排程 Spark 寫完到 Impala 跑有 30 分鐘的間距，我需要手動 REFRESH 嗎？如果是 2 分鐘呢？

**評分**：對進階讀者，這條資訊缺口會造成他在排程設計上的困難。建議補一個量級（如「預設輪詢間隔通常在秒到分鐘級，確切以你叢集配置為準；若 Spark 寫完與下游 Impala 查詢的間距短於一個輪詢週期，建議手動 REFRESH」）。

---

**缺口 4：`REFRESH` 的冪等性與排程失敗安全性沒提**

§6.6 建議「在 Spark 寫完那一步後面接一個 REFRESH」（L174），但沒說：
- 如果排程中 Spark 寫完、REFRESH 失敗（Impala 掛了或網路斷），整條排程怎麼辦？REFRESH 是冪等的嗎（重跑是否安全）？
- 如果 Spark 寫到一半失敗（partial write），REFRESH 後 Impala 是否會看到半筆資料？

對維運特徵庫的人，排程的 failure mode 是非常實際的問題。章節只給「接一個 REFRESH」，沒給 failure 後的處置。

---

**缺口 5：Hive 寫完之後，Spark 也需要做等價操作嗎？**

§6.6 全節的視角是「Spark 寫 → Impala 看不到 → REFRESH」。但特徵庫還有另一種常見流向：**Hive pipeline（業務端）寫或更新表 → 下游 Spark 訓練 job 讀**。

這個方向是否也有等價的 metadata 同步問題？Spark 讀取時是否有自己的 catalog 快取（Spark 的 catalog 是 `SparkCatalog`，也可能快取表資訊）？章節對這個方向完全靜默，讓讀者誤以為「同步問題只有 Impala 端」。

實務上 Spark 3.x 的 `spark.sql.catalog` 也會快取 Hive Metastore 的 schema 資訊，跨 session 重用的 SparkSession 尤其可能讀到過時 schema。

---

**缺口 6：`REFRESH` 之後 Impala 的 execution plan 需要重新 explain 嗎？**

進階使用者會有一個具體問題：對 Impala，`REFRESH` 更新了 metadata（分區清單、檔案清單），但 Impala 對同一張表的**cached execution plan（查詢計畫）**是否也自動作廢？還是說 REFRESH 只更新 catalog、舊 plan 可能繼續被用（基於過時的統計）？

`COMPUTE STATS`（統計量）與 `REFRESH`（metadata）是兩件事，§6.6 沒區分這兩個動作的職責。對效能導向的進階讀者，Spark 寫完後「需不需要同時補一個 `COMPUTE STATS`」是常見問題。

---

## §6.7 ACID 跨引擎限制矩陣

### 正面
- §6.7 補了完整的「為什麼需要 ACID」基礎（L190–207），對沒學過 DB 交易的讀者是必要的。
- 「表種小地圖」先立（L210–211 managed→{full ACID, insert-only} + external），再看矩陣表，結構正確。
- 設計原則「external Parquet 收掉所有跨引擎麻煩」（L221–225）是正確的主軸。
- 三個情境（更正/合規刪/upsert）對業務讀者高度可代入（L207）。

### 深度缺口

**缺口 7：Impala 讀 full ACID 表時，compaction lag 對讀到的資料有影響嗎？**

§6.7 說 Impala 可以讀 full ACID 表（L214–216）。但 ACID 表的讀取要把 base + delta 疊合計算最新值，**如果 compaction 沒跑（delta 累積多）**，Impala 讀到的是：（a）正確值但讀很慢（要掃很多 delta）、（b）可能讀不到最新 delta（Impala 是否支援讀 open transaction 的 delta）？

這個「Impala 讀 full ACID 的實際效能與正確性邊界」對「要不要讓 Impala 讀 Hive ACID 表」這個設計決策非常重要，但完全沒提。

---

**缺口 8：compaction 的運維責任誰扛？**

§6.7 提到「delta 累積多了要用 compaction 合併」（L200），但沒說：
- 誰觸發 compaction？只有 Hive 能跑（Spark/Impala 不能），那特徵庫的 compaction 要在哪裡排進排程？
- Compaction 跑的時候，Impala 或 Spark 能不能同時讀？（compaction 是否有 table-level lock？）
- 如果 ACID 表長期沒跑 compaction（特徵庫可能幾百個 Hive pipeline 的 ACID 表沒人管），會怎麼樣？

對維運特徵庫的工程師，compaction 的**運維 ownership** 是選 ACID 表最大的隱性成本之一。§6.7 只說「compaction 有這回事、合併增量用」，完全沒點出「這個成本需要有人持續排程執行，且只有 Hive 能做」。

---

**缺口 9：Spark 透過 HWC 讀 ACID 表，實際的效能與限制**

§6.7 說「Spark 讀/寫 managed 表需走 HWC」（L213–217 表格），但沒說：
- HWC 是不是在所有 Spark 版本/CDP 版本都開箱即用，還是需要額外配置（driver、classpath、hive.metastore.uris）？
- HWC 的效能相對直讀 external Parquet 有多少代價（I/O 模式不同：HWC 走 Hive JDBC 或 arrow-based？）？
- Spark 透過 HWC 讀 ACID 表是否支援 partition pruning 和謂詞下推（predicate pushdown）？（如果不支援，特徵訓練 job 每次全表掃，成本很高。）

對特徵庫場景（特徵表往往幾億列），「HWC 有沒有 pushdown」是選不選 ACID 表的關鍵決策因素。

---

**缺口 10：Spark 直讀 Hive ACID 表（不走 HWC）會怎樣——silent wrong answer 的風險**

§6.7 說 Spark 讀 managed 表需走 HWC，但沒警告**如果 Spark 不走 HWC、直接讀 ACID 表的 HDFS 路徑會怎樣**。

實務陷阱：Spark 直讀 ACID 表的 HDFS 目錄時，看到的是 base/ + delta_xxx/ 的目錄結構，**Spark 不知道這是 ACID 表**，會把所有 Parquet/ORC 檔案都讀進來（包含 delete delta），**算出的結果可能是「包含被刪除列」的錯誤資料，且沒有任何錯誤訊息**（silent wrong answer）。

這個「不走 HWC 讀 ACID 表 = silent wrong answer」的風險對特徵庫場景是真實地雷（特徵庫如果混進已刪除的客戶資料，訓練出來的模型可能靜默帶入髒資料）。§6.7 完全沒提這個坑。

---

**缺口 11：Hive INSERT OVERWRITE 對 ACID 表的行為——與 external 表不同**

§6.7（含 §6.9）把「整批 partition 覆寫」（INSERT OVERWRITE PARTITION）當成 external Parquet 的替代方案（L222–225，L263）。但讀者可能以為：「我有一張 Hive managed ACID 表，我可以對它 INSERT OVERWRITE PARTITION 嗎？」

答案在 Hive 3 + full ACID 表上有限制：full ACID 表不支援傳統的 `INSERT OVERWRITE`（或行為有版本差異），ACID 表的「覆寫」要改用 MERGE 或 TRUNCATE + INSERT。這個差異對「從 external Parquet 遷移到 ACID 表」的 DML 兼容性是常見踩坑點，但 §6.7 沒提。

---

## §6.5–6.7 橫跨的結構性缺口

**缺口 12：特徵庫場景的「全端流程」沒有一條整合說明**

本章 §6.9「一天的情境」（L261–279）用的是「凌晨 Spark 產表 → 白天 Impala 互動」這個**單讀者、單方向**的場景。對維運特徵庫的 analytics engineer，實際上是多方寫入的多環場景：

```
特徵計算 Spark ETL
  → 寫 Hive external Parquet（特徵主表）
  → 補分 Hive pipeline（業務端，寫同一張表的另一個分區）
  → Impala 查特徵分佈（QA 驗特徵）
  → 訓練 Spark job 讀特徵
  → 推論 Spark job 讀特徵 → 寫結果表
  → reverse ETL Spark job 讀結果表 → 寫業務端 Hive ACID 表
```

這個流裡有多個 metadata 同步點（每個寫操作後 Impala/Spark 可能都需要 REFRESH 或重新拉 schema）、有多寫端衝突風險（同一張特徵表兩個 Spark job 同時寫不同分區是否安全）、有 ACID/external 混用（reverse ETL 寫業務端是 ACID 表）。

§6.5–6.7 沒有一節把這個多端流程串起來，讀者得自己把各節的碎片拼合，而拼合中會發現有些缺口（如多寫端、schema drift、compaction ownership）根本沒有指引。

---

## 三級彙整

### A. 真深度缺口（對目標場景的進階讀者，會卡在實際決策上）

1. **缺口 3：自動同步的延遲量級未給**（§6.6）——知道「有延遲」不夠，進階讀者需要量級才能決定排程間距是否要加手動 REFRESH。建議補輪詢間隔的典型量級（秒到分鐘級），並給一個判斷門檻的指引。

2. **缺口 10：Spark 直讀 ACID 表 = silent wrong answer**（§6.7）——這是本章最高風險的缺失坑。不走 HWC 直讀 ACID 表 HDFS 路徑，Spark 會讀入 delete delta 導致結果包含已刪除列，且沒錯誤訊息。對特徵庫場景（特徵混入髒資料、模型靜默出錯）是真實地雷，必須在 §6.7 明確警告。

3. **缺口 8：compaction 的運維 ownership 沒說**（§6.7）——「只有 Hive 能跑 compaction」是選 ACID 表的隱性成本，決定了特徵庫維運架構（要排 Hive 的 compaction 作業、且 compaction 期間的讀取行為）。沒點出這個，「用 ACID 表」的決策指引是不完整的。

4. **缺口 5：Hive 寫 → Spark 讀的 metadata 同步問題沒提**（§6.6）——§6.6 只講「Spark 寫 → Impala 同步」，但 Spark 本身也有 catalog 快取，Hive 寫完後 Spark（長 session）可能讀到過時 schema，此方向在特徵訓練場景很常見，完全靜默。

### B. 可加強（進階讀者在做決策時有感，但不會卡死）

5. **缺口 7：Impala 讀 full ACID 的效能邊界**（§6.7）——說 Impala 能讀就好，但沒說 compaction lag 對讀效能的影響，讀者無法評估「要不要讓 Impala 直讀 ACID 表還是等 compaction」。

6. **缺口 9：HWC 的效能與 predicate pushdown 支援**（§6.7）——說「Spark 讀 managed 表要走 HWC」，但沒說 HWC 是否支援 partition pruning/pushdown，對特徵訓練 job 的效能影響是真實問題。

7. **缺口 6：REFRESH 後是否需要 COMPUTE STATS**（§6.6）——§6.6 沒區分「metadata 同步」（REFRESH）和「統計量更新」（COMPUTE STATS）兩件事，進階讀者會問「Spark 寫完 REFRESH 後，Impala 的 query plan 還用舊的 cardinality 估計嗎？」。

8. **缺口 4：REFRESH 的冪等性與排程失敗安全**（§6.6）——建議補一句說明 REFRESH 是冪等的（重跑安全）、但如果 Spark 寫到一半失敗後 REFRESH，Impala 看到的是 partial data（這是 external 表本身的問題，不是 REFRESH 的問題）。

9. **缺口 11：full ACID 表不支援 INSERT OVERWRITE 的行為差異**（§6.7）——DML 兼容性問題，對從 external 遷到 ACID 的讀者是踩坑點。

10. **缺口 2：external 表的 schema drift**（§6.5）——Spark 加欄後 Metastore 定義未更新（或反過來），是特徵庫加特徵時的常見坑，§6.5 完全未提。

### C. 不影響進階讀者（可留）

11. §6.1–6.3 的「引擎選用基礎定位」（決策樹、甜蜜點）對進階讀者略輕，但它是 §6.5–6.7 的必要鋪墊，保留合理。
12. §6.9「一天情境」的場景簡化（單寫端、單方向）作為示意可接受，但若能加一句「特徵庫的多端寫入場景可對照 §6.5–6.7 自行組合」，幫助進階讀者知道去哪拼。
13. §6.6 的 `REFRESH` vs `INVALIDATE METADATA` 主幹說明對進階讀者無誤，不需要改主線。

---

## 整體深度評定

**§6.5 共用表威力與陷阱**：對「讀端多引擎共用」夠，對「寫端衝突」和「schema 演化」深度不足。評定：**中等深度**（適合 SQL-first 讀者，對進階 analytics engineer 有感缺口）。

**§6.6 REFRESH vs INVALIDATE METADATA**：主線正確，落地建議有用。進階讀者的主要缺口在「同步延遲量級」、「反向流（Hive→Spark）」、「REFRESH vs COMPUTE STATS 區分」。評定：**中偏深**（主線夠，但排程設計層的細節需補）。

**§6.7 ACID 跨引擎限制矩陣**：基礎說明充分，三情境落地，矩陣表清楚。進階讀者的核心缺口是「silent wrong answer 的風險警告」、「compaction 運維 ownership」、「HWC 的 pushdown 與效能」。評定：**中等深度**（對 SQL-first 夠，對維運特徵庫的工程師有 3 個真實地雷沒覆蓋）。

**最關鍵的一個補強點**：缺口 10（Spark 直讀 ACID 表 = silent wrong answer）是本章對進階讀者最應該補上的安全警告，這個坑在特徵庫場景真實發生、損失高、沒有任何指引。
