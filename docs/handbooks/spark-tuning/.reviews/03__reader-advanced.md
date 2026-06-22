# 第 03 章 「SQL 寫法優化」審稿報告
**角色**：進階 analytics engineer（SQL 老手、營運共用特徵庫、大表 join / 特徵產出 / reverse ETL）
**評審目標**：深度 vs 營運實用性；不查單點技術對錯；不改稿。

---

## 逐節深度評

### §3.1 本章地圖
**評：夠用（導覽節，深度要求低）**
mermaid 一圖兩柱串起全章，清楚。沒有缺口；這個位置不需要深。

---

### §3.2 partition 裁剪
**評：夠營運，但有一個中階缺口值得補**

「36 倍」示意直覺好、`EXPLAIN PartitionFilters` 的驗證路徑清楚，是合格的「我改好了嗎？」錨點。

**缺口**：
- 只提「按欄位分區」，但實務上常見**多層分區**（`month` / `region` / `product_line`）。分區欄位的篩選順序、哪一層先裁影響最大，是 analytics engineer 手上常有的問題，但本節完全沒提。可以一句話帶過「多層分區時，只要命中最高層就省最多；細節見第 05 章」。
- 「partition 欄位怎麼選」forward-ref 到第 05 章 ✓，OK。

---

### §3.3 欄位裁剪（別 SELECT *）
**評：偏淺，但本章地位是「純賺、無腦做」，深度要求相對低**

原理講到欄式格式已足夠。

**缺口**：
- 實務上最讓人痛的是 **CTE / 子查詢層層 `SELECT *`**，最後只用到 3 欄——欄位裁剪的 pruning 會不會穿透 CTE 邊界，是 analytics engineer 常常不確定的地方。本節只說「包一層 view 用 `*` 會失效」但沒說 CTE 的情況。可補一句「CTE 若在 `SELECT *` 後立刻只 `.select(...)` 某幾欄，Catalyst 多數情況能穿透；但最保險的還是每層只帶需要的欄位，以 `EXPLAIN` 的 `ReadSchema` 確認」。
- 這對「做特徵寬表下游 join」的 AE 很貼近，值得多一句。

---

### §3.4 predicate pushdown 與失效場景
**評：夠營運，是本章少讀三節中最深的，深度恰當**

「用函數包住欄位 → 失效」+ 改法的 before/after 是標準坑。「包函數 → 連分區裁剪也失效」特別有價值，很多人以為這是兩個獨立機制。

**缺口**：
- 本節提了「Parquet 的 row-group 統計可跳塊」，但 **ORC 的 bloom filter / stripe statistics** 在 CDP Hive 表上更常用——這個讀者群用的是 Hive on CDP，ORC 是預設。一句話說明「ORC 的 stripe 統計也同樣生效、原理相同、以 `EXPLAIN PushedFilters` 驗即可」就夠，不需深入，但現在全篇以 Parquet 為主線，讀者拿 ORC Hive 表時可能懷疑「是不是不一樣？」。
- 「只對欄位 vs 常數」的說明夠清楚。

---

### §3.5 join 策略（broadcast vs sort-merge）+ 補充表
**評：夠深，補充表是加分項，`BroadcastNestedLoopJoin` 警訊講得特別好**

非等值 join → nested loop 的警訊，加上「先等值 join 再 WHERE 過濾範圍」的實務改法，是 analytics engineer 手上最常踩的大雷之一，這邊點出來很值錢。

**缺口**：
- **semi join / anti join 的 broadcast 行為**完全缺席。特徵庫過濾「出現在推論母體的客戶」時，`WHERE cust_id IN (SELECT cust_id FROM population)` 或 `LEFT ANTI JOIN` 是日常；這兩種在 EXPLAIN 裡有時走 `BroadcastHashJoin`（semi/anti 型），有時走 `SortMergeJoin`，規則跟一般 join 稍微不同。既然補充表已經收了五種策略，再補一欄「semi / anti join 走哪種」（或 forward-ref 到第 08 章）會讓這張表更完整。
- AQE 動態轉 broadcast 有提，但「什麼時候 AQE 轉不了（例如 AQE 無法在某些 outer join 時轉 broadcast）」只在 §3.6 的 hint 取捨一筆帶過。這裡可以一句話呼應。

---

### §3.6 broadcast hint
**評：夠營運，取捨三點講清楚了**

「hint 是建議不是命令」＋「hint 蓋過門檻、小心 OOM」＋「治本補統計」三點清楚，是使用者最需要知道的判斷樹。

**缺口**：
- 實務情境：**過濾後才小的表**（例如 `dim_customer WHERE city = '台北'` 過完剩 5 萬列）。這時 Spark 在計畫期估不準，AQE 通常能在 runtime 轉 broadcast——但若 AQE 沒轉（例如某種 outer join），手動 hint 加在過濾後的**子查詢或 CTE 別名**上，跟加在原表上效果是否一樣？這是 analytics engineer 會撞到的細節。一句「hint 加在子查詢別名上即可；Spark 仍會評估過濾後的大小」或「以 `EXPLAIN` 確認 `BroadcastHashJoin` 有出現」就夠。

---

### §3.7 join 兩個隱藏陷阱
**評：夠營運，型別不符講到「算錯」是最關鍵的一句**

陷阱一「轉成 NULL → 被悄悄少算」是資料正確性問題，不只是慢，這句話值錢。

**缺口**：
- 陷阱二（爆量 join）的解法「**先過濾、先聚合再 join**」只有一行帶過，對這個讀者群其實是高頻操作——特徵寬表先過月份、先 GROUP BY 再 join 是標準 pattern。可以補一個非常短的 before/after 示意，或者 forward-ref §3.11 的貫穿範例（本章有一個，但這邊沒呼應）。
- **多對多 join（join key 兩邊都不唯一）** 沒提。一對多已講，多對多沒提。在特徵庫裡「同一個客戶在同一個 snap_date 有多筆 label，也有多筆特徵」時就會踩到。一句話說「多對多的爆量比一對多更嚴重、應先確認至少一邊 join key 唯一或用 dedup 前處理」就夠。

---

### §3.8 聚合成本（GROUP BY / DISTINCT / COUNT DISTINCT）
**評：夠營運，approx_count_distinct 的說明到位**

HLL++ 原理（搬小摘要不搬原始值）講到這個程度對 SQL 讀者正好，不需要更深。「對帳數字不能用近似」的取捨也說了。

**缺口**：
- **多個不同欄位的 COUNT(DISTINCT)**（例如同一個 SELECT 裡有 `COUNT(DISTINCT cust_id)` 和 `COUNT(DISTINCT txn_id)`）比單個更貴，因為 Spark 必須為每個去重欄位做獨立的 shuffle。這在寫每日 KPI summary table 時很常見。本節說「多個 `COUNT(DISTINCT)` 疊在一起更糟」但沒說為什麼更糟，或者告訴讀者「可以拆成多個 subquery 或 CTE 再 JOIN，讓各自的 shuffle 最小化」。補一句說明或 forward-ref 就夠。
- **`COLLECT_SET` / `COLLECT_LIST`** 在特徵庫裡常用於「把同一客戶的所有交易類型收成陣列」，其 shuffle 成本跟 `COUNT(DISTINCT)` 同量級但本節未提。可在「類似高成本聚合」一句帶過。

---

### §3.9 window function 成本
**評：夠深，「同 PARTITION BY 共用一次 shuffle」是關鍵洞見**

「數 EXPLAIN 裡的 Exchange 個數」是很實用的驗證方法。

**缺口**：
- **`ROW_NUMBER() OVER (PARTITION BY x ORDER BY y DESC) = 1`** 拿最新一筆這個 pattern，在特徵產出和推論母體過濾時非常高頻。本節末說「有時 GROUP BY + 聚合更便宜，不一定要動用 window」——但沒說「在哪種情況下 window 更便宜、哪種情況下 GROUP BY 更便宜」。一句話的判斷原則（例如「若只要取單一最值，GROUP BY + MAX 通常少一次 shuffle；若要同時取多個欄位的當時值，window 比 self-join 便宜」）會讓這段更有操作性。
- **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`** 這類累積窗格在 running total 場景中常用，其成本（需要 order，隱含排序）跟只有 `PARTITION BY` 不同，本節沒提。可一句帶過或 forward-ref。

---

### §3.10 skew 處理
**評：夠深，三層結構（AQE → salting → 分流）是正確的操作層次**

AQE 門檻的細節（5 倍 + 256MB 的雙條件）放在折疊式說明、不遮主線，這個排版決策好。

**缺口**：
- **salting 的 SQL 骨架完全沒有**——本節說「要改寫 SQL、稍微囉嗦」但沒給任何 code 示意。對想動手的人，只有概念圖（打散後再合併）而沒有哪怕 3 行的 SQL 骨架，跨度有點大。`CONCAT(key, '_', FLOOR(RAND() * N))` 打散、`FLOOR(salted_key / N)` 還原這個 pattern 可以 3–5 行示意，或者 forward-ref 第 X 章有完整 salting 範例。
- **NULL key 分流**（陷阱三）雖然在「第三層」提了一句，但和陷阱一（§3.7 型別不符 → NULL）的連結沒打通。型別轉換靜默產生 NULL key → 觸發 skew，是兩個章節的合體大雷，一句呼應會讓讀者「啊，原來是同一類問題」。

---

### §3.11 改寫貫穿範例
**評：夠營運，四個問題點全有驗證路徑**

把章內所有招數串在一條查詢上、末段強調「用 EXPLAIN + UI 驗證才算閉環」，是這一節最有價值的收尾。

**缺口**：
- 範例裡 skew（§3.10）完全沒有出現。若 `dim_customer.segment` 是高度不均（例如「VIP 只有 200 人，一般戶有 999 萬人」），`GROUP BY c.segment` 本身沒有 skew，但若 join 的 key 是 `segment`（非 primary key join）就可能出問題。即使不在這個範例加 skew，一句「若 `cust_id` 有熱點，見 §3.10」的呼應就夠。
- 改寫後的驗證步驟（最後一段）列了四件事，但順序是「EXPLAIN → UI shuffle → spill」，沒提 **Join 算子 output rows** 的確認（§3.7 陷阱二教的）——如果型別對齊後反而讓某個 join 膨脹（例如之前因 NULL 對不到的列現在都對到了），output rows 會大幅增加，這個驗證步驟應在清單裡。

---

### §3.12 一句話帶走
**評：恰當，不需要更深**

把「少讀 → 少搬 → 驗證」壓成一句話，forward-ref 到 04/05/11 章，收得乾淨。

---

## 三級彙整

### 一級缺口（對這個讀者最痛、建議補）

1. **§3.10 salting 沒有 SQL 骨架**：概念圖沒有任何 code 示意，是本章跨度最大的空白。進階 AE 碰到 AQE 沒搞定的 skew，下一步就是 salting，但沒有連接點。補 3–5 行 SQL 示意或明確 forward-ref。

2. **§3.8 多個 COUNT(DISTINCT) 為何更糟 + 改法**：說了「更糟」但沒說為什麼、沒說替代寫法（拆 CTE + JOIN 各自去重）。這在 KPI summary table 建構時非常高頻。

3. **§3.5 semi / anti join 的 broadcast 行為未覆蓋**：補充表已列五種策略，但推論母體過濾（IN subquery / LEFT ANTI JOIN）不在表內。這是特徵庫 AE 的日常。

### 二級缺口（方向對但補一句或 forward-ref 就夠）

4. **§3.2 多層分區**：命中最高層省最多，一句話 + forward-ref 第 05 章。

5. **§3.7 陷阱二的「先聚合再 join」**：可補極短 before/after 或呼應 §3.11，現在一行帶過。

6. **§3.9「window vs GROUP BY 的判斷原則」**：末段說「有時 GROUP BY 更便宜」但沒給判斷條件。

7. **§3.4 ORC stripe 統計**：CDP 環境 ORC 是預設，一句確認「原理相同，以 EXPLAIN PushedFilters 驗」消除讀者疑慮。

8. **§3.10 NULL key → skew 與 §3.7 型別不符 → NULL 的連結**：兩節呼應一句即可。

### 三級缺口（加分項，不補不扣分）

9. **§3.3 CTE 的 column pruning 穿透性**：特徵寬表下游常見。

10. **§3.8 COLLECT_SET / COLLECT_LIST 的 shuffle 成本**：特徵產出常用，一句帶過或 forward-ref。

11. **§3.11 驗證清單補 Join output rows 確認**（陷阱二的驗證路徑）。

12. **§3.6 hint 加在子查詢別名上的效果確認**：以 EXPLAIN 驗即可，一句消除常見疑慮。

---

*審稿日期：2026-06-23 | 審稿人設定：進階 analytics engineer（SQL + 特徵庫營運）*
