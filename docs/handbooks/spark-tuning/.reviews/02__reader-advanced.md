# 第 02 章審稿：進階 Analytics Engineer 視角

> 審稿人設定：會 SQL 也會 Python、維護共用特徵庫、做 reverse ETL 回業務系統。核心關切：定期排程作業退化時，這章的診斷流程能不能在生產環境快速定位問題。

---

## 一、逐節深度評

### §2.1 先量再調，不要憑感覺

**評：夠營運**。「EXPLAIN 看計畫 vs UI 看執行」的二分法說得清楚，也點了「排程作業跑完才看」這個現實。沒有可挑剔的地方。

---

### §2.2 怎麼打開 Spark UI（在 CDP 上）

**評：夠營運，但有一個進階缺口**。

關鍵點「incomplete 清單讓你監看還在跑的作業」、「History Server 有 10 秒延遲」說到位了。對排程場景最有用的那句也說到了：History Server 讓你回頭翻已完成的作業、比較同一支作業「這週和上週各跑多久、搬了多少資料」。

**缺口**：那句只是一筆帶過，然後說「第 07 章監控會再談」。但「怎麼比對歷史 run」正是退化排查最常見的第一步——讀者此刻想知道的是：在 History Server application 清單上，我怎麼認出「同一支作業上週的那次」？靠什麼欄位（`app.name` 還是提交時間還是 Description）？UI 本身有沒有辦法橫向比對兩次 Duration / Shuffle Read，還是要靠外掛（例如 Spark event log 匯到 Grafana）？這個問題完全沒碰。

**建議**：補一句「如果你要比對兩次 run，靠 application 清單的『作業名稱 + 提交時間』篩同一支排程；Duration 和 Shuffle Read 欄位在清單頁即可對比；History Server 本身沒有趨勢圖，需要長期監控請見第 07 章」，讓讀者知道 UI 的邊界在哪，不用等到第 07 章才能開始對比。

---

### §2.3 跑之前先看計畫：`EXPLAIN`

**評：對初進者夠，對進階者偏淺一個層次**。

三個關鍵字（`Exchange` / `BroadcastHashJoin` vs `SortMergeJoin` / `PartitionFilters`）選得很對，是排查高頻問題的核心切入點。

**缺口一：EXPLAIN 的侷限沒講到**。進階者一定會碰到這個場景：EXPLAIN 顯示有 `PartitionFilters`，但 Stages 頁的 `Input Size` 卻遠比預期大。原因可能是分區統計過時（stats stale、Hive metastore 沒 ANALYZE TABLE）、或是 dynamic partition pruning 在某些 join 條件下沒觸發。這個「計畫說有裁但實際沒裁」的陷阱完全沒提。對特徵庫維護者來說這是高頻雷——`EXPLAIN` 給你信心、但執行時吃了整張表。

**缺口二：`EXPLAIN COST` 的角色**。既然提了 `EXTENDED` / `COST` / `CODEGEN`，但只說「日常用 FORMATTED 就夠」、沒解釋 `COST` 在什麼場景有額外用途（例如想知道 Spark 的 row count 估算準不準、broadcast 門檻判定是否被誤導）。進階者偶爾需要 `COST` 來理解為什麼 AQE 改了計畫，這裡應補一句 forward-ref 給 §4.2。

---

### §2.4 Jobs 頁籤：先掃一眼有沒有失敗、誰最慢

**評：夠營運，定位清楚**。

「FAILED 先點、Duration 最長的最該查」邏輯清晰，mock 面板也夠直觀。把 Jobs 頁定為「分流台」而非「病因台」，設計判斷正確。

**缺口**：對退化場景缺一個具體錨點。排程作業退化，Jobs 頁最明顯的徵兆不只是 Duration 變長，還有「stage 成功數 / 總數下降」或「task 重試次數增加（在 Description 展開後可見）」。對比「上週都是 Stages 3/3、這週變成 Stages 2/3 卡住」，這個視覺差異讓 Jobs 頁的退化診斷值大增，值得一句帶過。

---

### §2.5 SQL／DataFrame 頁籤：這條查詢花在哪一步

**評：夠深，是本章最強的一節**。

「哪個 Exchange 最貴」＋「output rows 有沒有暴增」的二元掃描法精準。AQE 的 `isFinalPlan` 放在這裡解釋也是對的位置——特別是「EXPLAIN 跟 SQL 頁對不起來是正常的」這句，能省掉很多混亂。

**缺口一（進階深度不足）**：`number of output rows` 在 shuffle 後的算子（`Exchange` 下游的 `HashAggregate`）如果遠超預期，有時是 AQE 的 skew join 分裂造成「邏輯上同一批資料被切成多個 task、計數疊加」的計數假象。這不影響正確性，但會讓進階者困惑「為什麼 output rows 比 input 多」。值得一句說清楚，或至少 forward-ref §4.2。

**缺口二**：`Exchange` 上除了 `shuffle bytes written`，還有 `fetch wait time`（shuffle read 端的等待時間）。如果 fetch wait time 大、shuffle bytes 不算大，問題可能不在「搬多少」而在「網路 / IO 競爭」，這個 dimension 沒碰到。對共用叢集的特徵庫作業這是真實場景。

---

### §2.6 Stages 頁籤：這個 stage 是 skew 還是 spill

**評：夠深、是本章骨幹**。

分位數的解釋方式（由五格看分佈）對 SQL-first 背景讀者非常友善，skew 的 Max ≫ Median 法則一句話就定性。示意表格具體，讓讀者知道「14 GB spill 不是誇張、是真的會發生」。

**缺口一（最重要的營運缺口）**：「這次 skew 是資料長大造成、還是 skew key 分佈本身惡化？」這兩種診斷需要不同的應對。本節只告訴你「Max ≫ Median = skew」，但沒說怎麼判斷 skew 是「新出現的」還是「一直都有但資料量超過閾值才爆發」。對排程維護者，這個判斷決定你是要改 SQL、還是只要等 AQE 配置跟上。一個補法：「如果上週同一個 stage 的 Max/Median 比例相近、這週突然變大，傾向是資料量長大；如果比例本身跳大，傾向是 skew key 分佈改變（例如某個熱門 key 突然爆量）」。這句話不長，但讓診斷從「認症狀」升級到「判病因走向」。

**缺口二**：`Failed Tasks` 欄在 Stage detail 裡（不是 Executors 頁）也有，而且能告訴你某個 stage 本身的 task failure 率——這個欄位在 §2.6 沒提，但對排程退化（某次 run 某個 stage 開始大量 task retry）很有用。

---

### §2.7 Executors 頁籤：資源夠不夠、誰在喊累

**評：夠營運，但少了一個進階角度**。

GC Time 佔比 10% 是紅色警訊、Failed Tasks 持續長大、executor 數量比申請少——三個進入點說清楚了，路由也正確（設定面 → 04 章）。

**缺口一（本節最大的進階缺口）**：`Failed Tasks` 的絕對數字和 `GC Time` 的趨勢，對「退化比對」最有用——但本節沒有告訴讀者「我要怎麼用 Executors 頁比對兩次 run」。特別是共用叢集環境，某週的 Failed Tasks 從 0 跳到 30+，根因可能是：(a) 資料量長大、(b) 鄰居作業資源競爭、(c) executor 所在節點有問題。從 Executors 頁的「哪幾個 executor 失敗了、失敗幾次、是不是同一台」可以輔助區分 (c) vs (a)(b)，但本節只說「那台一直在重試」，沒說怎麼進一步診斷是特定節點問題（例如某個 executor ID 的 Failed Tasks 特別多，其他台都正常）。

**缺口二（進階輕觸點）**：GC time 的兩個層面（minor GC vs full GC）在 Executors 頁是合計的，沒辦法從這裡區分。這不必在這章展開，但可以一句 forward-ref 說「如果要深挖 GC 是 Young gen 還是 Old gen 問題，需要到 executor log 看 GC 詳細輸出；本手冊範圍以 UI 可見的聚合指標為主」。

---

### §2.8 Storage 與 Environment 頁籤

**評：恰好，不淺不深，邊界設定合理**。

Storage 頁「materialize 前不出現」的警告說出來了。Environment 頁「設定沒生效」的場景非常具體，對進階者有直接使用價值。

**小缺口**：Environment 頁的 `spark.eventLog.dir`（event log 寫在哪）和 `spark.history.fs.logDirectory` 是「History Server 拿不到舊的 run」時的診斷切入點——這不是日常場景，但對「昨晚的排程跑完了、History Server 上卻找不到那筆 application」的排查有用。一句「如果 History Server 清單裡找不到你的 application，先到 Environment 頁核對 `spark.eventLog.enabled` 是否 true，並確認 log 路徑可達」，就填掉這個空白。

---

### §2.9 速查表

**評：夠好，可直接使用**。症狀映射正確，覆蓋面完整。沒有漏掉的高頻問題。

**小缺口**：`Executors 頁：Failed Tasks 一直長大` 的描述可以更精準——目前是「伴隨 OOM / executor lost」，但進階者知道 Failed Tasks 也可以因為 task timeout 或特定節點問題而非 OOM 觸發，「伴隨」的說法容易讓人以為這兩個總是同時出現。

---

### §2.10 三張 Checklist

**評：對入門偏強、對進階略薄**。

Checklist A（SQL 面）最完整，六項涵蓋了高頻場景。Checklist B（設定面）和 C（儲存面）各有一個進階缺口：

**Checklist B 缺口**：沒有「`shuffle.partitions` 設定是否仍有效（AQE 是否已接管），以及如果 AQE 在這次 run 沒觸發 partition coalescing，如何確認」的項目。進階者在共用叢集常遇到「AQE 開著但本次沒合併分區」的情況，Environment + Stages 的組合診斷沒在 checklist 裡。

**Checklist C 缺口**：`ANALYZE TABLE` 的頻率問題（特徵庫每週 ETL 後有沒有更新統計）決定了 partition pruning 和 broadcast 門檻的準確性，但沒有一項「確認 `last_analyzed` 時間戳是否夠新、或 `spark.sql.statistics.fallBackToHdfs` 是否開著」。對特徵庫維護者這是高頻場景。

---

### §2.11 完整驗屍

**評：夠扎實、閉環清楚**。

七步驟把本章零件串成一個完整診斷流程，「改完再跑一遍用同一張 Summary Metrics 驗證」的第 7 步特別好——很多人會忘記驗修改是否真的有效。

**缺口**：驗屍情境是「今天跑了 20 分鐘還沒完」（單次異常），但對排程維護者更常見的是「這三週以來每次都比上週慢 10-15%」（漸進退化）。這種情境的診斷起點不是「進 UI 找最慢的 job」，而是「先確認哪個 stage 的絕對時間在增長」，再判斷是資料量線性增長（正常）還是某個特定 stage 退化速度超過資料量增速（異常）。一個短段（3-4 句）補充這個漸進退化的起手式，本章在排程維護場景的實用性會明顯提升。

---

## 二、主要缺口彙整（三個層級）

### 層級一：建議補進本章的缺口（對目標讀者有直接營運價值，缺少會讓章節目的打折）

1. **漸進退化的起手式（§2.11 或新增一段）**：單次異常 vs 漸進退化的診斷起點不同。目前驗屍只示範前者。補 3-4 句說明「三週都在慢」場景的第一步：從 application 清單比對同名作業的 Duration 趨勢，鎖定開始惡化的那次，再進 Stages 頁確認哪個 stage 的時間增速超過資料量增速。

2. **「計畫說有裁但實際沒裁」的陷阱（§2.3）**：EXPLAIN 顯示 `PartitionFilters` 存在，但 Stages 的 `Input Size` 遠大於預期——要補一句說這種差異存在（stats stale 或 dynamic partition pruning 失效），告知讀者以 Stages 頁的實際數字為準，forward-ref §5.6 `ANALYZE TABLE`。

3. **skew 是新出現還是一直有、資料量長大才觸發（§2.6）**：兩種病因需要不同應對（SQL 改法 vs 等 AQE 調適 vs 改分區設計），本節只認症狀沒有這個判斷維度。補一句診斷指引，明顯提升 §2.6 的排程維護場景實用性。

### 層級二：值得補一句或 forward-ref、不必展開的點

4. **如何在 History Server 清單認出「同一支作業上週那次」，UI 橫向比對的邊界在哪（§2.2）**：讀者知道第 07 章才展開，但現在就需要知道 UI 本身能給什麼。

5. **某個 executor 的 Failed Tasks 特別多（是節點問題）vs 全部 executor 都在失敗（是作業問題）的區分方式（§2.7）**：一句話說「如果只有一、兩台 executor 集中失敗，優先懷疑節點問題而非 SQL/記憶體，聯繫叢集管理員確認」。

6. **`ANALYZE TABLE` 時間戳核對放進 Checklist C**：特徵庫 ETL 後需更新統計，否則 broadcast 門檻和 partition pruning 準確度都會漂移。

7. **History Server 找不到 application 時的 `eventLog` 設定診斷（§2.8 Environment）**：一句說「先到 Environment 頁核對 `spark.eventLog.enabled`」。

### 層級三：進階讀者值得標注但不影響主線的點

8. **`Exchange` 的 `fetch wait time` 維度（§2.5）**：搬多少 vs 等多久是不同的問題，共用叢集 IO 競爭時前者不大、後者很長。

9. **AQE skew join 分裂造成的 output rows 計數假象（§2.5）**：不影響正確性，但會讓進階者困惑。

10. **GC 聚合指標的侷限（minor vs full GC 不可分，需 executor log）**：一句 forward-ref，設定邊界。

---

## 三、分層建議（哪些可標進階/可跳）

目前全章沒有分層標記，對初中級讀者有用；但對進階者有幾段略囉嗦：

- **§2.2「兩個前提、一個延遲」段落**：event log / 10 秒更新週期的解釋適合初學者，進階者不需要。可標 `（基礎知識可跳）`。
- **§2.6「分位數的意思」解釋段落**：分位數定義對 SQL-first 讀者有必要，對有統計背景的進階讀者多餘。同樣可標。
- **§2.3 `EXPLAIN FORMATTED` 輸出範例的逐行解釋**：適合初學者，進階者直接看三個關鍵字就夠。可標 `（熟悉 SQL plan 的讀者可跳到關鍵字列表）`。

這三處若加上跳讀提示，進階讀者可節省 20-30% 閱讀時間，也不影響初學者路徑。

---

## 四、整體結論

本章的診斷架構健全——頁籤映射清楚、症狀判定有數字依據、forward-ref 路由正確。對「單次異常診斷」場景已夠用。

**主要未覆蓋的場景**：定期排程作業的**漸進退化**（連續幾週慢一點點）。這是進階 analytics engineer 最常遇到的維護場景，但本章的診斷流程全部以「今天這次突然很慢」為出發點，沒有給讀者一個「如何判斷是哪一次開始退化、退化是在哪個 stage」的入門指引。這一個缺口填掉，本章對排程維護場景的實用性會升一個層次。其餘缺口屬深度補充，優先級次之。
