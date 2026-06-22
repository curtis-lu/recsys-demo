# 02 章 reader 審稿（初階分析師視角）

**審稿人設定**：會 SQL、懂銀行業務資料、沒學過分散式系統；日常工具＝Hue/Impala ad-hoc + Spark 排程出名單 + 產特徵表。看到沒解釋術語會卡，嚴格扮演不放水。

---

## 一、三張 checklist 可操作性逐項審查

### Checklist A：讀 UI 改 SQL 寫法

**A-1**（SQL 頁看 Exchange 個數）
- 「DAG 裡的 `Exchange` 個數是不是比你預期多？」——我第一次看到 DAG 這個詞是在 §2.5，但章節開頭前置章（§2.1 那段）已先用過。**DAG 在前文沒有獨立解釋**，只在 §2.5 標注「查詢執行的流程圖（DAG）」，算是同行解釋，但對我來說「流程圖」跟「DAG」之間的連結感覺是同義詞混用，不知道哪個是正式名稱。
- 「比你預期多」——我怎麼知道幾個才是「正常」的預期？我連一個 SQL 裡該有幾次 Exchange 都不清楚。這一項沒辦法獨立打勾，要先讀 §3.5/§3.8 才有感覺。這是「要先懂修法才能診斷」的雞生蛋問題，初讀只能跳過。

**A-2**（EXPLAIN/SQL 頁看廣播 join）
- 操作本身很清楚：找 `BroadcastHashJoin` 還是 `SortMergeJoin`。可以打勾。
- 但「該被廣播的小表」怎麼判斷它算「小」？章節裡提到 10 MB 門檻是在 §2.8 的 Environment 頁段落（提到 `spark.sql.autoBroadcastJoinThreshold`），但那裡的說明只說「臨時改設定」，沒給出這個預設值是多少。**我不知道多小才會自動廣播**，所以「以為它會被廣播」這個前提對我來說是空的。

**A-3**（EXPLAIN 看 PartitionFilters）
- 操作最清楚的一項：跑 EXPLAIN，看 Scan 那行有沒有 `PartitionFilters`，有就代表月份被裁到了。可以照著做。
- 一個小問題：「`PushedFilters`」第一次出現在 §2.3 的 EXPLAIN 示意，說明是「被下推到讀檔層、提早篩掉資料的條件」，這個解釋夠了。但 checklist 這裡同時列出 `PartitionFilters` 和 `PushedFilters`，卻沒說兩個有何不同。我對照 §2.3 能理解 `PartitionFilters` 裁目錄、`PushedFilters` 篩列，但 **checklist 本身沒有這個區別提示，遇到只有 `PushedFilters` 但沒有 `PartitionFilters` 的情況，我不確定算不算正常**。

**A-4**（SQL 頁看 number of output rows 爆量）
- 「number of output rows ≫ 輸入」——比輸入「大很多」才算異常，那多少倍叫大？§2.5 的 mock 說「輸出爆成 3 億」、輸入 3000 萬，是 10 倍。但現實情況 2 倍是否要擔心？5 倍呢？沒有給出判斷門檻，只有「暴增」這個形容詞。可以照著找這個數字，但**不知道看到多少要發警報**。

**A-5**（SQL 頁看讀檔算子輸入列數）
- 和 A-1 類似問題：「遠多於你真正要用的」——多少叫遠多？我對自己這張表「該讀幾列」可能有感覺，但「遠多於」這個標準我設不出來。比起 A-3 的操作性（有沒有出現那個詞），A-5 比較依賴我對業務資料量的先驗知識。

**A-6**（Stages 頁看 Duration Max/Median）
- 「Max ≫ Median（skew）」——§2.6 用具體示意數字解釋得很好（Median 12 秒、Max 9.2 分），這項可以操作。
- 「是哪個 join/GROUP BY key 造成的」——這個問題 checklist 指向 §3.10，但沒有說我在哪個 UI 畫面上看得出「是哪個 key」。我翻完 §2.6，只知道看分位數，但不知道怎麼從數字反推是哪個 key 出了問題。**這一步的「找出元兇 key」在本章沒有操作路徑**，直接指去第 03 章有點突然。

---

### Checklist B：讀 UI 改 Spark 設定

**B-1**（Stages 頁 Shuffle spill 非零）
- 最清楚的一項。非零就是有問題，沒有模糊地帶。可以打勾。

**B-2**（Stages 頁看碎分區 vs AQE 合併）
- 「shuffle 後的分區數是不是過多、每個 task 的 Shuffle Read 都很小」——我怎麼從 Stages 頁看出「分區數」？§2.6 說的是 task 數，但沒說分區數怎麼看。**「task 數量多且每個 Input 很小」和「分區數過多」這兩個描述被混用，對我來說是不同的東西**，我不確定 task 數＝分區數這個等式是否成立、在什麼情況下成立。
- 「還是 AQE 已經幫你合併了？」——這句很好，但我不知道怎麼看 AQE 有沒有合併。§2.5 提到 SQL 頁有 `isFinalPlan`，但 checklist 這裡沒給操作方法。

**B-3**（Environment 頁確認設定值）
- 操作清楚：去 Spark Properties 找那個 key。§2.8 已解釋得夠清楚了。可以打勾。
- 但「跟你以為的一樣嗎」——我「以為」的值是多少？預設值 `spark.sql.autoBroadcastJoinThreshold = 10485760`（10 MB）這個數字只在 §2.8 間接提到但沒有明列，reviewer 文件裡有但正文沒有直接給出這個預設值。**不知道預設是多少，就不知道看到什麼值叫「正常」**。

**B-4**（Executors 頁 GC Time 比例）
- §2.7 給了具體比例說明（「GC Time 佔 Task Time 很大比例…4.2 分 / 40 分 ≈ 10%，紅色警訊」）。可以操作。但「多少比例叫高」這個數字我記起來了，因為 mock 裡有，但 **checklist 本身沒有重複寫出這個參考值**，如果不回去看 §2.7，checklist 這一項是浮空的。

**B-5**（Executors 頁拿到的 executor 數量）
- 「跟你申請的一樣嗎」——我怎麼知道我「申請」了幾台？這取決於排程設定，對於批次作業的初階使用者，這個值可能根本不知道在哪裡查。**不知道「預期應該是幾台」，就無法判斷現在顯示的是不是異常**。

---

### Checklist C：讀 UI 改寫表/儲存邏輯

**C-1**（Stages 頁 Input Size 掃了不該掃的分區）
- 「不該掃」怎麼判斷？還是需要先驗知識（這張表有哪些分區、我的 WHERE 應該裁掉哪些）。對懂業務資料的人可以操作，但需要預備知識。

**C-2**（Stages 頁 task 數暴增、每個 Input 很小）
- 具體、可操作。「task 數暴增」的標準仍不清楚，但「每個 Input 很小」搭配 task 數量多一起看，感覺上比較容易自己判斷。

**C-3**（Jobs 頁看 Output 大小/檔案數）
- 「是不是產生了一堆小檔」——Jobs 頁顯示的 Output 是 bytes，怎麼從 bytes 換算成「一堆小檔」？Jobs 頁沒有檔案數量那個欄位（§2.4 描述的欄位沒有這個）。**這個 checklist 項和 §2.4 描述的 Jobs 頁畫面對不起來**——我在 Jobs 頁看哪一格判斷「一堆小檔」？

**C-4**（查詢沒寫對 vs 表 partition 欄位選錯）
- 這一項要我自己判斷「是查詢問題還是表設計問題」，而且 EXPLAIN 那個畫面對我來說只顯示有沒有 PartitionFilters，看不出「是設計問題還是查詢寫法問題」。**這個判斷需要對兩者的理解超過本章範圍**。

**C-5**（SQL 頁/EXPLAIN 看格式問題）
- 「是不是格式不對（CSV 而非 Parquet/ORC）」——從 EXPLAIN 或 SQL 頁能看出格式嗎？§2.3 的 EXPLAIN 示意有 `Scan parquet card_txn`，裡面有 `parquet` 字樣，所以應該能從 Scan 算子判斷。但 checklist 這裡沒說看哪個字，也沒示意。**操作路徑不明確**。

---

## 二、逐頁籤 screen-by-screen 審查

### §2.2 怎麼打開 Spark UI

**進入門檻問題（最大障礙）**：
這一節告訴我去「Cloudera Manager → Spark → History Server Web UI」，或開 `http://<history-server-host>:18088`。

問題出在「`<history-server-host>`」——我是 SQL 分析師，我不知道公司的 history server 在哪台機器、hostname 是什麼。**這個 URL 是個我沒辦法自己填的空格**。Hue 有一個固定的 URL 我知道，但 History Server 是我從來沒開過的頁面。

- 「打開 Cloudera Manager」——Cloudera Manager 的 URL 是什麼？我知道 Hue，但 Cloudera Manager 是 IT/管理員在用的東西，我可能根本沒有權限或根本不知道網址。**這裡需要一句「通常向你的平台管理員或 IT 詢問這個連結，或由 Hue 的 Spark 作業連結直接點進去」之類的引導**。
- 「點你那次查詢的 App ID」——App ID 長什麼樣？`application_1234567890_0001` 這種格式嗎？完全沒有示意，清單上可能有幾十個 application，我怎麼認出哪個是我的？§2.2 說靠「使用者名稱、送出時間、作業名稱」，但欄位名稱叫什麼（User? Owner?）、順序怎麼排，都沒有說。
- **application 清單那個畫面沒有 mock**——第 2.4–2.8 節都有 mock 面板，但 §2.2 進去前的「History Server application 清單」沒有示意。這是我第一道門，卻沒有示意讓我對得上。

**event log 解釋**：
「Spark 邊跑邊把畫面要顯示的資訊寫成『事件記錄』（event log，寫到硬碟、之後還讀得到；CDP 預設開著）」——這一段寫得好，讓我不用擔心「跑完是不是就消失了」。這個擔心點確實是真實的，謝謝有解釋。

**10 秒延遲**：
這段很有用，告訴我看「還在跑」的查詢時畫面會落後，重整即可。消除了我「怎麼頁面沒更新」的疑惑。

---

### §2.3 EXPLAIN

**最好上手的一節**：語法清楚，直接加在 SQL 前面，Hue 就能跑。mock plan 有 ★ 標出三個關鍵字，照著找。

**術語第一次出現但沒解釋**：
- 「physical plan」——括號說明是「實際執行步驟」，夠了。
- 「logical plan」——提到 `EXTENDED` 會印，但沒解釋「邏輯計畫」是什麼。對我來說沒有影響，因為我被告知用 `FORMATTED` 就夠了。
- 「codegen」（`CODEGEN` 模式）——完全沒解釋，我不知道是什麼，但也被告知不用管。沒問題。

**由下往上讀**：
括號說「由下往上讀」，但 mock plan 的順序讓我有點困惑——我看到 `HashAggregate` 在最上面、`Scan` 在最下面，「由下往上」意思是說執行順序是從 Scan 開始嗎？**需要多一句說明「Spark 先從最底層（讀檔）開始執行，往上走到最後輸出，所以 plan 是由下往上看執行順序」**，不然「由下往上」這四個字在腦中是懸空的。

**BroadcastExchange vs Exchange 的區別**：
§2.3 有清楚說明「`BroadcastExchange` 不是 shuffle，別算進 shuffle 次數」，這個提醒很重要、也放對地方了。可以照著做。

---

### §2.4 Jobs 頁籤

**mock 面板對得上**：
顯示欄位（Job ID、Description、Duration、Stages、Tasks、Status）都是表格裡有、§2.4 正文也有列的。進度條用 `▓` 示意，讓我知道那是個視覺元件，OK。

**一個認知問題**：
「一個 application（例如你一條 `INSERT ... SELECT`）可能拆成好幾個 job」——為什麼一條 SQL 會變成好幾個 job？這個問題在第 01 章有沒有解釋過？沒讀第 01 章的人（或讀完忘了）在這裡會卡。§2.4 沒有解釋，只說可能有幾個。**初讀我的反應是「我只送了一條 SQL，為什麼有 Job 0、1、2、3 四個？」**

**「Description」欄點進去**：
「點 Description 進去往下追」——Description 欄顯示的文字是查詢本身嗎？是連結嗎？示意的 Description 是 `insert into mart_txn_summary …`（截斷的 SQL 文字）。但如果我的作業是 Oozie / Ploomber 排程送的，Description 會是什麼？這個疑問不影響閱讀，但在真實操作中可能遇到「Description 欄是奇怪的 ID」。

**FAILED 情境的後續**：
§2.4 說「若是 `OutOfMemoryError`／executor lost，多半是…去第 03/04 章」——但從 FAILED 到看到錯誤訊息，中間那幾次點擊（點哪個 job → 點哪個 stage → 看哪個格的訊息）這個路徑在這節沒有說。**FAILED 這個最緊急的情境，進入路徑最不清楚**。

---

### §2.5 SQL/DataFrame 頁籤

**最重要卻最難進入的一節**：
「SQL 頁籤是 SQL-first 的人最該細看的地方」——這個開場讓我很有動力，但隨後「點進你那條查詢」的操作步驟省略了。**我在 SQL 頁籤要先看到哪張清單、再點哪個查詢？** SQL 頁的入口（清單長什麼樣）沒有示意，直接跳到「點進去你會看到 DAG」。

**DAG 的 mock 是純文字**：
用縮排的文字表示「由下往上」的算子樹，這個格式跟真實畫面差距很大——真實的 SQL 頁 DAG 是圖形（方塊連線），文字 mock 的「縮排樹」讓人以為那是垂直列表。**第一次看到真實畫面可能認不出來**。

**「最貴 shuffle」的操作**：
「哪個 Exchange 的位元組最大，那就是這條查詢的主要成本所在」——我需要看每一個 Exchange 方塊上的數字、找最大的那個，這個操作清楚。但如果有三個 Exchange，方塊上顯示的欄位名稱是 `shuffle bytes written total` 還是 `data size`（§2.5 兩個名字都用）？**方塊上的標籤名稱不一致，會讓我不知道我找到的那格算不算**。

**AQE 那個附注**：
「`EXPLAIN` 看到 `isFinalPlan=false`」——在 EXPLAIN 的純文字輸出裡找 `isFinalPlan` 這個字串，這可以操作。這個附注提醒我「EXPLAIN 和 SQL 頁對不起來是正常的」，這個預防針打得很好，不然我會以為自己看錯了。

---

### §2.6 Stages 頁籤

**整章最清楚的一節**：
Summary Metrics 的五分位表（Min/25th/Median/75th/Max）搭配具體示意數字，讓 skew 的診斷非常直觀。mock 表裡 Duration Max（9.2 分）vs Median（12 秒）這個對比一眼就懂。

**「分位數」的解釋**：
正文有一段「把這個 stage 的所有 task 依某個指標從小排到大，再挑五個代表點」——這個解釋放在術語第一次出現的地方，很好。

**一個操作問題**：
「對 `Duration` 這一列」——Stages 頁的 Summary Metrics 表，我怎麼找到它？它是在 stage 清單裡、點進某個 stage 之後才出現，還是每個 stage 在清單上就顯示？這個導航路徑（「從 SQL 頁鎖定最貴 stage → Stages 頁 → 點進那個 stage → 往下找 Summary Metrics 表」）在 §2.6 開頭有說「從 SQL 頁鎖定了最貴的那個 stage，就到 Stages 頁籤點進它」，算是清楚了。OK。

**spill 的兩個欄位**：
`Shuffle spill (memory)` 和 `Shuffle spill (disk)` 都非零——這兩個數字是累計的還是某個特定 task 的最大值？mock 表裡 Max 欄顯示非零、Min 到 75th 都是 0，說明是「有少數 task spill、大多數沒有」。**沒有說明這兩個數字的對照邏輯（memory 的 14 GB vs disk 的 8.3 GB，memory 還比 disk 大？）**，我有點困惑——一般理解是資料從記憶體溢到磁碟，disk 數字不是應該≤memory？術語定義（§2.6 引自官方）說 memory 是 deserialized 形式、disk 是 serialized 形式，兩者不是同一份資料的大小——但這個解釋藏在 reviewer 文件的查證紀錄裡，正文的 mock 表和 §2.6 正文完全沒解釋為什麼 memory 數字（14 GB）比 disk（8.3 GB）大。**初讀者到這裡會停下來困惑，但不會把原因找出來**。

---

### §2.7 Executors 頁籤

**GC Time 的比例標準**：
§2.7 mock 說「4.2 分 / 40 分 ≈ 10%，紅色警訊」——好，有具體比例。但 §2.7 正文說「GC Time 相對 Task Time 是個小零頭」算正常（如 48 秒 / 42 分），這個「小零頭」是多少？5%？3%？沒有說。mock 給了一個「10% = 紅色」的案例，但警戒線在哪裡沒有說。

**Storage Memory 那欄**：
mock 顯示所有 executor 的 Storage Mem 都是 `0 / 8 GB`——「0 / 8 GB」是「使用了 0、上限 8 GB」嗎？這個格式對我來說需要一句說明（分子是用掉的、分母是上限）。

**「spill 要回 Stages 頁看，不是這裡」** 這個附注非常有用：防止我在 Executors 頁找不到 spill 欄位而誤以為沒有 spill 問題。這句話應該更早出現——我讀到 §2.6 時就會想「Executors 頁有沒有 spill 數字」，等到 §2.7 才看到這個說明有點晚。

---

### §2.8 Storage 與 Environment 頁籤

**Storage 頁的「cache 前要先 action」這個警告很有用**。這個行為反直覺（CACHE 完馬上來看卻是空的），提前說明防止我誤判。

**`SET` 語法**：
「`SET spark.sql.autoBroadcastJoinThreshold = ...`」——`SET` 在哪裡用？在 Hue 的 SQL 視窗嗎？在 pyspark session 嗎？這個語法對 SQL 分析師來說可能是新的，但指去 §4.3 說明，可以接受。

---

### §2.9 速查總表

這張表對我來說是這章最有用的東西——把「症狀→看哪格→去哪章」直接列出來，清楚。

**唯一的問題**：「怎麼判定異常」這欄大多給得很具體（「Max ≫ Median（差一個量級）」、「任一非零即是」），但「哪個 Exchange 位元組最大＝最貴」這個說法是「相對比較」，不是「絕對閾值」——如果我只有一個 Exchange，它就一定是最貴的，那要怎麼判斷它「夠貴到值得修」？這個表沒有能回答「多大叫值得處理」的絕對尺度。我理解可能不同 SQL 的基準不同，但初讀者到這裡還是會問這個問題。

---

### §2.10 三張 checklist

可操作問題已在第一節逐項列出，不重複。

整體格式：每項格式是「去哪個頁籤 → 看什麼 → 去哪章」，清楚。每項末尾都有節號連結，形式一致。

**一個格式問題**：三張 checklist 的標題（A/B/C）和§2.9 總表的症狀順序不完全對齊。例如，§2.9 第一列是「shuffle 過大」，對應 checklist A 第一項；但 §2.9 第三列是「spill」，對應 checklist B 第一項。閱讀時需要在兩個地方之間跳來跳去，不容易心裡追蹤。這不是大問題，但第一次讀時容易以為「找到 §2.9 的症狀就可以直接去三章，不用再看 checklist」，而 checklist 的操作細節其實比 §2.9 更多。

---

### §2.11 完整驗屍

這一節把整章串起來，是理解整個診斷流程最快的路徑。但我建議第一次讀可能要先讀這節再回去讀各節——問題是章節順序不這樣，所以第一次讀的人要走完 §2.2–§2.10 才到這裡。

**「讓它跑起來，從 History Server 的 incomplete 清單點進去」**——這個步驟隱含了我已經知道怎麼進 History Server，但 §2.2 的入門難度問題（上面已標）還沒解決，所以這一步對初次操作的人仍然卡。

**第 7 步「改完再跑一次、回頭看同一張 Summary Metrics 表」**：這個驗證閉環的概念很好，給了我一個「怎麼確認我的修改有效」的操作方法。這是這章最有價值的一句話之一。

---

## 三、術語未解釋清單（第一次出現且正文無解釋）

| 術語 | 出現位置 | 說明 |
|------|----------|------|
| DAG | §2.1 導言（「stage 圖」一提），§2.5 才解釋 | §2.5 說是「查詢執行的流程圖（DAG）」算是同行解釋，但「DAG」這個縮寫是什麼的縮寫從未說明 |
| application | §2.2 | 用過很多次，第 01 章應有定義；但 §2.2 開始就說「進 application 的 Spark UI」，初讀者在這裡不知道 application 指什麼（一條 SQL？還是一整個 Spark 程式？） |
| App ID | §2.2 | 說「點你那次查詢的 App ID」——沒有說長什麼樣 |
| event log | §2.2 | 說是「事件記錄」，算解釋了，但「寫到硬碟」的「寫到哪個硬碟？HDFS？」可能讓人好奇 |
| AQE | §2.5（附注） | 正文說是「Adaptive Query Execution」但只在附注說明。§2.3 說的「第 01 章說過 Spark 3.3 的 AQE」——依賴第 01 章的前提，若讀者跳過第 01 章這裡會失去脈絡 |
| materialize | §2.8 Storage 頁 | 說「被真正算出來（materialize）」，括號解釋算夠了 |
| action | §2.8 Storage 頁 | 說「要先有一個 action 觸發它」——action 是 Spark 術語（§1.x 應有解釋），但這裡對沒讀第 01 章的人是黑盒子 |
| salting | §2.6 表格 | 「[§3.10 處理 skew](…salting)」——salting 出現在連結文字，但完全沒有解釋，初讀者可能好奇「用鹽巴？」 |

---

## 四、三級彙整

### 嚴重（讓我真的卡住、無法繼續操作）

1. **§2.2 History Server 入口沒有手把手**：`<history-server-host>` 要填什麼、Cloudera Manager 是什麼/在哪裡、application 清單長什麼樣（沒有 mock）、怎麼認出哪個是我的 App ID。初階使用者到這裡會停下來去問 IT，進不去就什麼都做不了。
2. **Checklist A-1 & A-5 的「正常基準」是空的**：「比你預期多幾個 Exchange」、「遠多於你真正要用的列數」——沒有讓我知道「預期是幾個」的任何錨點。這兩項的 checklist 格子我打不了勾。
3. **Checklist C-3（Jobs 頁看小檔）**：Jobs 頁的哪一格顯示「檔案數量」？§2.4 描述的欄位是 Output（位元組），沒有檔案數欄位。這個 checklist 項和正文描述的 Jobs 頁畫面對不起來，我找不到那格在哪。

### 中度（需要回頭翻、或靠先驗知識補足，但不致整個卡死）

4. **§2.5 SQL 頁的入口**：我在 SQL 頁籤看到的第一個畫面是什麼？清單？如何點進我那條查詢？這個「第一步」省略了。
5. **§2.6 Shuffle spill 的 memory vs disk 數字大小關係**：mock 裡 memory（14 GB）大於 disk（8.3 GB），直覺上很奇怪（以為 disk 是 memory 溢出去的），正文和 mock 都沒解釋，留下困惑。
6. **Checklist B-5 的「你申請了幾台」**：不知道怎麼查「預期的 executor 數量」，這項 checklist 對不知道自己申請設定的人打不了勾。
7. **B-3（autoBroadcastJoinThreshold 預設值）**：預設 10 MB 這個數字正文沒明列，Environment 頁確認設定時不知道「10 MB 算正常還是異常」。
8. **A-6「哪個 key 造成 skew」的 UI 路徑**：Stages 頁看分位數能知道「有 skew」，但看不出「是哪個 key」，checklist 這項直接跳去 §3.10，中間這步缺失。

### 輕度（形容詞模糊，但整體意思可以理解）

9. **A-4 爆量 join 的倍數門檻**：「暴增」沒有給出倍數標準（10 倍？3 倍？），但直觀上「明顯大很多」自己大概能感知。
10. **B-4 GC Time 的「正常比例」**：mock 給了 10% 是紅色，但正常的上限（5%？）沒有說。
11. **§2.4 一條 SQL 拆成多個 job 的原因**：這個觀念依賴第 01 章，但初讀者到這裡可能沒有記憶了。一句「如第 01 章所述，Spark 可能把一條 SQL 拆成多個 job」的連回提醒就夠了。
12. **§2.7 Storage Memory 欄的「0 / 8 GB」格式**：分子是用掉的、分母是上限，這個格式沒有說明但能猜到。
