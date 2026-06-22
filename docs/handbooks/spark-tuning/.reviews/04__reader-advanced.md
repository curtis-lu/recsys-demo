# 第 04 章審稿 — 進階 Analytics Engineer 視角
> 人設：會 SQL 也會一點 Python，營運共用特徵庫供訓練、做 reverse ETL 回業務系統，要替長期排程作業配置資源、處理多租戶。
> 審稿重點：「替長期排程配資源、跟同事共用叢集」這條線的深度與缺口。不查技術對錯、不改稿。

---

## 逐節深度評

### §4.1 心法：先別急著轉旋鈕——AQE-first
**評：夠營運**

兩類旋鈕（SQL 層 vs 資源層）的風險梯度切得很清楚，「調錯不是慢一點，是 OOM 或佔住資源餓死同事」這句話拉到排程/多租戶情境，對我直接有效。沒有缺口。

---

### §4.2 AQE 自動幫你做的三件事
**評：夠（背景知識層），不要求更深**

AQE 三件事的白話說明對我理解後續資源配置取捨已足夠，這節定位是「給你底氣不亂調」的鋪陳，不是操作節，深一步反而噪音。

---

### §4.3 確認 AQE 開著、怎麼用 `SET` 改設定
**評：夠營運**

SQL 層 `SET` 即時生效 vs 資源層啟動時定死的對比表，加上「你 `SET executor.memory` 但 Executors 頁籤沒動」的徵兆——這是我真的會踩的坑，交代得夠清楚。

---

### §4.5 記憶體模型
**評：偏深（基礎對了，但有一個運算缺口直接影響排程作業）**

M / execution / storage / overhead 四層結構說清楚了，spill 的救法優先序也對。

**缺口（會踩）**：§4.6 的 worked example 算 heap 時用「20 ÷ 1.1 ≈ 18g」，但腳注說「未扣 driver／ApplicationMaster／節點其他開銷」。對排程作業而言，**driver 的記憶體是另一筆單獨的 YARN container**，driver 預設 1g heap（`spark.driver.memory`）＋對應 overhead，在叢集中是被單獨佔用的。如果我的作業要大量 `collect()` 到 driver（例如 broadcast 的小表先在 driver 組好再推出去、或 `toPandas()` 做輕量後處理），driver 撐爆是獨立的雷，跟 executor 記憶體模型是兩件事。本章第一次碰資源配置，`driver.memory` 完全沒出現，讀者很容易以為「只要把 executor 算好就行了」。這不只是「刻意簡化」，而是排程作業常踩的真實故障點。

---

### §4.6 core/mem/台數 worked example
**評：夠，但兩個實務缺口**

`100 core / 400 GB → 20 × 5 core × 18g` 這個計算鏈教學價值高，把 §4.5 的記憶體模型接到參數設定，我看得懂。

**缺口一（估算誤差，略偏低）**：範例腳注只說「未含 driver／AM／其他開銷」但沒給「大概要扣多少」的感覺。對沒做過的人，「乾淨對切」反而會讓他配到跑起來就 OOM 或 AM 佔掉最後幾台導致 executor 一直拿不到。一個辦法是加一句規則——例如「YARN 上習慣先扣 1–2 台給 driver + AM，剩下再除」——哪怕只是「方向性」，也比完全沒錨點好。

**缺口二（YARN container 上限）**：YARN 的 `yarn.scheduler.maximum-allocation-mb` / `maximum-allocation-vcores` 是每台 container 的硬上限，配 18g heap 可能在某些叢集直接被 YARN 拒掉（拿不到 container、作業卡在 ACCEPTED 狀態不動）。這是我或同事替排程作業第一次配資源時最容易的死法之一，本章一字未提。

---

### §4.7 dynamic allocation 與多租戶
**評：夠（觸及了），但 queue 隔離面完全缺席**

「設合理的 `maxExecutors`、別餓死同事」這個核心建議對了，串流不開的警告也有。

**缺口（最大缺口）：Queue 隔離根本沒出現。**

在多租戶 CDP / YARN 環境裡，資源的第一道防線不是 `maxExecutors`，而是 **YARN queue**（Capacity Scheduler 或 Fair Scheduler 的 queue）。隊列定義了「這個團隊或這條流水線最多能拿幾 % 的叢集」，這在 IT/平台端設，不在 `spark-submit` 裡設。對我這個需要替共用特徵庫配排程作業的角色，最重要的實務流程是：

1. 確認我的作業打到哪個 queue（`--queue prod_etl` 或 `spark.yarn.queue=...`）。
2. 知道那個 queue 的 capacity 上限是多少，然後才去想 `maxExecutors` 要設多少（設超過 queue 上限沒用，YARN 不給）。
3. 多支排程作業在同一 queue 搶資源時，YARN 怎麼分——這才是「餓死同事」的真實機制。

現在 §4.7 只說「設 `maxExecutors` 別讓它無上限」，但沒說 queue 是什麼、我在哪裡看、怎麼知道上限是多少。這條線對「排程作業多租戶」的讀者來說是最大的資訊空白。

另一個缺口：**作業排程順序（priority/preemption）**。CDP Capacity Scheduler 可設 preemption，高優先 queue 可以搶低優先 queue 的資源。對「SLA 關鍵作業用 `minExecutors` 確保基本資源」（§4.7 已提）這個建議，如果沒有 preemption 作保底，`minExecutors` 在資源緊張時能不能真的拿到，取決於 queue/preemption 設定。完全沒提這層，讀者會誤以為設了 `minExecutors` 就萬事大吉。

---

### §4.8 排程作業貫穿範例
**評：偏淺（框架對，但缺量化錨點與實際操作卡點）**

流程的五步順序正確，最後的 `maxExecutors=30` 示意也有幫助。但讀者（包括我）看完還是不知道：

1. **這 30 是怎麼來的？** 有沒有一個粗估規則（例如「看 queue capacity 是 N core，`maxExecutors` 不超過 N ÷ `executor.cores` 的 60%」）？沒有估算依據，「30」對讀者是空的。
2. **第 4 步的資源配置，在 CDP 上具體在哪設？** `spark-submit --conf` 還是 Livy session 的 JSON body 還是 Cloudera Manager 的 spark.conf override？對 Hue SQL 族來說，「在工作階段啟動時給」說了等於沒說，因為他們從來沒有自己開 `spark-submit`。
3. **第 5 步「上線後持續看」的退化訊號是什麼？** 資料量成長導致 spill 嚴重、GC overhead 上升、作業牆鐘時間比上週多 20%——哪個先現形？章末說「多半又是回到第 1 步」，但對跑了半年的排程作業，通常不是改 SQL 就能解，而是真的要加資源或重新配，這裡的描述跟實際情況脫節。

---

## 三級彙整

### A 級缺口（排程/多租戶這條線的根本資訊空白，應補）

1. **Queue 隔離（§4.7 缺）**：`spark.yarn.queue` 的作用、怎麼查自己打到哪個 queue、queue capacity 上限是 `maxExecutors` 的先決條件——不補這一塊，§4.7 的「設合理 `maxExecutors`」對讀者是沒有錨點的建議。
2. **Driver 記憶體（§4.5/§4.6 缺）**：`spark.driver.memory` 是獨立 YARN container，大量 `collect()` / broadcast 組裝在 driver 端時是獨立的爆雷點，排程作業的完整資源配置不能只算 executor。

### B 級缺口（讓「配資源」這件事更落地，建議補）

3. **YARN container 上限（§4.6 缺）**：第一次配資源最容易踩的死法——配超過 `yarn.scheduler.maximum-allocation-mb/vcores`，作業卡在 ACCEPTED 不動，現在完全沒提。
4. **§4.8 的 `maxExecutors` 估算依據**：「30」從哪來？跟 queue capacity 的換算關係？沒有粗估規則，示例的教學價值打折。
5. **`minExecutors` + preemption 的搭配**：SLA 作業靠 `minExecutors` 保底，前提是 queue preemption 設定是否支援，現在兩件事沒接起來。

### C 級缺口（可選，加了更完整但不影響核心理解）

6. **§4.8「上線後持續看」的退化訊號具體化**：「多半又是回到第 1 步」在長期穩定運行的排程作業上不夠現實，退化通常先以 GC overhead 或 spill 突增的形式出現，指向的解法未必是改 SQL。
7. **Broadcast 撐爆 driver 的具體門檻**（與 A-2 相關）：`autoBroadcastJoinThreshold` 調大帶來的是「driver 先 collect 這張表、再廣播」，廣播的 10x 放大（序列化前後）在 driver heap 裡的佔用，與 `driver.memory` 的關係——§4.4 提了 broadcast 的記憶體風險但沒說是 driver 端的問題，讀者容易以為只是 executor 的事。
