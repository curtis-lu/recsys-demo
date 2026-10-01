# recsys_tfb — 批次排序建模框架

`recsys_tfb` 替每個 **query group**（例如「某個月的某位客戶」）把一組候選 **item** 依模型分數排出名次，一次處理一整批。下游拿名次決定每個對象先推什麼：資源不夠對每個人推所有東西時，每個人只推排在前面的幾個。

本文件用兩個示例講解，兩個都只是示例，框架不限定這兩種應用：

- **銀行產品推薦**：每個月替每位客戶排一次所有產品。這是主線，`conf/base/` 的設定就是它。
- **廣告曝光**：替一次請求裡展示的幾個素材排先後。它多用了幾個選用設定，放在 [`examples/ad/`](examples/ad/README.md)。

本文件由淺到深分三段：

| 段 | 讀完你會知道 | 章節 |
|---|---|---|
| 它是什麼 | 框架在做什麼、你的問題適不適用 | §1–§3 |
| 怎麼用 | 框架怎麼運作、第一次怎麼跑完一輪 | §4–§6 |
| 用到再查 | 出事或改設定時看哪裡、常見的觀念誤會、還有哪些文件 | §7–§9 |

---

## 1. 這是什麼

你準備好資料，框架負責組訓練資料、訓練、評估、產出排序結果；中間「哪個模型可以上線」由你拍板。

```
你寫 SQL                  框架                                         你決定
────────                  ────                                         ──────
上游原始資料
  │ source ETL
  ▼
feature_table ─┐
label_table   ─┼─▶ dataset ─▶ training ─▶ evaluation --post-training ─▶ promote
sample_pool   ─┘   組訓練資料   訓練＋調參    上線前的評估報表               （人工）
                                                                          │
inference_population ─────────────────────────────▶ inference ◀──────────┘
  （這次要替誰排）                                     │
                                                      ▼
                                              ranked_predictions（排序結果表）
                                                      │ 等答案出來之後
                                                      ▼
                                              evaluation（上線後監控）
```

每一段都是一條獨立的 pipeline，用同一種指令執行：`python -m recsys_tfb <pipeline> --env production`（`production` 是你的設定目錄名稱，見 §5 步驟 0）。

---

## 2. 核心概念

### 一筆候選、一個 query group

框架不管你的欄位叫什麼，只認幾個**角色**。你在 `parameters.yaml` 的 `schema.columns` 告訴它哪一欄扮演哪個角色（你自己的設定放在 `conf/production/`，見 §5 步驟 0）。

| 角色 | 意思 | 銀行產品推薦 | 廣告曝光 |
|---|---|---|---|
| `time` | 一次排序所屬的時段 | `snap_date`，每月月底 | `snap_date`，每週一 |
| `entity` | 替誰排 | `cust_id`，一位客戶 | `[user_id, slot_id]`，某個使用者在某個版位 |
| `occasion`（選用） | 同一時段裡的哪一次排序 | 不用 | `request_id`，一次請求 |
| `item` | 被排的東西 | `prod_name`，一個產品 | `[campaign_id, creative_format]`，活動 × 素材格式 |
| `event`（選用） | 同一組裡同一個 item 有好幾列時，分辨每一列 | 不用 | 不用 |
| `label` | 答案 | 客戶後來有沒有申辦這個產品 | 這次展示有沒有被點 |
| `score`、`rank` | 模型給的分數、組內名次 | 框架產生 | 框架產生 |

**query group** ＝ `time` ＋ `entity`（宣告了 `occasion` 就再加上它）。名次只在同一個 query group 裡比。一個銀行示例的 query group 長這樣：

```
query group：2025-12-31 × 客戶 A
  item          score   rank   label
  fund_stock     0.62      1       1    ← 排第一，而且真的申辦了
  ccard_cash     0.31      2       0
  exchange_usd   0.08      3       0
```

**一欄該放哪個角色**：問一句「它是這次排序替誰做的，還是這次排序裡互相競爭的選項？」前者放 `entity`，後者放 `item`。以廣告的 `slot_id`（版位）為例：

- 一個版位要放哪個素材時，互相競爭的是素材；版位不會跟別的版位搶同一個位置，所以 `slot_id` 放 `entity`。
- 放在 `entity` 還有一個好處：`feature_table` 以 `time` ＋ `entity` 接到候選上，版位的特徵（位置、頁面類型）才放得進去。
- 硬放進 `item` 的話，同一個活動在三個版位會變成三個互不相干的 item；版位的特徵只能改放候選層級特徵表，inference 就不能用了。
- 代價：同一個人變成三個 entity。示例因此設了 `dataset.train_split_keys: [user_id]`，同一個人不會被切到 train 與 train_dev 兩邊。

一個 `entity` 或 `item` 可以由好幾欄組成，寫成清單即可（item 會被拼成一欄，欄名固定叫 `item`，值例如 `c01-banner`）。

**模型看到什麼**：對每一筆候選，模型拿到的是

- 這個 entity 在這個時段的特徵（來自 `feature_table`）；
- 這是哪一個 item（item 本身是一個類別特徵）；
- 選用：這筆候選自己的特徵（來自候選層級特徵表，見下）。

item 自己的屬性（例如產品類型）不會另外變成特徵；模型學的是每個 item 跟 entity 特徵之間的關係。

### 候選集合：兩種情境

一個 query group 裡有哪些候選，由你的 `sample_pool` 決定。框架不檢查它屬於哪一種，但兩種的指標意思不一樣：

- **全網格**（預設的前提）：這個 entity 有資格的每一個 item 各一列。label 是 0 的意思是「可以選、沒有選」。銀行示例是這種。
- **被展示的子集**：只有過去被某個系統挑出來展示過的 item 才有一列，通常一列就是一次展示。label 是 0 的意思是「看到了、沒反應」；沒被展示的 item 沒有答案，不是負例。廣告示例是這種，要用 `occasion` 或 `event` 把資料形狀講清楚（見下一節）。

### 兩個選用角色：`occasion` 與 `event`

預設的粒度（一組 ＝ entity × 時段，組裡每個 item 一列）裝不下某些資料，例如展示紀錄。這兩個角色各回答一個問題：

1. **名次要在什麼範圍裡比？** 只讓同一次請求裡一起被排的候選互相比，就宣告 `occasion`（例如 `request_id`）。
2. **在那個範圍裡，同一個 item 會有好幾列嗎？** 會，就宣告 `event` 分辨每一列（例如曝光 ID）。

兩題的答案合起來是四種設定：

| | 組裡同一個 item 只有一列 | 組裡同一個 item 有好幾列 |
|---|---|---|
| **一組 ＝ entity × 時段** | 什麼都不宣告。銀行示例 | 只宣告 `event`。把廣告資料改成「使用者 × 版位 × 週」一組，同一個素材一週會被曝光好幾次，用 `impression_id` 分辨 |
| **一組 ＝ 一次請求** | 只宣告 `occasion`。廣告示例現在的設定：一次請求裡的素材不會重複 | 兩個都宣告。例如資訊流一次載入 6 格，同一個素材出現在第 1 格和第 5 格 |

宣告之後，它們會成為認出一筆候選的欄位（identity）的一部分；`occasion` 還會讓 query group 變小。各自的代價與宣告時要改哪些地方，見 [`impression-data-shapes.md`](docs/operations/user-guides/impression-data-shapes.md)。

### 你準備的表、框架產出的東西

**你準備的來源表**（Hive；通常用框架的 source ETL 跑你的 SQL 產生）：

| 表 | 一列是什麼 | 用途 |
|---|---|---|
| `feature_table` | 一個 entity 在一個時段的特徵 | 模型的輸入 |
| `label_table` | 一筆候選的答案 | 訓練與評估的標準答案；沒有出現的候選當成負例 |
| `sample_pool` | 一筆候選 | 決定每個 query group 裡要排哪些候選 |
| `inference_population` | 這次要被排序的一個 entity | 只給 inference 用：決定這次替誰排 |

另外可以選擇性加一張**候選層級特徵表**（在 `catalog.yaml`——登記每張表在哪、什麼格式的設定檔——加一個 `candidate_feature_table` 條目）：一列是一筆候選的特徵，例如「這次展示之前 30 分鐘的瀏覽次數」。代價是 inference 不能用（見 §3）。各表的欄位與範例資料見 [`data-lineage.html`](docs/data-lineage.html)。

**框架產出的東西**：

- **模型版本**：training 產生一個 `model_version`，目錄裡有模型、最佳參數與訓練診斷，還有 test 的預測 `training_eval_predictions`。
- **評估報表**：evaluation 產生 HTML 報表，看排序指標、各 item 與各客群的表現、跟熱門度基準線比。主要指標是 **mAP**：每個 query group 看正例排得多前面，算一個分數（AP；正例全排在最前面時是 1），再對所有 query group 平均。
- **排序結果表** `ranked_predictions`：inference 的產出，每列是 `time`、`entity`、`item`、`score`、`rank`、`model_version`。通過完整性與名次檢查才會寫進去。

`score` 是模型的原始分數，只用來排先後，不保證能當機率讀（見 §8 Q3）。

---

## 3. 適不適合你的問題

先看你的資料和需求落在哪一列：

| 你的情況 | 框架怎麼接 | 代價 |
|---|---|---|
| 每個對象有一組候選，定期批次排一次 | 預設，不用多宣告 | — |
| 資料是展示紀錄（一列一次展示） | 用 `occasion` 或 `event` 宣告資料形狀 | 離線指標只重排被展示過的候選，會受當初誰被展示、擺在哪個位置影響。不宣告框架也不會報錯，只是數字的意思變了 |
| 同一時段、同一對象有好幾次請求，每次各自排 | 宣告 `occasion` | query group 變小，隨便排的 mAP 也會偏高，只能在同一批資料裡互比；上線後監控模式的 evaluation 不能用 |
| 同一個 query group 裡同一個 item 有好幾列 | 宣告 `event` | 上線後監控模式的 evaluation 不能用 |
| 有「候選當下」才有的特徵 | 加一張候選層級特徵表 | inference 不能用：評分要交給線上算得出這些特徵的系統 |
| 不同對象可選的 item 不同 | 訓練端：`sample_pool` 只放有資格的候選。推論端：「替誰排」由 `inference_population` 決定 | 推論端每個 entity 都配上整份 item 清單，沒有設定可以逐人限制；要限制，在下游過濾 `ranked_predictions`，或改 inference 的程式碼 |
| entity 或 item 由好幾欄組成 | 在 `schema.columns` 寫成清單 | item 會被拼成一欄，原本的欄不會各自變成特徵 |
| item 很多，或常常有新 item | item 清單事先列出，或從 train 期間的資料數出來 | 模型只認得清單裡的 item。上線後才出現的新 item，要等它出現在 train 期間、重跑 dataset 與 training 才會被評分。item 很多時的效能沒量過 |
| `time` 是日或週，不是月 | 直接用 | 設計上以週為下限；日可以跑，但沒量過 time 值很多時的效能 |
| 資料量很大 | Spark 處理 | training 在 driver 單機上跑，train 資料要放得進 driver 的記憶體；用 train 抽樣控制大小 |

**目前不支援**：

- **線上、即時評分。** 框架只做批次的離線推論。
- **每個 item 各一個模型。** 所有 item 共用一個模型，item 是其中一個特徵。
- **LightGBM 以外的模型。** 介面允許擴充，但目前只有 LightGBM。
- **來源表的 `time` 欄不叫 `snap_date`。** source ETL 的輸出檢查目前把這個欄名寫死（issue #390，未修）。修好之前，`time` 欄請取名 `snap_date`。

**執行環境的限制**：PySpark 3.3.2，跑在 Hadoop／HDFS／Hive 上；純 CPU。Spark 裡不能用 UDF、執行時不能連外網、不能安裝額外套件——所以特徵只能用 Spark SQL 的內建函式算，不能在執行時呼叫外部服務。

---

## 4. 框架怎麼運作

- **pipeline 由 node 組成，讀寫集中在 catalog。** 每個 node 只寫資料處理邏輯；讀哪張表、寫到哪、什麼格式，都寫在 `conf/base/catalog.yaml`。換 Hive database 或表名不用改程式。
- **Spark 處理大量資料，driver 做訓練與評分。** 抽樣、前處理、組訓練資料都在 Spark 做。進 training 才把資料複製到 driver 本機，轉成 LightGBM 的 `.bin` 快取，調參的每一次試驗都不用重讀 Hive。inference 也在 driver 上分批評分。
- **每一層產物都有版本。** 版本 ID 由「會影響這層產物的設定」算出來，分三層：

  ```
  base_dataset_version        前處理器、val、test
     └─ train_variant_id      train、train_dev（只改 train 的抽樣，只重建這層以下；
           │                  train_dev 是從 train 同一段時間切出的一小份，見 §8 Q5）
           └─ model_version   模型（上面兩個版本 ＋ 模型設定）
  ```

  設定沒變就沿用已經產出的東西；只有受影響的那幾層會重算。不同實驗的版本可以並存。`latest` 指最近一次產出的資料版本，`best` 指你 promote 過、inference 預設使用的模型版本。改了哪個設定要重跑什麼，見 [`troubleshooting.md`](docs/operations/user-guides/troubleshooting.md) §3。
- **框架給建議，你拍板。** 三件會改變模型意思或上線結果的事由你決定：哪些欄是類別欄（`scripts/suggest_categorical_cols.py` 給候選）、抽樣比例與樣本權重（`scripts/sampling_overrides_editor.py` 給建議）、哪個模型上線（`scripts/promote_model.py`）。
- **越早擋錯越好。** 只看設定就判斷得出的錯，在啟動 Spark 之前就擋；要看資料的，在讀到資料的第一步擋；排序結果在發布之前檢查。細節見 [`pipeline-checks.md`](docs/operations/user-guides/pipeline-checks.md)。
- **中斷了可以接著跑。** 已經產出的版本會直接沿用；可以只跑某一段（`--from-node`、`--only-node`，見 [`pipeline-slicing.md`](docs/operations/user-guides/pipeline-slicing.md)）；`hpo_checkpointing` 開著時（預設就開），調參中斷後可以只補跑沒做完的試驗；source ETL 可以從失敗的那張表接著跑（`--restart-from`）。

---

## 5. 快速上手

這一節帶你把自己的題目跑完一輪。主線用銀行產品推薦；廣告情境不一樣的地方，放在每一步最後的「**廣告情境**」框裡。

開始前：照 [`using-a-release.md`](docs/operations/user-guides/using-a-release.md) 取得框架的一個發行版（git tag）並裝好，而且執行環境已經連得上 Spark 與 Hive。**想先看框架跑一次**：在本機建好環境（[`local-spark-setup.md`](docs/operations/dev-setup/local-spark-setup.md)）後，`bash scripts/local_e2e.sh` 用合成資料跑完銀行示例，`bash examples/ad/run_e2e.sh` 跑完廣告示例。

### 步驟 0：建自己的設定目錄

```bash
mkdir -p conf/production
```

- 本文件用 `production` 當你的環境名稱。名字可以自己取，換了就把後面每條指令的 `--env production` 一起換；但不要用 `local`，那是不帶 `--env` 時的預設，給本機測試用。
- `conf/production/` 疊在 `conf/base/` 上面。檔名跟 `conf/base/` 裡的一樣，裡面只寫你要改的鍵，同名的鍵以你的為準。
- **不要直接改 `conf/base/`**。那是框架附的示例設定，升級時會被新版蓋掉。
- **pipeline 指令都要帶 `--env production`**。不帶的話預設是 `local`，你的設定一條都不會生效，而且不會有任何錯誤。
- 兩個例外沒有環境分層：來源 SQL（`conf/sql/etl/`）與 Spark 連線設定（看 `SPARK_CONF_DIR`）。怎麼處理見 [`using-a-release.md`](docs/operations/user-guides/using-a-release.md) §5。
- **已知缺口**：底下還有一層 key 的值（例如 `inference:` 底下的 `products`）是合併、不是取代——你可以改或加 key，但刪不掉 `conf/base/` 裡已有的 key。目前有三個鍵因此要直接改 `conf/base/`，後面用到時會提醒；代價是升級時這幾處要自己合併（issue #477）。

`conf/base/` 裡的值是銀行示例專屬的（欄名、item 名、日期、表名），換成你的資料一定要改。每一步會講要改哪些；步驟 5 有一張總表，讓你跑之前對一次。

### 步驟 1：定義排序契約

寫 SQL 或調模型之前，先把題目對應到框架的角色，跟需求方確認：

| 要決定的事 | 銀行產品推薦 |
|---|---|
| 排序拿來做什麼 | 每個月決定每位客戶優先推哪幾個產品 |
| `time` | `snap_date`，每月月底 |
| `entity` | `cust_id` |
| `item` | `prod_name` |
| 候選 | 這位客戶當時有資格申辦的每一個產品 |
| `label` | 月底之後一段固定期間內，有沒有申辦這個產品 |
| 下游怎麼用 | 每位客戶取前 K 名 |
| 主要看的指標 | mAP 或 recall@K，K 等於下游實際推得出去的名額 |

`label` 就是模型真正會學的行為。拿「點擊」當 label，模型學到的是點擊傾向，不等於申辦意願或收益。為什麼指標以 query group 為單位、為什麼不用門檻，見 §8 Q2、Q3。

不同客戶可選的產品不同時（例如沒做過風險評估的客戶不能買基金），訓練端要在 `sample_pool` 的 SQL 裡就只放有資格的候選；推論端的做法見 §3。

> **廣告情境**：`entity` 是 `[user_id, slot_id]`，`occasion` 是 `request_id`，候選是這次請求真的展示過的素材，`label` 是這次展示有沒有被點。

### 步驟 2：建來源表

在 `conf/sql/etl/` 寫 SQL，產出 `feature_table`、`label_table`、`sample_pool`、`inference_population`。每張表的 SQL 檔、partition、`primary_key` 與品質檢查，設在 `parameters_{feature,label,sample_pool,inference_population}_etl.yaml` 的 `tables`；寫進哪個 database 設在同一檔的 `variables.target_db`。pipeline 讀表用的 database 是 `parameters.yaml` 的 `hive.db`，通常設成同一個。要換表名就改 `catalog.yaml`。

三件最容易錯的事：

- `feature_table` 只能用 `time` 當下已經知道的資訊。框架不檢查這件事。
- `label_table` 的日期要等觀察窗結束、資料到齊才能用。
- `sample_pool` 放的是「全部有資格的候選」，不是只放發生過事件的。

先用一個日期試：

```bash
# 不寫輸出表：只檢查上游 partition、欄位與資料量（但 target_db 不存在會建立它，檢查失敗會寫 audit，見 source_etl.md）
python -m recsys_tfb feature_etl --env production --source-check --target-dates 2026-01-31
python -m recsys_tfb label_etl   --env production --source-check --target-dates 2026-01-31

# 檢查通過再寫表
python -m recsys_tfb feature_etl     --env production --target-dates 2026-01-31
python -m recsys_tfb label_etl       --env production --target-dates 2026-01-31
python -m recsys_tfb sample_pool_etl --env production --target-dates 2026-01-31
```

`--source-check` 查什麼，要先寫在 ETL 設定的 `source_checks`（例如 `feature_etl.source_checks`）；`conf/base/` 的是空的，不寫的話它只印一行警告就結束，什麼都沒查。

單日沒問題，再把 `--target-dates` 擴大到 train、val、test 需要的所有日期；inference 要的日期在步驟 6 再產。完整設定見 [`source_etl.md`](docs/pipelines/source_etl.md)。

> **廣告情境**：`sample_pool` 與 `label_table` 都要帶 `request_id`，兩張表的 `primary_key` 也要加上它。候選層級特徵表在 `catalog.yaml` 加一個 `candidate_feature_table` 條目指過去。SQL 範例在 `examples/ad/conf/sql/etl/`。

### 步驟 3：設定 schema 與資料切分

在 `conf/production/parameters.yaml` 寫角色對應與 item 清單：

```yaml
schema:
  columns:
    time: snap_date
    entity: [cust_id]
    item: prod_name
    label: label
  categorical_values:
    prod_name: [ccard_bill, ccard_cash, fund_stock]   # 你全部的 item
```

欄名換了，`catalog.yaml` 裡各表 `columns`、`partition_cols` 寫的欄名也要跟著換（item 由多欄組成時，框架寫的表裡欄名固定叫 `item`）。只有預測表缺 entity 欄時，training 一開始會擋；其他表不檢查，沒換會在寫表時失敗。

同一份 item 清單也要寫進 `inference.products`（`parameters_inference.yaml`），框架會檢查兩處一致。不想逐一列出的話，item 那格寫 `from_train_data`（例：`prod_name: from_train_data`），框架從 train 期間的 `sample_pool` 數出清單。這時 `inference.products` 必須拿掉，留著會被擋；而它在 `conf/base/` 裡已經有一份，從 `conf/production/` 刪不掉，**要直接刪掉 `conf/base/parameters_inference.yaml` 裡的 `products`**（已知缺口）。代價是上線後才出現的新 item，要等它出現在 train 期間、重跑 dataset 與 training 才會被評分（見 [`dataset.md`](docs/pipelines/dataset.md) §3.10）。

在 `parameters_dataset.yaml` 設定：

- `dataset.train_snap_dates`、`dataset.val_snap_dates`、`dataset.test_snap_dates`：照時間先後、互不重疊（為什麼照時間切、train_dev 是什麼，見 §8 Q5）。
- `dataset.prepare_model_input.categorical_columns`：類別欄，一定要包含 item 欄（為什麼，見 §8 Q1）。
- `dataset.prepare_model_input.drop_columns`：不該進模型的欄，例如 identity 欄、label、觀察窗日期。
- `dataset.sample_group_keys`：train 分層抽樣的分層欄。
- `dataset.sample_ratio_overrides`：各分層的抽樣比例（哪些資料可以抽、哪些不行，見 §8 Q4）。`conf/base/` 裡示例的 key 用的是銀行的 item，換了 item 會被擋；從 `conf/production/` 刪不掉，**要直接改 `conf/base/parameters_dataset.yaml` 的這一段**（已知缺口）。
- `dataset.carry_columns`：之後設樣本權重要用、但不是特徵的欄。

類別欄與抽樣比例可以先讓工具給建議，你再審：

```bash
python scripts/suggest_categorical_cols.py <database>.feature_table --max-cardinality 30
python scripts/sampling_overrides_editor.py profile <database>.sample_pool
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
```

- `suggest_categorical_cols.py` 遇到大表，可以加 `--where` 只讀部分分區、或 `--sample-fraction` 抽樣加速（見 [`dataset.md`](docs/pipelines/dataset.md)）。
- `sampling_overrides_editor.py` 預設讀 `conf/base/` 的示例設定，用 `--params`、`--train-params`、`--base-params` 改指到你的設定檔；`to-yaml` 產出兩段：`sample_weights` 貼到 `conf/production/parameters_training.yaml`；`sample_ratio_overrides` 要取代 `conf/base/parameters_dataset.yaml` 裡示例的那一段（貼到 `conf/production/` 會跟示例的 key 合併而被擋，見上面）。用法見 [`sampling-overrides-editor.md`](docs/operations/user-guides/sampling-overrides-editor.md)。

> **廣告情境**：`schema.columns` 加 `occasion: request_id`；`request_id` 不能列進 `categorical_columns`；`catalog.yaml` 裡 `training_eval_predictions` 的欄位要加上 `request_id`（預測表只寫宣告過的欄，沒加會被設定檢查擋下）。完整清單見 [`impression-data-shapes.md`](docs/operations/user-guides/impression-data-shapes.md)。

### 步驟 4：設定模型與評估

在 `parameters_training.yaml`：

- `training.algorithm_params.objective`（模型學什麼）：第一版用 `binary`，流程最好驗證。想讓模型直接學組內順序，再試 `lambdarank` 或 `rank_xendcg`，並把 `training.algorithm_params.metric` 改成 `ndcg` 或 `map`。
- `training.hpo_objective`（調參時在 val 上看什麼）：`conf/base/` 預設 `macro_per_item_map`，讓每個 item 一樣重，避免熱門 item 主導調參；想讓每個 query group 一樣重，改 `mean_ap`。它跟 objective 的差別見 §8 Q6。
- `training.sample_weight_keys`：`conf/base/` 是銀行的 item 欄 `prod_name`，改成你的 item 欄，沒改會被設定檢查擋下。要用 `sampling_overrides_editor.py` 產生權重的話，還要加上 label 欄（例如 `[prod_name, label]`），不然它會報錯。
- 第一次試跑可以先調低 `training.n_trials` 與 `training.num_iterations`，確認資料流沒問題再調回來。

在 `parameters_evaluation.yaml`：

- `evaluation.k_values` 設成下游實際的名額（例如只推 3 個產品，就特別看 @3）。
- `evaluation.segment_columns` 列出要分開看的客群，避免整體數字蓋掉某個客群變差。
- `evaluation.item_categories` 是 item 大類報表。不用的話，把 `evaluation.item_categories.enabled` 設成 `false`；要用的話，`conf/base/` 裡銀行的大類刪不掉，**要直接改 `conf/base/parameters_evaluation.yaml` 的 `mapping`**（已知缺口）。

> **廣告情境**：一次請求平均只有約 4 個候選，組太小時 mAP 分不太出好壞。示例改用 `training.hpo_objective: pooled_average_precision`（把每一列當一次二元預測）。這個目標要留下一部分沒有正例的組：`dataset.val_zero_positive_group_ratio` 與 `dataset.test_zero_positive_group_ratio` 都要大於 0，`catalog.yaml` 的 `training_eval_predictions` 也要加上 `zero_positive_group_weight` 欄。示例的設定在 `examples/ad/conf/base/`，說明見 [`training.md`](docs/pipelines/training.md) §3.2。

### 步驟 5：跑之前，對一次一定要設的鍵

下面這些鍵在 `conf/base/` 裡是銀行示例的值，換成你的資料一定要改。`conf/base/` 裡標 ★ 的就是這些。

| 檔案 | 鍵 | 設成什麼 |
|---|---|---|
| `parameters.yaml` | `hive.db` | 你的 Hive database |
| | `schema.columns` 的 `time`、`entity`、`item`、`label` | 你的欄名 |
| | `schema.categorical_values.<item 欄>` | 你全部的 item，或 `from_train_data` |
| `catalog.yaml` | 各表 `partition_cols`、`columns` 裡的 `snap_date`、`cust_id`、`prod_name`、`label` | 你的欄名 |
| `parameters_<名稱>_etl.yaml`（四個） | `<名稱>_etl.variables.target_db`、`<名稱>_etl.tables` | 你的 database、你的來源表定義 |
| `parameters_dataset.yaml` | `dataset.train_snap_dates`、`dataset.val_snap_dates`、`dataset.test_snap_dates` | 你的日期 |
| | `dataset.prepare_model_input.categorical_columns`、`dataset.prepare_model_input.drop_columns` | 你的類別欄、不進模型的欄 |
| | `dataset.sample_group_keys`、`dataset.carry_columns` | 你的分層欄、權重要用的欄 |
| | `dataset.sample_ratio_overrides` | 你的分層比例，或整段清空（**直接改 `conf/base/`**） |
| `parameters_training.yaml` | `training.sample_weight_keys` | 你的 item 欄；用 `sampling_overrides_editor.py` 產生權重時再加上 label 欄 |
| `parameters_inference.yaml` | `inference.snap_dates` | 要評分的日期 |
| | `inference.products` | 你全部的 item；用 `from_train_data` 時要刪掉（**直接改 `conf/base/`**） |
| `parameters_evaluation.yaml` | `evaluation.snap_date` | 要評估的日期 |
| | `evaluation.item_categories` | 不用就 `enabled: false`；要用就改 `mapping`（**直接改 `conf/base/`**） |

`evaluation.segment_columns` 也標了 ★：沒改不會出錯，只是分群報表會註明找不到那一欄。其他鍵都有通用的預設值，跑通之後再調。

### 步驟 6：依序執行

```bash
python -m recsys_tfb dataset    --env production            # 1. 組出四份資料
python -m recsys_tfb training   --env production            # 2. 訓練；log 會印出這次的 model_version
python -m recsys_tfb evaluation --env production --post-training --model-version <model_version>
                                                            # 3. 上線前評估報表
python scripts/promote_model.py --env production --dry-run  # 4. 列出候選版本
python scripts/promote_model.py <model_version>             # 5. 看完報表，人工 promote

# 6. 替推論日期產出 inference 要讀的兩張表，再評分、發布
python -m recsys_tfb feature_etl              --env production --target-dates <推論日期>
python -m recsys_tfb inference_population_etl --env production --target-dates <推論日期>
python -m recsys_tfb inference                --env production

# 7. 那一期的答案出來後：補上 label，評估已發布的結果
python -m recsys_tfb label_etl  --env production --target-dates <推論日期>
python -m recsys_tfb evaluation --env production
```

- **產出在哪**：模型在 `data/models/<model_version>/`（目錄名就是 model_version）；評估報表在 `data/evaluation/<model_version>/<日期>/report.html`。
- **第 3 步**的報表是決定要不要上線的依據：先確認模型贏過熱門度基準線，各 item、各客群沒有明顯變差。一定要帶 `--model-version`：不帶的話，evaluation 評的是 `best`（上一個 promote 過的版本），不是剛訓練好的這個。
- **第 5 步**：training 不會自己上線。從沒 promote 過，inference 會停下並提示你先 promote；以前 promote 過，inference 會安靜地繼續用舊模型。指定版本號時 promote 不讀設定，所以不用帶 `--env`。怎麼挑版本見 [`promoting-a-model.md`](docs/operations/user-guides/promoting-a-model.md)。
- **日期都寫在設定裡**：第 3 步評 `evaluation.snap_date`，它要在 `dataset.test_snap_dates` 裡；第 6 步替 `inference.snap_dates` 排；第 7 步要先把 `evaluation.snap_date` 改成推論日期。
- **第 7 步**要等那一期的 label 觀察窗結束、答案補齊才跑。

> **廣告情境**：只跑到第 3 步。宣告了候選層級特徵表，inference 在入口就會停下（評分要交給線上系統）；宣告了 `occasion`，上線後監控模式的 evaluation 也不能用。

### 步驟 7：驗收第一版

pipeline 顯示成功不代表結果對。至少再確認：

- 來源表在各自的鍵上沒有重複，各日期、各 item 的資料量合理。
- `feature_table` 沒有用到觀察窗內或之後才產生的欄位。
- train、val、test 的日期互不重疊、照時間先後；test 留到最後才看。
- 抽樣與樣本權重沒有讓冷門 item 或重要客群消失；training 的套用報告裡沒對上的 key 已經看過。
- 評估報表：模型贏過熱門度基準線，各 item、各客群沒有明顯變差。
- `ranked_predictions` 每個 query group 的候選都完整。
- 隨機抽幾個人看實際排序，符合產品資格、法遵與常識。

---

## 6. 各 pipeline 做什麼

**source ETL**（`feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`）：照設定檔的順序跑你的 SQL，寫出來源表。寫之前可以先檢查上游（`--source-check`），寫完檢查列數、重複鍵、NULL 比例；可以一次跑多個日期、失敗後從中間接著跑、只印出 SQL 不執行。→ [`source_etl.md`](docs/pipelines/source_etl.md)

**dataset**：先檢查設定跟資料對不對得上；依日期切出 train、train_dev、val、test，train 依你的設定分層抽樣；只用 train 期間的資料建前處理器（類別編號、要用的特徵欄），再套到所有資料；最後接上特徵與 label，產出四份模型輸入。沒有任何正例的 query group 每個 split 各自決定留多少：預設 train 全留、val 與 test 全丟（排序指標算不了它）。→ [`dataset.md`](docs/pipelines/dataset.md)

**training**：把四份資料複製到 driver 本機；用 train 訓練、train_dev 決定樹長到第幾棵停、val 挑超參數（Optuna）；產出最終模型，對 test 預測並算指標；可以另外輸出特徵重要度與 SHAP 診斷，並記錄到 MLflow。可以排除特徵、設樣本權重，都不用重建 dataset。→ [`training.md`](docs/pipelines/training.md)

**evaluation**：有兩種模式。`--post-training` 評 training 對 test 的預測（上線前）；預設模式評 inference 發布的結果（上線後，等答案出來）。以 query group 為單位算 mAP、precision、recall 等 @K 指標，再拆到各 item、各客群、各 item 大類，並跟熱門度基準線比；也可以跟另一個模型版本或外部預測表比。報表附一組自動診斷，判讀方式見 [`evaluation-diagnosis.md`](docs/pipelines/evaluation-diagnosis.md)。→ [`evaluation.md`](docs/pipelines/evaluation.md)

**inference**：讀 `inference_population`，每個 entity 配上整份 item 清單，用 `best`（或 `--model-version` 指定的版本）評分、排名；檢查通過才發布到 `ranked_predictions`。→ [`inference.md`](docs/pipelines/inference.md)

---

## 7. 出事了、改了設定

- **指令失敗**：它停在哪個階段，就代表問題在哪一類東西上（設定、資料、還是結果）。
- **跑成功但結果不對**：多半是資料或設定的常見錯誤，框架不會報錯。
- **改了設定**：看改的是哪一層版本的輸入，決定要重跑哪幾條 pipeline。

三件事都在 [`troubleshooting.md`](docs/operations/user-guides/troubleshooting.md)。

---

## 8. 常見的觀念誤會

做過二元分類的人，照舊習慣做，在這裡最容易想歪的六件事。

**Q1. 每個產品各訓一個模型？**

- 你原本會：每個產品一個二元分類模型，各自調參。
- 這裡不一樣：所有 item 共用一個模型，item 是其中一個特徵。同一個 query group 裡的分數出自同一個模型，才能直接比大小排名次。
- 所以：`sample_pool` 不要拆成每個 item 一份；item 欄一定要列在 `categorical_columns`。

**Q2. 用 AUC 或 logloss 評估？**

- 你原本會：把所有列攤平，算一個 AUC。
- 這裡不一樣：下游看的是**每位客戶自己的**名次。一個模型可以 AUC 很好、組內卻排錯：例如它只學會「資產多的客戶什麼產品都比較會買」，所有列攤平排得很好，但同一位客戶的產品順序是錯的。所以評估以 query group 為單位：每組算一個分數，再平均（mAP）。
- 所以：看報表的 mAP@K、recall@K，K 依下游的名額選。mAP 怎麼算見 [`metrics.html`](docs/metrics/metrics.html)。

**Q3. 設一個門檻決定推薦誰？score 是機率嗎？**

- 你原本會：分數大於 0.5 就推。
- 這裡不一樣：每組取前 K 名，不需要門檻，`score` 只用來排先後。`binary` objective 的分數落在 0 到 1 之間，但框架不保證它準；`lambdarank`／`rank_xendcg` 的分數沒有上下界，根本不在機率的尺度上。框架不提供機率校準。
- 所以：下游用 `rank`。真的需要機率，由下游自己校準、自己驗證。

**Q4. 負例太多，抽掉一大半沒關係？**

- 你原本會：負例太多就隨機丟掉一些，讓正負比較平衡。
- 這裡不一樣：train 可以抽，它只改變模型看到的正負比例（用 `sample_ratio_overrides` 依 label 分層抽）。但評估比的是「組內完整候選的名次」，抽掉組裡的一部分候選，mAP 就在比另一件事了。所以 val 只能整個 entity 一起抽（`val_sample_ratio`），test 不抽。
- 所以：抽樣只在設定檔裡做；不要在 `sample_pool` 的 SQL 裡先把負例刪掉——那等於把候選從組裡拿走。

**Q5. 隨機切 train 和 test？**

- 你原本會：隨機切 80／20。
- 這裡不一樣：上線時永遠是拿過去預測未來，所以照時間切：train → val → test，依時間先後、互不重疊。train 同一段時間裡再切一小份 train_dev（`train_dev_ratio`），同一個 entity 只會在其中一邊。

  | 資料 | 用途 |
  |---|---|
  | train | 訓練 |
  | train_dev | 單次訓練裡決定樹長到第幾棵就停 |
  | val | 比較多組超參數，挑最好的 |
  | test | 最後才看，給上線前評估 |

- 所以：設好 `train_snap_dates`、`val_snap_dates`、`test_snap_dates`，例如 train 2025-01～10 → val 2025-11 → test 2025-12。

**Q6. 訓練看 logloss，挑參數也看 logloss？**

- 你原本會：模型最佳化什麼，調參就看什麼。
- 這裡不一樣：這是三個設定，各管一件事。

  | 設定 | 管什麼 | 算在哪份資料上 |
  |---|---|---|
  | `training.algorithm_params.objective` | 模型學什麼（例如 `binary`） | train |
  | `training.algorithm_params.metric` | 單次訓練裡樹長到第幾棵停（`binary` 時是 `binary_logloss`） | train_dev |
  | `training.hpo_objective` | 多組超參數裡挑哪一組；選項裡沒有 logloss | val |

  所以就算用 `binary` objective、用 logloss 決定何時停，挑參數看的仍然是排序排得好不好。
- 所以：第一版用 `binary` ＋ `conf/base/` 預設的 `macro_per_item_map`（每個 item 一樣重）；想讓每個 query group 一樣重，改 `mean_ap`；資料是展示紀錄、組很小，改 `pooled_average_precision`（見 §5 步驟 4 的廣告情境）。

---

## 9. 建議閱讀順序

**① 第一次接觸：照順序讀完**

1. 本文件。
2. [`data-lineage.html`](docs/data-lineage.html)：資料怎麼流、每張表長什麼樣。
3. [`using-a-release.md`](docs/operations/user-guides/using-a-release.md)：取得發行版、安裝、設定放哪、怎麼升級。
4. 看你的情境：全網格就跳過這步；展示紀錄就讀 [`impression-data-shapes.md`](docs/operations/user-guides/impression-data-shapes.md)，再讀 [`examples/ad/README.md`](examples/ad/README.md)。
5. [`metrics.html`](docs/metrics/metrics.html)：mAP 怎麼算、報表怎麼讀。

**② 做自己的題目：照 pipeline 順序，做到哪讀到哪**

[`source_etl.md`](docs/pipelines/source_etl.md) → [`dataset.md`](docs/pipelines/dataset.md) → [`training.md`](docs/pipelines/training.md) → [`evaluation.md`](docs/pipelines/evaluation.md) → [`promoting-a-model.md`](docs/operations/user-guides/promoting-a-model.md) → [`inference.md`](docs/pipelines/inference.md)

每條 pipeline 的 node 流程圖：[`dataset`](docs/diagrams/dataset-pipeline.html)、[`training`](docs/diagrams/training-pipeline.html)、[`inference`](docs/diagrams/inference-pipeline.html)。

**③ 遇到事情：查就好，不用讀完**

| 你遇到 | 看這份 |
|---|---|
| 指令失敗、結果怪、改了設定不知道重跑什麼 | [`troubleshooting.md`](docs/operations/user-guides/troubleshooting.md) |
| 想知道每一層檢查擋什麼、哪些擋不住 | [`pipeline-checks.md`](docs/operations/user-guides/pipeline-checks.md) |
| 只重跑一段、中斷後接著跑 | [`pipeline-slicing.md`](docs/operations/user-guides/pipeline-slicing.md)、調參中斷見 [`training.md`](docs/pipelines/training.md) §4.7 |
| 多評估一個月份，不重訓 | [`adding-an-eval-month.md`](docs/operations/user-guides/adding-an-eval-month.md) |
| 看不懂某個詞 | [`CONTEXT.md`](CONTEXT.md)（詞彙表） |

**④ 想懂背後的原理：選讀**

- [`design-principles.md`](docs/design-principles.md)：版本、檢查與其他設計取捨的理由。
- GBDT 手冊，依序讀：[二元分類](docs/handbooks/gbdt/gbdt_binary_classification.md) → [類別不平衡](docs/handbooks/gbdt/gbdt_class_imbalance.md) → [多 item 不平衡](docs/handbooks/gbdt/gbdt_multiitem_imbalance.md) → [learning-to-rank](docs/handbooks/gbdt/gbdt_learning_to_rank.md)。沒有網路時，開 `docs/handbooks/gbdt/export/` 裡的 `*_offline.html`。
