# recsys_tfb — 排序問題批次建模框架，以銀行產品推薦為示例

> 專案入口文件。各深入主題的連結見 [§5 文件地圖](#5-文件地圖--建議閱讀順序)。

## 0. 這是什麼

recsys_tfb 是一套處理**排序問題**的批次建模框架：對每個查詢群組（query group），把群組內的候選項目依模型分數由高到低排名。本文件以**銀行產品推薦**為示例，但框架不限定於這個應用。

> **和二元分類差在哪？**
> 二元分類替每個產品各自判斷客戶會不會買；排序則把同一位客戶在同一個快照日面對的所有候選產品放進**同一組**互相比較，決定誰該排前面。我們要的是組內的**相對名次**，不是每筆的絕對機率。這一組對象，就是一個 query group。

框架用一組**可配置的欄位角色**來描述資料，在 `conf/base/parameters.yaml` 的 `schema` 區塊設定。每個 query group 預設由 `time + entity` 界定，組內把 `item` 依 `score` 排出 `rank`：

| schema 角色 | 意義 | 銀行產品推薦示例 |
|---|---|---|
| `time` | 時間切點 | `snap_date`，快照日 |
| `entity` | 被排序、要分配資源的對象 | `cust_id`，客戶 |
| `item` | 每個對象的候選項目 | `prod_name`，金融產品 |
| `label` | 是否發生，0 或 1 | 客戶是否承作該產品 |
| `score` / `rank` | 模型分數與名次 | 推薦優先順序 |
| query group | 一次排名的範圍 | 每位客戶、每個快照日 |

**方法與評估**

- **方法可選**：只訓練一個共用的 LightGBM 模型，訓練目標可切換 —— pointwise（預設 `binary`）或 learning-to-rank（`lambdarank`、`rank_xendcg`）。設定見 `conf/base/parameters_training.yaml`。
- **評估固定**：不論用哪種目標訓練，評估一律是 per query group（每位客戶、每個快照日）的排序指標 mAP，而非逐筆準確率。這也是它與「逐產品二元分類」最大的不同，詳見手冊 [`gbdt_learning_to_rank.md`](gbdt_learning_to_rank.md)。
- **規模**：生產環境每週批次推論，約 1,000 萬客戶 × 22 產品 × 1,500 特徵。本 repo 的合成資料較小，只有 8 產品，供試跑與示意。

## 1. 應用情境

### 要解決的問題

行銷團隊人力有限，無法對每位客戶推銷每一支產品。框架把它變成一個**排序問題**：對每位客戶，把候選產品依模型分數排名，讓 PM 依名次決定**優先聯繫哪些客戶、優先推薦哪些產品**。一般化來說，就是對每個 query group，把候選 `item` 排名，供下游依名次分配有限資源。

### 限制條件（生產環境）

- **引擎**：PySpark 3.3.2，跑在 Hadoop / HDFS / Hive 上，流程由 Ploomber DAG 編排。
- **三條硬限制**：不可用 Spark UDF、無對外網路、不可安裝額外套件。
- **硬體**：純 CPU，4 核心 / 128GB 記憶體。
- **影響**：重運算一律走 Spark SQL / DataFrame；模型訓練是 driver 上的單機 LightGBM。

### 輸入與輸出

**輸入** —— 三張由 `source_etl` 維護的 Hive 來源表。下表用 schema 角色說明；完整欄位與範例資料見 [`docs/data-lineage.html`](docs/data-lineage.html)。

| 來源表 | 內容 | 主鍵（角色） |
|---|---|---|
| `feature_table` | 每位客戶在每個快照日的特徵寬表 | `time, entity` |
| `label_table` | 客戶是否承作某產品的 ground truth（`label` 0/1） | `time, entity, item` |
| `sample_pool` | 每個 query group 要納入排名的候選範圍，並帶分群欄供分層抽樣 | `time, entity, item` |

**輸出** —— 一張 Hive 表，示例名為 `ranked_predictions`。每個 query group 內，`item` 依 `score` 由高到低排出 `rank`：

| 欄位 | 角色 | 說明 |
|---|---|---|
| `cust_id`、`score`、`rank` | `entity`、`score`、`rank` | 資料欄：客戶、分數、名次 |
| `snap_date`、`prod_name`、`model_version` | `time`、`item`、版本 | partition 維度 |

---

## 2. 快速上手

_本節尚未完成。將涵蓋 pipeline 總覽、資料流總覽、各 pipeline 的用途與執行指令，以及指令速查。_

## 3. FAQ

_本節尚未完成。_

## 4. 常見錯誤

_本節尚未完成。_

## 5. 文件地圖 / 建議閱讀順序

_本節尚未完成。將整理 docs/ 各文件與既有手冊的導覽與建議閱讀順序。_
