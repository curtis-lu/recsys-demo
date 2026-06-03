# recsys_tfb — 排序問題批次建模框架(示例情境:銀行產品推薦)

> 入口文件。深入主題見 [§5 文件地圖](#5-文件地圖--建議閱讀順序)。

## 0. 這是什麼

一句話:一套**通用排序問題**的批次建模框架——對每個「查詢群組(query group)」,把候選項目依模型分數排名。本 repo 以**銀行產品推薦**為示例情境,但架構與方法不限定於此。

- **抽象(以 schema 角色描述)**:框架用可配置的欄位角色 `time / entity / item / label / score / rank`(定義見 `src/recsys_tfb/core/schema.py`)。對每個 query group(預設 `time + entity`),把 `item` 依 `score` 由高到低給出 `rank`。
- **方法(可配置,非固定)**:單一共享模型(LightGBM),訓練 `objective` 可選——**pointwise**(預設 `binary`)或 **learning-to-rank**(`lambdarank` / `rank_xendcg`)。不論訓練 objective 為何,**評估一律是 per-`(time,entity)` 的排序指標(mAP)**。配置見 `conf/base/parameters_training.yaml`。
- **示例情境(銀行產品推薦)**:`time=snap_date`、`entity=cust_id`、`item=prod_name`、query group = 每位客戶每月。輸出表欄位 `snap_date, cust_id, prod_name, score, rank`,供行銷 PM 依名次決定先聯繫誰、推哪支產品。
- **規模(生產,以產品推薦為例)**:每週批次,~10M 客戶 × 22 產品 × ~1500 特徵。本 repo 內的合成資料較小(8 個產品),僅供示意。

## 1. 應用情境

### 要解決的問題

**通用**:這是一個**排序問題**——對每個 query group(由 `time + entity` 定義),把候選 `item` 依模型分數由高到低排名,供下游依名次分配有限資源。

- **示例(產品推薦)**:query group = 每位客戶每月,`item` = 金融產品;行銷 PM 資源有限,依名次決定先推誰、推哪支。
- 不論訓練 objective 是 pointwise(`binary`)或 LTR(`lambdarank` 等),**評估都用排序指標**(per-`(time,entity)` mAP)。pointwise 與 LTR 的概念差異見手冊 [`gbdt_learning_to_rank.md`](gbdt_learning_to_rank.md)(離線:[`_offline.html`](gbdt_learning_to_rank_offline.html))。

### 限制條件(生產環境)

- **引擎**:PySpark 3.3.2 on Hadoop / HDFS / Hive(Ploomber DAG 編排)。
- **三條硬限制**:不可用 Spark UDF、無網路、不可安裝額外套件。
- **資源**:CPU-only(4 core / 128GB RAM)。

→ 影響:重運算一律走 Spark SQL / DataFrame;模型訓練(LightGBM)是 driver 單機。

### 輸入 / 輸出資料長相

以 schema 角色描述(括號為產品推薦示例);完整 schema 與範例見 [`docs/data-lineage.html`](docs/data-lineage.html)。

**輸入** — 三張 Hive 來源表(由 `source_etl` 維護):

| 表 | 角色內容 | 主鍵(角色) | 產品推薦示例 |
|---|---|---|---|
| `feature_table` | 每個 `entity × time` 的特徵寬表 | `time, entity` | `snap_date, cust_id` |
| `label_table` | 每 `(time, entity, item)` 的 ground truth(`label` 0/1) | `time, entity, item` | + `prod_name` |
| `sample_pool` | 訓練 / 評估的候選母體(含分群欄) | `time, entity, item` | + 分群欄 |

**輸出** — 一張 Hive 表:每個 query group 之下,`item` 依 `score` 由高到低取得 `rank`。角色欄位 `time, entity, item, score, rank`(示例:`snap_date, cust_id, prod_name, score, rank`;另以 `model_version` 區分版本)。

> 範例列以 repo 內合成資料(8 產品)示意,非生產數字。

---

<!-- 以下為骨架,由後續 Task 填入 -->

## 2. 快速上手

<!-- Task 2:Pipeline 總覽(ASCII)· data-lineage 總覽 · 各 pipeline 一句話+指令 · commands 速查 -->
_(建置中)_

## 3. FAQ

<!-- Task 3 -->
_(建置中)_

## 4. 常見錯誤

<!-- Task 3 -->
_(建置中)_

## 5. 文件地圖 / 建議閱讀順序

<!-- Task 3:導覽 docs/ 各檔 + 既有 gbdt_* 手冊 -->
_(建置中)_
