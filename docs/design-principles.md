# 設計原則

> 這個框架為什麼長這樣。讀完你會知道：哪些東西是可換的、哪些不變量被刻意守住、為什麼產物要這樣切版本。
> 要動手改設定請配合 [`change-guide.md`](change-guide.md)；要看資料流請配合 [`data-lineage.html`](data-lineage.html)。

## 0. 定位：抽象排序框架

核心是**排序（ranking / learning-to-rank）**，不是某個特定應用。對每個 query group（`time` × `entity`），把候選 `item` 依模型分數排名，供下游依名次分配有限資源。商業銀行產品推薦只是一個 instantiation。

這個定位驅動了下面所有設計：欄位**角色**可配置、來源表由使用者自定、方法（pointwise / LTR）可切換。

## 1. Schema 角色抽象

框架不認得「客戶」「產品」，只認得**角色**：`time`、`entity`、`item`、`label`、`score`、`rank`。全部在 `conf/base/parameters.yaml` 的 `schema` 配置。

- query group ＝ `time` × `entity`；排名與排序指標都在組內進行。
- 程式碼一律從 schema 解析欄名（`get_schema(parameters)`），**不**硬編 `prod_name` / `cust_id`。
- 好處：換應用只改 schema 與來源表 SQL，pipeline 程式碼不動。

## 2. 方法可切換（單一共用模型）

只訓練**一個**共用模型，訓練目標可切換：pointwise（`binary`，預設）或 learning-to-rank（`lambdarank` / `rank_xendcg`）。**評估永遠**是 per query group 的排序指標（mAP），與訓練目標無關。

→ 模型細節見 [`pipelines/training.md`](pipelines/training.md)；排序 vs 分類的數學見手冊 `gbdt_learning_to_rank.md`。

## 3. 三層資料版本 + model_version

產物依「對什麼設定敏感」切成三層 hash 版本（`core/versioning.py`），讓實驗不必整批重算：

| 版本 | 由什麼決定 | 設計意義 |
|---|---|---|
| `base_dataset_version` | 非抽樣 dataset 設定 ＋ schema ＋ feature_table 欄位指紋 | 調抽樣不會讓前處理 / val / test 失效 |
| `train_variant_id` | train 抽樣設定 | 換 train 抽樣只重算 train 系列 |
| `calibration_variant_id` | calibration 抽樣設定 | 換校準抽樣只重算校準系列 |
| `model_version` | model-defining training 子集 ＋ 上面三個 id | 資料版本一變，模型版本跟著變；純 logging 設定不翻版 |

→ 細節見 [`pipelines/dataset.md`](pipelines/dataset.md)。

## 4. 一致性不變量：單一真實來源、收齊一次、fail-loud

所有 item-set / column-role 不變量集中定義在 `core/consistency.py`，**每條只定義一次**（pure predicate），各 pipeline 不得各自散落 ad-hoc 檢查。

- **兩道閘**：設定閘（CLI 一啟動，`validate_config_consistency`）與資料閘（`dataset` 第一個節點，`validate_data_consistency`）。
- **collect-all**：一次列出所有問題，讓你一輪修完，而不是修一個撞一個。
- **fail-loud**：把錯誤擋在跑完長流程**之前**——寧可啟動就報錯，也不要悄悄算出壞結果（例如未知 item 被編成 -1）。

→ 所有錯誤訊息與修法見 README §4；要新增不變量必須加在 `consistency.py`。

## 5. fit / transform 解耦（防洩漏）

前處理（編碼字典）只在 **train 期** feature_table 上 fit，再對整張表 transform 一次、各 split 共用。fit 不碰 val / test / 未來資料 → 結構上杜絕洩漏。`preprocessor` 是跨 dataset / training / inference 共用的合約。

## 6. 決定性分層抽樣

抽樣是 `crc32(identity | site | seed) % BUCKETS < ratio·BUCKETS` 的**決定性 hash**：同設定 → 同結果（可重現）；不同 `site`（train vs calibration）即使同 seed 也抽到獨立的桶。分層（`sample_group_keys`）確保各組保有足夠正例。

## 7. Handle-based DAG 銜接

訓練是 **driver 上的單機 LightGBM**，資料與模型都駐留 driver-local fs。DAG 之間傳的是**輕量 handle**（`io/handles.py` 的 `ParquetHandle` / `LgbDatasetHandle`），不是materialized DataFrame：

- cache 節點把 Hive split `copyToLocal` 成 driver-local parquet，回傳 `ParquetHandle`；下游用到才 `.to_pandas()`。
- 好處：大資料不重複經 catalog 序列化；cache skip-if-exists（有 `_SUCCESS` marker 就不重建）。

## 8. ModelAdapter 抽象（演算法無關）

pipeline 不直接呼叫 LightGBM，而是透過 `ModelAdapter` ABC（`models/base.py`）：`train` / `predict`（回傳機率 1-D 陣列）/ `save` / `load` / `feature_importance` / `log_to_mlflow` / `prepare_train_inputs`。

- `get_adapter(algorithm)` 從 `ADAPTER_REGISTRY` 取實作；未知演算法 → `ValueError` 列出可用。
- `CalibratedModelAdapter` 包裝一個 base adapter，加上校準器：`predict` 回校準後分數、`predict_uncalibrated` 回原始輸出。
- **要換演算法**：實作 `ModelAdapter` 並註冊，pipeline 不動。

## 9. 宣告式 catalog

資料產物在 `conf/base/catalog.yaml` 宣告，節點以**名稱**引用。`DataCatalog`（`core/catalog.py`）按 `type` 分派 loader（registry 含 `HiveTableDataset` / `JSONDataset` / `ModelAdapterDataset` / `TextDataset` / `ParquetDataset` / `PickleDataset`）。

- 未註冊的中間產物 `save` 時**自動建 `MemoryDataset`**（只活在單次 run）。
- ⚠️ footgun：若某個本該持久化的表沒在 catalog 註冊，會悄悄 fallback 成 MemoryDataset → standalone 讀取靜默壞掉。所以 `ranked_predictions` 在 catalog 額外宣告了讀取端 entry（見 [`data-lineage.html`](data-lineage.html)）。

## 10. 生產限制驅動的取捨

目標環境：no Spark UDF、no 對外網路、no 額外套件、CPU-only。因此：

- 重運算一律走 Spark SQL / DataFrame（不寫 UDF）。
- 模型訓練是 driver 單機 LightGBM（分散式 cluster 對單機 GBDT 無幫助）→ 故有第 7 點的 driver-local handle 設計。
- model artifact 走 Python `open()` 寫 driver-local（不認 `hdfs://`）。

## 接下來

- 動手改設定的逐情境 SOP → [`change-guide.md`](change-guide.md)
- 所有一致性錯誤訊息 → README §4
- 資料流與 schema → [`data-lineage.html`](data-lineage.html)
