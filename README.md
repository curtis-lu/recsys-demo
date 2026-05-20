# recsys_tfb — 批次產品推薦排序框架

> 本文件為公司環境使用者 / 維運者 / 後續開發者的入口文件。
> 內容皆以目前 repo 的程式與設定為準（`src/recsys_tfb/`、`conf/`、`scripts/`）。
> 細節文件：
> - [docs/config-and-versioning.md](docs/config-and-versioning.md)：設定讀取規則、Schema 資料契約、版本 hash 規則
> - [docs/pipeline-runbook.md](docs/pipeline-runbook.md)：各 pipeline 與 scripts 操作、restart、promote、evaluation、錯誤排查
> - [docs/change-sop.md](docs/change-sop.md)：增加 feature / product / schema / training 設定的修改 SOP
> - [docs/metrics.md](docs/metrics.md)：評估指標（程式實際算什麼、輸出格式、報表分段）；概念語意見 [docs/metrics_concept_map.html](docs/metrics_concept_map.html)

---

## 1. 專案定位

這是一套**批次排序推薦框架**。問題形式固定為：

```
customer / entity  ×  product / item  ×  binary label  ->  ranking score
```

對每個 `(snap_date, cust_id)` 群組內的所有候選產品輸出分數並排名，供下游依排名做推薦優先順序。預設場景是**銀行金融產品推薦**（每月底 snapshot、客戶 × 多類金融產品 × 是否承作），但欄位命名與資料契約皆可設定化，可移植到其他「實體 × 品項 × 二元標籤 → 排序」的批次 ranking 場景（見 [docs/change-sop.md](docs/change-sop.md)）。

核心特性：

- 輕量 Kedro-like pipeline 框架（`Node` / `Pipeline` / `Runner` / `Catalog` / `ConfigLoader`），無外部 orchestrator 依賴。
- 三層 hash 版本管理（`base_dataset_version` / `train_variant_id` / `calibration_variant_id` / `model_version`），讓抽樣實驗不會作廢前處理 artifact。
- 設定靜態一致性閘 + 資料一致性閘，在跑 pipeline 前 fail-loud。
- LightGBM + Optuna HPO，支援機率校準與 per-(segment,product) sample weight。

---

## 2. 標準執行流程

CLI 一律是 `python -m recsys_tfb <command> [--options]`，**沒有 `run` 子指令、沒有 `--pipeline` flag**。指令清單以 `src/recsys_tfb/__main__.py` 為準；輔助 scripts 以 `python scripts/<name>.py` 執行。

> 重要：
> - **沒有 `source_etl` 單一指令**。Source ETL 是 `feature_etl`、`label_etl`、`sample_pool_etl` 三個獨立指令。
> - training 完成**不會**自動成為 inference 預設模型。
> - inference 未指定 `--model-version` 時讀 `data/models/best`。
> - `data/models/best` 必須由 `scripts/promote_model.py` **手動**建立 / 更新。
> - `scripts/suggest_categorical_cols.py` 與 `scripts/sampling_overrides_editor.py` 是建模流程的一部分（前者在定義 categorical 欄位時用、後者在調整抽樣 / 冷門產品加權時用），其輸出需人工貼回 `conf/` 後才生效。

### 標準一輪流程（以月底 snap_date 為例）

```bash
# 1. Source ETL：產出 feature_table / label_table / sample_pool（三個獨立指令）
python -m recsys_tfb feature_etl     --env production --target-dates 2025-01-31
python -m recsys_tfb label_etl       --env production --target-dates 2025-01-31
python -m recsys_tfb sample_pool_etl --env production --target-dates 2025-01-31

# 1a.（選用，僅新增/調整 categorical feature 時）掃「已存在」的表推測 categorical 欄位
#     掃描目標必須已存在：feature_etl 完成後的 feature_table，或某個既有上游來源表。
python scripts/suggest_categorical_cols.py ml_recsys.feature_table
#   -> data/profiling/<stem>_categorical.yaml  （人工檢視後貼回 parameters_dataset.yaml）

# 1b.（選用，僅調抽樣 / 冷門產品加權時）profile sample_pool → 瀏覽器編輯 → 產 YAML snippet
#     需 sample_pool_etl 已完成（sample_pool 已存在）。
python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
#   -> data/profiling/sampling_overrides_editor.html  （瀏覽器編輯後 Export JSON）
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
#   -> 貼回 parameters_dataset.yaml (sample_ratio_overrides) /
#           parameters_training.yaml (sample_weights)

# 2. Dataset：一致性閘 → 抽樣切分 → 前處理 → 各 split model_input（版本由參數自動推導）
python -m recsys_tfb dataset --env production

# 3. Training：LightGBM + Optuna HPO，產出 versioned model（不會自動 promote）
python -m recsys_tfb training --env production

# 4. 手動 promote：建立 / 更新 data/models/best symlink
python scripts/promote_model.py <model_version>

# 5. Inference / Baselines / Evaluation
python -m recsys_tfb inference  --env production
python -m recsys_tfb baselines  --env production
python -m recsys_tfb evaluation --env production
```

步驟 1a / 1b 是**按需的準備 / 調參輔助步驟**（非每輪固定要跑）：只在新增 categorical feature 或調整抽樣 / sample weight 時需要。兩者都讀「已存在」的表/檔（`suggest_categorical_cols` 掃 `feature_etl` 後的 `feature_table` 或既有上游來源表；`sampling_overrides_editor` 掃 `sample_pool_etl` 後的 `sample_pool`），輸出皆為 `data/profiling/` 下的 snippet，**需人工貼回 `conf/` 對應檔案**後，後續 `dataset` / `training` 才會吃到。因此放在對應來源表已產出之後、`dataset` 之前。

### 指令選項（以 `__main__.py` 為準）

| 指令 | 選項 | 說明 |
|---|---|---|
| `feature_etl` / `label_etl` / `sample_pool_etl` | `--env/-e`、`--target-dates`、`--restart-from` | `--target-dates` 為逗號分隔日期；未在 config 設定 `target_dates` 時必填 |
| `dataset` | `--env/-e` | 每次都從參數重算版本，無版本選項 |
| `training` | `--env/-e`、`--base-dataset-version`、`--train-variant`、`--calibration-variant` | 三個 version 選項預設為對應 `latest` symlink |
| `inference` | `--env/-e`、`--model-version` | 未指定 `--model-version` 時讀 `models/best` |
| `evaluation` | `--env/-e`、`--model-version`、`--post-training` | `--post-training` 讀 `training_eval_predictions`，否則讀 `ranked_predictions` |
| `baselines` | `--env/-e` | — |

`--env` 預設值為 `local`；公司環境請明確帶 `--env production`。各 pipeline 在 CLI entry 會先跑 `validate_schema_config` 與 `validate_config_consistency`，任何設定矛盾會在跑 pipeline 前一次列出並以 exit code 1 結束。操作細節（restart、promote 規則、evaluation 兩種模式）見 [docs/pipeline-runbook.md](docs/pipeline-runbook.md)。

---

## 3. 整體架構

### 框架元件（`src/recsys_tfb/core/`）

- **`Node`**（`core/node.py`）：包一個 function，宣告 `inputs` / `outputs` 名稱。
- **`Pipeline`**（`core/pipeline.py`）：一組 Node，依資料依賴做 Kahn 拓樸排序；**獨立的零入度節點按 list 宣告順序執行**（所以 dataset 把一致性閘放第一個是有意義的）。
- **`Runner`**（`core/runner.py`）：依拓樸順序逐一執行 Node；輸入名稱前綴 `@` 代表傳入 catalog dataset handle（而非載入資料）；中間 `MemoryDataset` 用完即釋放。
- **`DataCatalog`**（`core/catalog.py`）：依 `catalog.yaml` 建立 dataset 實例（`HiveTableDataset` / `ParquetDataset` / `JSONDataset` / `ModelAdapterDataset` / `PickleDataset` / `TextDataset`）；存到未註冊名稱時自動建 `MemoryDataset`。
- **`ConfigLoader`**（`core/config.py`）：讀取與合併 YAML，見 §4。

### Pipeline 清單（`pipelines/__init__.py` 註冊）

`dataset`、`training`、`inference`、`evaluation`、`baselines`。Source ETL 走獨立的 `SQLRunner`（不在上述 registry，由 `feature_etl` / `label_etl` / `sample_pool_etl` 指令驅動）。

### 資料流與 lineage（含 pipeline 與 scripts）

```text
   公司上游來源表
        │ feature_etl / label_etl / sample_pool_etl
        ▼ (SQLRunner，CTAS/INSERT OVERWRITE + checks)
   feature_table   label_table   sample_pool
        │               │            │
        │               │            │   ── 選用/按需（來源表已產出後才能跑）──────────┐
        │               │            │   scripts/suggest_categorical_cols.py            │
        │（讀已存在的 feature_table 或既有上游表）────────►  → data/profiling/*.yaml      │
        │               │            │                                                 │
        │               │            │（讀 sample_pool）  scripts/sampling_overrides_   │
        │               │            └────────────────►  editor.py profile → 瀏覽器     │
        │               │                                編輯 → to-yaml → snippet       │
        │               │                                                               ▼
        │               │                       人工貼回 conf/base/parameters_dataset.yaml
        │               │                       (categorical_columns / sample_ratio_overrides)
        │               │                       與 parameters_training.yaml (sample_weights)
        │               │                                                               │
        └───────┬───────┴─────┬──────┘   ◄─── dataset/training 讀合併後的 parameters ────┘
                ▼             ▼
            ┌──────────────────────────────────────────────────────────────┐
            │ dataset  (validate_data_consistency → 抽樣切分 → fit/apply     │
            │          preprocessor → build_model_input per split)          │
            └──────────────────────────────────────────────────────────────┘
                │ preprocessor / category_mappings / *_model_input
                ▼                                              （版本: base / train_variant / calibration_variant）
            ┌──────────────────────────────────────────────────────────────┐
            │ training (cache → Optuna HPO → (calibration) → predict_test → │
            │          compute_test_mAP_spark → mlflow)                     │
            └──────────────────────────────────────────────────────────────┘
                │ data/models/<model_version>/{model.txt,best_params,        training_eval_predictions
                │ evaluation_results,manifest}                               (Hive)
                ▼
        scripts/promote_model.py  (手動：比對 evaluation_results.json mAP)
                │ data/models/best -> <model_version>  (symlink)
                ▼
            ┌──────────────────┐        ┌──────────────────┐
            │ inference        │        │ baselines        │
            │ → ranked_        │        │ → baseline_      │
            │   predictions    │        │   metrics        │
            └──────────────────┘        └──────────────────┘
                │ ranked_predictions            │ baseline_metrics
                └───────────────┬───────────────┘
                                ▼
            ┌──────────────────────────────────────────────┐
            │ evaluation  (prepare_eval_data → compute_     │
            │  metrics → report.html)                       │
            │  預設讀 ranked_predictions；--post-training    │
            │  改讀 training_eval_predictions               │
            └──────────────────────────────────────────────┘
```

Lineage 對照表（artifact → 產生者 → 消費者 → 對應版本）：

| Artifact | 產生者 | 消費者 | 版本層級 |
|---|---|---|---|
| `data/profiling/<stem>_categorical.yaml` | `scripts/suggest_categorical_cols.py` | 人工貼回 `parameters_dataset.yaml` | 無（離線輔助）|
| `data/profiling/sampling_overrides_editor.html` / `_export.json` | `scripts/sampling_overrides_editor.py profile` / 瀏覽器 | `scripts/sampling_overrides_editor.py to-yaml` | 無（離線輔助）|
| `feature_table` / `label_table` / `sample_pool`（Hive）| `feature_etl` / `label_etl` / `sample_pool_etl` | `dataset`、`baselines`、`evaluation` | 由上游 snap_date 分區 |
| `preprocessor` / `category_mappings` / `val/test_model_input` | `dataset` | `training` | `base_dataset_version` |
| `train/train_dev_model_input` | `dataset` | `training` | `base` + `train_variant_id` |
| `calibration_model_input` | `dataset`（calibration 啟用）| `training` | `base` + `calibration_variant_id` |
| `data/models/<mv>/{model.txt,best_params,evaluation_results,manifest}` | `training` | `promote_model.py`、`inference`、`evaluation` | `model_version` |
| `training_eval_predictions`（Hive）| `training` | `evaluation --post-training`、`compute_test_mAP_spark` | `model_version` |
| `data/models/best`（symlink）| `scripts/promote_model.py`（手動）| `inference` / `evaluation`（未指定 `--model-version` 時）| 指向某 `model_version` |
| `ranked_predictions` / `score_table`（Hive）| `inference` | `evaluation`（預設）| `model_version` |
| `baseline_metrics` | `baselines` | `evaluation`（報表 baseline 段）| 由 eval snap_date |
| `data/evaluation/<mv>/<snap_date>/report.html` | `evaluation` | 人工 / 監控 | `model_version` |

---

## 4. 設定讀取邏輯（摘要）

`ConfigLoader(conf_dir, env)`（`core/config.py`）：

1. 讀 `conf/base/*.yaml`，再讀 `conf/<env>/*.yaml`。
2. 對每個檔名（stem），用 env 的內容對 base 做 **deep-merge override**（dict 遞迴合併，非 dict 直接取代）。
3. `get_parameters()` 把所有 `parameters.yaml` 與 `parameters_*.yaml` 合併成一包 parameters。
4. `get_catalog_config()` 對 `catalog.yaml` 做 `${...}` runtime placeholder 替換（支援巢狀 key，如 `${hive.db}`、`${base_dataset_version}`）。

> ⚠️ 多個 `parameters_*.yaml` 合併時**沒有保證的穩定優先順序**（程式以 set 走訪 stem）。請避免不同 parameter 檔案出現同名 key；若無法避免，務必確認 deep-merge 結果是你要的。

完整規則（含 placeholder、env overlay 行為）見 [docs/config-and-versioning.md](docs/config-and-versioning.md)。

---

## 5. Schema 與資料契約（重點）

`schema.columns`（`conf/base/parameters.yaml`）定義角色欄位，預設值見 `core/schema.py`：

| 角色 | 預設欄位 |
|---|---|
| `time` | `snap_date` |
| `entity` | `[cust_id]`（永遠 normalize 成 list）|
| `item` | `prod_name` |
| `label` | `label` |
| `score` | `score` |
| `rank` | `rank` |

- `identity_columns` 為**程式推導**：`[time] + entity + [item]`，預設 `[snap_date, cust_id, prod_name]`。
- 進入 dataset pipeline 的 `feature_table`、`label_table`、`sample_pool` 都必須遵守這套欄位命名。
- `sample_pool` 至少要含 `identity_columns`；若 sampling/group/carry 用到 `cust_segment_typ`、`label` 等欄位，這些欄位也必須存在於 `sample_pool`。
- `schema.item`（`prod_name`）必須是 categorical feature，且必須出現在 `schema.categorical_values`。
- `inference.products` 必須與 `schema.categorical_values[<item>]` 為**相同集合**。
- `sample_pool` 的 item 覆蓋率必須**等於**宣告產品集合（雙向集合相等）。
- `label_table` 不能出現未宣告產品。
- train / calibration / val / test 的 snap_date 集合**兩兩不可重疊**。
- `feature_table` 必須涵蓋 dataset 用到的所有 snap_date（train ∪ calibration ∪ val ∪ test）。
- ranking task 中 item 欄位**必須留在 feature columns**（即 `prod_name` 要在 `dataset.prepare_model_input.categorical_columns`），否則模型無法區分同一 customer 下不同 product，HPO mAP 會塌成常數。

這些不變量由 `core/consistency.py`（設定靜態閘 A1–A9）與 dataset pipeline 第一個節點 `validate_data_consistency`（資料閘 B1）強制；違反時 fail-loud。完整清單與錯誤訊息對照見 [docs/config-and-versioning.md](docs/config-and-versioning.md)。

---

## 6. 版本管理（重點）

目前是**多層 hash 版本機制**（`core/versioning.py`），不是單層 `dataset_version`。dataset pipeline 每次依參數重算版本並更新 `latest` symlink；training 產出 versioned model 目錄但**不**自動 promote。

| 版本 | 由什麼決定 | 影響的 artifact |
|---|---|---|
| `base_dataset_version` | 非抽樣 dataset 參數 + canonical schema（含 `categorical_values`）+ feature_table fingerprint（欄位名+型別，**有序**）| preprocessor、category_mappings、preprocessed_feature_table、val/test model_input |
| `train_variant_id` | train 抽樣設定：`sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio` | train / train_dev model_input |
| `calibration_variant_id` | calibration 抽樣設定（僅在啟用 calibration 時）| calibration model_input |
| `model_version` | model-defining training 參數（`training:` block）+ `base_dataset_version` + `train_variant_id` +（選用）`calibration_variant_id` | model.txt、best_params、evaluation_results、manifest |

關鍵規則：

- `training:` block 進 `model_version` hash；其中 `algorithm_params` 的 `verbosity`、`log_period`、`num_threads` **不**影響 `model_version`。
- `spark`、`mlflow`、`cache` 等 ops-only 設定**不**影響任何版本。
- `training.sample_weights` 屬 `training:` block → **改它會 bust `model_version`，但不會改 `train_variant_id`**。
- `dataset.carry_columns` 不是抽樣 key → 改它會 **bust `base_dataset_version`**（parquet schema 變）。
- `sample_group_keys` 同時屬 train 與 calibration 抽樣 → 改它會同時改 `train_variant_id` 與 `calibration_variant_id`，但不改 `base_dataset_version`。
- `manifest.json` 記錄 `version` / `pipeline` / `created_at` / `git_commit` / `parameters` / 各層版本 / `artifacts` 等 lineage。

哪些修改改哪個版本的完整表格見 [docs/config-and-versioning.md](docs/config-and-versioning.md) 與 [docs/change-sop.md](docs/change-sop.md)。

---

## 7. 輔助 Scripts

只列與公司流程相關的 scripts（皆為 standalone Typer / argparse 工具，不屬 production DAG，但屬建模流程一環）。詳細選項與流程見 [docs/change-sop.md](docs/change-sop.md)。

### `scripts/suggest_categorical_cols.py`

```bash
python scripts/suggest_categorical_cols.py ml_recsys.feature_table   # Hive table
python scripts/suggest_categorical_cols.py /path/to/x.parquet        # 或 parquet 路徑
```

掃 Hive table 或 parquet 推測 categorical 欄位：string / bool 直接視為 categorical；低 cardinality numeric（預設 nunique ≤ `--max-cardinality 20`）也建議為 categorical。輸出 YAML snippet 到 `data/profiling/<stem>_categorical.yaml`，**人工檢視後貼進** `conf/base/parameters_dataset.yaml` 的 `categorical_columns`。**用於定義 / 新增 categorical feature 時。** 透過 `spark.table()` / `spark.read.parquet()` 讀**已存在**的表/檔，故掃描目標必須先存在——`feature_etl` 完成後的 `feature_table`，或某個既有上游來源表；不能在來源表尚未產出前執行。

### `scripts/sampling_overrides_editor.py`

```bash
python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool   # 或 parquet 路徑
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
```

`profile`：對 `sample_pool` 中 train snap_dates 的 per-`cust_segment_typ` × `prod_name` 算 positive/negative，依 target neg:pos 與 cold-product 公式給建議值，輸出 self-contained HTML editor。瀏覽器編輯 ratio / weight 後 Export JSON。`to-yaml`：把 JSON 轉成兩段 sparse YAML（會重用一致性 predicate 做 A5 / A9 驗證，未宣告產品 fail loud）：

- `dataset.sample_ratio_overrides` → 貼回 `conf/base/parameters_dataset.yaml`；key 格式 `"<cust_segment_typ>|<prod_name>|0"`（label 分量固定 `0`，代表 downsample 負例）。
- `training.sample_weights` → 貼回 `conf/base/parameters_training.yaml`；key 格式 `"<cust_segment_typ>|<prod_name>"`。

**用於調整 downsampling ratio / 冷門產品 sample weight 時。** 版本影響：`sample_ratio_overrides` 改 `train_variant_id`（需重跑 dataset）；`sample_weights` 改 `model_version`（不需重跑 dataset）。

### `scripts/promote_model.py`

```bash
python scripts/promote_model.py <model_version>      # 指定版本
python scripts/promote_model.py                      # 自動選 overall_map 最高
python scripts/promote_model.py --dry-run            # 只列各版本比較，不 promote
```

手動建立 / 更新 `data/models/best` symlink。promote 前檢查必要 artifact（`model.txt`、`best_params.json`、`evaluation_results.json`），缺則報錯。自動選版時依各版本 `evaluation_results.json` 的 `overall_map` 取最高。**training 完成後必須執行此步，inference 預設模型才會切換。**

---

## 8. 常見錯誤（速查）

| 症狀 | 多半原因 |
|---|---|
| 找不到 `models/best` / inference 報 best symlink 不存在 | training 後尚未 `scripts/promote_model.py` promote |
| `feature_table missing required snap_dates` | feature_table 缺 dataset 用到的某個 snap_date |
| HPO mAP 每個 trial 都一樣（塌成常數）| `prod_name` 沒列入 `categorical_columns`，item 沒進 feature |
| `inference.products disagrees with schema.categorical_values` | 兩處產品清單不一致 |
| `Data consistency check failed`（sample_pool / label item）| sample_pool item 集合 ≠ 宣告產品，或 label 出現未宣告產品 |
| dataset 報缺 identity / group / carry 欄位 | sample_pool 沒帶 `cust_segment_typ` / `label` 等欄位 |
| `restart_from='...' not found in tables` | `--restart-from` 表名拼錯，須與 ETL YAML `tables[].name` 一致 |
| 訓練 cache 行為異常 / partial cache | `cache.root` 不可寫，或上次 run 中斷留下無 `_SUCCESS` 的目錄（會自動清除重建）|

完整排查步驟見 [docs/pipeline-runbook.md](docs/pipeline-runbook.md)。
