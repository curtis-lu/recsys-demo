# 設定讀取、Schema 資料契約、版本管理

本文件補充 [README.md](../README.md) §4–§6 的細節，皆以 `src/recsys_tfb/core/config.py`、`core/schema.py`、`core/consistency.py`、`core/versioning.py`、`src/recsys_tfb/__main__.py` 為準。

---

## 1. 設定讀取（`core/config.py`）

### 1.1 載入與合併

`ConfigLoader(conf_dir, env)`：

1. `conf/base/` 下所有 `*.yaml` 依檔名（stem）載入。
2. `conf/<env>/` 下所有 `*.yaml` 依 stem 載入。
3. 對每個 stem，`self._config[stem] = _deep_merge(base, env)`：
   - dict 對 dict → 遞迴合併；
   - 其餘型別（list、scalar）→ env 值**整個取代** base 值（list 不會 append/merge）。
4. env 目錄不存在時，`_load_yaml_dir` 回傳空 dict，等同只用 base。

### 1.2 取參數

- `get_parameters()`：把所有 stem 為 `parameters` 或 `parameters_*` 的設定，逐一 `_deep_merge` 成一包 dict。
  - ⚠️ 走訪順序來自 `self._config`，其 key 集合由 `set(base) | set(env)` 建立；**跨檔案的合併順序未保證穩定**。不同 `parameters_*.yaml` 請避免同名 top-level key；無法避免時須自行確認 deep-merge 結果。
- `get_parameters_by_name("parameters_dataset")`：取單一檔案（base + env overlay）的 dict；找不到 raise `KeyError`。CLI 用它取各 pipeline 專屬參數（如 `parameters_dataset`、`parameters_training`）。

### 1.3 Catalog placeholder 替換

`get_catalog_config(runtime_params)`：對 `catalog.yaml` 所有字串值做 `${key}` 替換。

- `runtime_params` 會被 `_flatten_params` 攤平成 dotted key（`{"hive": {"db": "x"}}` → `hive.db=x`），所以支援 `${hive.db}`。
- CLI 在執行各 pipeline 前注入的 runtime key 至少有：`base_dataset_version`、`train_variant_id`、`calibration_variant_id`、`model_version`、`snap_date`（不適用的層級填 `__none__` 佔位）。
- inference 未指定 `--model-version` 時，CLI 會把 `catalog["model"]["filepath"]` 中的 model hash 換成 `best`，讓 model artifact 走 `data/models/best` symlink。

### 1.4 spark 設定

`__main__._load_spark_config` 取 `parameters.yaml` 與 `parameters_<pipeline>.yaml` 的 `spark:` block（pipeline 覆蓋 base），再跑 `${env.*}` 與 `${vdclient.*}` placeholder 解析（無法解析時 drop）。`spark` block 屬 ops-only，不影響任何版本 hash。

---

## 2. Schema 與資料契約（`core/schema.py` + `core/consistency.py`）

### 2.1 角色欄位與推導

`get_schema(parameters)` 從 `parameters["schema"]["columns"]` 取角色欄位（缺項回退預設）：

| key | 預設 | 型別規則 |
|---|---|---|
| `time` | `snap_date` | 非空字串 |
| `entity` | `[cust_id]` | 字串或非空 list[字串]，永遠 normalize 成 list |
| `item` | `prod_name` | 非空字串 |
| `label` | `label` | 非空字串 |
| `score` | `score` | 非空字串 |
| `rank` | `rank` | 非空字串 |

- `identity_columns` = `[time] + entity + [item]`（推導，不可有重複）。
- `categorical_values` 來自 `schema.categorical_values`（mapping：欄位 → 非空 list）。
- `get_schema_for_hash()` 用於版本 hash：排除推導的 `identity_columns`，但**包含 `categorical_values`**（所以新增產品會 bust `base_dataset_version`）。

### 2.2 設定靜態一致性閘（Layer 1，`validate_config_consistency`）

CLI entry（`__main__._load_config_and_setup`）會跑 `validate_schema_config` 與 `validate_config_consistency`，**collect-all 後一次 raise**（`ConfigConsistencyError`，`ValueError` 子類，exit 1）。各不變量（單一定義在 `core/consistency.py`）：

| 代號 | 規則 | 觸發訊息關鍵字 |
|---|---|---|
| A1 | 同一欄位同時在 `drop_columns` 與 `categorical_columns`（角色矛盾）| declared in BOTH ... drop_columns and categorical_columns |
| A2 | `categorical_columns` 顯式設定但漏掉 `schema.item` | schema.item=... is missing from ... categorical_columns |
| A3 | item 是宣告 categorical，但 `schema.categorical_values` 沒有它的清單 | has no schema.categorical_values |
| A4 | `inference.products` ≠ `schema.categorical_values[item]`（集合不等）| inference.products disagrees with schema.categorical_values |
| A5 | `sample_ratio_overrides` 的 item 分量不在宣告產品 | sample_ratio_overrides references item value(s) |
| A6 | YAML / SQL / 合成資料的硬編產品清單不一致（由 lint 測試強制，非執行期）| — |
| A7 | ranking objective（`lambdarank`/`rank_xendcg`）配非 ranking metric 或 entity 為空 | is a ranking objective but ... |
| A8 | `training.search_space` 宣告式 schema 不合法（型別、low<high、log/step 互斥等）| search_space ... |
| A9 | `training.sample_weights` 的 product 分量不在宣告產品 | training.sample_weights references product value(s) |

### 2.3 資料一致性閘（Layer 2 B1，`validate_data_consistency`）

dataset pipeline 的**第一個節點**（side-effect、`outputs=None`），在任何抽樣/前處理前對 dataset 用到的 snap_date 視窗做 `distinct(item)` 檢查（`core/consistency.py::item_coverage_errors`），違反 raise `DataConsistencyError`：

- `sample_pool` 的 item 集合與宣告產品 `schema.categorical_values[item]` 必須**雙向相等**：
  - 資料有、config 沒宣告 → 會被 encode 成 `-1`（與 null 同碼）汙染訓練/評分。
  - config 宣告、sample_pool 從未產生 → 永遠無法被評分/推薦。
- `label_table`：只擋「資料有、config 沒宣告」的未知產品（宣告了但 label 無正例屬 B3，目前刻意不報）。

> A2/A3 另有 `preprocessing/_spark.py` 的執行期後備 guard（identity-cat guard、item-in-feature-columns guard），訊息同源。

---

## 3. 版本管理（`core/versioning.py`）

所有 hash 為 `sha256(canonical)[:8]`；canonical 為 `yaml.dump(payload, sort_keys=True)`。

### 3.1 四個版本如何算

- **`compute_feature_table_fingerprint(columns)`**：對**有序**的 `(欄位名, 型別)` 序列 hash。欄位順序會傳遞到 `preprocessing` 的 `feature_columns`，進而決定 LightGBM 特徵順序，所以**重排欄位也會 bust**。CLI（`dataset` 指令）從 `spark.table(feature_table).schema.fields` 即時取得實際 schema 算 fingerprint。
- **`compute_base_dataset_version(params_dataset, schema_hash, fingerprint)`**：對 `{dataset: 去掉所有抽樣 key 的 dataset 參數, schema: schema_hash, feature_table_fingerprint}` hash。被剝除的抽樣 key（`ALL_SAMPLING_KEYS`）：`sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio`、`calibration_sample_ratio`、`calibration_sample_ratio_overrides`。
- **`compute_train_variant_id(params_dataset)`**：只 hash `dataset` 下 `TRAIN_SAMPLING_KEYS = {sample_ratio, sample_ratio_overrides, sample_group_keys, train_dev_ratio}` 子集。
- **`compute_calibration_variant_id(params_dataset)`**：只 hash `CALIBRATION_SAMPLING_KEYS = {calibration_sample_ratio, calibration_sample_ratio_overrides, sample_group_keys}` 子集（僅 calibration 啟用時計算）。
- **`compute_model_version(params_training, base_v, train_v, cal_v?)`**：對 `_model_version_payload`（= `training:` block，去掉 `algorithm_params` 中 `verbosity`/`log_period`/`num_threads`）的 canonical 字串，串接 `base_v` + `train_v` +（選用）`cal_v` 後 hash。`spark`/`mlflow`/`cache` 因結構上只取 `training:` 而被排除。`training:` 下新增的 key 預設會進 hash（安全的過度失效，絕不靜默碰撞）。

### 3.2 哪些修改改哪個版本

| 修改 | base_dataset_version | train_variant_id | calibration_variant_id | model_version | 不變 |
|---|---|---|---|---|---|
| `dataset` 非抽樣參數（`train/val/test/calibration_snap_dates`、`prepare_model_input.drop_columns` / `categorical_columns`、`carry_columns`、`val_sample_ratio` 等）| ✅ | — | — | ✅（透過 base 傳遞）| |
| `schema.columns` / `schema.categorical_values`（含新增/移除 product）| ✅ | — | — | ✅（傳遞）| |
| `feature_table` 實體 schema（欄位增刪、改型、**改順序**）| ✅ | — | — | ✅（傳遞）| |
| `dataset.sample_ratio` / `sample_ratio_overrides` / `train_dev_ratio` | — | ✅ | — | ✅（傳遞）| |
| `dataset.sample_group_keys` | — | ✅ | ✅ | ✅ | |
| `dataset.calibration_sample_ratio` / `calibration_sample_ratio_overrides` | — | — | ✅ | ✅（calibration 啟用時，傳遞）| |
| `training:` block（`algorithm_params`*、`n_trials`、`num_iterations`、`early_stopping_rounds`、`final_model_strategy`、`search_space`、`calibration.{enabled,method}`、`sample_weights`）| — | — | — | ✅ | |
| `training.algorithm_params.verbosity` / `log_period` / `num_threads` | — | — | — | — | ✅ 不應改版本 |
| `spark` / `mlflow` / `cache` | — | — | — | — | ✅ 不應改版本 |

\* `algorithm_params` 除 `verbosity`/`log_period`/`num_threads` 外都進 `model_version`。

> 注意：`model_version` 是 `training:` payload 串接 `base_v`+`train_v`(+`cal_v`)。因此任何改到 `base_dataset_version` / `train_variant_id` / `calibration_variant_id` 的修改都會**間接改 `model_version`**（即使 `training:` 沒動）。

### 3.3 目錄、symlink 與 manifest

- dataset pipeline 結束後（在 `__main__.py` post-run，非 pipeline node）寫 manifest 並更新 symlink：
  - `data/dataset/<base_v>/`（base manifest）+ `data/dataset/latest -> <base_v>`
  - `data/dataset/<base_v>/train_variants/<train_v>/` + `train_variants/latest -> <train_v>`
  - 啟用 calibration 時：`.../calibration_variants/<cal_v>/` + `calibration_variants/latest`
- training 寫 `data/models/<model_version>/`（manifest + `parameters_training.json`），**`symlink_target=None`，不自動 promote**。
- inference 寫 `data/inference/<model_version>/<snap_date>/` + 更新 `data/inference/latest`。
- training/inference/evaluation 預設透過 `latest` symlink 或 model manifest 解析 `base_dataset_version` / `train_variant_id` / `calibration_variant_id`：
  - `training`：base 用 `data/dataset/latest`，variant 用 `<base>/train_variants/latest`、`calibration_variants/latest`（可被 `--base-dataset-version` / `--train-variant` / `--calibration-variant` 覆蓋）。
  - `inference` / `evaluation`：讀指定（或 `best`）model 的 `manifest.json` 取得各層版本，缺欄位才回退 `latest`。
- `manifest.json` 欄位：`version`、`pipeline`、`created_at`(UTC ISO)、`git_commit`(short HEAD)、`parameters`、`run_id`，以及視 pipeline 而定的 `base_dataset_version` / `train_variant_id` / `calibration_variant_id` / `model_version` / `parent_version` / `variant_kind` / `feature_table_fingerprint` / `artifacts`。
