# Pipeline Runbook

各 pipeline 的實際行為、操作方式與錯誤排查。皆以 `src/recsys_tfb/pipelines/` 與 `src/recsys_tfb/__main__.py` 為準。指令格式一律 `python -m recsys_tfb <command> [--options]`。

---

## 1. Source ETL（`feature_etl` / `label_etl` / `sample_pool_etl`）

> Source ETL 的 SQL 目前是開發用假 SQL；本文件只描述框架與操作，不解釋 `conf/sql/etl/**/*.sql` 業務邏輯。

### 行為（`pipelines/source_etl/sql_runner.py`）

- **沒有 `source_etl` 單一指令**；三個指令各自由對應 `conf/base/parameters_<stage>.yaml` 的 `<stage>.tables` list 驅動，**list 順序即執行順序**。
- 每個 `--target-dates` 的日期會綁定到 SQL 變數 `${target_date}` 與 Hive 分區欄 `snap_date`，逐一迭代。
- 對每個 table：`SQLRenderer` 渲染 SQL → 用 `SELECT * FROM (...) LIMIT 0` 推斷欄位 → 組 aligned SELECT → 表不存在走 **Hive CTAS**、已存在走 **INSERT OVERWRITE** → 跑 output checks → 寫 audit。
- `depends_on` 在 `SQLRunner` 初始化時驗證「相依表必須在 list 中更早出現」；**這是順序驗證，不是完整 DAG scheduler**（不會自動補跑相依表）。
- `--restart-from <table>`：跳過 list 中該表**之前**的所有表，從指定表續跑；指定的表名不在 `tables` list 會 raise `ValueError`（表名須與 YAML `tables[].name` 完全一致）。
- `--target-dates` 未給且 config 也沒 `target_dates` → 報錯結束。

### dry-run / rendered SQL

- `dry_run` 預設值：`etl_config.get("dry_run", env == "local")`。即 `--env production` 時預設**實跑**（除非 YAML 顯式設 `dry_run`，如 `parameters_sample_pool_etl.yaml` 設 `dry_run: false`）。
- dry-run 只印出組好的 INSERT OVERWRITE（**不**檢查表是否存在、跳過分區 CAST），不執行 Spark；不寫 audit。
- 若 `etl_config.rendered_sql_dir` 有設，會把渲染後 SQL 寫到 `<rendered_sql_dir>/<run_id>/<snap_date>/<table>.sql`（dry-run 與實跑皆會寫）。

### 檢查（`pipelines/source_etl/checks.py`）

- `source_checks`（YAML 的 `source_checks`，跑在 ETL 執行前；dry-run 略過）：partition 是否存在、最小 row count、schema drift（缺欄/型別不符 → fail；新增欄看 `allow_new_columns`）。某 snap_date source check 失敗 → 跳過該 snap_date、繼續其餘日期。
- `quality_checks`（YAML 每個 table 的 `quality_checks`，跑在該表寫入後）：
  - schema contract：當 table 宣告 `primary_key` 時**無條件**檢查這些欄是否存在；
  - `min_row_count`、`max_duplicate_key_ratio`（需 `primary_key`）、`max_null_ratio`。
- output check 失敗 → 該 snap_date 標記 failed、停止該 snap_date 後續表；SQL/Spark 執行錯誤 → 寫完當次 audit summary 後中止整個 run。
- audit 寫入 Hive `<target_db>.etl_audit_log`（`AuditRecord`：run_id / snap_date / table / status / row_count / duration / error）。

`sample_pool_etl` 依賴 `feature_etl` / `label_etl` 的產出；目前以 list 順序與 `depends_on` 文件化，跨 DAG 新鮮度未強制（可日後用 `source_checks` 補）。

---

## 2. Dataset pipeline（`python -m recsys_tfb dataset --env production`）

`dataset` 每次都從參數重算 `base_dataset_version` / `train_variant_id`（+ calibration），跑完更新對應 `latest` symlink（版本機制見 [config-and-versioning.md §3](config-and-versioning.md)）。節點順序（`pipelines/dataset/pipeline.py`）：

1. **`validate_data_consistency`**（第一個節點，資料閘 B1，side-effect）：sample_pool item 集合與宣告產品雙向相等、label_table 無未知產品；違反 raise `DataConsistencyError`，**在任何抽樣前失敗**。
2. **`select_train_keys`** → `sample_keys`：先 `validate_date_splits`（train/cal/val/test snap_date 兩兩不重疊）；依 `train_snap_dates` 從 `sample_pool` 過濾，用 `sample_group_keys` 做**確定性分層抽樣**（`crc32(identity_key | site | seed) % HASH_BUCKETS < ratio * HASH_BUCKETS`），支援 `sample_ratio_overrides`（key 為 `sample_group_keys` 值以 `|` 串接）。`carry_columns` 一併帶出。
3. **`split_train_keys`** → `train_keys` / `train_dev_keys`：對 `cust_id` 做 hash bucket，依 `train_dev_ratio` 切分；**同一 `cust_id` 的所有 row 必在同一邊**（避免客戶跨 split 洩漏）。
4. **`select_val_keys`** → `val_keys`：`val_snap_dates` 全量；`val_sample_ratio < 1` 時對 `cust_id` 做確定性抽樣。
5. **`select_test_keys`** → `test_keys`：`test_snap_dates` 全量，不抽樣。
6. **`fit_preprocessor_metadata`** → `preprocessor` / `category_mappings`：category mapping **只用 train window** fit（防 val/test leakage）；identity categorical（如 `prod_name`，feature_table 沒有）取自 `schema.categorical_values`；feature_table 缺任一 train snap_date → `ValueError`。
7. **`apply_preprocessor_to_features`** → `preprocessed_feature_table`：對**所有** dataset snap_date（train ∪ cal ∪ val ∪ test）套用一次 encoding；缺 snap_date → `ValueError`。
8. **`build_model_input`**（train / train_dev / val / test，calibration 啟用時加一個）：keys 左 join `label_table`（join miss → label 補 `0`）→ 左 join `preprocessed_feature_table` → 選 identity + label + feature + carry 欄 → decimal 欄 cast double。

### 目前前處理做的事

- drop 不需要的欄（`drop_columns`）。
- 推導 `feature_columns`（identity categorical 在前，接 feature_table 欄位扣除 drop / 非 categorical identity / label）。
- categorical encoding（依 train window fit 出的 mapping；**未 mapping 到的值 → `-1`**，與 null 同碼）。
- `DecimalType` 特徵欄 cast 成 `double`（identity / label 不 cast）。
- **空值目前不做 imputation**；label join miss 補 `0`。
- ranking 不變量：`schema.item`（`prod_name`）必須落在 `feature_columns`，否則 `DataConsistencyError`。

### 未來若要加標準化 / normalization

- 應在 fit 節點（`fit_preprocessor_metadata` 類比）產生統計量（mean/std 等），存進 preprocessor metadata。
- apply 時對 train/val/test/calibration 各 split **套用同一組統計量**（統計量只能由 train window fit，避免 leakage）。
- 需檢視 `build_model_input` / model input 節點的 input 與 preprocessor metadata 結構是否要調整。
- 注意是否需要 bust `base_dataset_version`：新增前處理行為通常會改 dataset 參數或 preprocessor 結構，應確認版本 hash 有跟著變（避免 cache 撞舊資料）。

---

## 3. Training pipeline（`python -m recsys_tfb training --env production`）

節點（`pipelines/training/pipeline.py` + `nodes.py`）：

1. **cache 節點**（train / train_dev / val / test，calibration 選用）：把對應 Hive model_input 分區子樹 `copyToLocal` 到 driver-local parquet（路徑由 `cache.root` + 版本層級組成）。已有 `_SUCCESS` → cache hit 直接用；目錄存在但無 `_SUCCESS`（partial）→ 清除重建。
2. **`prepare_lgb_train_inputs`**：把 train / train_dev 物化成 `lgb.Dataset` binary（跨 trial 不重新分箱）。
3. **`tune_hyperparameters`**：Optuna `TPESampler`，`n_trials` 次；`search_space` 為宣告式 ParamSpec list；train_dev 用於 **early stopping**，val 用於 **HPO mAP 評估**（`compute_mean_ap`，只算含正例的 query group）。回傳 `best_params`、`best_iteration`、HPO 最佳 trial 的 model。
4. **`finalize_model`**：依 `training.final_model_strategy`：
   - `hpo_best`（預設）：直接用 HPO 最佳 trial 的 adapter。
   - `refit_on_full`：用 `train + train_dev` 串接重訓，`num_iterations = best_iteration`、無 early stopping（支援 ranking / binary objective，含 sample weight）。
5. **`calibrate_model`**（`training.calibration.enabled=true` 時）：用 calibration split fit `CalibratedModelAdapter`，方法由 `training.calibration.method`（如 `sigmoid`/`isotonic`）控制。
6. **`predict_and_write_test_predictions`**：對 test parquet 逐 `(snap_date, prod_name)` 分區預測；Pass 0 先掃 label 找出每個 snap_date「至少一個正例」的 `cust_id` 集合，**只對這些 customer 評分**（無正例 customer 對 mAP 無貢獻）；寫入 Hive `training_eval_predictions`（`cust_id, score, score_uncalibrated, label`，分區 `snap_date/prod_name/model_version`）。
7. **`compute_test_mAP_spark`** → `evaluation_results.json`：Spark-native 計算 `overall_map`、`per_item_map_attr`、`n_queries` / `n_excluded_queries`；若有 calibration（`score != score_uncalibrated`）另附 `uncalibrated` 子段與 `calibration_method`。
8. **`log_experiment`**：寫 MLflow（params / metrics / model）。

### Sample weights（`training.sample_weights`）

- key 固定 `"<cust_segment_typ>|<prod_name>"`，value 為 LightGBM sample weight（建議 ≥ 1.0，只 boost；稀疏，只列 ≠ 1.0 的組），主要用於冷門產品 boost。
- **只作用於 train / train_dev**（透過 `extract_Xy(..., with_weights=True)`），val / calibration / evaluation **不**加權。
- product 分量由一致性閘 **A9**（`weight_unknown_items`）檢查，必須存在於 `schema.categorical_values[item]`。
- 改 `sample_weights` 會 bust `model_version`，但**不改 `train_variant_id`**（屬 `training:` block，非 dataset 抽樣設定；故不需重產 dataset）。

### 產出與 promote

- 產出：`data/models/<model_version>/` 下 `model.txt`（+ calibrator）、`best_params.json`、`evaluation_results.json`、`manifest.json`、`parameters_training.json`，並另寫 `training_eval_predictions`（供 evaluation `--post-training` 重用）。
- **training 完成不代表 production inference 已切換**：`symlink_target=None`，不自動 promote。必須跑 `scripts/promote_model.py`。

---

## 4. Inference pipeline（`python -m recsys_tfb inference --env production`）

- 未指定 `--model-version` → 透過 `data/models/best` symlink 解析（CLI 同時把 catalog `model` filepath 的 hash 換成 `best`）；指定但目錄不存在 → 報錯。
- 各層 dataset 版本從該 model 的 `manifest.json` 取得（缺欄位回退 `latest`）。
- 節點（`pipelines/inference/`）：
  1. `build_scoring_dataset`：`feature_table` 過濾 `inference.snap_dates` 取 customers，與 `inference.products` **cross join**，左 join feature_table。
  2. `apply_preprocessor`：套用 training 時保存的 preprocessor。
  3. `predict_scores`：依 `(snap_date, prod_name)` chunk 預測（控記憶體）；`inference.use_calibration=false` 且 model 為 calibrated 時改用 uncalibrated 分數；注入 `model_version` 欄。
  4. `rank_predictions`：在 `[time] + entity` group 內依 `score` desc 用 `row_number` 排 `rank`。
  5. `validate_predictions`：6 項檢查 — row_count_match、score_range（[0,1]）、no_missing、completeness（每 group 恰 `len(inference.products)` 筆）、rank_consistency（1..N 且隨 rank 遞減）、no_duplicates（identity 去重）；任一失敗 raise `ValidationError`。
- 寫出 `score_table` 與 `ranked_predictions`（Hive，分區 `snap_date/prod_name/model_version`），更新 `data/inference/latest`。

---

## 5. Evaluation

### `evaluation`（`python -m recsys_tfb evaluation --env production [--model-version X] [--post-training]`）

- 可針對 `best`（不帶 `--model-version`）或指定 `--model-version`。
- 預測來源：
  - 預設（無 `--post-training`，月度監控）：讀 `ranked_predictions`（inference 產出）。
  - `--post-training`：讀 `training_eval_predictions`（training 產出）；該表不存 `rank`，`prepare_eval_data` 會用 `rank_within_query` 即時補上 `rank` 欄。
- `prepare_eval_data` 把預測過濾到解析出的 `model_version`，與 `label_table`（含 `evaluation.segment_sources` 外部分群來源 left join）依 identity 欄 join。
- `compute_metrics` → `evaluation_metrics`；`generate_report` → `data/evaluation/<model_version>/<snap_date>/report.html`。
- `compute_baseline_metrics`：popularity baseline —— 把 `eval_predictions` 每列的 `score` 換成該產品在 `evaluation.snap_date` 之前 `evaluation.baseline.lookback_months` 窗口的申購數（`label_table` 的 `sum(label)`；無歷史則 fallback 用全部、warn 可能 leakage），再算 slim 指標（`overall` + `per_item`）餵報表 baseline 比較段；`report.sections.baseline=false` 時回傳 `None`、該段略過。
- 指標定義、輸出格式（`overall_map` / `per_item_map_attr` / `@K` / `_attr@K`）、報表分段與設定見 [metrics.md](metrics.md)；概念語意見 [metrics_concept_map.html](metrics_concept_map.html)。

---

## 6. 錯誤排查

| 症狀 / 訊息 | 原因 | 處理 |
|---|---|---|
| `No 'best' symlink found in .../models` | training 後尚未 promote | 跑 `python scripts/promote_model.py <model_version>` |
| `feature_table missing required (train_)snap_dates: [...]` | feature_table 缺 dataset 用到的 snap_date | 補跑 `feature_etl` 該 snap_date，或修正 `*_snap_dates` 設定 |
| `Config consistency check failed (...)` | A1–A9 設定矛盾（exit 1）| 依訊息逐項修正 `parameters*.yaml`，可一次修完所有列出的問題 |
| `Data consistency check failed (...)` | sample_pool item 集合 ≠ 宣告產品，或 label 出現未宣告產品 | 對齊 `schema.categorical_values[item]` 與 sample_pool/label SQL |
| `schema.item='prod_name' is missing from derived feature_columns` | `prod_name` 沒列入 `dataset.prepare_model_input.categorical_columns` | 把 item 欄加回 `categorical_columns` |
| `inference.products disagrees with schema.categorical_values[item]` | 兩處產品清單不一致（A4）| 同步 `inference.products` 與 `schema.categorical_values` |
| dataset / build_model_input 報缺欄 | sample_pool 沒帶 identity / `sample_group_keys` / `carry_columns` / `label` | 修正 `sample_pool` 來源使其包含這些欄位 |
| `restart_from='...' not found in tables: [...]` | `--restart-from` 表名與 ETL YAML `tables[].name` 不符 | 用 YAML 中正確表名 |
| `Partial cache detected at ..., clearing before retry` | 上次 training cache 中斷無 `_SUCCESS` | 屬正常自我修復（會清除重建）；若反覆出現檢查 `cache.root` 是否可寫 / 空間 |
| `<dataset> input must be a Spark DataFrame` | cache 節點拿到非 Spark DataFrame | 確認 dataset pipeline 先成功產出對應 Hive model_input |
| inference `ValidationError: N sanity check(s) failed: ...` | 預測輸出未通過健全性檢查 | 看 `failures` 內容（completeness/score_range/rank 等）回推 scoring/preprocessor/model 問題 |
| evaluation `parameters['model_version'] missing` | 未經 CLI 解析直接跑 pipeline | 一律用 `python -m recsys_tfb evaluation ...`（CLI 會解析 model_version）|
