# 修改情境 SOP

常見兩類異動：

- **同一推薦問題做實驗**：抽樣、sample weight、HPO、calibration、加 feature。
- **套用到其他排序問題**：改 schema 欄位命名 / entity / item / 產品清單與上游。

版本影響的精確定義見 [config-and-versioning.md §3.2](config-and-versioning.md)。下表「重跑」欄假設要讓改動實際生效所需的最小 pipeline 序列；凡 `model_version` 改變，最後都要重新 `promote_model.py` 才會切換 inference 預設模型。

---

## A. 同一推薦問題做實驗

| 異動 | 要改哪些檔 | 重跑哪些 pipeline | 變動版本 | 常見注意事項 |
|---|---|---|---|---|
| **增加一般 feature** | 上游 feature table / `conf/sql/etl/feature/*.sql`；視情況 `conf/base/parameters_dataset.yaml` 的 `drop_columns`（不要的欄）| `feature_etl` → `dataset` → `training` → `promote` → `inference` | `base_dataset_version`（feature_table fingerprint 變）→ 連帶 `model_version` | 新欄會進 `feature_columns`；確認不需要的欄有列入 `drop_columns`；feature_table 須涵蓋所有 dataset snap_date |
| **增加 categorical feature** | 同上，並把欄名加入 `parameters_dataset.yaml` 的 `prepare_model_input.categorical_columns`；可先用 `scripts/suggest_categorical_cols.py` 掃表產生建議 | `feature_etl` → `dataset` → `training` → `promote` → `inference` | `base_dataset_version` → `model_version` | 不可同時出現在 `drop_columns` 與 `categorical_columns`（A1）；mapping 由 train window fit，未見值 encode 成 `-1` |
| **修改 train sampling ratio / overrides** | `parameters_dataset.yaml` 的 `dataset.sample_ratio` / `sample_ratio_overrides` | `dataset` → `training` → `promote` → `inference` | `train_variant_id` → `model_version`（`base_dataset_version` 不變）| overrides key 為 `sample_group_keys` 值以 `\|` 串接；item 分量須在 `schema.categorical_values[item]`（A5）|
| **用 editor 調 downsampling / cold-product weight** | 見下方「sampling overrides editor 流程」 | 視改到的部分（見下）| 見下 | `to-yaml` 會跑 A5/A9 驗證，未宣告產品 fail loud |
| **修改 `training.sample_weights`** | `conf/base/parameters_training.yaml` 的 `training.sample_weights` | `training` → `promote` → `inference`（**不需重跑 dataset**）| `model_version`（**不改 `train_variant_id`**）| key 為 `"<cust_segment_typ>\|<prod_name>"`，只 boost train/train_dev；product 分量受 A9 檢查；`cust_segment_typ` 須在 sample_pool 且列入 `dataset.carry_columns` |
| **修改 calibration sampling** | `parameters_dataset.yaml` 的 `calibration_sample_ratio` / `calibration_sample_ratio_overrides`（需 `dataset.enable_calibration: true`）| `dataset` → `training` → `promote` → `inference` | `calibration_variant_id` → `model_version` | calibration 須在 dataset 與 training 兩邊都啟用（`dataset.enable_calibration` 與 `training.calibration.enabled`）|
| **修改 training 超參 / search space** | `parameters_training.yaml` 的 `training.search_space` / `algorithm_params` / `n_trials` / `num_iterations` / `final_model_strategy` 等 | `training` → `promote` → `inference` | `model_version`（dataset 版本不變）| `search_space` 為 ParamSpec list，受 A8 schema 驗證；ranking objective 須配 ranking metric（A7）；改 `verbosity`/`log_period`/`num_threads` 不會改版本 |
| **修改 inference snap_dates / products** | `conf/base/parameters_inference.yaml` 的 `inference.snap_dates` / `inference.products` | `inference`（→ `evaluation`）| 無（inference 不算版本）| `inference.products` 必須與 `schema.categorical_values[item]` 為相同集合（A4）；改產品集合屬「增/減 product」見 §B |
| **修改 source ETL YAML 或 SQL template** | `conf/base/parameters_<stage>.yaml` 的 `tables` / `source_checks` / `quality_checks`；`conf/sql/etl/**/*.sql` | 對應 `*_etl` →（若影響 feature_table schema）`dataset` → `training` → `promote` → `inference` | 若 feature_table schema（欄名/型別/順序）變 → `base_dataset_version` → `model_version`；否則無 | `depends_on` 只做順序驗證；`--restart-from` 表名須與 `tables[].name` 一致 |

### sampling overrides editor 流程（`scripts/sampling_overrides_editor.py`）

standalone Typer 工具，不屬 production DAG，作為調整 downsampling / cold-product weighting 的輔助：

```bash
# 1. profile：讀 Hive table 或 parquet，對 train snap_dates 的
#    per-cust_segment_typ × prod_name 算 positive/negative 數，
#    依 target neg:pos 與 cold-weight 公式給建議值，輸出 HTML editor
python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
#   -> data/profiling/sampling_overrides_editor.html

# 2. 在瀏覽器開啟，調整 ratio / weight，Export JSON

# 3. to-yaml：把 JSON 轉成兩段 sparse YAML（會跑 A5/A9 驗證）
python scripts/sampling_overrides_editor.py to-yaml \
    data/profiling/sampling_overrides_export.json
```

- 產出 `dataset.sample_ratio_overrides`（貼到 `conf/base/parameters_dataset.yaml`，key 格式 `"<cust_segment_typ>|<prod_name>|0"`，label 分量固定 `0` 表示 downsample 負例）。
- 產出 `training.sample_weights`（貼到 `conf/base/parameters_training.yaml`，key 格式 `"<cust_segment_typ>|<prod_name>"`）。
- 影響版本不同：改 `sample_ratio_overrides` → `train_variant_id`（需重跑 dataset）；改 `sample_weights` → `model_version`（不需重跑 dataset）。
- `to-yaml` 重用一致性 predicate 做 A5/A9，export 參照未宣告 product 會 fail loud。

### suggest_categorical_cols（`scripts/suggest_categorical_cols.py`）

```bash
python scripts/suggest_categorical_cols.py ml_recsys.feature_table
python scripts/suggest_categorical_cols.py /path/to/x.parquet
#   -> data/profiling/<stem>_categorical.yaml
```

- 可掃 Hive table 或 parquet。string / bool 直接視為 categorical；低 cardinality numeric（預設 nunique ≤ `--max-cardinality 20`）也建議為 categorical。
- 輸出為可貼進 `parameters_dataset.yaml` 的 `categorical_columns` YAML snippet（仍須人工審視）。

### promote_model（`scripts/promote_model.py`）

```bash
python scripts/promote_model.py <model_version>      # 指定版本
python scripts/promote_model.py                      # 自動選 overall_map 最高
python scripts/promote_model.py --dry-run            # 只列各版本比較，不 promote
python scripts/promote_model.py --models-dir /path   # 自訂 models 目錄（預設 data/models）
```

- 手動建立 / 更新 `data/models/best` symlink（舊式 `best/` 目錄會被移除改成 symlink）。
- promote 前檢查必要 artifact：`model.txt`、`best_params.json`、`evaluation_results.json`；缺則報錯不 promote。
- 自動選版時依各版本 `evaluation_results.json` 的 `overall_map` 取最高。

---

## B. 套用到其他排序問題

| 異動 | 要改哪些檔 | 重跑哪些 pipeline | 變動版本 | 常見注意事項 |
|---|---|---|---|---|
| **修改 schema column name**（如 `snap_date`→`obs_date`、`cust_id`→`acct_id`、`prod_name`→`channel`）| `conf/base/parameters.yaml` 的 `schema.columns`；連帶 `parameters_dataset.yaml` 的 `drop_columns`/`categorical_columns`、`sample_group_keys`、`carry_columns`、`inference.products`、上游 ETL SQL 產出的欄名 | 全鏈：`*_etl` → `dataset` → `training` → `promote` → `inference` → `evaluation`| `base_dataset_version` → `model_version` | `identity_columns` = `[time]+entity+[item]` 由 schema 推導，不可重複；所有 pipeline 都讀 `get_schema()`，但 ETL SQL / 來源表欄名需自行對齊 |
| **修改 entity / item 定義**（換成「客戶 × 通路」等）| `schema.columns.entity` / `item`；`schema.categorical_values[<new item>]`；`inference.products`；sample_pool/label 來源邏輯 | 全鏈 | `base_dataset_version` → `model_version` | item 必須是 categorical 且在 `categorical_values`（A2/A3）；item 必須留在 feature columns（ranking 不變量）；ranking objective 需非空 entity（A7）|
| **增加 / 移除 product** | `parameters.yaml` 的 `schema.categorical_values[item]`；`parameters_inference.yaml` 的 `inference.products`；`conf/sql/etl/label/*.sql` 的 `candidate_prod` CTE 與 `CASE WHEN apply_type` mapping；`conf/sql/etl/sample_pool/sample_pool.sql` 的 `prod` CTE；若新增 label source category 還要動 `parameters_label_etl.yaml` 的 `tables:` 與 `label_table.sql` 的 `UNION ALL`；`sample_ratio_overrides`；`sample_weights`；任何產品清單 lint / 測試 | 全鏈：`*_etl`（產出新產品 row）→ `dataset` → `training` → `promote` → `inference` | `base_dataset_version`（schema hash 變）→ `model_version` | **必須同步多處**：`schema.categorical_values` 與 `inference.products` 須相同集合（A4）；sample_pool item 集合須等於宣告集合、label 不得有未宣告產品（B1）；overrides/weights 的 product 分量受 A5/A9 檢查；ETL SQL 的 hardcoded 產品清單（UNION ALL / CASE WHEN）grep `prod_name` 不一定看得到，要逐檔檢查；移除產品時記得清掉對應 overrides/weights |

### 「增加 feature」最小檢查清單

1. 上游 feature table 或 `conf/sql/etl/feature/*.sql` 產出新欄。
2. `conf/base/parameters_dataset.yaml`：
   - 不要當特徵的欄 → 加入 `prepare_model_input.drop_columns`；
   - categorical 特徵 → 加入 `prepare_model_input.categorical_columns`（不可與 drop 同時）。
3. 若異動牽涉 identity / product / schema → 同步改 `conf/base/parameters.yaml`（`schema.*`）。
4. 重跑：`feature_etl` → `dataset` → `training` → `promote_model.py` → `inference`（必要時 `evaluation`）。
5. 版本：`base_dataset_version` 變（feature_table fingerprint），連帶 `model_version` 變。

### 「增加 product」最小檢查清單（須同步更新）

新增 product 有兩種情境，差別在於是否要新增 label source category。**共通步驟**先做，再依情境補做：

**共通步驟**

1. `conf/base/parameters.yaml`：`schema.categorical_values[<item>]` 加入新產品。
2. `conf/base/parameters_inference.yaml`：`inference.products` 同步加入（與 `categorical_values[<item>]` 須相同集合，A4）。
3. `conf/sql/etl/sample_pool/sample_pool.sql`：`prod` CTE 內 `UNION ALL SELECT '<new_prod>' AS prod_name`（sample_pool item 集合須等於宣告集合，B1）。
4. `sample_ratio_overrides` / `training.sample_weights`：如需針對新產品調整則新增（key 分量受 A5/A9 檢查）。
5. 任何硬編產品清單的 lint / 測試（A6 由 `tests/.../test_product_consistency.py` 強制）一併更新。
6. 重跑全鏈：`*_etl` → `dataset` → `training` → `promote_model.py` → `inference`（必要時 `evaluation`）。

**情境 A：新產品來自既有 label source**（如再多一種信用卡產品）

- 在對應的 `conf/sql/etl/label/<source>.sql`（例如 `label_ccard.sql`）的 `candidate_prod` CTE 多 `UNION ALL SELECT '<new_prod>'`。
- 同檔的 `label_event` 內 `CASE WHEN apply_type = '<...>' THEN '<new_prod>'` mapping 也要加（否則 row 永遠是 NULL 被 dedup 過濾掉）。
- **不需動** `parameters_label_etl.yaml` 與 `label_table.sql`。

**情境 B：新產品來自新的 label source category**（如新增 `label_loan`）

- 新增 `conf/sql/etl/label/<source>.sql`，自己處理 candidate × event × dedup。
- `conf/base/parameters_label_etl.yaml`：`tables:` list 新增一筆（`name` / `sql_file` / `partition_by: {prod_name: STRING, snap_date: DATE}` / `primary_key: [snap_date, cust_id, prod_name]` / `quality_checks`），執行順序由 list order 決定。
- `conf/sql/etl/label/label_table.sql`：加一段 `UNION ALL SELECT * FROM ${target_db}.<new_source> WHERE snap_date = '${target_date}'`。
- `parameters_label_etl.yaml` 中 `label_table` 的 `depends_on` 列入新表名（順序驗證用）。

### 「移除 product」最小檢查清單

對稱於上面，重點是**清乾淨殘留**避免 A5/A9 / B1 fail loud：

1. `conf/base/parameters.yaml`：`schema.categorical_values[<item>]` 移除該產品。
2. `conf/base/parameters_inference.yaml`：`inference.products` 同步移除。
3. `conf/sql/etl/sample_pool/sample_pool.sql`：`prod` CTE 移除對應 `UNION ALL` 行（否則 sample_pool 出現未宣告產品，B1 raise）。
4. `conf/sql/etl/label/<source>.sql`：移除 `candidate_prod` 中該行與 `CASE WHEN` mapping（否則 label_table 出現未宣告產品，B1 raise）。
5. 若整類 source 都不要了：從 `parameters_label_etl.yaml` `tables:` 與 `label_table.sql` `UNION ALL` 中移除，並刪除對應 SQL 檔。
6. **清乾淨 overrides / weights 殘留 key**：
   - `parameters_dataset.yaml` 的 `sample_ratio_overrides`（key 含該 `prod_name` 分量者）。
   - `parameters_training.yaml` 的 `training.sample_weights`（同上）。
   - 殘留會被 A5（overrides）/ A9（weights）的 product 分量檢查擋下。
7. 硬編產品清單的 lint / 測試（A6）一併更新。
8. 重跑全鏈並重新 promote。
