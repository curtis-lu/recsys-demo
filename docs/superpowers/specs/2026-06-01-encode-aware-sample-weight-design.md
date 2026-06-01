# 編碼感知的 sample_weight 查表（Encode-aware sample_weight lookup）

- 日期：2026-06-01
- 狀態：設計待 review
- 相關：PR #53（sample_weight 觀測性 log）；on-hold 的「多槽 `.bin` cache（設計 B）」為正交議題

## 1. 背景與問題

`sample_weight` 的權重查表是用欄位的**原始字串值**去比對 `training.sample_weights`
的 key（`_compute_row_weights` 對每個 `weight_keys` 欄位做 `astype(str)`、用 `"|"` 串成複合
key、再 `map` 到權重表）。但 model_input parquet 裡，欄位有兩種儲存形態：

- **identity categorical**（如 `prod_name`）：存**原始字串**，編碼延後到 extract（deferred）。
- **feature categorical**（如 `cust_segment_typ_2a`）：被 `apply_preprocessor_to_features`
  （`preprocessing/_spark.py:360`）**提前編成 int**（code = `category_mappings[col]` 的 list
  index、unknown → -1）。

於是「同時是 model feature 的 categorical 欄位」無法當 `sample_weight_keys`：parquet 裡是 int
`"5"`、權重表 key 是字串 `"mass"`，永遠不 match → 權重靜默全 1.0。使用者只能：把欄位 `drop_columns`
移出 feature（失去特徵）、或在 `sample_pool` 另開不同名的原始欄位用 `carry_columns` 帶入（繁瑣）。
此外現有一致性閘 A9a（`weight_key_columns_unavailable`）只允許 `identity ∪ {label} ∪
carry_columns`，把 feature 欄位擋掉——但擋法與「同名 carry 會在 `build_model_input` 撞名」交織，
對使用者很反直覺。

## 2. 目標 / 非目標

**目標**
- 任何在 model_input 內的 categorical 欄位（identity 原始 或 feature 編碼）都能直接當
  `sample_weight_keys`，`sample_weights` 表維持**人類可讀字串**。不需 `carry_columns`、不需改名、
  欄位照常當 feature。
- 權重是否生效、哪些規則沒對到，要能在**隨 model 走、可稽核**的地方被發現（不只埋在 log）。
- 完全向後相容；不改 `model_version` 雜湊語意；權重維持 training-layer（不碰 parquet / dataset cache）。

**非目標**
- 數值（非 categorical）feature 當 weight key（`astype(str)` 對 float 脆弱；明確不支援，A9a 仍擋）。
- 把權重搬進 dataset pipeline / parquet（會把便宜的 training 旋鈕變貴；明確排除）。
- 多槽 `.bin` cache（設計 B，正交、另案）。

## 3. 設計總覽

核心：**把 config 端的 `sample_weights` 表 key 翻譯到 parquet 空間**（只翻小表、不動每一 row），
再沿用既有的 `_compute_row_weights` 不變。

```
training.sample_weights (config, 人類可讀字串 key)
  │
  │ _translate_weight_table(sample_weights, weight_keys, category_mappings, identity_columns)
  │   逐段：該段欄位是 encoded feature(在 category_mappings 且非 identity)
  │           → 字串值翻成 str(category_mappings[col].index(值))
  │         否則(identity / label / carry) → 維持原字串
  │   回傳 (translated_weights, unknown_values)
  ▼
translated_weights (parquet 空間的 key)
  │ _compute_row_weights(pdf, weight_keys, translated_weights)   ← 不變
  │   pdf 每欄 astype(str) 串 "|"，map translated_weights，fillna(1.0)
  ▼
w (np.float64) → lgb.Dataset(weight=w)
```

例：`weight_keys=[cust_segment_typ_2a, prod_name]`、表 key `"mass|ccard_ins"`。
`cust_segment_typ_2a` 是 feature → `mass` 翻成其 code（如 `"5"`）；`prod_name` 是 identity → 維持
`ccard_ins`。translated key = `"5|ccard_ins"`，正好對上 pdf 的 `astype(str)` 結果（`5` 為 int code、
`ccard_ins` 為原始字串）。

正確性依據：feature 編碼（`_encode_categoricals`, `_spark.py:103-104`，`enumerate(categories)`）
與 identity 編碼（`extract.py` 的 `pd.Categorical(..., categories=known).codes`）**同一套方案**
（code = `category_mappings[col]` index、unknown = -1）。

## 4. 元件

### 4.1 `_translate_weight_table(...)`（新增純函式，`io/extract.py`）
- 簽名：`(sample_weights: dict, weight_keys: list, category_mappings: dict, identity_columns: list) -> tuple[dict, dict]`
- 回傳 `(translated_weights, unknown_values)`，`unknown_values = {col: [config 中不存在於 category_mappings[col] 的值]}`。
- 純函式、無 I/O、無 Spark。每個表 key 依 `"|"` 切 `len(weight_keys)` 段；逐段判斷是否 encoded feature
  （`col in category_mappings and col not in identity_columns`）決定翻譯或保留。
- 未知值（feature 段的值不在 `category_mappings[col]`）：該 key 整條**略過**（不放進 translated_weights
  → 對應 row 維持 1.0），並收進 `unknown_values`。

### 4.2 `_row_weights_from_pdf(...)`（修改簽名，`io/extract.py`）
- 新簽名：`(pdf, parameters, preprocessor_metadata)`。caller `extract_Xy` / `extract_Xy_with_groups`
  本來就持有 `preprocessor_metadata`，thread 進去即可。
- 流程：解析 `weight_keys` → `_translate_weight_table` → `_compute_row_weights(pdf, weight_keys, translated)`
  → 維持 PR #53 既有的 `sample_weight ACTIVE / INACTIVE / 0-match WARNING` log。
- 缺欄位的 graceful all-ones backstop 維持。

### 4.3 A9a 放寬（`core/consistency.py::weight_key_columns_unavailable`）
- `available` 由 `identity ∪ {label} ∪ carry_columns` 改為
  `identity ∪ {label} ∪ carry_columns ∪ categorical_columns`。
- `categorical_columns` 由 config 可得（`_get_preprocessing_config`），故仍是純 config 檢查。
- 效果：feature categorical 過閘；**非** categorical 的數值 feature 仍被擋（符合非目標）。
- A9b（arity）、A9c（product 分量對 `schema.categorical_values[item]`）維持不變。
  注意：feature categorical 的合法值來自**資料**（非 config），故 config-gate 無法驗其值；交由 §7 的
  runtime / manifest 診斷處理。

### 4.4 權重診斷與持久化（`resolve_weight_diagnostics` + manifest）
- 新增純/輕量函式 `resolve_weight_diagnostics(train_pdf_or_handle, parameters, preprocessor_metadata) -> dict`，
  重用 `_translate_weight_table`，並對 train 的 weight-key 欄位做**一次便宜的 distinct**（只讀那幾欄），
  比對出 `unmatched_keys`。輸出：
  ```jsonc
  "sample_weight": {
    "enabled": true,
    "weight_keys": ["cust_segment_typ_2a", "prod_name"],
    "n_weight_entries": 12,
    "unmatched_keys": ["afflunet|ccard_ins"]   // 對到 0 個 train row 的 entry(此處 segment 打錯)
                                                // 全對到時為 []
  }
  ```
- `unmatched_keys` 是**資料驅動**的完整訊號：label / prod_name / segment 打錯、編碼不符全部涵蓋
  （優於只查 encoded-category typo）。
- 計算點：training **node**（`preprocessor_metadata` 與 train parquet handle 在 scope）。
- 持久化（兩處，皆「不埋在 log」）：
  1. 寫 `data/models/<model_version>/sample_weight_report.json`（與 SHAP 等診斷同模式；會自動列入
     manifest 的 `artifacts`，見 `__main__._dir_artifacts`）。
  2. 關鍵欄位經 `_write_pipeline_manifest` 既有的 `extra_metadata` 通道併入 `manifest.json`
     的 `sample_weight` 區塊（最顯眼處）。
- 確切 wiring（node 產出 → `extra_metadata`）於實作計畫定案；§10。

## 5. 明確支援的情境

- **label 當 key**（單獨或複合，如 `[prod_name, label]`）：label 在 parquet 是 raw int，非
  encoded categorical（不在 `category_mappings`）→ 走「維持原值」分支、不翻譯；A9a 既有 `available`
  已含 label。今日即可用（既有測試 `test_three_key_segment_prod_label`），新設計零特例。
  語意 = class / 正樣本加權；純不平衡建議優先用 LightGBM `scale_pos_weight`，label-key 的價值在與
  product/segment 複合的精準加權。
- **identity（prod_name）、carry_columns 欄位**：非 encoded feature → 不翻譯，行為與今日完全一致。
- **混合複合 key**（feature 段 + identity/label 段）：逐段分別翻譯/保留。

## 6. 不變量

- **向後相容**：既有 config（prod_name / carry / label key）所有段都走「不翻譯」分支，結果逐位元相同。
- **model_version**：`sample_weights` / `sample_weight_keys` 仍在被雜湊的 `training:` block；翻譯所依賴的
  `category_mappings` 由 `base_dataset_version` 決定，已在 `.bin` cache key 內。雜湊語意不變。
- **權重層級**：仍是 training-layer，不寫 parquet、不影響 dataset cache。

## 7. 錯誤處理（未知 / 不中）

- 決議：**WARNING + 該規則當不中（不 raise）**，與既有 graceful 哲學一致，不中斷 production training。
- 三層可見：
  1. runtime `sample_weight ACTIVE / 0-match WARNING`（PR #53，extract 時）。
  2. `manifest.json::sample_weight.unmatched_keys`（隨 model、可稽核）。
  3. `sample_weight_report.json`（version dir，完整）。

## 8. 測試計畫

- `_translate_weight_table`（純）：feature 段翻譯、混合複合 key、identity-only 不翻譯、carry-only 不翻譯、
  未知值進 `unknown_values` 且該 key 被略過、空表/空 keys。
- `_row_weights_from_pdf`：feature categorical key 現在能產生非 1.0（過去靜默 1.0）；caplog 的
  `sample_weight ACTIVE` 對 feature key 觸發。
- A9a：feature categorical 過閘、數值 feature 仍被擋、既有 identity/label/carry 仍過。
- `resolve_weight_diagnostics`：`unmatched_keys` 對 label/prod_name/segment 打錯與編碼不符皆抓到；
  全中時為 `[]`；manifest 區塊形狀。
- 既有 `_compute_row_weights` / `TestExtractWithWeights` 測試不變（回傳值不變）。

## 9. 影響檔案

- `src/recsys_tfb/io/extract.py`：`_translate_weight_table`（新）、`_row_weights_from_pdf`（簽名 + 翻譯）、
  caller threading。
- `src/recsys_tfb/core/consistency.py`：A9a `available` 加 `categorical_columns`。
- `src/recsys_tfb/pipelines/training/nodes.py`：`resolve_weight_diagnostics` 計算 + 寫
  `sample_weight_report.json`。
- `src/recsys_tfb/__main__.py`：把診斷併入 `extra_metadata` → `manifest.json`（wiring，§10）。
- 對應 `tests/`。

## 10. Plan-time 待定

- node 產出的診斷 dict 如何送達 `__main__._write_pipeline_manifest` 的 `extra_metadata`
  （catalog 輸出 vs 從 version dir 的 `sample_weight_report.json` 回讀 vs __main__ 端輕量重算）。
  傾向：node 寫 `sample_weight_report.json`、`__main__` 回讀其精簡欄位併入 manifest，避免在 __main__ 做資料運算。
- `resolve_weight_diagnostics` 的 distinct 是走 pandas（version dir 已 materialize 的 train parquet）
  或 pyarrow compute；以最小記憶體足跡為準（只讀 weight-key 欄位）。
