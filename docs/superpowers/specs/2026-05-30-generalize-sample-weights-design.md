# Generalize `sample_weights` to composite-key (mirror `sample_group_keys`)

**Date:** 2026-05-30
**Branch:** `feat/generalize-sample-weights`
**Status:** Approved design — pending implementation plan

## Problem

`training.sample_weights` 目前 key 寫死為 `"<cust_segment_typ>|<prod_name>"`
兩段式（`io/extract.py` 的 `SEGMENT_COLUMN` 常數 + 固定 2 元素 concat；
`consistency.py` A9 固定驗 `split("|")[1]`）。使用者無法：

- 只用 `prod_name` 設 weight（得枚舉每個 segment）。
- 用 `prod_name × label` 或其他維度組合設 weight。

目標：把 weight key 泛化成由欄位清單驅動，與 dataset 的
`sample_group_keys` / `sample_ratio_overrides`（`pipelines/dataset/helpers_spark.py:76-84`）
完全對稱。

## Approved Decisions

1. **Key 來源**：新增**獨立**的 `training.sample_weight_keys`（不複用
   `dataset.sample_group_keys`）。weight 維度與 dataset 抽樣維度解耦。
2. **預設值**：`sample_weight_keys: [prod_name]` —— 「只針對 prod_name 設
   weight」開箱即用。現行 `sample_weights` 為 `{}`，無行為破壞。
3. **Value 語意**：任意正數，預設 1.0。`>1` boost、`<1` down-weight；
   consistency 擋 `<=0`。沒列到的 row = 1.0（稀疏表，與現行一致）。
4. **Consistency 閘**：欄位依賴與 key 段數**兩者都硬 error**（collect-all）。

## Data Dependency (核心風險)

training 在 `io/extract.py` 讀的 train/train_dev model_input parquet
**物理上只含**：`identity (snap_date, cust_id, prod_name)` ∪
`dataset.carry_columns` ∪ `label` ∪ encoded feature 欄
（見 `pipelines/dataset/helpers_spark.py:54` `return_cols = identity_key +
carry_columns`，再由 `build_model_input` 併入 label + encoded features）。

因此：

- `sample_weight_keys` 的每個欄位**必須** ∈
  `identity_columns ∪ {label} ∪ dataset.carry_columns`。這同時保證
  (a) 欄位物理存在於 parquet、(b) 為**原始值**（feature 欄在 parquet 是
  encode 過的整數碼，拿來當 key 會對不上原始標籤）。
- 跨檔依賴：`sample_weight_keys` 在 `parameters_training.yaml`，想用非
  identity/label 的欄位（如 `cust_segment_typ`）**必須先在
  `parameters_dataset.yaml` 的 `carry_columns` 列出**。
- **版本層級不同**：改 `carry_columns` → bust `base_dataset_version` →
  **要重跑 dataset pipeline**；只改 `sample_weight_keys` / `sample_weights`
  （欄位已被 carry）→ 只 bust `model_version`，dataset 不重產。
- 沒對齊的後果：欄位不在 parquet → weight **靜默全 1.0**，無錯誤
  （即現行 dev 合成資料無 `cust_segment_typ` 的踩坑）。
- `validate_config_consistency` 拿到跨兩檔合併後的 dict，可在 config-static
  階段（Spark cold start 前）擋下此依賴。

## Components

### 1. Config schema — `conf/base/parameters_training.yaml`

```yaml
training:
  # weight key 由哪些欄位組成（順序即 "|" 串接順序）
  # 必須 ⊆ identity ∪ {label} ∪ dataset.carry_columns；改此清單 bust model_version
  sample_weight_keys:
    - prod_name
  # key = sample_weight_keys 值用 "|" 串接；value = LightGBM sample_weight
  # 任意正數(>0)：>1 boost、<1 down-weight；沒列到的 row = 1.0（稀疏表）
  # 只作用於 train/train_dev，val/calibration/evaluation 不加權
  sample_weights: {}
```

舊註解（固定 `<cust_segment_typ>|<prod_name>`）改寫；保留「bust model_version、
不動 train_variant_id」「只作用 train/train_dev」說明。

### 2. `src/recsys_tfb/io/extract.py` — 泛化 weight 計算

- `_compute_row_weights(pdf, weight_keys, sample_weights)`：單欄直接 `astype(str)`；
  多欄用 `str.cat(..., sep="|")` 逐欄串接（鏡像 `helpers_spark.py:76-79`）。
  `keys.map(sample_weights).fillna(1.0)`。空 `sample_weights` 或空 `weight_keys`
  → 全 1.0。
- `_row_weights_from_pdf(pdf, parameters)`：從 `parameters["training"]` 讀
  `sample_weight_keys` + `sample_weights`；保留 graceful guard —— 任一 key 欄位
  不在 `pdf.columns` 就回全 1.0（consistency 已擋，這是執行期後備）。
- 移除寫死的 `SEGMENT_COLUMN` 常數。
- `extract_Xy` / `extract_Xy_with_groups` 的 `with_weights` 分支介面不變。

### 3. `src/recsys_tfb/core/consistency.py` — A9 泛化 + 資料依賴閘

三個 predicate（collect-all，一次 raise）：

- `weight_key_columns_unavailable(parameters)` (**A9a**)：`sample_weight_keys`
  中 ∉ `identity ∪ {label} ∪ carry_columns` 的欄位。error 訊息明確提示「把欄位
  加進 `dataset.carry_columns` 並重跑 dataset pipeline（bust base_dataset_version）」。
- `weight_key_arity_mismatch(parameters)` (**A9b**)：`sample_weights` key 段數
  `!= len(sample_weight_keys)`。
- `weight_unknown_items(parameters)` (**A9c**，泛化舊版)：若
  `schema.item ∈ sample_weight_keys`，用 `sample_weight_keys.index(item)` 取
  產品分量，驗 ∈ `resolved_item_values`（取代寫死 `[1]`）。

`validate_config_consistency` 加對應三段 error。模組 docstring 的 Invariant
legend A9 → 拆 A9a/A9b/A9c。

### 4. Versioning — 無需改

`sample_weight_keys` 在 `training:` block 內、不在
`MODEL_VERSION_IRRELEVANT_PARAMS` → 自動納入 `model_version` hash。**不**進
`TRAIN_SAMPLING_KEYS`（weight 是訓練層、不影響 dataset 抽樣產物）。

## Testing

`tests/test_io/test_extract.py`：
- 單欄 `[prod_name]`、多欄 `[cust_segment_typ, prod_name, label]`。
- 空 `sample_weight_keys` / 空 `sample_weights` → 全 1.0。
- value `<1` down-weight、`>1` boost、unmatched = 1.0。
- key 欄位缺於 pdf → graceful 全 1.0。

`tests/test_core/test_consistency.py`：
- A9a 欄位未 carry → raise。
- A9b key 段數對不上 → raise。
- A9c 產品分量未宣告 → raise（含單欄 `[prod_name]` 與多欄含 item 兩情境）。
- 合法 config 通過；collect-all 一次回報多錯。

## Out of Scope

- 不複用/不更動 `dataset.sample_group_keys`。
- 不支援 prefix（比 keys 短的部分 key）匹配。
- 不改 dataset pipeline 抽樣邏輯。
