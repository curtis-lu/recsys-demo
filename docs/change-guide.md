# 修改指引（change guide）

> 要改設定時：**改哪些檔 → 會觸發哪道閘 → 要重跑什麼**。每個情境都從一致性不變量（`core/consistency.py`）回推。
> 所有錯誤訊息與修法見 README §4；資料流見 [`data-lineage.html`](data-lineage.html)；設計背景見 [`design-principles.md`](design-principles.md)。

## 速查表

| 你想改 | 主要改哪些檔 | 觸發的閘 | 重跑範圍 |
|---|---|---|---|
| 加一個 item（候選項目） | `schema.categorical_values`、來源 SQL、`inference.products` | A3 / A4 / B1 | bust `base` → 全部重跑 |
| 加一個特徵欄 | feature ETL SQL、`categorical_columns` / `drop_columns` | A1 | bust `base` → dataset → 之後 |
| 改訓練目標（binary↔LTR） | `parameters_training.yaml` `objective` / `metric` | A7 | bust `model_version` → 只 training 之後 |
| 改抽樣 / 樣本權重 | `sample_ratio_overrides`、`sample_weights` / `sample_weight_keys` | A5 / A9 | 視情況 bust `train_variant` 或只 `model_version` |
| 改 schema 角色（換應用） | `parameters.yaml` `schema` | 多條 | bust `base` → 全部 |
| 新增 model structure / 一致性不變量 | `core/consistency.py`（predicate）、`parameters_training.yaml`、`product_categories`（頂層） | A15（或新不變量） | bust `model_version` → 只 training 之後 |

> 「bust `base`」＝ `base_dataset_version` 翻版，base / train / calibration 全層與下游模型都要重算。

---

## 情境 1：加一個 item（候選項目 / 產品）

**改哪些檔**
- `conf/base/parameters.yaml`：`schema.categorical_values[item]` 加入新值（A3：item 是 identity 類別欄，必須宣告完整值清單）。
- 來源 SQL：`sample_pool` 的 SQL 要**產出**該 item（B1 要求 sample_pool 的 item 集合與宣告**雙向相等**）；label 的 SQL 要能產出該 item 的 label（B1 要求 label item ⊆ 宣告）。
- `conf/base/parameters_inference.yaml`：`inference.products` 同步成相同集合（A4）。
- 若 `sample_ratio_overrides` / `sample_weights` 有 item 維度的 key，補對應值（A5 / A9c）。

**觸發的閘**：設定閘 A3、A4、A5 / A9c（啟動即檢查）；資料閘 B1（dataset 第一個節點）。

**重跑**：schema 變 → bust `base_dataset_version` → `source_etl`（產新 item 資料）→ `dataset` → `training` →（`inference` → `evaluation`）。

---

## 情境 2：加一個特徵欄

**改哪些檔**
- feature ETL：`conf/sql/etl/feature/*.sql` 產出新欄，寫進 `feature_table`（寫入路徑支援 **append-only 加欄**）。
- `conf/base/parameters_dataset.yaml`：
  - 若是**類別欄** → 加進 `prepare_model_input.categorical_columns`（會被 int 編碼）。
  - 若要**排除** → 加進 `drop_columns`。**別同時**放兩邊（A1）。
  - 若要當 `sample_weights` 維度 → 加進 `carry_columns`。

> 注意：**非 identity 的類別特徵欄不需要** `schema.categorical_values`——編碼字典由 `fit_preprocessor_metadata` 從資料學。只有 `item` 這種 identity 類別欄才需要在 `categorical_values` 宣告。

**觸發的閘**：A1（同欄不可同時 drop ＋ categorical）。

**重跑**：feature_table 欄位指紋變 → bust `base_dataset_version` → `dataset` → `training` →（之後）。

---

## 情境 3：改訓練目標（pointwise ↔ learning-to-rank）

**改哪些檔**
- `conf/base/parameters_training.yaml`：`algorithm_params.objective` 改成 `binary` / `lambdarank` / `rank_xendcg`。
- 用 LTR 時 `algorithm_params.metric` 必須是排序指標（`ndcg` / `map`；留空自動帶 `ndcg`），且 `schema.entity` 非空（query group 要有定義）。

**觸發的閘**：A7（ranking objective 必須配 ranking metric ＋ query group）。

**重跑**：**不**動 dataset。`objective` 是 model-defining → bust `model_version` → 只重跑 `training` →（`inference` → `evaluation`）。

---

## 情境 4：改抽樣 / 樣本權重

**改哪些檔**（這些通常用工具從 `sample_pool` 推導，不手填）
- 抽樣：`conf/base/parameters_dataset.yaml` 的 `sample_ratio` / `sample_ratio_overrides`（用 `scripts/sampling_overrides_editor.py`）。
- 權重：`conf/base/parameters_training.yaml` 的 `sample_weights` / `sample_weight_keys`。

**觸發的閘**：A5（override 的 item 值要存在）、A9a（weight 維度欄必須在 train model_input：identity ∪ {label} ∪ `carry_columns` ∪ 類別欄）、A9b（key 段數 ＝ `sample_weight_keys` 欄數）、A9c（weight 的 item 值要存在）。

**重跑**
- 改 **train 抽樣** → bust `train_variant_id` → 重跑 `dataset`（train 系列）＋ `training`。
- 只改 **權重值**（維度欄已 carry）→ **不** bust dataset（weight 在 training 端讀 carry 欄套用）→ bust `model_version` → 只重跑 `training`。
- 若新權重維度欄**還沒** carry → 先把它加進 `carry_columns`（這會 bust `base`！）再重跑 `dataset`。

---

## 情境 5：改 schema 角色（換到別的應用）

**改哪些檔**
- `conf/base/parameters.yaml` 的 `schema`：`time` / `entity` / `item` / `label` 等角色對應到新應用的欄名與值。
- 來源表 SQL（feature / label / sample_pool）依新角色重寫。
- `inference.products` 等隨 item 改。

**觸發的閘**：多條（A2 / A3 / A4…）會在啟動時一次列出。

**重跑**：bust `base_dataset_version` → 全部。

---

## 情境 6：新增 model structure / 一致性不變量

**改哪些檔**
- `core/consistency.py`：**這是唯一允許的地方**——新增一個 predicate（pure function），在 `validate_config_consistency` 的 collect-all 迴圈裡呼叫。**不得**在各 pipeline 內以 ad-hoc 方式散落。A15（`model_structure_errors`）是範例：它驗 `model_structure` 合法值、`per_group_plus_rank` 搭配條件（grouping 覆蓋 item set、ranking objective、校準關閉），單一來源、單次 raise（設計原則見 [`design-principles.md`](design-principles.md) §4）。
- `conf/base/parameters_training.yaml`：在 `training:` 下加新 key（如 `model_structure`、`stage1`、`stage2`）。`model_version` 自動 hash `training:` 下所有 model-defining key（`core/versioning.py`），**不需手動登記**——只要新 key 位在 `training:` 區塊，切換結構就自動 bump 版本。
- **grouping / mapping 資料放頂層 `product_categories`，不放 `schema.*`**：`schema` 變動影響 `base_dataset_version`（全層重跑）；放頂層只 bust `model_version`（僅 training 之後）。修改 `product_categories.mapping` 不會觸發 dataset 重建，這是刻意設計。

**觸發的閘**：新增的 predicate（啟動即檢查，collect-all）。

**重跑**：不動 dataset。`model_structure` 等 training 設定是 model-defining → bust `model_version` → 只重跑 `training` →（`inference` → `evaluation`）。

> **Two-stage stacking 範例**：`model_structure: per_group_plus_rank`、`stage1`/`stage2` config、`product_categories` 頂層 mapping → A15 一致性閘驗 → `CompositeModelAdapter` 訓練 → `model_version` bump。設計細節見 [`pipelines/training.md` §Two-stage stacking](pipelines/training.md#two-stage-stacking)。

---

## 通則

- 改完先空跑啟動（任何 `python -m recsys_tfb <pipeline>`），讓設定閘一次把問題列完再修——比跑到一半才爆省時間。
- 不確定改了會 bust 哪層？對照速查表的「重跑範圍」，或看 [`design-principles.md`](design-principles.md) §3。
- 上線一律人工：`scripts/promote_model.py` 設 `best`（README §3 Q5）。
