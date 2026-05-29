# Training Diagnostics：特徵統計、原生 importance、SHAP

**日期**：2026-05-29
**Branch**：`feat/training-diagnostics`
**狀態**：設計已核可,待寫 implementation plan

## 1. 動機與範圍

目前 `log_experiment`（`src/recsys_tfb/pipelines/training/nodes.py`）只記錄 params / metrics / model artifact，**沒有任何特徵層級或可解釋性資訊**。本次擴充在 training pipeline 產出三類診斷並記錄到 MLflow：

主要用途（使用者確認）：**業務可解釋性（SHAP）** + **模型診斷 / 健康檢查**。
明確排除：跨週期 drift 監控、稽核/合規、**收斂診斷**（後者經討論後放棄，不跑任何 train-set 預測）。

## 2. 架構：方案 B（獨立診斷 node + 薄記錄層）

新增三個**純計算** node，各自吃 DAG 已存在的 handle、輸出結構化 artifact；`log_experiment` 退化成薄的 MLflow 記錄層，只 `log_artifact` + 記幾個 scalar metric，不做重運算。

理由：每個診斷函式是 pure Python over driver-local parquet / booster → 不碰 Spark、秒級單元測試，符合 repo §測試效能 與 small well-bounded units 原則；SHAP（最重）可獨立 config flag 開關。

| Node | Inputs（DAG 既有 handle） | Output（catalog artifact） | 計算量 |
|---|---|---|---|
| `compute_feature_statistics` | `train_parquet_handle`, `preprocessor`, `parameters` | `feature_statistics`（JSON） | 輕 |
| `compute_feature_importance` | `model`, `parameters` | `feature_importance`（JSON） | 極輕 |
| `compute_shap_diagnostics` | `model`, `test_parquet_handle`, `preprocessor`, `parameters` | `shap_diagnostics`（JSON + PNG） | 重（gated） |

### 資料流

三個 diagnostic node 掛在 `model` / `compute_test_mAP_spark` 之後、`log_experiment` 之前，彼此獨立（可並行）。`log_experiment` 維持現有 input，**再加上**這三個診斷產物（in-memory dict / artifact 路徑），負責上傳 MLflow。

`compute_shap_diagnostics` 受 `diagnostics.shap.enabled` 控制（最重，預設可關）。當任一診斷 disabled，對應 node 回傳空結果 / 略過，`log_experiment` 容忍缺項。

### 產物存放

沿用既有慣例 `data/models/${model_version}/`，新增子目錄 `diagnostics/`：

```
data/models/${model_version}/diagnostics/
  feature_statistics.json
  feature_importance.json
  shap_global.json
  shap_summary.png
  shap_per_item.json
  shap_examples.json
  waterfall_*.png
```

catalog artifact 比照現有 `model` / `best_params` / `evaluation_results`（JSONDataset，filepath 用 Python `open()` 寫，不認 `hdfs://`）。

## 3. 各 node 計算內容與輸出格式

### 3.1 `compute_feature_statistics`

對 cached train parquet（driver-local），**只讀 `preprocessor["feature_columns"]`** 省記憶體，逐欄計算：

- `null_rate`
- 數值欄：`mean` / `std` / `min` / `max`
- 所有欄：`n_distinct`（cardinality）
- 衍生旗標：`single_value`（n_distinct ≤ 1）、`high_null`（null_rate ≥ 門檻）

列數超過 `feature_stats.sample_rows` 門檻時抽樣，避免 ~10M×~1500 撐爆記憶體。優先用 pyarrow column-wise compute（null count / min / max）以降低成本。

**輸出** `feature_statistics.json`：
```json
{ "<feature>": { "null_rate": 0.0, "mean": .., "std": .., "min": .., "max": ..,
                 "n_distinct": .., "single_value": false, "high_null": false } }
```

### 3.2 `compute_feature_importance`

從 `model` 取兩種 LightGBM importance（不需資料）：

- `split`（現有 `LightGBMAdapter.feature_importance()`）
- `gain`（擴充 adapter：`feature_importance(kind="split"|"gain")` 或新增 `feature_importance_gain()`；底層 `booster.feature_importance(importance_type="gain")`）

依 gain 排序；標出 `dead_features`（split == 0）。需處理 `CalibratedAdapter` 委派（已委派給 base，沿用即可）。

**輸出** `feature_importance.json`：ranked list（feature, split, gain）+ `dead_features` 清單。

### 3.3 `compute_shap_diagnostics`（重點：效率）

用 `extract_Xy`（`src/recsys_tfb/io/extract.py`）從 test parquet 取前處理後 numpy X，依下述抽樣後做 SHAP。

**效率保障（核心原則：只算一次、重複利用）**

1. **單次計算、三用**：整個 node 只呼叫一次 `shap_values(X_sample)`，得到一個 `[n_sample × n_feature]` 矩陣；全域 / per-item / 個例**全部從這一個矩陣聚合**，不重算、絕不跑 22 次。
2. **`tree_path_dependent`**（TreeExplainer 預設）：tree 專用、精確、**不需 background dataset**，複雜度隨 tree 結構而非特徵數，對 ~1500 寬表友善；**絕不退回 KernelExplainer**。
3. **依 item 分層抽樣**：抽到總量 `shap.sample_rows`（預設 2000），每個 item 至少 `min_rows_per_item`，把總工作量釘死在 `sample_rows × n_trees`。
4. **Budget guard + 計時**：用 `log_step` 計時；若 `sample_rows × best_iteration` 超過門檻，自動降抽樣並 warn，避免拖垮 pipeline。

**三個層級（皆從同一 SHAP 矩陣聚合）**

- **全域**：`mean(|shap|)` 排 `top_k` + `mean(signed shap)`（方向：推高/推低分數）→ `shap_global.json` + `shap_summary.png`（beeswarm，matplotlib 3.10.9 可用）。
- **per-item（22 產品）= 族群代表**（使用者確認的框架）：
  - 依 item 分層，**item 內純隨機**抽樣；每 item 至少 `min_rows_per_item`，**不足全取**（take-all fallback）。
  - per-item 聚合**如實反映該產品族群、不被高分樣本偏置**。
  - 每個 item 在輸出附覆蓋率 metadata：`n_sampled`、`n_positive`、`score_min/max/mean`；`n_sampled < min_rows_per_item` 時標 `low_coverage: true`。
  - → `shap_per_item.json`：`{ "<item>": { "top_features": [...], "n_sampled": .., "n_positive": .., "score_min/max/mean": .., "low_coverage": bool } }`
- **代表性個例**：取 predicted-score 最高/最低各 `n_examples` 筆的逐特徵 SHAP（waterfall 資料）→ `shap_examples.json` + `waterfall_*.png`。
  - 「為何被推薦」的高分故事由此段承載；個例抽樣**保證涵蓋稀有但高分的產品**（每個 item 至少一筆高分個例），補足 per-item 族群代表抽樣可能抽不到稀有高分的缺口。

## 4. `log_experiment` 改動（薄記錄層）

維持現有 input，新增三個診斷產物的輸入。新增：

- `mlflow.log_artifacts(diagnostics_dir)`（或逐檔 `log_artifact`）上傳整個 `diagnostics/`。
- 幾個 scalar summary metric：`n_dead_features`、`n_high_null_features`、`n_single_value_features`。

容忍任一診斷 disabled / 缺項（對應 metric 略過、artifact 不存在則不上傳）。**不**新增任何收斂/overfitting metric。

## 5. Config（關鍵：不得影響 model_version）

新增 **top-level** `diagnostics` block（與 `mlflow` / `cache` 同層），**不可**放進 `training:`。

理由：`compute_model_version` 的 `_model_version_payload`（`src/recsys_tfb/core/versioning.py:124`）**只雜湊 `training:` block**；top-level `spark` / `mlflow` / `cache` 結構性排除。註解明寫「`training:` 底下新增 key 預設納入雜湊（safe over-invalidation）」。因此 `diagnostics` 放 top-level → 自動排除於版本雜湊外。

```yaml
diagnostics:
  feature_stats:
    enabled: true
    sample_rows: 500000
    high_null_threshold: 0.5
  feature_importance:
    enabled: true
  shap:
    enabled: true
    sample_rows: 2000
    top_k: 30
    n_examples: 5
    min_rows_per_item: 30
```

放置檔案：`conf/base/parameters_training.yaml`（作為該檔的 top-level key，與既有 `training:` 同層；確認載入後在 parameters dict 根層、不在 `training` 之下）。

**驗證**：新增測試斷言 `compute_model_version` 對 `diagnostics` block 的變動**不變**（沿用 `tests/test_core/test_versioning.py` 模式）。

## 6. 測試策略

三個診斷函式皆 pure Python over 小 parquet / 小 booster → 秒級單元測試，**不碰 Spark cold start**（符合 §測試效能 與 feedback：把測試跑快、不略過）：

- `compute_feature_statistics`：幾十列含 null / 單一值 / 高基數的小 parquet → 驗 null_rate / mean / n_distinct / single_value / high_null 旗標 / 抽樣門檻邊界。
- `compute_feature_importance`：極小合成 booster → 驗 split/gain 兩種值、排序、dead-feature 偵測、`CalibratedAdapter` 委派。
- `compute_shap_diagnostics`：極小 booster + 幾十列含稀有 item 的資料 → 驗 (a) 只呼叫一次 `shap_values`（mock/spy 計數）、(b) 全域 top-k 與方向、(c) per-item 分層 + take-all fallback + `low_coverage` 旗標 + 覆蓋率 metadata、(d) 個例 high/low 選取與稀有高分涵蓋、(e) budget guard 觸發降抽樣。
- `log_experiment`：mock mlflow，驗 artifact 上傳與 scalar metric；驗任一診斷 disabled 時容錯。
- `versioning`：`diagnostics` 變動不影響 `compute_model_version`。

## 7. 決策摘要

| # | 決策 |
|---|---|
| 1 | 方案 B：3 個獨立純計算 node + `log_experiment` 薄記錄層 |
| 2 | **不做**收斂診斷（不跑 train-set 預測） |
| 3 | `diagnostics` 為 **top-level** config block，結構性排除於 model_version 外 + 不變性測試 |
| 4 | SHAP 單次計算三用、`tree_path_dependent`、依 item 分層、budget guard |
| 5 | per-item = **族群代表**（item 內隨機 + min_rows_per_item + take-all + 覆蓋率 metadata / low_coverage）；高分「為何被推薦」交給代表性個例段（保證涵蓋稀有高分產品） |
| 6 | 產物存 `data/models/${model_version}/diagnostics/`，比照現有 JSONDataset artifact |
