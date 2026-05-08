# Explicit `train_snap_dates`: Dataset Pipeline 對週頻上游 Robust 化

**Status:** Draft
**Date:** 2026-05-08
**Author:** curtis-lu (with Claude)

## Goal

把 `dataset` pipeline 的訓練窗從「`train_snap_date_start/end` range」改成「`train_snap_dates: [...]` 顯式 list」，並在 `fit_preprocessor_metadata` / `apply_preprocessor_to_features` 兩個 node 加上以該 list 為基礎的 snap_date filter，讓 dataset pipeline 對未來「上游 feature_table 同時含週中 + 月底兩種 cadence」的情境 robust。

## Motivation

業務規劃將 inference 從月頻切到週頻，feature_etl 上游 `feature_table` 因此會變成**週中 + 月底兩種 snap_date 並存**：每週一筆給 inference，月底另外補一筆給 training。Training cadence 不變，仍用 12 個月底 snapshot。

目前 dataset pipeline 對訓練窗的表達是 `train_snap_date_start/end` range，且 `apply_preprocessor_to_features` 完全沒有 snap_date filter（前一輪 audit 發現），衍生兩個問題：

1. **正確性風險（cache stale）**：`preprocessed_feature_table` cache key 來自 `base_dataset_version` hash，hash 不含「上游有哪些 snap_date」。上游從「只月底」變成「週中 + 月底」時，hash 不變、cache 命中、但實際 row 集合悄悄變了 → 違反 ML reproducibility。
2. **效率浪費**：apply 全量 encode 一張多月歷史的 feature_table，週頻上線後資料量變 4× 以上，cache 體積與後續 join 成本同步放大；訓練實際只用月底 row。

Range 表示法本身也不貼合 ML lineage 慣例 ——「dataset = 顯式 row 集合」是 DVC / MLflow 等工具的常識。改成 list 後，filter 集合明確、上游 cadence 變動時 fail-loud（缺值就 raise）而非 silently 吞下。

## Non-Goals

- **不**動 inference pipeline 的週頻切換（包含 `parameters_inference.yaml`、ETL 排程改每週、週中 distribution shift monitoring）—— 留作後續 spec。
- **不**動 source_etl / feature_etl / label_etl / sample_pool_etl —— 這些 pipeline 的 `target_dates` 已是 list 表示法、無月底假設。
- **不**保留 `train_snap_date_start/end` 的向下相容支援 —— 完全替代，避免兩條 code path。
- **不**重構 `validate_date_splits` 之外的其他 validation 邏輯。
- **不**自動清除既有 orphan cache（`cached_*_model_input` 因 hash 跳號變孤兒檔案）—— 不在 spec 範圍。
- **不**改 `train_variant_id` / `calibration_variant_id` / `model_version_id` 的 hash 計算邏輯 —— 它們會因 `base_dataset_version` 變號而連帶變，這是預期行為。

## Architecture

### 設計原則

1. **單一事實來源**：「dataset pipeline 用了哪些 snap_date」由一個 helper `collect_dataset_snap_dates` 集中回答，避免 fit / apply / select_keys 各自展開。
2. **Fit / Apply filter 範圍刻意不同**：`fit_preprocessor_metadata` 只看 `train_snap_dates`（保留現有「不洩漏 val/test categorical 值」的設計）；`apply_preprocessor_to_features` 看 union（涵蓋所有下游 split 需要的 snap_date）。
3. **缺值嚴格 raise**：feature_table 缺任何被 dataset 用到的 snap_date 都 raise，不論 split。`feature_table` 必須能可重現 dataset。
4. **顯式優先**：`train_snap_dates` 是必填 list；range 完全移除。`calibration/val/test_snap_dates` 維持既有的 list 表示法。
5. **零跨 backend 邏輯漂移**：helper 純 datetime 邏輯，pandas / spark 兩 backend 共用同一個函式。

### 資料流變更

| Node | Before | After |
|---|---|---|
| `select_train_keys` | range filter on sample_pool | `isin(train_snap_dates)` |
| `fit_preprocessor_metadata` | range filter on feature_table（只 train 區間） | `isin(train_snap_dates)` + 缺值 raise |
| `apply_preprocessor_to_features` | 全量處理 | `isin(collect_dataset_snap_dates)` + 缺值 raise |
| `validate_date_splits` | range vs list 混合 overlap check | 純 list 兩兩交集 overlap check（簡化） |
| `select_val/test/calibration_keys` | 已是 list filter | 不動 |

### 不變的部分

- `source_etl` / `feature_etl` / `label_etl` / `sample_pool_etl`
- `inference` / `training` / `evaluation` / `baselines` 全部 pipeline
- catalog 結構（包含 `preprocessed_feature_table` 仍是 MemoryDataset、`cached_*_model_input` 仍是 ParquetDataset）
- ETL SQL（依 `${target_date}` 動態綁，無月底假設）
- node input/output 簽章（filter 是 internal 邏輯改動）
- `Pipeline.create_pipeline()` wiring

## Detailed Design

### 1. 設定檔變更：`conf/base/parameters_dataset.yaml`

**Before**:
```yaml
dataset:
  train_snap_date_start: "2025-01-31"
  train_snap_date_end: "2025-10-31"
  calibration_snap_dates: ["2025-11-30"]
  val_snap_dates: ["2025-12-31"]
  test_snap_dates: ["2026-01-31"]
```

**After**:
```yaml
dataset:
  # 月底 snap_date list（顯式列出，避免上游 cadence 變動時 silently 吞下）
  # 產生：pd.date_range(start, end, freq="ME").strftime("%Y-%m-%d").tolist()
  train_snap_dates:
    - "2025-01-31"
    - "2025-02-28"
    - "2025-03-31"
    - "2025-04-30"
    - "2025-05-31"
    - "2025-06-30"
    - "2025-07-31"
    - "2025-08-31"
    - "2025-09-30"
    - "2025-10-31"
  calibration_snap_dates: ["2025-11-30"]
  val_snap_dates: ["2025-12-31"]
  test_snap_dates: ["2026-01-31"]
```

（日期沿用現行 `parameters_dataset.yaml` 範圍 `2025-01-31 ~ 2025-10-31`，共 10 個月底；訓練窗實際長度由 PM 決策，不在此 spec 範圍）

### 2. 新 helper：`pipelines/dataset/nodes_shared.py`

放進既有檔案（不新開 `_shared.py`），與 `validate_date_splits` 並列：

```python
def collect_dataset_snap_dates(parameters: dict) -> list[pd.Timestamp]:
    """Return sorted union of train/cal/val/test snap_dates as pd.Timestamps.

    Single source of truth for "which snap_dates does the dataset pipeline use".
    Used by apply_preprocessor_to_features (all splits) — fit_preprocessor_metadata
    deliberately uses only train_snap_dates to prevent val/test leakage into the
    category-mapping fit.
    """
    ds = parameters["dataset"]
    dates: set[pd.Timestamp] = set()
    dates.update(pd.Timestamp(d) for d in ds["train_snap_dates"])
    dates.update(pd.Timestamp(d) for d in ds.get("calibration_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("val_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("test_snap_dates", []))
    return sorted(dates)
```

語意：
- `train_snap_dates` 必填（缺則 KeyError）
- `cal/val/test_snap_dates` 用 `.get(..., [])`（calibration disabled 情境）
- 回傳 `pd.Timestamp` list 而非 string，呼叫端直接餵 `.isin()` / Spark `F.lit()`

### 3. `validate_date_splits` 簡化

```python
def validate_date_splits(parameters: dict) -> None:
    ds = parameters.get("dataset", {})
    sets = {
        "train":       set(str(d) for d in ds.get("train_snap_dates", [])),
        "calibration": set(str(d) for d in ds.get("calibration_snap_dates", [])),
        "val":         set(str(d) for d in ds.get("val_snap_dates", [])),
        "test":        set(str(d) for d in ds.get("test_snap_dates", [])),
    }
    overlaps = []
    names = list(sets.keys())
    for i, a in enumerate(names):
        for b in names[i+1:]:
            common = sets[a] & sets[b]
            if common:
                overlaps.append(f"{a} & {b}: {sorted(common)}")
    if overlaps:
        raise ValueError(f"Date splits overlap: {'; '.join(overlaps)}")
```

### 4. `select_train_keys` 改動（pandas + spark）

```python
# Before (pandas)
start = pd.Timestamp(ds["train_snap_date_start"])
end = pd.Timestamp(ds["train_snap_date_end"])
all_dates = sample_pool[time_col].unique()
train_dates = [d for d in all_dates if start <= pd.Timestamp(d) <= end]

# After (pandas + spark 共用語意)
train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]
```

`select_keys()` 內部已有 `isin(target_dates)` 過濾，所以即使 sample_pool 沒有某個 train_snap_date 也只會少 row、不會壞 —— 真正的缺值偵測由 `fit_preprocessor_metadata` 負責（有意義的錯誤點）。

### 5. `fit_preprocessor_metadata` 改動（pandas + spark）

```python
# After (pandas)
train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

# 缺值 raise
ft_dates = set(feature_table[time_col].unique())
missing = sorted(set(train_dates) - ft_dates)
if missing:
    raise ValueError(
        f"feature_table missing required train_snap_dates: "
        f"{[d.strftime('%Y-%m-%d') for d in missing]}"
    )

train_features = feature_table[feature_table[time_col].isin(train_dates)]
```

Spark 對應改 `F.col(time_col).isin(train_dates)`；缺值偵測：
```python
ft_dates = {row[time_col] for row in feature_table.select(time_col).distinct().collect()}
```
（cardinality 小，~12-52 個 date，cost 可忽略）

### 6. `apply_preprocessor_to_features` 改動（pandas + spark）

```python
# After (pandas)
needed_dates = collect_dataset_snap_dates(parameters)

ft_dates = set(feature_table[time_col].unique())
missing = sorted(set(needed_dates) - ft_dates)
if missing:
    raise ValueError(
        f"feature_table missing required snap_dates: "
        f"{[d.strftime('%Y-%m-%d') for d in missing]}"
    )

result = feature_table[feature_table[time_col].isin(needed_dates)]
result = result[keep_cols].copy()
# ... 後續 encode_categoricals 不變
```

Spark 對應改 `feature_table.filter(F.col(time_col).isin(needed_dates)).select(*keep_cols)`，缺值偵測同 fit。

## Hash & Cache 影響

### Dataset variant hash 自然跳號

`base_dataset_version` hash 包含 schema + 非 sampling 的 dataset params。`train_snap_date_start/end` 變成 `train_snap_dates: [...]` 後，flatten params 的字串內容變了，hash 自動跳號。Hash 邏輯本身**不需動**。

連帶影響：
- `train_variant_id` hash 包含 train-sampling subset → 不直接受影響
- `calibration_variant_id` 同理
- `model_version_id = f(training_params, dataset_variant_ids)` → 因 base_dataset_version 變而連帶變

實務上：升級後第一次跑 `dataset` pipeline 會產生新的 `base_dataset_version_<新hash>`，不 reuse 舊版。要 reproduce 舊版仍可顯式 `--model-version <old_id>`，因為 `manifest.json` 沒被刪。

### 既有 cache 處理

| Cache | 類型 | 處理 |
|---|---|---|
| `preprocessed_feature_table` | MemoryDataset（不持久化） | 重跑自動產生，無需手動清 |
| `cached_*_model_input` | ParquetDataset on host-local fs，path 含 `base_dataset_version` | base_dataset_version 變 → cache path 自然不同 → 自動產生新檔；舊檔變 orphan |

**結論**：cache 不需手動 invalidate；舊 cache 變成 orphan parquet 檔，可選擇日後清理（不在這個 spec 範圍）。

### Manifest backwards compatibility

`manifest.json` 的 schema 不變，只是新值。舊 manifest 仍可手動讀，不影響任何既有檔案。

## Testing Strategy

### 既有測試的 fixture 機械替換

把 `train_snap_date_start/end` 換成 `train_snap_dates: [...]`：

| 測試檔範圍 | 影響 |
|---|---|
| `tests/pipelines/dataset/test_*.py`（pandas + spark） | parameters fixture |
| `tests/pipelines/dataset/test_validate_date_splits.py` | 移除 range case；新增 list overlap case |
| `tests/preprocessing/test_*.py` | fit/apply 的 parameters fixture |
| `tests/pipelines/test_pipeline_versioning.py` | dataset variant hash assertion 值會變 |
| dev-cluster 合成資料 12 個月底 fixture | 對應 12 entries 的 list |

絕大多數是機械式替換，邏輯不變。

### 新增 test cases

**A. 上游有週中 row 時，filter 正確排除**
- Fixture：feature_table 同時含 `2025-01-31`（月底）與 `2025-01-15`（週中）
- 期待：`apply_preprocessor_to_features` 輸出只有月底 row
- pandas + spark 各一份

**B. fit 與 apply 的 filter 範圍不同**
- Fixture：feature_table 含 train + val + test 各一個月底
- 驗證：`fit_preprocessor_metadata` 的 category_mappings 只反映 train row；`apply_preprocessor_to_features` 輸出涵蓋 train+val+test
- 防止未來重構誤把兩者統一

**C. `collect_dataset_snap_dates` 單元測試**
- 純函數測試：去重、sorted、空 cal/val/test 處理、calibration disabled 情境
- 不需 spark/pandas，純 dict in / list out

**D. `validate_date_splits` 純 list overlap**
- train ∩ cal、train ∩ val、cal ∩ val、val ∩ test 各一個 case
- 三方 overlap（train ∩ cal ∩ val）
- 全互斥（happy path）

**E. 缺值 raise**
- E1: feature_table 缺某個 train_snap_date → `fit_preprocessor_metadata` raise（具名列出缺失日期）
- E2: feature_table 缺某個 cal/val/test snap_date → `apply_preprocessor_to_features` raise
- pandas + spark 各一份

### 不需新增

- Cross-backend cross-validation（`test_spark_pandas_cross_validation.py`）—— filter 改動後仍應通過，跑一次確認即可
- Integration test —— `tests/scenarios/` 自動覆蓋 filter 行為

## 一次性升級 Runbook

按時間順序：

1. 改 `parameters_dataset.yaml`（必要）
2. 改 code（fit / apply / select_train_keys / validate_date_splits + helper）
3. 全跑 unit tests + integration scenarios
4. dev-cluster end-to-end smoke：`dataset` → `training` → `inference` → `evaluation` 各跑一次，確認新 hash 串得起來
5. 確認 `latest` symlink 指向新 `base_dataset_version`（不需手動，跑完就更新）

## 文件更新

- **`CLAUDE.md`**：「12 個月月底快照」→「**Training**：N 個月底 snapshot（顯式 `train_snap_dates` list 配置）」。其他段落（dev-cluster / SPARK_CONF_DIR）不動。
- **`conf/base/parameters_dataset.yaml`** 註解：把 `# --- Train 日期範圍 ---` 換成 list 用法說明，加 helper snippet（產生月底 list 的 one-liner）。
- 本 design doc

不改：`spec/`、`graphify-out/`（merge 後另行 rebuild graph，CLAUDE.md 已有規則）；既有 model lineage 文件、manifest schema 說明（schema 沒變）。

## Out-of-Scope（後續 spec）

- 週頻 inference 配置切換（`parameters_inference.yaml` 改週 list、ETL 排程改每週）
- 週中 distribution shift monitoring
- 訓練 cadence 是否改為「每週訓練、月底 snap」這類混合模式
- 既有 orphan cache 清理腳本
