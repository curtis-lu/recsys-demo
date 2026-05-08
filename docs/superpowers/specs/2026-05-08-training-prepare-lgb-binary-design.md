# Training Pipeline：cache layer 改回傳 handle，新增 lgb.Dataset binary 預備層

- 日期：2026-05-08
- 影響範圍：`src/recsys_tfb/io/handles.py`（新檔）、`src/recsys_tfb/io/extract.py`（新檔，從 nodes.py 抽出）、`src/recsys_tfb/models/base.py`、`src/recsys_tfb/models/lightgbm_adapter.py`、`src/recsys_tfb/models/calibrated_adapter.py`、`src/recsys_tfb/pipelines/training/nodes.py`、`src/recsys_tfb/pipelines/training/pipeline.py`、`conf/base/parameters*.yaml`（移除 `cache.enabled`）、`tests/test_pipelines/test_training/test_cache_nodes.py`（重寫）、`tests/test_io/test_handles.py`（新檔）、`tests/test_models/test_lightgbm_adapter.py`（新增 case）、`tests/test_models/test_calibrated_adapter.py`（新增 case）、`tests/test_pipelines/test_training/test_pipeline_integration.py`（新檔）
- 狀態：Draft（待 user review）
- 接續：`2026-05-07-training-cache-hdfs-copy-design.md`（cache layer 持久化機制；本 spec 在其上新增 algorithm-native binary 層 + 修記憶體生命週期問題）

## 動機

現行 cache layer（`_cache_or_passthrough`）的契約是「cache node 輸出 pandas DataFrame，由 framework auto-MemoryDataset 接管」。這個契約有兩個 production 問題：

### 問題 1：driver memory OOM（exit code 137）

Pipeline DAG：

```
cache_train_model_input        ──┐
cache_train_dev_model_input    ──┼──→ tune → train → calibrate → evaluate
cache_val_model_input          ──┤
cache_calibration_model_input  ──┘
```

四個 cache node 之間沒有 dependency edge，topological sort 把它們全排在 `tune_hyperparameters` 之前。當 driver 跑 `cache_val_model_input` 時，記憶體裡同時駐留：

- `cached_train_model_input`（pandas）
- `cached_train_dev_model_input`（pandas）
- 正在 collect 的 `cached_val_model_input`（pandas + Spark→pandas peak）
- 即將進來的 `cached_calibration_model_input`

且 `cached_val_model_input` 的 last consumer 是 pipeline 最後一個 node（`evaluate_model`）→ MemoryDataset auto-release 整個 run 都不會被觸發。實測 driver memory 64GB 在 `cache_val_model_input` 階段被 OOM-killer 收掉。

### 問題 2：Optuna trial 重複 lgb.Dataset binning

`LightGBMAdapter.train`（`lightgbm_adapter.py:31-32`）在每個 Optuna trial 都重建 `lgb.Dataset`，即每個 trial 都重新做 histogram binning。N=100 trial 的 HPO 等於 binning 跑 100 次。這個成本完全可以靠 binary cache 攤掉。

### 額外副作用：sub-optimal categorical 處理

`_extract_Xy` 把 `prod_name` 等 deferred categorical 用 `category_mappings` 轉成 int codes 餵給 lgb，但 `lgb.Dataset` **沒有**收到 `categorical_feature=` 參數，lgb 把這些 int 欄位當有序數值處理。理論上是 sub-optimal split，metric 上有改進空間。

## 約束（hard constraints）

1. **演算法可插拔**：`ModelAdapter` 抽象不能崩；未來換 XGBoost / Catboost 不應該動 pipeline 結構
2. **不依賴 `spark.master` 模式**：cache layer 自帶 HDFS↔local copy（沿用 `2026-05-07` spec 的 mechanics）
3. **保留 cache 的 between-run 收益**：跨 pipeline run 仍要能 skip Hive scan + skip lgb binning
4. **`ParquetDataset(backend=pandas)` 即將棄用**：新邏輯不依附在這個 backend 上

## 範圍

### 本次納入

1. 新增 `src/recsys_tfb/io/handles.py`：`ParquetHandle` / `LgbDatasetHandle` dataclasses
2. `src/recsys_tfb/io/extract.py`：從 `nodes.py` 抽出 `_extract_Xy` 改吃 `ParquetHandle`
3. `ModelAdapter` 加 abstract method `prepare_train_inputs(...)`
4. `LightGBMAdapter` 實作 `prepare_train_inputs`：parquet→numpy→`lgb.Dataset(..., categorical_feature=cat_idx).save_binary(...)`，含 binning reference linkage
5. `CalibratedModelAdapter.prepare_train_inputs` raise `NotImplementedError`
6. `pipelines/training/nodes.py` 重寫：4 個 cache node 改回傳 `ParquetHandle`；新增 `prepare_lgb_train_inputs` node；下游 4 個 node 改吃 handle
7. `pipelines/training/pipeline.py` 加 `prepare_lgb_train_inputs` 進 DAG
8. 移除 `cache.enabled` 設定（廢 dev passthrough）
9. 加上 `categorical_feature=` 給 lgb.Dataset（**注意：metric drift 風險，分 PR**）

### 本次不納入

- `categorical_feature=` 的 metric 驗收作業：屬於 PR2 的事，PR1 byte-equal 確認後才送 PR2
- 廢除 `_extract_Xy` 內 deferred categorical encoding：保留 int code path 不動，只額外傳 categorical_feature 給 lgb
- val / calibration 的 algorithm-native cache：YAGNI（val 走 predict、calibration 走 sklearn，binary 沒幫助）
- `lgb.Dataset` 構建參數（`max_bin` 等）變動的自動 cache invalidation：本次只用預設值，未來真要 tune 再加 hash subdir
- pandas backend 廢除：跟本 refactor 解耦
- Optuna trial 內部 lgb.Dataset 物件級重用：本次只解 binning 級重用（從 disk 載 .bin），物件級重用是另一層議題

## 設計

### 資料流與 DAG

```
HDFS Hive
   │
   ▼
cache_train_model_input        ─→ train_parquet_handle      ──┐
cache_train_dev_model_input    ─→ train_dev_parquet_handle   ─┼──→ prepare_lgb_train_inputs ──→ (train_lgb_handle, train_dev_lgb_handle)
cache_val_model_input          ─→ val_parquet_handle           │             │     │
cache_calibration_model_input  ─→ calibration_parquet_handle   │             │     │
                                                                ▼             ▼     ▼
                                            tune_hyperparameters(train_lgb, dev_lgb, val_parquet, ...) → best_params
                                                                              │     │
                                                                              ▼     ▼
                                                              train_model(train_lgb, dev_lgb, best_params, ...) → model
                                                                              ▼
                                                              calibrate_model(model, calibration_parquet, ...) → model'  [optional]
                                                                              ▼
                                                              evaluate_model(model, val_parquet, ...) → evaluation_results
```

### 三層形式（vs `2026-05-07` spec）

| 層 | 形式 | 演算法綁定？ | 本次變更 |
|---|---|---|---|
| 持久化（disk）— parquet | local parquet on driver | ❌ | mechanism 不變；輸出由 DataFrame 改為 `ParquetHandle`（path）|
| 持久化（disk）— algorithm-native | `lgb.Dataset.bin` | ✅ | **新層**：由新 prepare node + adapter 產生 |
| transient（in-memory） | pandas / numpy | ❌ | 改 lazy：下游各自 `to_pandas` 在 function scope 內，離開即 GC |
| 演算法 native（runtime） | `lgb.Dataset` 物件 | ✅ | 從 `.bin` 載入，不重 binning |

第二層是新加的，由 `LightGBMAdapter` 負責。`xgb.QuantileDMatrix` 之類未來自然落在這層（同位、但 algorithm-namespaced subdir）。

### Handle 型別契約

`src/recsys_tfb/io/handles.py`：

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ParquetHandle:
    path: str

    def to_pandas(self):
        import pandas as pd
        return pd.read_parquet(self.path, engine="pyarrow")


@dataclass(frozen=True)
class LgbDatasetHandle:
    bin_path: str
    role: str  # "train" | "train_dev"

    def load(self, reference=None):
        import lightgbm as lgb
        return lgb.Dataset(self.bin_path, reference=reference)
```

`frozen=True` 確保 handle 在 framework MemoryDataset 內不可被誤改。Handle 是 lightweight，由 framework auto-MemoryDataset 接管不會引發記憶體問題。

### Adapter 契約

`src/recsys_tfb/models/base.py`：

```python
@abstractmethod
def prepare_train_inputs(
    self,
    train_handle: ParquetHandle,
    train_dev_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    cache_dir: str,
) -> tuple[LgbDatasetHandle, LgbDatasetHandle]:
    """Materialize algorithm-native train/dev datasets to disk; return handles.

    Skip-if-exists: if cache_dir already has a valid _SUCCESS marker, just
    return handles without re-building.
    """
```

`LightGBMAdapter.prepare_train_inputs` 行為（pseudo）：

```python
def prepare_train_inputs(self, train_h, dev_h, prep_meta, parameters, cache_dir):
    lgb_dir = Path(cache_dir) / "lgb"
    success = lgb_dir / "_SUCCESS"

    if success.exists():
        return (LgbDatasetHandle(str(lgb_dir / "train.bin"), "train"),
                LgbDatasetHandle(str(lgb_dir / "train_dev.bin"), "train_dev"))

    if lgb_dir.exists():
        shutil.rmtree(lgb_dir)
    lgb_dir.mkdir(parents=True, exist_ok=True)

    X_tr, y_tr = extract_Xy(train_h, prep_meta, parameters)
    X_dev, y_dev = extract_Xy(dev_h, prep_meta, parameters)

    cat_idx = _categorical_indices(prep_meta)  # PR2 才啟用；PR1 此值為 None

    ds_train = lgb.Dataset(X_tr, label=y_tr, categorical_feature=cat_idx,
                           free_raw_data=True).construct()
    ds_train.save_binary(str(lgb_dir / "train.bin"))

    ds_dev = lgb.Dataset(X_dev, label=y_dev, reference=ds_train,
                         categorical_feature=cat_idx, free_raw_data=True).construct()
    ds_dev.save_binary(str(lgb_dir / "train_dev.bin"))

    success.touch()

    return (LgbDatasetHandle(str(lgb_dir / "train.bin"), "train"),
            LgbDatasetHandle(str(lgb_dir / "train_dev.bin"), "train_dev"))
```

`free_raw_data=True` 比舊 `lightgbm_adapter.py:31` 改成放掉 raw 引用，配合即時刪除 `X_tr` / `X_dev` 中介本地變數，控制記憶體 peak。

`CalibratedModelAdapter.prepare_train_inputs`：

```python
def prepare_train_inputs(self, *args, **kwargs):
    raise NotImplementedError(
        "CalibratedModelAdapter wraps a trained adapter; prepare_train_inputs "
        "must be called on the underlying adapter (e.g. LightGBMAdapter) before "
        "calibration is applied."
    )
```

### Pipeline 改動

`pipelines/training/pipeline.py::create_pipeline`：

```python
nodes = [
    Node(cache_train_model_input,
         inputs=["train_model_input", "parameters"],
         outputs="train_parquet_handle"),
    Node(cache_train_dev_model_input,
         inputs=["train_dev_model_input", "parameters"],
         outputs="train_dev_parquet_handle"),
    Node(cache_val_model_input,
         inputs=["val_model_input", "parameters"],
         outputs="val_parquet_handle"),
    # ... calibration cache 條件加入

    Node(prepare_lgb_train_inputs,
         inputs=["train_parquet_handle", "train_dev_parquet_handle",
                 "preprocessor", "parameters"],
         outputs=["train_lgb_handle", "train_dev_lgb_handle"]),

    Node(tune_hyperparameters,
         inputs=["train_lgb_handle", "train_dev_lgb_handle",
                 "val_parquet_handle", "preprocessor", "parameters"],
         outputs="best_params"),
    Node(train_model,
         inputs=["train_lgb_handle", "train_dev_lgb_handle",
                 "best_params", "preprocessor", "parameters"],
         outputs=train_model_output),
    # ... evaluate / calibrate / log_experiment
]
```

下游 node 改造：

- `tune_hyperparameters`：trial 內 `ds_train = train_lgb.load()`、`ds_dev = train_dev_lgb.load(reference=ds_train)`，直接餵 `lgb.train`；val 在 `extract_features` step 內 `val_parquet.to_pandas()` → `extract_Xy` → numpy（function scope 結束 GC）
- `train_model`：同上，但只用 train + dev handle
- `calibrate_model`：簽名改 `calibration_parquet_handle`，內部 `to_pandas` → `extract_Xy` → numpy → `fit_calibrator`
- `evaluate_model`：簽名改 `val_parquet_handle`，內部同上

`adapter.train(X_tr, y_tr, X_dev, y_dev, params)` 簽名**不變**——但 caller（tune / train）改成預先 load lgb.Dataset 後傳給一個新 internal helper，或更簡單：在 `LightGBMAdapter.train` 加 keyword-only path，使其能接受預載的 `lgb.Dataset` 物件（向後相容 numpy 輸入）。本 spec 的方向採後者：

```python
class LightGBMAdapter:
    def train(self, X_train, y_train, X_val, y_val, params, *,
              train_dataset=None, val_dataset=None):
        if train_dataset is None:
            train_dataset = lgb.Dataset(X_train, label=y_train, free_raw_data=False)
        if val_dataset is None:
            val_dataset = lgb.Dataset(X_val, label=y_val, reference=train_dataset,
                                      free_raw_data=False)
        # ... lgb.train(...)
```

`tune_hyperparameters` 在 trial 內把預載 Dataset 用 keyword 傳入；caller 傳 `X_train=None, y_train=None`，adapter 在 `train_dataset is not None` 分支完全忽略 numpy 參數（不檢查、不引用）。其他 adapter（未來 XGBoost 等）若不採用 prepare-once 模式仍可正常吃 numpy。實作驗收：

- `LightGBMAdapter.train` 內第一行明確判斷 `train_dataset is not None`，後續所有路徑分流
- `tune_hyperparameters` / `train_model` 完全不再構建 numpy X_tr / y_tr / X_dev / y_dev（`extract_Xy` 對 train / dev 的呼叫被 prepare 階段吃掉）
- 對 `val_model_input` 仍須在 trial 內 `extract_Xy` 取 numpy（用於 predict + AP）

### Cache layout

```
<cache_root>/
└── <base_dataset_version>/
    ├── train_variants/<train_variant_id>/
    │   ├── train_model_input.parquet/
    │   │   └── _SUCCESS
    │   ├── train_dev_model_input.parquet/
    │   │   └── _SUCCESS
    │   └── lgb/                                ← 新
    │       ├── train.bin
    │       ├── train_dev.bin
    │       └── _SUCCESS
    ├── calibration_variants/<calibration_variant_id>/
    │   └── calibration_model_input.parquet/
    │       └── _SUCCESS
    ├── val_model_input.parquet/
    │   └── _SUCCESS
    └── test_model_input.parquet/
        └── _SUCCESS
```

`lgb/` 跟它依賴的 train + train_dev parquet 同層，自然 lifecycle co-location。algorithm-namespaced 子層（`lgb/`、未來 `xgb/`）以子目錄區分。

### Cache invalidation

| 變動 | 機制 | 自動 invalidate？ |
|---|---|---|
| 上游 Hive 資料變 | `<base_dataset_version>` hash 變 → 新目錄樹 | ✅ |
| Train sampling 變 | `<train_variant_id>` 變 | ✅ |
| Preprocessor `feature_columns` / `categorical_columns` 變 | 由 `<base_dataset_version>` 涵蓋 | ✅（**前提：spec 假設 base_dataset_version 把 preprocessor 納入 hash；implementation 第一步須驗證**） |
| LGB 構建層參數（`max_bin` 等）變 | 不在 path | ❌（手動 `rm -rf lgb/`） |

### Dev / passthrough 變動

廢除 `cache.enabled=false`（不再支援 in-memory passthrough）。所有環境（含 unit test）一律走 parquet 持久化，dev / test 使用 `cache.local_root=tmp_path`。

理由：
- 兩條 code path（passthrough / cached）已造成下游 type 分支困擾；單一 code path 簡化心智負擔
- 合成 unit test 資料極小，tmp parquet IO 成本可忽略
- 現有 dev workflow 走 `~/dev-cluster` 本來就 cache.enabled=true，行為一致

## 部署順序：**兩個 PR**

### PR1：結構性 refactor（不動 metric）

- 全部上述程式改動
- `categorical_feature=cat_idx` **設為 `None`**（保留 hook 但不啟用）
- 跑同隨機種子的 byte-equal regression：合成資料 mAP / per-product AP 跟 main 完全一致
- 風險回退：revert PR commit，既有 parquet cache 仍生效，不需清資料

### PR2：啟用 categorical_feature=

- 改 `_categorical_indices` 真的回傳 cat 欄位 index
- 跑 before/after 合成資料 metric baseline 比對
- 若 mAP drift > 1% 標記為「預期 metric drift」並要 PM 確認接受
- 若 mAP **下降** → block，先驗 cat index 是否餵錯
- 風險回退：revert PR2 + 手動 `rm -rf <cache_root>/*/train_variants/*/lgb/`

## 測試

### Unit

- `tests/test_io/test_handles.py`（新檔）：`ParquetHandle.to_pandas()` 對齊 `pd.read_parquet`；`LgbDatasetHandle.load()` 載入正常 + 帶 reference 的版本；frozen dataclass 不可改 path
- `tests/test_models/test_lightgbm_adapter.py`（新增 case）：
  - `prepare_train_inputs` 寫出 `_SUCCESS` + train.bin + train_dev.bin
  - Cache hit 行為：第二次呼叫時 mock `lgb.Dataset.construct` 計次驗證未被呼叫
  - Partial cache：刪 `_SUCCESS` 後重新呼叫 → 重建
  - PR2 階段：assert `categorical_feature_` 含預期欄位
  - Reference linkage：dev Dataset 的 binning thresholds 跟 train 一致
- `tests/test_models/test_calibrated_adapter.py`（新增 case）：`prepare_train_inputs` 噴 `NotImplementedError` 帶清楚訊息
- `tests/test_pipelines/test_training/test_cache_nodes.py`（**整檔重寫**）：
  - `_materialize_parquet_handle` 寫出 parquet + `_SUCCESS`，回傳 ParquetHandle
  - Cache hit：mock `copy_hdfs_to_local` 計次驗證未被呼叫
  - Partial cache：parquet 目錄存在但無 `_SUCCESS` → rmtree 重建
  - 刪除舊 `_cache_or_passthrough` / `cache.enabled=false` 相關 case

### Integration

- `tests/test_pipelines/test_training/test_pipeline_integration.py`（新檔）：
  - 跑完整 training pipeline（合成資料 + tmp_path 為 cache_root）
  - Assert `lgb/train.bin` + `lgb/train_dev.bin` + `lgb/_SUCCESS` 都存在
  - 第二次 run 同 cache_root → `lgb.Dataset.construct` 沒被呼叫（使用 unittest.mock 計次）
  - `evaluation_results` 兩次 run 完全一致

### Regression（非自動測試，PR1 → PR2 過渡用）

- PR1 merge 前：在合成資料上跑 main branch 的 training pipeline，存 `evaluation_results.json` 為 baseline
- PR1：assert metric **byte-equal** baseline
- PR2：assert metric drift 在預期範圍 + 人工 review 接受

## 風險

| 風險 | 機率 | mitigation |
|---|---|---|
| `lgb.Dataset.save_binary` 跟 `reference=` 在 lgb 4.6.0 的兼容性問題 | 低 | unit test 含 round-trip assertion；pin lgb 版本 |
| Preprocessor metadata 沒被 `base_dataset_version` 完整 hash → cache stale | 中 | implementation 第一步先驗證；若不足則補 hash 或加 fingerprint subdir |
| `categorical_feature=` 餵錯 column index | 中 | unit test 用真實 categorical column；PR2 強制要 PM 確認 metric |
| 廢 `cache.enabled` 後既有開發者本地 workflow 斷掉 | 低 | CHANGELOG 寫清楚；提供 dev 用 tmpdir cache_root 範例 |
| ABC abstract method 加上去後其他繼承樹（mock adapter）破掉 | 低 | grep 所有 `class.*ModelAdapter` subclass 補實作 |
| `prepare_lgb_train_inputs` 仍有記憶體 peak（X_tr + X_dev + ds_train + ds_dev 同時在）| 中 | adapter 實作內按 `build train → save → free → build dev with reference → save` 順序，不在同 scope hold 兩份 raw + 兩份 Dataset；`free_raw_data=True` |
| Adapter `train()` signature 加 keyword-only args 後 caller 沒對齊 | 低 | grep 所有 `adapter.train(` 用法；keyword-only 確保誤呼舊 positional 仍工作 |

## 相依與後續

- 本 spec 的成立前提：`base_dataset_version` hash 已涵蓋 preprocessor 相關設定（`feature_columns` / `categorical_columns` 等）。實作前置作業：grep `pipelines/dataset/` 內的 `base_dataset_version` hash function（依現有 commit 訊息推測為 hash_dataset_params 之類），確認 preprocessor 欄位有納入。若未納入，本 spec scope 擴增為先補 hash 再做後續工作。
- PR1 merge 後 → PR2 啟用 cat handling
- 未來如果新增 XGBoost adapter，`xgb/` 子目錄與 `XGBoostAdapter.prepare_train_inputs` 的開發路徑由本 spec 鋪好
- 未來若需要 tune `lgb.Dataset` 構建層參數（`max_bin` 等），須加 `lgb/<construct_hash>/` 子層
