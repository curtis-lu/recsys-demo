## Context

Training 和 Inference pipeline 目前直接耦合 LightGBM API：training nodes 建立 `lgb.Dataset`、呼叫 `lgb.train()`、回傳 `lgb.Booster`；inference nodes 接收 `lgb.Booster` 呼叫 `.predict()`；I/O 使用專用 `LightGBMDataset`。這導致新增演算法（XGBoost、ranking model）需大幅修改 pipeline nodes。

現有耦合點：
- `pipelines/training/nodes.py`：直接使用 `lgb.Dataset`、`lgb.train()`，hard-code `objective: "binary"`、`metric: "binary_logloss"`
- `pipelines/inference/nodes_pandas.py` 和 `nodes_spark.py`：參數型別為 `lgb.Booster`
- `io/lightgbm_dataset.py`：專用 LightGBM I/O
- `conf/base/catalog.yaml`：model entry 使用 `LightGBMDataset` type
- `pipelines/training/nodes.py` `log_experiment()`：呼叫 `mlflow.lightgbm.log_model()`

## Goals / Non-Goals

**Goals:**
- 建立 `ModelAdapter` ABC，定義演算法無關的統一介面
- 實作 `LightGBMAdapter`，封裝所有 LightGBM 特有邏輯
- 建立 `ModelAdapterDataset` I/O，支援 `model_meta.json` sidecar，取代 `LightGBMDataset`
- 重構 training/inference nodes 透過 adapter 介面操作模型
- 將 hard-coded 的 objective/metric 移至 `parameters_training.yaml`

**Non-Goals:**
- XGBoostAdapter 實作（僅確保 ABC 不對 LightGBM 隱式依賴）
- Probability calibration（延後至獨立 phase）
- Pipeline topology 變更（維持現有 4-node training pipeline）
- Strategy 2-4 支援

## Decisions

### 1. Thin Adapter 模式（vs. Adapter as Pipeline Artifact）

採用 Thin Adapter：adapter 封裝核心操作，training nodes 保持現有結構，只替換 `lgb.*` 呼叫為 adapter 方法呼叫。

**替代方案**：Adapter as Pipeline Artifact — adapter 本身作為 pipeline 資料流物件經 Catalog 管理。需要新增 `create_adapter` node，重構 pipeline topology。

**理由**：現有 node 結構已驗證，最小改動達成目標，符合 YAGNI。

### 2. train() 為 mutable 設計

`adapter.train()` 呼叫後 adapter 持有已訓練模型（`self._booster`），不回傳 model 物件。`train_model()` node 回傳整個 adapter 實例。

**理由**：adapter 封裝模型的完整生命週期（train → predict → save），mutable 設計讓 save/predict 可以直接存取內部模型，避免額外傳遞。

### 3. predict() 統一回傳 np.ndarray

所有 adapter 的 `predict()` 一律回傳 numpy array。演算法特有的格式轉換在 adapter 內部處理。

**理由**：inference nodes 只需要機率值 array，不需要知道底層演算法。介面最簡單。

### 4. 資料轉換在 adapter.train() 內部

`lgb.Dataset` 等演算法特有格式的建立在 `adapter.train()` 內部完成，外部只傳入 numpy array。

**理由**：記憶體效率最佳（轉換在 train 內部立即發生，不需同時持有兩份）、封裝完整。

### 5. ModelAdapterDataset + model_meta.json sidecar

新增 `ModelAdapterDataset` 取代 `LightGBMDataset`。save 時寫 model 檔 + `model_meta.json`（記錄 algorithm、adapter_class）。load 時讀 meta 自動選擇正確 adapter。

**替代方案**：統一用 PickleDataset。

**理由**：保留 LightGBM 原生 .txt 格式的可讀性與跨平台相容性，同時支援未來多演算法的自動載入。

### 6. YAML config 定義搜索空間（非 adapter.suggest_hyperparameters()）

繼續用 `parameters_training.yaml` 的 `search_space` 定義 Optuna 搜索空間。adapter 不包含調參邏輯。

**理由**：符合現有模式（外部化設定），adapter 職責保持精簡。

### 7. adapter.log_to_mlflow() 封裝 MLflow 記錄

每個 adapter 提供 `log_to_mlflow()` 方法，內部呼叫對應的 `mlflow.xxx.log_model()`。

**替代方案**：統一用 `mlflow.log_artifact()`；或在 `log_experiment` node 中條件判斷。

**理由**：保留各演算法的 MLflow 原生整合（LightGBM flavor 支援 model serving），封裝在 adapter 中不污染 node 邏輯。

### 8. Adapter 註冊表（Registry）

使用簡單的 dict registry 映射 algorithm name → adapter class：

```python
ADAPTER_REGISTRY = {"lightgbm": LightGBMAdapter}
```

`get_adapter(algorithm: str) -> ModelAdapter` 工廠函數從 registry 取得 adapter class 並實例化。

**理由**：比 if/else 擴展性好，新增演算法只需在 registry 加一行。

## Risks / Trade-offs

- **[Risk] 現有 model 檔案不相容** → Migration：ModelAdapterDataset load 時若找不到 `model_meta.json`，fallback 到 LightGBM 載入（假設舊模型皆為 LightGBM）
- **[Risk] adapter.train() 在 Optuna 迴圈中重複建立 adapter** → 每個 trial 建立新 adapter 實例，記憶體壓力可控（LightGBM Booster 在 GC 後釋放）
- **[Trade-off] mutable adapter 設計** → 不適合並行訓練，但目前使用場景為循序執行，可接受
