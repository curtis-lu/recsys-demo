## ADDED Requirements

### Requirement: ModelAdapter ABC 介面
系統 SHALL 提供 `ModelAdapter` 抽象基類（`src/recsys_tfb/models/base.py`），定義以下抽象方法：`train`、`predict`、`save`、`load`、`feature_importance`、`log_to_mlflow`。所有演算法 adapter SHALL 繼承此 ABC 並實作所有方法。

#### Scenario: ABC 不可直接實例化
- **WHEN** 嘗試直接實例化 `ModelAdapter()`
- **THEN** SHALL 拋出 `TypeError`

#### Scenario: 子類必須實作所有抽象方法
- **WHEN** 子類未實作任一抽象方法（如缺少 `predict`）
- **THEN** 實例化時 SHALL 拋出 `TypeError`

### Requirement: ModelAdapter.train 介面
`train(X_train, y_train, X_val, y_val, params)` SHALL 接收 numpy array 與參數 dict，在 adapter 內部完成訓練。訓練後 adapter SHALL 持有已訓練模型。params SHALL 包含演算法參數（objective、metric 等）及訓練控制參數（num_iterations、early_stopping_rounds）。

#### Scenario: train 後可 predict
- **WHEN** 呼叫 `adapter.train(X, y, X_val, y_val, params)` 完成
- **THEN** 後續呼叫 `adapter.predict(X)` SHALL 回傳有效的機率值 array

#### Scenario: 演算法特有格式在內部轉換
- **WHEN** 傳入 numpy array 作為 X_train
- **THEN** adapter 內部 SHALL 自行建立演算法特有資料格式（如 lgb.Dataset），外部不需處理

### Requirement: ModelAdapter.predict 介面
`predict(X)` SHALL 接收 numpy array，回傳 `np.ndarray` 機率值。

#### Scenario: 回傳型別為 numpy array
- **WHEN** 呼叫 `adapter.predict(X)` 且 X 為 (n, m) shape 的 numpy array
- **THEN** SHALL 回傳 shape 為 (n,) 的 `np.ndarray`

#### Scenario: 機率值範圍
- **WHEN** adapter 為二元分類模型
- **THEN** predict 回傳值 SHALL 介於 [0, 1]

### Requirement: ModelAdapter.save 和 load 介面
`save(filepath)` SHALL 將模型儲存至指定路徑。`load(filepath)` SHALL 從指定路徑載入模型至 adapter 內部。

#### Scenario: save 後 load 結果一致
- **WHEN** adapter 訓練後呼叫 `save(path)`，再建立新 adapter 呼叫 `load(path)`
- **THEN** 新 adapter 的 `predict(X)` 結果 SHALL 與原 adapter 完全一致

### Requirement: ModelAdapter.feature_importance 介面
`feature_importance()` SHALL 回傳 `dict[str, float]`，key 為特徵名稱，value 為重要性分數。

#### Scenario: 回傳所有特徵
- **WHEN** 模型以 m 個特徵訓練
- **THEN** feature_importance() SHALL 回傳恰好 m 個 key-value pair

### Requirement: ModelAdapter.log_to_mlflow 介面
`log_to_mlflow()` SHALL 使用演算法對應的 MLflow integration 記錄模型 artifact。

#### Scenario: LightGBM adapter 的 MLflow 記錄
- **WHEN** 呼叫 LightGBMAdapter 的 `log_to_mlflow()`
- **THEN** SHALL 呼叫 `mlflow.lightgbm.log_model()`

### Requirement: Adapter Registry
系統 SHALL 提供 `ADAPTER_REGISTRY` dict 和 `get_adapter(algorithm: str) -> ModelAdapter` 工廠函數。Registry 映射演算法名稱（字串）到 adapter class。

#### Scenario: 取得已註冊的 adapter
- **WHEN** 呼叫 `get_adapter("lightgbm")`
- **THEN** SHALL 回傳 `LightGBMAdapter` 實例

#### Scenario: 未註冊的演算法
- **WHEN** 呼叫 `get_adapter("unknown_algo")`
- **THEN** SHALL 拋出 `ValueError` 並列出可用的演算法名稱

### Requirement: LightGBMAdapter 實作
系統 SHALL 提供 `LightGBMAdapter`（`src/recsys_tfb/models/lightgbm_adapter.py`），繼承 `ModelAdapter`，封裝所有 LightGBM 特有邏輯。

#### Scenario: train 使用 early stopping
- **WHEN** params 包含 `early_stopping_rounds: 10` 且模型在 validation set 上連續 10 輪未改善
- **THEN** 訓練 SHALL 提前停止

#### Scenario: save 使用原生格式
- **WHEN** 呼叫 `save(filepath)`
- **THEN** SHALL 使用 `lgb.Booster.save_model()` 儲存為 LightGBM 原生 .txt 格式

#### Scenario: load 使用原生格式
- **WHEN** 呼叫 `load(filepath)`
- **THEN** SHALL 使用 `lgb.Booster(model_file=filepath)` 載入模型
