## MODIFIED Requirements

### Requirement: Optuna 超參數搜索
系統 SHALL 提供 `tune_hyperparameters` 純函數，接收 X_train、y_train、X_train_dev、y_train_dev 與 parameters，使用 Optuna 搜索最佳超參數。SHALL 根據 `parameters.training.algorithm` 建立對應的 ModelAdapter 實例進行訓練與評估。優化目標為 train_dev 上的 mAP。搜索空間與 trial 數量 SHALL 由 parameters 設定。函數 SHALL 回傳 best_params dict。

#### Scenario: 基本超參數搜索
- **WHEN** 提供合成資料的 X_train、y_train、X_train_dev、y_train_dev，parameters 設定 n_trials=3 且 algorithm="lightgbm"
- **THEN** 回傳 dict 包含 learning_rate、num_leaves、max_depth 等超參數，且所有值在搜索空間範圍內

#### Scenario: 透過 adapter 介面訓練
- **WHEN** tune_hyperparameters 執行每個 Optuna trial
- **THEN** SHALL 建立 ModelAdapter 實例，呼叫 `adapter.train()` 和 `adapter.predict()`，不直接使用 `lgb.Dataset` 或 `lgb.train()`

#### Scenario: 可重現性
- **WHEN** 以相同 random_seed 執行兩次超參數搜索
- **THEN** 兩次回傳的 best_params SHALL 完全一致

#### Scenario: 搜索空間可設定
- **WHEN** parameters 中指定自訂 search_space（例如 num_leaves 範圍 [16, 64]）
- **THEN** 所有 trial 的 num_leaves SHALL 在 [16, 64] 範圍內

### Requirement: 模型訓練
系統 SHALL 提供 `train_model` 純函數，接收 X_train、y_train、X_train_dev、y_train_dev、best_params 與 parameters，使用 ModelAdapter 訓練模型。SHALL 根據 `parameters.training.algorithm` 建立對應 adapter，呼叫 `adapter.train()`。函數 SHALL 回傳訓練完成的 ModelAdapter 實例。

#### Scenario: 基本模型訓練
- **WHEN** 提供合成資料與有效的 best_params，algorithm="lightgbm"
- **THEN** 回傳 ModelAdapter 實例（LightGBMAdapter），可對新資料呼叫 `predict()` 產生機率值

#### Scenario: 回傳型別為 ModelAdapter
- **WHEN** train_model 完成
- **THEN** 回傳值 SHALL 為 ModelAdapter 子類實例（非 lgb.Booster）

#### Scenario: Early stopping 生效
- **WHEN** 設定 early_stopping_rounds=10 且模型在 train_dev 上的 loss 連續 10 輪未改善
- **THEN** 訓練 SHALL 提前停止，實際迭代次數小於 num_iterations

#### Scenario: 預測輸出為機率
- **WHEN** 用訓練好的 adapter 對 X_val 做預測
- **THEN** 輸出值 SHALL 介於 [0, 1] 之間

### Requirement: mAP 評估
系統 SHALL 提供 `evaluate_model` 純函數，接收 model（ModelAdapter）、X_val、y_val、val_set 與 parameters，計算 ranking-aware mAP。SHALL 呼叫 `model.predict()` 取得預測分數。

#### Scenario: 完美排序的 mAP
- **WHEN** 模型預測分數完美排序所有正例在負例之前
- **THEN** mAP SHALL 等於 1.0

#### Scenario: 隨機排序的 mAP
- **WHEN** 模型預測分數為隨機值
- **THEN** mAP SHALL 顯著低於 1.0

#### Scenario: per-product AP 報告
- **WHEN** val_set 包含多種 prod_name
- **THEN** evaluation_results SHALL 包含 per_product_ap dict，key 為產品名稱，value 為該產品的 AP

#### Scenario: 全 0 label 的 query 處理
- **WHEN** 某個 (snap_date, cust_id) query 的所有 label 均為 0
- **THEN** 該 query SHALL 被排除於 mAP 計算，evaluation_results 中記錄排除的 query 數量

#### Scenario: Backward compatible evaluation
- **WHEN** training pipeline runs evaluate_model node
- **THEN** output dict has same structure: overall_map, per_product_ap, n_queries, n_excluded_queries

### Requirement: MLflow 實驗記錄
系統 SHALL 提供 `log_experiment` 純函數，接收 model（ModelAdapter）、best_params、evaluation_results 與 parameters，將訓練結果記錄至 MLflow。SHALL 呼叫 `model.log_to_mlflow()` 記錄模型 artifact。SHALL 額外記錄 `algorithm` 為 MLflow param。

#### Scenario: 記錄完整實驗資訊
- **WHEN** 呼叫 log_experiment 並提供所有輸入
- **THEN** MLflow run SHALL 包含：所有 best_params 作為 params、overall_map 作為 metric、model artifact（透過 adapter.log_to_mlflow()）、algorithm 作為 param

#### Scenario: MLflow tracking URI 可設定
- **WHEN** parameters 中指定 mlflow.tracking_uri
- **THEN** 記錄 SHALL 寫入指定位置

#### Scenario: 實驗名稱可設定
- **WHEN** parameters 中指定 mlflow.experiment_name
- **THEN** MLflow run SHALL 建立在指定 experiment 下

### Requirement: algorithm 和 algorithm_params 設定
`parameters_training.yaml` SHALL 包含 `training.algorithm`（字串）和 `training.algorithm_params`（dict）設定。`algorithm` 指定使用的演算法名稱，`algorithm_params` 包含傳給演算法的固定參數（如 objective、metric）。

#### Scenario: 預設 LightGBM 設定
- **WHEN** `parameters_training.yaml` 設定 `algorithm: lightgbm` 和 `algorithm_params: {objective: binary, metric: binary_logloss, verbosity: -1}`
- **THEN** training nodes SHALL 使用 LightGBMAdapter，並將 algorithm_params 傳給 adapter.train()

#### Scenario: algorithm_params 與 search_space 合併
- **WHEN** best_params 來自 Optuna 搜索，algorithm_params 來自 config
- **THEN** train_model SHALL 將兩者合併（best_params 優先），傳給 adapter.train()
