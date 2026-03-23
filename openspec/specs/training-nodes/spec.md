## ADDED Requirements

### Requirement: Optuna 超參數搜索
系統 SHALL 提供 `tune_hyperparameters` 純函數，接收 X_train、y_train、X_train_dev、y_train_dev 與 parameters，使用 Optuna 搜索 LightGBM 最佳超參數。優化目標為 train_dev 上的 mAP。搜索空間與 trial 數量 SHALL 由 parameters 設定。函數 SHALL 回傳 best_params dict。

#### Scenario: 基本超參數搜索
- **WHEN** 提供合成資料的 X_train、y_train、X_train_dev、y_train_dev，parameters 設定 n_trials=3
- **THEN** 回傳 dict 包含 learning_rate、num_leaves、max_depth 等 LightGBM 超參數，且所有值在合理範圍內

#### Scenario: 可重現性
- **WHEN** 以相同 random_seed 執行兩次超參數搜索
- **THEN** 兩次回傳的 best_params SHALL 完全一致

#### Scenario: 搜索空間可設定
- **WHEN** parameters 中指定自訂 search_space（例如 num_leaves 範圍 [16, 64]）
- **THEN** 所有 trial 的 num_leaves SHALL 在 [16, 64] 範圍內

### Requirement: LightGBM 模型訓練
系統 SHALL 提供 `train_model` 純函數，接收 X_train、y_train、X_train_dev、y_train_dev、best_params 與 parameters，訓練 LightGBM 二元分類器。SHALL 使用 train_dev 作為 early stopping 的 eval set。函數 SHALL 回傳訓練完成的 LightGBM Booster 物件。

#### Scenario: 基本模型訓練
- **WHEN** 提供合成資料與有效的 best_params
- **THEN** 回傳 LightGBM Booster 物件，可對新資料呼叫 predict() 產生機率值

#### Scenario: Early stopping 生效
- **WHEN** 設定 early_stopping_rounds=10 且模型在 train_dev 上的 loss 連續 10 輪未改善
- **THEN** 訓練 SHALL 提前停止，實際迭代次數小於 num_iterations

#### Scenario: 預測輸出為機率
- **WHEN** 用訓練好的模型對 X_val 做預測
- **THEN** 輸出值 SHALL 介於 [0, 1] 之間

### Requirement: mAP 評估
系統 SHALL 提供 `evaluate_model` 純函數，接收 model、X_val、y_val、val_set（含 snap_date、cust_id、prod_name）與 parameters，計算 ranking-aware mAP。SHALL 以 (snap_date, cust_id) 為 query group，在每個 query 內按預測分數排序計算 Average Precision，再對所有 query 取平均。SHALL 同時計算 per-product AP。

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
- **THEN** 該 query SHALL 被排除於 mAP 計算（AP 無定義），evaluation_results 中記錄排除的 query 數量

### Requirement: MLflow 實驗記錄
系統 SHALL 提供 `log_experiment` 純函數，接收 model、best_params、evaluation_results 與 parameters，將訓練結果記錄至 MLflow。

#### Scenario: 記錄完整實驗資訊
- **WHEN** 呼叫 log_experiment 並提供所有輸入
- **THEN** MLflow run SHALL 包含：所有 best_params 作為 params、overall_map 作為 metric、model 作為 artifact

#### Scenario: MLflow tracking URI 可設定
- **WHEN** parameters 中指定 mlflow.tracking_uri
- **THEN** 記錄 SHALL 寫入指定位置

#### Scenario: 實驗名稱可設定
- **WHEN** parameters 中指定 mlflow.experiment_name
- **THEN** MLflow run SHALL 建立在指定 experiment 下

## MODIFIED Requirements

### Requirement: Metric computation delegation
training/nodes.py SHALL import `compute_ap` and `compute_map` (previously `_compute_ap` and `_compute_map`) from `recsys_tfb.evaluation.metrics` instead of defining them locally.

The `evaluate_model` function's behavior SHALL remain unchanged — same inputs, same outputs, same metric values.

#### Scenario: Backward compatible evaluation
- **WHEN** training pipeline runs evaluate_model node
- **THEN** output dict has same structure: overall_map, per_product_ap, n_queries, n_excluded_queries

#### Scenario: Identical metric values
- **WHEN** same data is evaluated before and after refactoring
- **THEN** all metric values are numerically identical

#### Scenario: Import from evaluation module
- **WHEN** training/nodes.py is inspected
- **THEN** it contains `from recsys_tfb.evaluation.metrics import compute_ap` (no local `_compute_ap` definition)


## MODIFIED Requirements

### Requirement: evaluate_model uses schema for identity columns
The `evaluate_model` function SHALL obtain identity columns from `get_schema(parameters)` instead of hard-coding `["snap_date", "cust_id", "prod_name"]`.

#### Scenario: Default identity columns
- **WHEN** called with parameters without `schema` section
- **THEN** SHALL use `["snap_date", "cust_id", "prod_name"]` (identical to current behavior)

#### Scenario: Custom identity columns
- **WHEN** called with `schema.columns.entity: ["branch_id", "cust_id"]`
- **THEN** SHALL use `["snap_date", "branch_id", "cust_id", "prod_name"]` for groupby and ranking
