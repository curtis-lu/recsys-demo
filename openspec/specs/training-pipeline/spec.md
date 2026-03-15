## ADDED Requirements

### Requirement: Training pipeline DAG 定義
系統 SHALL 提供 `create_pipeline()` 函數，回傳包含 4 個節點的 Pipeline：tune_hyperparameters → train_model → evaluate_model → log_experiment。Pipeline 的輸入 SHALL 為 dataset pipeline 的輸出（X_train、y_train、X_train_dev、y_train_dev、X_val、y_val、val_set、parameters）。

#### Scenario: Pipeline 節點順序正確
- **WHEN** 呼叫 create_pipeline()
- **THEN** Pipeline SHALL 包含 4 個節點，拓撲排序後順序為 tune → train → evaluate → log

#### Scenario: Pipeline 可獨立執行
- **WHEN** catalog 中已有 dataset pipeline 的所有輸出
- **THEN** training pipeline SHALL 可獨立執行（不需先執行 dataset pipeline）

#### Scenario: 中間產出持久化
- **WHEN** Pipeline 執行完畢
- **THEN** best_params、model、evaluation_results SHALL 通過 catalog 持久化至磁碟

### Requirement: Training 參數設定
系統 SHALL 通過 `conf/base/parameters_training.yaml` 提供所有訓練相關設定，包含 n_trials、early_stopping_rounds、num_iterations、搜索空間、MLflow 設定。

#### Scenario: 預設參數可直接執行
- **WHEN** 使用預設 parameters_training.yaml 設定
- **THEN** training pipeline SHALL 可直接執行完成，不需額外設定

#### Scenario: 參數可覆寫
- **WHEN** 在 local/parameters_training.yaml 中設定 n_trials=5
- **THEN** 該設定 SHALL 覆寫 base 的預設值
