## MODIFIED Requirements

### Requirement: Training pipeline DAG 定義
系統 SHALL 提供 `create_pipeline()` 函數，回傳包含 5 個節點的 Pipeline：tune_hyperparameters → train_model → evaluate_model → log_experiment → compare_model_versions。Pipeline 的輸入 SHALL 為 dataset pipeline 的輸出（X_train、y_train、X_train_dev、y_train_dev、X_val、y_val、val_set、parameters）。

#### Scenario: Pipeline 節點順序正確
- **WHEN** 呼叫 create_pipeline()
- **THEN** Pipeline SHALL 包含 5 個節點，拓撲排序後順序為 tune → train → evaluate → log → compare_versions

#### Scenario: Pipeline 可獨立執行
- **WHEN** catalog 中已有 dataset pipeline 的所有輸出
- **THEN** training pipeline SHALL 可獨立執行（不需先執行 dataset pipeline）

#### Scenario: 中間產出持久化至版本目錄
- **WHEN** Pipeline 執行完畢
- **THEN** best_params、model、evaluation_results、preprocessor、category_mappings SHALL 通過 catalog 持久化至版本化時間戳目錄（由 `${model_version}` 模板變數透過 ConfigLoader `runtime_params` 解析）
