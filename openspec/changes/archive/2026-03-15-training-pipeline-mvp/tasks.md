## 1. 設定與 Catalog 配置

- [x] 1.1 新增 `conf/base/parameters_training.yaml`：定義 n_trials、early_stopping_rounds、num_iterations、搜索空間、MLflow 設定（experiment_name、tracking_uri）
- [x] 1.2 更新 `conf/base/catalog.yaml`：新增 best_params（JSONDataset）與 evaluation_results（JSONDataset）兩個 entry
- [x] 1.3 更新 `conf/production/catalog.yaml`：新增對應的 HDFS 路徑 entries

## 2. mAP 評估函數

- [x] 2.1 在 `src/recsys_tfb/pipelines/training/nodes.py` 實作 `compute_average_precision` 內部函數：單一 query 的 AP 計算
- [x] 2.2 實作 `evaluate_model` 節點函數：接收 model、X_val、y_val、val_set、parameters，計算 overall mAP 與 per-product AP，回傳 evaluation_results dict
- [x] 2.3 撰寫 mAP 計算的單元測試：完美排序（AP=1.0）、隨機排序、全 0 label query 排除、per-product 切片

## 3. Optuna 超參數搜索

- [x] 3.1 實作 `tune_hyperparameters` 節點函數：定義 Optuna objective（每個 trial 訓練 LightGBM + 計算 train_dev mAP），回傳 best_params dict
- [x] 3.2 撰寫超參數搜索測試：驗證回傳格式正確、可重現性（同 seed 同結果）

## 4. LightGBM 模型訓練

- [x] 4.1 實作 `train_model` 節點函數：使用 best_params 訓練 LightGBM Booster，含 early stopping（train_dev 為 eval set）
- [x] 4.2 撰寫模型訓練測試：驗證回傳 Booster 物件、predict 輸出在 [0,1]、early stopping 生效

## 5. MLflow 實驗記錄

- [x] 5.1 實作 `log_experiment` 節點函數：記錄 params、metrics、model artifact 至 MLflow
- [x] 5.2 撰寫 MLflow 記錄測試：驗證 run 包含完整 params 與 metrics

## 6. Pipeline 組裝與整合

- [x] 6.1 建立 `src/recsys_tfb/pipelines/training/pipeline.py`：定義 4 節點 DAG（tune → train → evaluate → log）
- [x] 6.2 更新 `src/recsys_tfb/pipelines/training/__init__.py`：import 並回傳 pipeline
- [x] 6.3 撰寫 pipeline 整合測試：驗證節點順序、輸入輸出連接正確
- [x] 6.4 端到端測試：先執行 dataset pipeline 再執行 training pipeline，驗證所有 artifact 產出
