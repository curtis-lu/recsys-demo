## Context

Dataset pipeline 已完成，產出 X_train/y_train（訓練集）、X_train_dev/y_train_dev（開發驗證集）、X_val/y_val（最終驗證集）三組模型就緒資料。Training pipeline 目前為空殼（`create_pipeline()` 回傳空 Pipeline）。

現有框架提供 Node、Pipeline、Runner、DataCatalog 等基礎設施，所有 I/O 通過 catalog 管理。catalog 中已定義 `model`（PickleDataset）和 `preprocessor`（PickleDataset）兩個 training 輸出項目。

MVP 採用 Strategy 1：單一 LightGBM 二元分類器，prod_name 作為特徵（已在 dataset pipeline 中編碼為整數）。

## Goals / Non-Goals

**Goals:**
- 實作 LightGBM 訓練節點，支援 early stopping（使用 train_dev set）
- 實作 Optuna 超參數搜索，以 train_dev 上的 mAP 為優化目標
- 實作 mAP 評估節點，支援 overall 與 per-product 報告
- 整合 MLflow 記錄實驗（參數、指標、模型）
- 所有訓練邏輯為純函數，遵循既有框架慣例
- 新增完整測試

**Non-Goals:**
- Strategy 2/3/4（一對多、排序層）
- precision@K、recall@K、nDCG、MRR 等進階指標（後續迭代）
- 概率校準、規則重排序
- Spark-native 訓練
- Ploomber DAG 整合
- SHAP 解釋性分析

## Decisions

### 1. 訓練節點的拆分方式

**選擇：** 拆分為四個純函數節點：
1. `tune_hyperparameters` — Optuna 搜索最佳超參數
2. `train_model` — 用最佳超參數在 train set 訓練最終模型
3. `evaluate_model` — 在 val set 計算 mAP 指標
4. `log_experiment` — 將結果記錄至 MLflow

**替代方案：** 將 tuning + training 合併為單一節點 → 職責不清，且不利於跳過 tuning 直接訓練。

**理由：** 遵循 Kedro 設計原則：每個節點單一職責，可獨立測試與重用。tune 與 train 分離後，可在 catalog 中持久化 best_params，支援跳過 tuning 直接重訓。

### 2. Optuna 超參數搜索策略

**選擇：** 每個 trial 在 train set 上訓練 LightGBM，以 train_dev set 計算 mAP 作為 objective value。搜索空間涵蓋 learning_rate、num_leaves、max_depth、min_child_samples、subsample、colsample_bytree、reg_alpha、reg_lambda。

**替代方案：** 使用 LightGBM 內建 CV → 無法利用三組時間分割的優勢，且 CV 的分割與業務邏輯不一致。

**理由：** train_dev 為 out-of-time 資料，模擬真實推論場景。用 train_dev mAP 選模型比隨機 CV 更能反映泛化能力。

### 3. mAP 的計算方式

**選擇：** 自行實作 mAP 計算：
- 以 `snap_date + cust_id` 為 query group（一位客戶在某月的所有產品推薦）
- 每個 query 內按預測分數排序，計算 Average Precision
- 對所有 query 取平均得到 mAP
- 同時計算 per-product AP（以 prod_name 為 group）

**替代方案：** 使用 sklearn.metrics.average_precision_score → 它計算的是單一二元分類的 AP，不是 ranking-aware mAP。

**理由：** 推薦場景下的 mAP 需要以客戶為單位做 ranking，sklearn 的 AP 不支援此語義。自行實作約 30 行程式碼，可精確控制 query 分組邏輯。

### 4. MLflow 整合方式

**選擇：** `log_experiment` 節點接收 model、best_params、evaluation_results，透過 MLflow API 記錄。MLflow tracking URI 通過 parameters 設定，預設為 local file store（`mlruns/`）。

**替代方案：** 在每個節點內部各自記錄 MLflow → 難以管理 run lifecycle，且違反純函數原則。

**理由：** 集中記錄確保單一 MLflow run 包含完整實驗資訊。生產環境可更換 tracking URI 至遠端 server。

### 5. Early stopping 策略

**選擇：** 在 `train_model` 中使用 LightGBM callbacks（`early_stopping` + `log_evaluation`），以 train_dev set 作為 eval_set，監控 binary_logloss。

**理由：** Early stopping 防止過擬合，train_dev 為 out-of-time 資料，比 train set 內部切割更可靠。callbacks 為 LightGBM 原生機制，不需額外依賴。

### 6. 評估結果的儲存格式

**選擇：** evaluation_results 以 dict 格式存為 JSON，包含：
```json
{
  "overall_map": 0.85,
  "per_product_ap": {"prod_A": 0.9, "prod_B": 0.8, ...},
  "n_queries": 1000,
  "val_snap_dates": ["2025-03-01"]
}
```

**理由：** JSON 人類可讀，方便比較不同實驗。與 category_mappings 使用相同的 JSONDataset。

## Risks / Trade-offs

- **[合成資料規模]** 合成資料僅 200 客戶 × 5 產品，mAP 計算可能不具統計意義 → 可接受，目的是驗證 pipeline 端到端可行性，真實資料會有 10M 客戶。
- **[Optuna 搜索時間]** 搜索空間大 + trial 數多可能耗時 → 通過 parameters 設定 `n_trials`（預設 20），開發環境可調低。
- **[MLflow 離線限制]** 生產環境無網路，MLflow 只能用 local file store → 已符合設計（預設 `mlruns/`），不依賴遠端 server。
- **[mAP 自行實作風險]** 可能有 edge cases（全 0 label 的 query）→ 測試中覆蓋邊界情況，確保與手動計算一致。
