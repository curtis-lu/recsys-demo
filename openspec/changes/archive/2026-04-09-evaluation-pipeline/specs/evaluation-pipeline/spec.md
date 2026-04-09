## ADDED Requirements

### Requirement: Evaluation pipeline 註冊與 CLI 執行
系統 SHALL 在 pipeline registry 中註冊 `evaluation` pipeline，並支援透過 CLI 執行 `python -m recsys_tfb evaluation --env <env>`。

#### Scenario: 執行 evaluation pipeline
- **WHEN** 使用者執行 `python -m recsys_tfb evaluation --env local`
- **THEN** 系統 SHALL 解析 model_version（預設跟隨 `data/models/best` symlink）、從 `parameters_evaluation.yaml` 讀取 snap_date，執行 evaluation pipeline 並產出 metrics.json 與 report.html

#### Scenario: 指定 model_version 執行
- **WHEN** 使用者執行 `python -m recsys_tfb evaluation --env local --model-version abc12345`
- **THEN** 系統 SHALL 使用指定的 model_version 讀取對應 inference 產出並評估

### Requirement: 重用 inference 產出
Evaluation pipeline 的 `prepare_eval_data` 節點 SHALL 從 catalog 讀取 inference pipeline 已產出的 `ranked_predictions`，不重新執行推論。

#### Scenario: 讀取 inference 結果
- **WHEN** `prepare_eval_data` 節點執行
- **THEN** 系統 SHALL 從 `data/inference/${model_version}/${snap_date}/ranked_predictions.parquet` 讀取預測結果，並與 `label_table` join 補齊 label 欄位

#### Scenario: inference 結果不存在
- **WHEN** 指定的 model_version + snap_date 下沒有 `ranked_predictions`
- **THEN** 系統 SHALL 拋出明確錯誤訊息，指出缺少 inference 產出

### Requirement: 支援 pandas 與 spark 雙 backend
Evaluation pipeline SHALL 支援 `backend: pandas` 與 `backend: spark` 兩種模式。

#### Scenario: Pandas backend 計算指標
- **WHEN** backend 為 pandas
- **THEN** `compute_metrics` 節點 SHALL 重用現有 `evaluation/metrics.py` 的 `compute_all_metrics` 函數計算排名指標

#### Scenario: Spark backend 計算指標
- **WHEN** backend 為 spark
- **THEN** `compute_metrics` 節點 SHALL 使用 Spark SQL 全程計算排名指標（AP、nDCG、MRR、Precision@K、Recall@K），不 collect 大表到 driver

#### Scenario: 雙 backend 結果一致
- **WHEN** 對同一份資料分別以 pandas 和 spark backend 計算
- **THEN** 兩個 backend 的指標輸出 SHALL 在合理浮點誤差內一致

### Requirement: 指標計算涵蓋多維度
`compute_metrics` 節點 SHALL 計算以下維度的排名指標：overall、per-product、per-segment、per-product-segment，以及 macro/micro average。

#### Scenario: 完整指標輸出
- **WHEN** `compute_metrics` 完成計算
- **THEN** 輸出的 `evaluation_metrics` (JSON) SHALL 包含 overall、per_product、per_segment、per_product_segment、macro_avg、micro_avg 結構，與現有 `compute_all_metrics` 輸出格式一致

#### Scenario: 可配置的 k_values
- **WHEN** `parameters_evaluation.yaml` 中設定 `k_values: [3, 5, 10, "all"]`
- **THEN** 系統 SHALL 為每個 K 值計算對應的 @K 指標

### Requirement: 產出 HTML 報告
`generate_report` 節點 SHALL 產出自包含的 HTML 報告。

#### Scenario: 產出報告含完整分析
- **WHEN** `generate_report` 執行
- **THEN** 報告 SHALL 包含指標摘要、分數分佈、排名熱力圖、校準曲線等視覺化，重用現有 `evaluation/report.py`、`distributions.py`、`calibration.py` 模組

#### Scenario: 有 baseline 時納入比較
- **WHEN** 對應 snap_date 的 `baseline_metrics` 存在
- **THEN** 報告 SHALL 納入 model vs baseline 的比較分析（delta 表、並排圖表）

#### Scenario: 無 baseline 時仍可產出報告
- **WHEN** 對應 snap_date 的 `baseline_metrics` 不存在
- **THEN** 報告 SHALL 正常產出，只包含模型本身的評估結果

### Requirement: Evaluation 產出路徑對應 model_version 與 snap_date
Evaluation artifacts SHALL 儲存在 `data/evaluation/${model_version}/${snap_date}/` 下。

#### Scenario: 產出路徑結構
- **WHEN** evaluation pipeline 完成
- **THEN** 以下檔案 SHALL 存在：
  - `data/evaluation/${model_version}/${snap_date}/metrics.json`
  - `data/evaluation/${model_version}/${snap_date}/report.html`
  - `data/evaluation/${model_version}/${snap_date}/manifest.json`

#### Scenario: Manifest 與 symlink
- **WHEN** evaluation pipeline 完成
- **THEN** 系統 SHALL 寫入 manifest.json（含 model_version、snap_date、git_commit、時間戳）並更新 `data/evaluation/latest` symlink

### Requirement: 集中管理評估參數，避免 hard-coded 值
系統 SHALL 從 `conf/base/parameters_evaluation.yaml` 讀取所有評估參數，pipeline nodes 中不得 hard-code 可配置值。

#### Scenario: 參數內容
- **WHEN** evaluation 或 baselines pipeline 讀取參數
- **THEN** 參數檔 SHALL 包含：snap_date、k_values、metric 類型清單、segment_columns、segment_sources、baseline_type、lookback_months、report 選項（include_baseline_comparison、include_calibration、include_distributions、n_calibration_bins）

#### Scenario: 欄位名透過 schema 取得
- **WHEN** pipeline nodes 需要欄位名（snap_date、cust_id、prod_name、score、rank、label）
- **THEN** 系統 SHALL 透過 `get_schema(parameters)` 取得，不在 nodes 中 hard-code 欄位名字串

### Requirement: 刪除 evaluate_model.py script
Pipeline 化完成後 SHALL 刪除 `scripts/evaluate_model.py`。

#### Scenario: Script 移除
- **WHEN** evaluation pipeline 可正常執行
- **THEN** `scripts/evaluate_model.py` SHALL 被刪除，其功能完全由 pipeline 取代
