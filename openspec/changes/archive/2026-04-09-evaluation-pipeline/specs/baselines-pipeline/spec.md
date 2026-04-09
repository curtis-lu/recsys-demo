## ADDED Requirements

### Requirement: Baselines pipeline 註冊與 CLI 執行
系統 SHALL 在 pipeline registry 中註冊 `baselines` pipeline，並支援透過 CLI 執行 `python -m recsys_tfb baselines --env <env>`。

#### Scenario: 執行 baselines pipeline
- **WHEN** 使用者執行 `python -m recsys_tfb baselines --env local`
- **THEN** 系統 SHALL 從 `parameters_evaluation.yaml` 讀取 snap_date，計算 popularity baseline 並產出 baseline_predictions 與 baseline_metrics

### Requirement: Baselines 不綁定 model_version
Baselines pipeline SHALL 只依賴 snap_date，不依賴任何 model_version。

#### Scenario: Runtime params 不含 model_version
- **WHEN** baselines pipeline 執行
- **THEN** runtime params SHALL 只包含 `snap_date`，catalog 路徑為 `data/baselines/${snap_date}/`

#### Scenario: 不同模型實驗不需重算 baseline
- **WHEN** 已針對某 snap_date 執行過 baselines pipeline
- **THEN** 不同 model_version 的 evaluation pipeline SHALL 可直接讀取該 baseline 結果

### Requirement: 支援 pandas 與 spark 雙 backend
Baselines pipeline SHALL 支援 `backend: pandas` 與 `backend: spark` 兩種模式。

#### Scenario: Pandas backend
- **WHEN** backend 為 pandas
- **THEN** 系統 SHALL 重用現有 `evaluation/baselines.py` 的 `generate_global_popularity_baseline` / `generate_segment_popularity_baseline`

#### Scenario: Spark backend
- **WHEN** backend 為 spark
- **THEN** 系統 SHALL 使用 Spark SQL 計算 per-product / per-segment popularity rate，cross join 客戶列表產出 baseline predictions

### Requirement: 計算 baseline 指標
Baselines pipeline SHALL 產出 baseline predictions 並計算對應的排名指標。

#### Scenario: Baseline 產出
- **WHEN** baselines pipeline 完成
- **THEN** 以下 artifacts SHALL 存在：
  - `data/baselines/${snap_date}/baseline_predictions.parquet`（含 snap_date, cust_id, prod_name, score, rank）
  - `data/baselines/${snap_date}/baseline_metrics.json`（與 evaluation_metrics 格式一致）
  - `data/baselines/${snap_date}/manifest.json`

#### Scenario: Manifest 與 symlink
- **WHEN** baselines pipeline 完成
- **THEN** 系統 SHALL 寫入 manifest.json（含 snap_date、git_commit、時間戳）並更新 `data/baselines/latest` symlink

### Requirement: Baseline 類型可配置
Baselines pipeline SHALL 支援從 `parameters_evaluation.yaml` 配置 baseline 類型。

#### Scenario: Global popularity baseline
- **WHEN** 參數設定 `baseline_type: global_popularity`
- **THEN** 系統 SHALL 用 snap_date 前歷史資料計算每個產品的正向率作為排名依據

#### Scenario: Segment popularity baseline
- **WHEN** 參數設定 `baseline_type: segment_popularity` 且指定 `segment_column`
- **THEN** 系統 SHALL 用 (segment, product) 級別的正向率計算分段特定的排名
