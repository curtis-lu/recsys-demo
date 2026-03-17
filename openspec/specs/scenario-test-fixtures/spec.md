### Requirement: 工作目錄隔離
`conftest.py` SHALL 提供 fixture 為每個情境建立隔離的工作目錄，包含完整的 `conf/` 和 `data/` 結構。

#### Scenario: 建立隔離工作目錄
- **WHEN** fixture 以 `scenario_name="scenario_1"` 和情境資料/設定參數呼叫
- **THEN** 建立 `tests/scenarios/output/scenario_1/` 目錄，內含：
  - `conf/base/` — 複製自專案 `conf/base/*.yaml`
  - `conf/{scenario_name}/` — 包含情境覆蓋設定
  - `data/feature_table.parquet` — 情境專用特徵資料
  - `data/label_table.parquet` — 情境專用標籤資料

#### Scenario: 不影響專案既有目錄
- **WHEN** 情境測試執行完畢
- **THEN** 專案根目錄的 `conf/` 和 `data/` 內容不變

### Requirement: Pipeline 執行封裝
`conftest.py` SHALL 提供 `run_pipeline(work_dir, pipeline_name, env_name)` helper，在指定工作目錄下用 subprocess 執行 pipeline CLI。

#### Scenario: 執行 dataset pipeline
- **WHEN** 呼叫 `run_pipeline(work_dir, "dataset", "scenario_1")`
- **THEN** 以 `subprocess.run(cwd=work_dir)` 執行 `python -m recsys_tfb run --pipeline dataset --env scenario_1`，失敗時拋出 CalledProcessError

### Requirement: Model Promote 封裝
`conftest.py` SHALL 提供 `promote_model(work_dir)` helper，在情境工作目錄下執行 model promote。

#### Scenario: 在工作目錄內 promote model
- **WHEN** 呼叫 `promote_model(work_dir)` 且 `work_dir/data/models/` 下有已完成訓練的模型
- **THEN** 建立 `work_dir/data/models/best` symlink 指向 mAP 最高的模型版本

### Requirement: 設定覆蓋寫入
fixture SHALL 將情境專用參數以 YAML 格式寫入工作目錄的 `conf/{env}/` 下，利用 ConfigLoader 的 deep merge 機制覆蓋 base 設定。

#### Scenario: 覆蓋 inference snap_dates
- **WHEN** 傳入 `{"parameters_inference": {"inference": {"snap_dates": ["2024-04-30"]}}}`
- **THEN** 寫入 `conf/{env}/parameters_inference.yaml`，ConfigLoader 載入後 `inference.snap_dates` 為 `["2024-04-30"]`
