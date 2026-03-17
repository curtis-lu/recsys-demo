## MODIFIED Requirements

### Requirement: CLI run command
The system SHALL provide a `run` command via `python -m recsys_tfb run` that executes a named pipeline in a specified environment. The CLI SHALL load config BEFORE building the pipeline, extract the `backend` parameter from parameters, and pass it to `get_pipeline`. `python -m recsys_tfb run --pipeline <name>` SHALL 根據 pipeline 類型計算版本 ID 並注入 runtime_params。

#### Scenario: Run a pipeline with default environment
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset`
- **THEN** the system loads config from `conf/base/` merged with `conf/local/` (default env), extracts `backend` from parameters (default "pandas"), builds the pipeline with that backend, builds a DataCatalog, and executes via Runner

#### Scenario: Run with production environment (spark backend)
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset --env production`
- **THEN** the system loads config with `conf/production/parameters.yaml` overriding `backend: spark`, builds the pipeline with Spark nodes, and executes

#### Scenario: 執行 dataset pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p dataset`
- **THEN** 系統 SHALL 計算 dataset_version hash，以 `runtime_params={"dataset_version": hash}` 注入 catalog，pipeline 完成後寫入 manifest 並更新 latest symlink

#### Scenario: 執行 training pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p training`
- **THEN** 系統 SHALL 解析 dataset_version（預設 latest），計算 model_version hash，以 `runtime_params={"model_version": hash, "dataset_version": dataset_version}` 注入 catalog，pipeline 完成後寫入 manifest

#### Scenario: 執行 inference pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p inference`
- **THEN** 系統 SHALL 解析 best symlink 取得實際 model hash，讀取 model manifest 解析 dataset_version。以 `runtime_params={"model_version": actual_hash, "dataset_version": resolved, "snap_date": date}` 注入 catalog。model 和 preprocessor 的 catalog entry filepath 中 `${model_version}` SHALL 替換為 `"best"`（透過 symlink 讀取），其餘 entry（如 inference output）SHALL 使用實際 hash。pipeline 完成後寫入 manifest 並更新 `data/inference/latest` symlink。

#### Scenario: Unknown pipeline name
- **WHEN** user executes `python -m recsys_tfb run --pipeline nonexistent`
- **THEN** the system SHALL exit with an error message listing available pipeline names

#### Scenario: Pipeline execution failure
- **WHEN** a pipeline node raises an exception during execution
- **THEN** the CLI SHALL log the error and exit with a non-zero exit code
