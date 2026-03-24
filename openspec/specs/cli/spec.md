### Requirement: CLI run command
The system SHALL provide a `run` command via `python -m recsys_tfb run` that executes a named pipeline in a specified environment. The CLI SHALL load config BEFORE building the pipeline, extract the `backend` parameter from parameters, and pass it to `get_pipeline`. `python -m recsys_tfb run --pipeline <name>` SHALL 根據 pipeline 類型計算版本 ID 並注入 runtime_params。The CLI SHALL read `enable_calibration` from the dataset parameters and pass it to `get_pipeline` when running the dataset pipeline.

#### Scenario: Run a pipeline with default environment
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset`
- **THEN** the system loads config from `conf/base/` merged with `conf/local/` (default env), extracts `backend` from parameters (default "pandas"), builds the pipeline with that backend, builds a DataCatalog, and executes via Runner

#### Scenario: Run with production environment (spark backend)
- **WHEN** user executes `python -m recsys_tfb run --pipeline dataset --env production`
- **THEN** the system loads config with `conf/production/parameters.yaml` overriding `backend: spark`, builds the pipeline with Spark nodes, and executes

#### Scenario: Dataset pipeline with calibration enabled
- **WHEN** `parameters_dataset.yaml` has `enable_calibration: true` and user executes `python -m recsys_tfb --pipeline dataset --env local`
- **THEN** the CLI SHALL pass `enable_calibration=True` to `get_pipeline`, resulting in a pipeline that includes calibration nodes

#### Scenario: Dataset pipeline with calibration disabled
- **WHEN** `parameters_dataset.yaml` has `enable_calibration: false` and user executes `python -m recsys_tfb --pipeline dataset --env local`
- **THEN** the CLI SHALL pass `enable_calibration=False` to `get_pipeline`, resulting in a pipeline without calibration nodes

#### Scenario: Non-dataset pipelines unaffected
- **WHEN** user executes `python -m recsys_tfb --pipeline training --env local`
- **THEN** the CLI SHALL NOT pass `enable_calibration` to `get_pipeline`

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

### Requirement: CLI help
The system SHALL display usage information when invoked with `--help`.

#### Scenario: Show help
- **WHEN** user executes `python -m recsys_tfb --help`
- **THEN** the system SHALL display available commands and options

### Requirement: Parameters injection
The CLI SHALL load parameters from ConfigLoader and inject them into the DataCatalog as a MemoryDataset named `parameters` before pipeline execution.

#### Scenario: Parameters available to nodes
- **WHEN** a pipeline is executed via CLI
- **THEN** nodes that declare `parameters` as an input SHALL receive the merged parameters dict from all `parameters*.yaml` files

### Requirement: Conf directory resolution
The CLI SHALL resolve the `conf/` directory relative to the project root (the directory containing `pyproject.toml` or the current working directory).

#### Scenario: Default conf directory
- **WHEN** the CLI is run from the project root
- **THEN** it SHALL look for config files in `./conf/`

### Requirement: Dataset version CLI 選項
CLI SHALL 支援 `--dataset-version` 選項，允許手動指定要使用的 dataset 版本。

#### Scenario: 指定 dataset 版本執行 training
- **WHEN** 執行 `python -m recsys_tfb run -p training --dataset-version a1b2c3d4`
- **THEN** 系統 SHALL 使用 `a1b2c3d4` 作為 dataset_version 而非 latest

#### Scenario: 未指定時使用 latest
- **WHEN** 執行 `python -m recsys_tfb run -p training`（不帶 --dataset-version）
- **THEN** 系統 SHALL 解析 `data/dataset/latest` symlink 取得 dataset_version

#### Scenario: 指定的版本不存在
- **WHEN** 執行 `python -m recsys_tfb run -p training --dataset-version nonexistent`
- **THEN** 系統 SHALL 輸出錯誤訊息指出版本目錄不存在，以非零 exit code 結束

### Requirement: 版本 log 輸出
CLI SHALL 在每個 pipeline 啟動時 log 輸出所有相關的版本 ID。

#### Scenario: Dataset pipeline 版本 log
- **WHEN** dataset pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Dataset version: {dataset_version}`

#### Scenario: Training pipeline 版本 log
- **WHEN** training pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Model version: {model_version}` 和 `Dataset version: {dataset_version}`

#### Scenario: Inference pipeline 版本 log
- **WHEN** inference pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Model version: {actual_model_hash}`、`Dataset version: {dataset_version}`


## MODIFIED Requirements

### Requirement: CLI initializes structured logging before pipeline execution
`__main__.py` SHALL call `setup_logging()` with the logging config and RunContext before running any pipeline. The `run_id` SHALL be passed to the manifest writing logic.

#### Scenario: Logging setup on pipeline run
- **WHEN** `python -m recsys_tfb --pipeline dataset --env local` is executed
- **THEN** structured logging SHALL be initialized before the pipeline runner starts, and a log file SHALL be created in the configured log directory

#### Scenario: run_id propagated to manifest
- **WHEN** a pipeline completes
- **THEN** the manifest.json SHALL contain the same `run_id` as the log records
