## Why

專案目前只有 PRD 和設計原則文件，尚無任何程式碼。要開發 MVP pipeline（Dataset Building + Training），需要先有一個 Kedro-inspired 輕量框架作為基礎，提供資料存取、設定管理、pipeline 編排等核心能力。沒有這個骨架，後續的 pipeline 開發無法開始。

## What Changes

- 建立 `pyproject.toml` 定義套件元資料、依賴與開發工具設定
- 建立專案目錄結構（`src/recsys_tfb/`、`conf/`、`tests/`）
- 實作 `ConfigLoader`：載入分層 YAML 設定檔（base + env overlay）
- 實作 I/O 抽象層：`AbstractDataset` 介面 + `ParquetDataset`、`PickleDataset` 實作
- 實作 `DataCatalog`：根據 catalog YAML 實例化 Dataset，提供統一的 `load/save/exists` 介面
- 實作 `Node`、`Pipeline`、`Runner`：函數封裝、拓撲排序、帶日誌計時的執行引擎
- 為所有 core 和 io 模組撰寫單元測試

## Capabilities

### New Capabilities

- `config-loader`: 分層 YAML 設定載入，支援 base + env 覆寫合併
- `io-datasets`: 資料存取抽象層，支援 ParquetDataset（pandas/PySpark）和 PickleDataset
- `data-catalog`: 根據 YAML 設定實例化 Dataset 物件，提供統一資料存取介面
- `pipeline-engine`: Node/Pipeline/Runner 三件組，支援拓撲排序執行與結構化日誌

### Modified Capabilities

（無，這是全新專案）

## Impact

- **新增檔案**：`pyproject.toml`、`src/recsys_tfb/core/`、`src/recsys_tfb/io/`、`src/recsys_tfb/utils/`、`tests/test_core/`、`tests/test_io/`
- **依賴**：pyspark, lightgbm, scikit-learn, mlflow, optuna, pandas, numpy, pyarrow, pytest（定義於 pyproject.toml，此階段僅用到 pyspark, pyyaml, pandas, pytest）
- **不影響**：尚無既有程式碼或 API
