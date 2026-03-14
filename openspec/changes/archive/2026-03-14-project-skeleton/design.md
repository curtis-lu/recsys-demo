## Context

全新專案，目前零程式碼。需要建立 Kedro-inspired 輕量框架，提供 config、I/O、catalog、pipeline 執行等核心能力。框架必須夠輕量（不依賴 Kedro 本身），但保留其核心設計原則：transformation 與 I/O 分離、pipeline-oriented、externalized config。

目標環境是 PySpark 3.3.2 on Hadoop，但開發環境用本地 Parquet + pandas。框架須同時支援兩種 backend。

## Goals / Non-Goals

**Goals:**
- 提供可測試、可擴展的框架基礎，讓 pipeline 開發者只需撰寫純函數 + YAML config
- ConfigLoader 支援 base/env 分層合併
- I/O 層抽象化，開發和正式環境透過 config 切換，程式碼不變
- Pipeline engine 支援拓撲排序、結構化日誌、步驟計時
- 所有模組有單元測試

**Non-Goals:**
- CLI 入口點（Step 1.6，下一個 change）
- Config YAML 檔案內容（Step 1.7，下一個 change）
- HiveDataset（正式環境才需要，後續加入）
- Safe rerun / checkpoint 機制
- Dataset Building / Training pipeline 邏輯
- Ploomber 整合

## Decisions

### 1. ConfigLoader 用 OmegaConf 風格的深度合併，但只用 stdlib

**選擇**：純 Python dict 深度合併 + PyYAML
**替代方案**：OmegaConf（功能強大但多一個依賴）、Dynaconf
**理由**：目標環境無法安裝額外套件，PyYAML 已在依賴中。深度合併邏輯簡單，自己寫即可。

### 2. AbstractDataset 使用 ABC + load/save/exists 三方法介面

**選擇**：`abc.ABC` 定義抽象基底類別
**替代方案**：Protocol（duck typing）、dataclass-based
**理由**：ABC 強制子類別實作所有方法，避免遺漏。介面簡單明確，與 Kedro 的 `AbstractDataset` 一致。

### 3. ParquetDataset 透過 config 參數切換 pandas / PySpark backend

**選擇**：單一 `ParquetDataset` 類別，`backend` 參數（`"pandas"` 或 `"spark"`）
**替代方案**：分成 `PandasParquetDataset` 和 `SparkParquetDataset` 兩個類別
**理由**：減少類別數量，讓 catalog.yaml 更簡潔。兩種 backend 的 load/save 邏輯差異不大。

### 4. DataCatalog 使用字串 → Dataset 映射，由 YAML type 欄位解析類別

**選擇**：catalog.yaml 中每個 entry 指定 `type: ParquetDataset` 等字串，DataCatalog 用 registry dict 解析
**替代方案**：用完整 Python path 如 `recsys_tfb.io.parquet_dataset.ParquetDataset`
**理由**：簡短類別名更易讀，且框架內建的 dataset 種類有限，用 registry 即可。

### 5. Pipeline 拓撲排序用 Kahn's algorithm

**選擇**：基於入度的 BFS 拓撲排序
**替代方案**：DFS-based 或直接按宣告順序
**理由**：Kahn's algorithm 自然能偵測循環依賴，實作簡單，且與 Kedro 行為一致。

### 6. Runner 執行模式：Sequential only（MVP）

**選擇**：僅支援循序執行
**替代方案**：支援平行執行（ThreadPoolRunner 等）
**理由**：MVP 階段不需要平行執行，循序執行易於除錯和日誌追蹤。後續可擴展。

## Risks / Trade-offs

- **[風險] 自建框架維護成本** → 保持最小化，僅實作必要功能。每個模組 < 200 行。
- **[風險] ParquetDataset 單一類別承擔兩種 backend** → 若日後差異變大可拆分，目前程式碼量小不會造成問題。
- **[取捨] 不用 OmegaConf** → 失去變數插值等進階功能，但符合「無額外套件」的限制。
- **[取捨] 不含 CLI** → 此 change 只建框架核心，CLI 作為下一個 change 加入，保持增量開發。
