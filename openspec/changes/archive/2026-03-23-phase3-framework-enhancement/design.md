## Context

recsys_tfb 是一個 Kedro-inspired 的 ML pipeline 框架，用於商業銀行產品推薦排序模型。目前 Phase 1-2（修正 + config-driven schema + structured logging）已完成。

**現狀問題：**
- Runner 執行 pipeline 時，所有中間產物（MemoryDataset）累積在記憶體中直到 pipeline 結束，不會釋放
- 抽樣機制綁定 label table，無法使用包含更多分層欄位的獨立客戶主檔
- Val set 固定為全量，開發環境迭代緩慢，且與 memory release 前無法有效降低記憶體尖峰

**生產環境限制：** CPU 4 core、128GB RAM、PySpark 3.3.2、無網路、無額外套件

## Goals / Non-Goals

**Goals:**
- Runner 自動釋放不再需要的 MemoryDataset，降低記憶體尖峰用量
- 抽樣池來源可設定為獨立 table，提升分層抽樣的彈性
- Val set 在 prepare_model_input 階段支援可選抽樣，加速開發迭代
- 所有變更向後相容（除了 sample pool 輸入變更需重新產生假資料）

**Non-Goals:**
- 不改變 DataCatalog 公開介面（不新增 release API）
- 不實作 lazy loading / 延遲載入機制
- 不處理 ParquetDataset 等持久化 dataset 的釋放（它們在磁碟上）
- 不在此階段實作 Group-specific sampling（不同群組不同比例）

## Decisions

### Decision 1: 記憶體釋放邏輯放在 Runner，不改 Catalog 介面

**選擇：** Runner 內部分析 DAG 依賴，自動釋放 MemoryDataset

**替代方案：**
- (A) Catalog 新增 `release(name)` 公開 API → 增加介面複雜度，且 Catalog 不應知道 pipeline 依賴關係
- (B) 延遲載入 + 自動釋放 → 改動過大，pandas backend 下收益有限

**理由：** Pipeline 已有拓撲排序，Runner 可直接分析每個 dataset 的最後消費者。邏輯集中在 Runner，不汙染其他元件。

### Decision 2: MemoryDataset 新增 release() 方法 + DataCatalog 新增 get_dataset()

**選擇：** MemoryDataset 新增 `release()` 將 `_data` 設為 None；DataCatalog 新增 `get_dataset(name)` 回傳 dataset instance

**理由：** Runner 需要判斷 dataset 類型（只對 MemoryDataset 釋放），因此需要 `get_dataset()` 存取器。`release()` 方法比直接操作 `_data` 更語意明確。

### Decision 3: Sample pool 作為 catalog 中的具名 ParquetDataset

**選擇：** 在 `catalog.yaml` 新增 `sample_pool` 定義，`select_sample_keys` 改接收此 dataset

**理由：** 遵循現有框架模式——所有資料來源透過 Catalog 管理，config-driven。

### Decision 4: Val 抽樣在 prepare_model_input 而非 split_keys

**選擇：** `split_keys` 和 `build_val_dataset` 仍建立全量 val set，僅在 `prepare_model_input` 轉換 numpy 時抽樣

**替代方案：** 在 `split_keys` 階段就對 val_keys 抽樣 → 磁碟上的 val_set 也會是抽樣版本，無法做完整離線評估

**理由：** 保留全量 val set 在磁碟上供 `evaluate_model.py` 做完整評估；搭配 memory release，val_set DataFrame 在 `prepare_model_input` 完成後會被自動釋放。

## Risks / Trade-offs

- **[Risk] Memory release 過早釋放仍需要的資料** → 最後消費者分析基於確定性的拓撲排序，測試涵蓋多消費者場景驗證正確性
- **[Risk] sample_pool.parquet 不存在導致 pipeline 失敗** → 更新 `generate_synthetic_data.py` 同步產出，文件說明需先產生假資料
- **[Risk] val_sample_ratio 設太小導致評估指標不可靠** → 預設值為 1.0（全量），僅在開發環境手動調低
- **[Trade-off] MemoryDataset 為主要受益對象** → 當前 catalog.yaml 大部分中間產物定義為 ParquetDataset，memory release 主要對自動建立的 MemoryDataset 生效。未來若中間產物改為 MemoryDataset 以提升效能，此機制即刻發揮作用
