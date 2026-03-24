## ADDED Requirements

### Requirement: ModelAdapterDataset I/O
系統 SHALL 提供 `ModelAdapterDataset`（`src/recsys_tfb/io/model_adapter_dataset.py`），繼承 `AbstractDataset`，負責 ModelAdapter 的序列化與反序列化。

#### Scenario: save 產生 model 檔與 meta sidecar
- **WHEN** 呼叫 `save(adapter)` 且 adapter 為 LightGBMAdapter
- **THEN** SHALL 寫入兩個檔案：model 檔（由 adapter.save() 處理）與 `model_meta.json`（包含 algorithm、adapter_class）

#### Scenario: load 根據 meta 自動選擇 adapter
- **WHEN** 呼叫 `load()` 且目錄包含 `model_meta.json` 記錄 `algorithm: "lightgbm"`
- **THEN** SHALL 自動建立 `LightGBMAdapter` 實例並載入模型，回傳 adapter

#### Scenario: model_meta.json 結構
- **WHEN** save 完成
- **THEN** `model_meta.json` SHALL 包含至少：`algorithm`（str）、`adapter_class`（str，含完整 module path）、`saved_at`（ISO timestamp）

#### Scenario: 向後相容舊模型
- **WHEN** load 時目錄無 `model_meta.json`（舊版模型）
- **THEN** SHALL fallback 到 LightGBM 載入方式，並記錄 warning log

### Requirement: 取代 LightGBMDataset
`ModelAdapterDataset` SHALL 完全取代 `LightGBMDataset`。`catalog.yaml` 中 model entry 的 type SHALL 改為 `ModelAdapterDataset`。

#### Scenario: catalog.yaml 更新
- **WHEN** 查看 `conf/base/catalog.yaml` 的 model entry
- **THEN** type SHALL 為 `ModelAdapterDataset`（非 `LightGBMDataset`）

#### Scenario: LightGBMDataset 移除
- **WHEN** 查看 `src/recsys_tfb/io/` 目錄
- **THEN** `lightgbm_dataset.py` SHALL 不再存在（或標記為 deprecated）
