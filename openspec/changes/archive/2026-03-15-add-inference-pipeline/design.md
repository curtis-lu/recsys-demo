## Context

Training pipeline (Strategy 1 MVP) 已產出 LightGBM 模型（`data/models/model.pkl`）與 preprocessor（`data/models/preprocessor.pkl`）。目前缺少批次推論能力，無法對新資料產出客戶產品推薦排名。

現有架構採 Kedro-inspired 設計：純函數 nodes → Pipeline DAG → Runner 執行 → DataCatalog 管理 I/O。Dataset 與 Training pipeline 已遵循此模式。Inference Pipeline 將沿用相同架構。

核心挑戰：訓練時的 `prepare_model_input` 會從訓練資料建立 `category_mappings`，推論時需套用已存在的 preprocessor，且推論資料無 label 欄位。

## Goals / Non-Goals

**Goals:**
- 建立可獨立執行的 inference pipeline，對指定 snap_date 的客戶進行全產品批次評分
- 重用訓練時產出的 model 與 preprocessor artifacts，確保無 data leakage
- 輸出包含 cust_id、prod_code、score、rank 的結構化結果
- 遵循現有 Kedro-inspired 架構慣例

**Non-Goals:**
- 推論結果監控（分數分佈、漂移偵測）
- Rule-based reranking
- 分區寫入（Partitioned Parquet / Hive）— MVP 先用單一 Parquet 檔案
- PySpark 大規模執行最佳化
- Ploomber 排程整合

## Decisions

### 1. 獨立 inference nodes 而非重構現有 prepare_model_input

**選擇**：在 `pipelines/inference/nodes.py` 新增 `apply_preprocessor` 函數，複製 `prepare_model_input._transform` 的 ~6 行轉換邏輯。

**替代方案**：
- 重構 `prepare_model_input` 支援 train/inference 雙模式 — 拒絕，因為會增加工作中函數的複雜度且有破壞風險
- 抽出共用 preprocessing 模組 — 對 6 行程式碼過度工程化，可日後再做

**理由**：零風險動到穩定的 training pipeline；推論的前處理需求可能隨專案演進而與訓練分歧（如未知類別處理）。

### 2. 用 cross-join 建立評分資料集而非假 label table

**選擇**：`build_scoring_dataset` 直接從 feature_table 取不重複 `(snap_date, cust_id)` 與產品列表做笛卡爾積。

**替代方案**：建立全零 label table 後呼叫現有 `build_dataset` — 語義不正確且浪費記憶體。

**理由**：推論無 label 概念，用 cross-join 明確表達意圖。

### 3. 產品列表來自設定檔

**選擇**：`parameters_inference.yaml` 中列出完整產品列表。

**理由**：產品列表是業務定義，不應從資料自動推導。同時可驗證與 `category_mappings` 一致性。

### 4. 中間結果使用 MemoryDataset

**選擇**：僅持久化 `scoring_dataset`（除錯用）和 `ranked_predictions`（最終輸出）。`X_score` 和 `score_table` 為記憶體中間變數。

**理由**：減少不必要的磁碟 I/O，保持 catalog 簡潔。

## Risks / Trade-offs

- **_transform 邏輯重複** → 僅 6 行程式碼，若未來變更需同步修改兩處。若前處理邏輯變複雜再考慮抽出共用模組。
- **產品列表設定與模型不一致** → `apply_preprocessor` 會將未知類別編碼為 -1，可在該函數中加入警告日誌。
- **大規模資料效能** → cross-join 在 10M 客戶 × 22 產品下可能佔用大量記憶體。MVP 先用 pandas，生產環境需切換至 PySpark。此為已知的後續工作。
