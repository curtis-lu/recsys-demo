## Context

目前 Dataset Building Pipeline 採用 3-way temporal walk-forward split（train in-time sampled → train-dev out-of-time sampled → val out-of-time full）。train 與 train-dev 以日期分隔，導致兩者分佈可能不一致。缺少獨立 calibration set（Phase 7c 需要）與 test set（Phase 8 需要）。抽樣比例為全域統一，無法針對特定客群調整。

現有關鍵函數：`select_sample_keys`、`split_keys`、`build_dataset`、`prepare_model_input`，分別有 pandas 與 spark 兩套實作。Pipeline 定義在 `pipeline.py`，透過 `create_pipeline(backend)` 建構。

## Goals / Non-Goals

**Goals:**
- 將資料切割重構為 5-way split：train, train-dev, calibration (optional), validation, test
- Train 與 train-dev 共用日期，按 cust_id 比例切分，確保分佈一致
- 支援 per-group 自訂抽樣比例（sample_ratio_overrides）
- Calibration split 由 flag 控制，pipeline 條件式建構
- 維持 pandas / spark 雙 backend 支援
- 下游 pipeline（training, inference）不受影響

**Non-Goals:**
- 不修改 Training Pipeline 節點（Phase 7c 再處理 calibration 消費）
- 不修改 Evaluation Pipeline（Phase 8 再處理 test set 消費）
- 不修改 build_dataset 函數（通用 join 邏輯不變）
- 不實作 CalibratedModelAdapter（屬 Phase 7c 範疇）

## Decisions

### D1: Train / Train-dev 切分方式 — 按 cust_id 比例切分

**選擇**：同日期範圍內，先分層抽 cust_id pool，再按 `train_dev_ratio` 隨機切分 cust_id 為 train / train-dev 兩組。同一 cust_id 在所有 snap_dates 歸屬同一 split。

**替代方案**：
- 維持日期切分（現狀）— 分佈不一致，不利 early stopping
- 按 row 隨機切分 — 同一客戶可能同時出現在 train 和 train-dev，造成資訊洩漏

**理由**：按 cust_id 切分確保 train 與 train-dev 分佈一致（相同日期、相同抽樣策略），且避免客戶跨 split 的資訊洩漏。

### D2: sample_ratio_overrides 格式 — 組合序列化 key

**選擇**：多欄位 sample_group_keys 時，以 `"|"` 串接各欄位值為 key（如 `"VIP|1": 1.0`）。

**替代方案**：
- 巢狀 dict（各欄位獨立 override，取交集或最小值）— 語義模糊、多欄位交互規則難定義
- 巢狀 dict（依序套用）— 實際比例為乘積，不直覺

**理由**：組合 key 語義明確，每個組合有確定的抽樣比例，無需定義多欄位交互規則。

### D3: Calibration 條件式建構 — Pipeline 層級分支

**選擇**：`create_pipeline(backend, enable_calibration)` 在建構時決定是否包含 calibration nodes。不含 calibration 時使用 `prepare_model_input`（10 outputs），含時使用 `prepare_model_input_with_calibration`（12 outputs）。

**替代方案**：
- 永遠包含 calibration nodes，disabled 時產出空 DataFrame — 增加不必要的 node 執行
- 單一 prepare_model_input 接受 Optional 參數 — Pipeline Node 框架不支援 optional inputs

**理由**：Pipeline 層級分支最乾淨，不執行不需要的 nodes，與現有 backend 分支機制一致。

### D4: Validation 與 Test 抽樣策略

**選擇**：
- Validation：全量，可選 `val_sample_ratio` 純隨機抽 cust_id（因超參搜尋記憶體壓力）
- Test：全量，不抽樣（評估可批次計算，無記憶體壓力）

**理由**：Validation 在超參搜尋中被頻繁載入記憶體，需要可選降採樣。Test 只在最終評估使用一次，可批次處理。

### D5: 日期驗證 — 在首個 node 執行 preflight check

**選擇**：在 `select_sample_keys` 函數開頭驗證 calibration_snap_dates / val_snap_dates / test_snap_dates 互不重疊。

**理由**：fail fast，避免 pipeline 跑到一半才因日期衝突失敗。

## Risks / Trade-offs

- **[Breaking change]** `split_keys` 移除、`prepare_model_input` 簽名變更 → **Mitigation**: 一次性重構，所有測試同步更新，無外部消費者
- **[Node 數量增加]** 從 6 個 nodes 增至 9-11 個 → **Mitigation**: 每個 node 職責更單一、更易測試，Pipeline DAG 可讀性提升
- **[Overrides 複雜度]** sample_ratio_overrides 增加設定負擔 → **Mitigation**: 預設為空 dict，不使用時零成本；YAML 註解提供設定範例
- **[Test data 月份不足]** 現有 synthetic data 可能不夠 5 個 split → **Mitigation**: 測試 fixture 獨立控制月份數，不依賴 synthetic data generator
