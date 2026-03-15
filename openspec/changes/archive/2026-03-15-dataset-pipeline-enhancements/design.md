## Context

Dataset building pipeline 目前支援兩組資料集切分（train / val）、固定以 snap_date 做分層抽樣、category_mappings 僅存在 preprocessor.pkl 中。使用者已修改 label SQL，在 label table 中新增 `cust_segment_typ` 欄位，以便支援依客群的分層抽樣。

現有 pipeline 流程：select_sample_keys → split_keys → build_train → build_val → prepare_model_input (5 nodes)

## Goals / Non-Goals

**Goals:**
- 讓 `select_sample_keys` 的分層抽樣 group by 欄位可由 YAML 設定
- 將資料集切分為三組（train / train-dev / val），snap_dates 互不重疊
- category_mappings 以 JSON 檔案獨立保存，提升可觀測性
- 新增 JSONDataset I/O 類別

**Non-Goals:**
- Spark-native 實作（正式環境的 Hive 處理）：待 Strategy 1 + mAP 評估完成後再實作
- 修改 training pipeline（下游適配為後續工作）
- 新增更多特徵工程邏輯

## Decisions

### 1. 三組資料集的時間分割策略

**選擇：** 在 YAML 中分別設定 `train_dev_snap_dates` 和 `val_snap_dates`，其餘日期自動歸入 train。

**替代方案：** 用 train_snap_dates 明確列出所有 train 日期 → 過於冗長，不利維護。

**理由：** 三組日期互不重疊的約束由 `split_keys` 函數驗證。train 為剩餘日期的設計最簡潔，且新增月份時不需修改設定。

### 2. select_sample_keys 的 group by 欄位傳遞方式

**選擇：** 通過 `parameters["dataset"]["sample_group_keys"]` 傳入欄位清單，函數自動從 label_table 提取所需欄位。

**替代方案：** 將額外欄位預先加入 sample_keys 的 key columns → 會改變下游 join 邏輯，增加複雜度。

**理由：** group by 欄位僅用於分層抽樣，不需保留在輸出中。函數內部提取、分組、採樣後只回傳 `["snap_date", "cust_id"]`。

### 3. val 資料集的取得方式

**選擇：** `split_keys` 新增 `label_table` 參數，直接從完整 label_table 取得 val dates 的所有 unique keys（不經抽樣）。

**理由：** val 代表全量資料，用於最終模型評估。從原始 label_table 取 keys 確保不受 sample_ratio 影響。

### 4. category_mappings 的儲存方式

**選擇：** 新增 JSONDataset 類別，category_mappings 作為 `prepare_model_input` 的額外輸出，通過 catalog 持久化為 JSON 檔案。

**替代方案：** 僅用 logger.info 輸出 → 不便查閱歷史記錄。

**理由：** JSON 格式人類可讀，方便版本追蹤和比對。同時 category_mappings 仍保留在 preprocessor dict 中供推論使用。

### 5. cust_segment_typ 在 prepare_model_input 中的處理

**選擇：** 將 `cust_segment_typ` 加入 `drop_cols`，不作為模型特徵。

**理由：** 此欄位用於分層抽樣，非模型訓練特徵。若未來需作為特徵，可從 drop_cols 移除並加入 categorical_cols。

## Risks / Trade-offs

- **[小群體採樣]** 當 `sample_group_keys` 產生的分組中樣本數過少時，`groupby.sample(frac=...)` 的取整可能導致某些組被完全略過 → 暫不處理，可透過日誌觀察。未來可考慮加入每組最低樣本數的保護機制。
- **[train-dev 與 val 用途混淆]** 下游 training pipeline 目前使用 X_val/y_val 做訓練中驗證 → 需更新為使用 X_train_dev/y_train_dev，此為後續工作。
- **[合成資料一致性]** 更新合成資料後需重新執行 `generate_synthetic_data.py`，已存在的 parquet 檔案會被覆寫。
