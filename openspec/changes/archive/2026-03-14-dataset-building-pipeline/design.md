## Context

Phase 1 框架骨架已完成（ConfigLoader、DataCatalog、Pipeline/Runner、CLI）。SQL ETL 範例檔已就緒，定義了 feature 和 label 表的 schema。目前 dataset pipeline 和 training pipeline 都是空 stub。

開發環境不連 Hive，使用本地合成 Parquet 檔案模擬 ETL 產出。合成資料的 schema 必須對齊 SQL 產出。

**Feature table schema**（來自 `feature_concat.sql`）：
- `snap_date`: date — 快照日期
- `cust_id`: string — 客戶 ID
- `total_aum`: float — 總資產
- `fund_aum`: float — 基金資產
- `in_amt_sum_l1m`: float — 近一月轉入金額
- `out_amt_sum_l1m`: float — 近一月轉出金額
- `in_amt_ratio_l1m`: float — 轉入金額佔比
- `out_amt_ratio_l1m`: float — 轉出金額佔比

**Label table schema**（來自 `label_exchange.sql` / `label_fund.sql`）：
- `snap_date`: date — 快照日期
- `cust_id`: string — 客戶 ID
- `apply_start_date`: date — 申請觀察期起始
- `apply_end_date`: date — 申請觀察期結束
- `label`: int (0/1) — 是否有購買行為
- `prod_name`: string — 產品名稱（fx, usd, stock, bond, mix）

## Goals / Non-Goals

**Goals:**
- 產出可直接餵入 LightGBM 的 X_train, y_train, X_val, y_val
- 合成假資料 schema 對齊 SQL ETL 產出
- 所有 node 為純函數，可測試、可重用
- Strategy 1 MVP：單一二元分類器，prod_name 作為 categorical feature
- 支援多 snap_date 訓練（12 個月快照）

**Non-Goals:**
- 不實作 PySpark 版本的 node（MVP 用 pandas）
- 不實作 safe rerun / checkpoint 機制
- 不實作 feature engineering（ETL SQL 已完成）
- 不實作 Strategy 2/3/4
- 不處理推論 pipeline 的資料流

## Decisions

### 1. 合成資料生成方式：Python script 產生固定種子隨機資料

用 numpy/pandas 產生合成資料，寫入 `data/` 目錄的 Parquet 檔。使用固定 random seed 確保可重現。

**替代方案**：手動 CSV → 太難維護；fixture factory → 過度設計。

### 2. 抽樣策略：依 snap_date 分層抽樣

對 label_table 按 snap_date 做 stratified sampling，確保每個月的資料量比例一致。使用 pandas `groupby` + `sample`。

**替代方案**：依 prod_name 分層 → 會在 snap_date 上不均勻；依兩者 → MVP 過度複雜。

### 3. 切分策略：依時間切分（temporal split）

按 snap_date 切分，較早的月份做訓練、最後 N 個月做驗證。避免 data leakage（同一客戶的不同時間點可能分到不同集）。

**替代方案**：random split → 會有 temporal leakage；client-based split → MVP 不需要。

### 4. 模型輸入準備：pandas DataFrame，prod_name 做 label encoding

`prepare_model_input` 將 join 後的資料集轉為數值矩陣。prod_name 作為 categorical feature 使用 LightGBM 原生 categorical 支援（不需 one-hot）。Preprocessor 物件記錄轉換邏輯供推論重用。

**替代方案**：one-hot encoding → 產品多時維度爆炸；target encoding → MVP 不需要。

### 5. Pipeline 資料流

```
label_table ──→ select_sample_keys ──→ sample_keys
                                            │
                                     split_keys
                                      │        │
                               train_keys    val_keys
                                 │               │
feature_table ──→ build_train_dataset    build_val_dataset
label_table  ──→      │                       │
                  train_set              val_set
                       │                    │
                  prepare_model_input ←─────┘
                       │
              X_train, y_train, X_val, y_val, preprocessor
```

每個 node 接收 pandas DataFrame，回傳 pandas DataFrame（或 dict of arrays）。中間結果由 Runner 透過 MemoryDataset 暫存，不寫入磁碟。最終的 preprocessor 透過 PickleDataset 持久化。

## Risks / Trade-offs

- **[風險] 合成資料分佈不代表真實** → 緩解：合成資料僅供開發和測試跑通 pipeline，不用於評估模型效果
- **[風險] pandas 處理大量資料記憶體不足** → 緩解：正式環境用 PySpark 前處理再轉 pandas，MVP 假資料量小
- **[風險] prod_name categorical encoding 推論時遇到未知類別** → 緩解：preprocessor 記錄已知類別清單，未知類別設為 -1
- **[取捨] 中間資料不持久化** → 減少 I/O 複雜度，但無法 resume 中斷的 pipeline。MVP 可接受
