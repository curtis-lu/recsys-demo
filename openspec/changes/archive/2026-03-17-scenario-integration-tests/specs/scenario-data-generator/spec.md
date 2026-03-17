## ADDED Requirements

### Requirement: 產生多月份合成特徵資料
`data_generator.py` SHALL 提供 `generate_feature_table(rng, snap_dates, num_customers, extra_columns)` 函式，產生指定 snap_dates 和客戶數的 feature_table DataFrame。

#### Scenario: 產生基礎 6 個月特徵資料
- **WHEN** 呼叫 `generate_feature_table(rng, snap_dates=["2024-01-31", ..., "2024-06-30"], num_customers=200, extra_columns=False)`
- **THEN** 回傳 DataFrame 含 1200 行（200 客戶 × 6 月）、欄位為 `snap_date, cust_id, total_aum, fund_aum, in_amt_sum_l1m, out_amt_sum_l1m, in_amt_ratio_l1m, out_amt_ratio_l1m`

#### Scenario: 產生含額外欄位的特徵資料
- **WHEN** 呼叫 `generate_feature_table(rng, snap_dates=..., num_customers=200, extra_columns=True)`
- **THEN** 回傳 DataFrame 額外包含 `txn_count_l1m`（int）和 `avg_txn_amt_l1m`（float）兩個欄位

### Requirement: 產生多月份合成標籤資料
`data_generator.py` SHALL 提供 `generate_label_table(rng, snap_dates, num_customers, products)` 函式，產生指定 snap_dates、客戶數和產品列表的 label_table DataFrame。

#### Scenario: 產生基礎 5 產品標籤資料
- **WHEN** 呼叫 `generate_label_table(rng, snap_dates=["2024-01-31", ..., "2024-06-30"], num_customers=200, products=["fx", "usd", "stock", "bond", "mix"])`
- **THEN** 回傳 DataFrame 含 6000 行（200 × 5 × 6）、欄位為 `snap_date, cust_id, cust_segment_typ, apply_start_date, apply_end_date, label, prod_name`

#### Scenario: 產生含額外產品的標籤資料
- **WHEN** 呼叫 `generate_label_table(rng, ..., products=["fx", "usd", "stock", "bond", "mix", "ploan", "mloan"])`
- **THEN** 回傳 DataFrame 含 8400 行（200 × 7 × 6），`prod_name` 唯一值為 7 個

### Requirement: 可重複性
資料產生 SHALL 使用傳入的 `numpy.random.Generator` 物件確保可重複性。

#### Scenario: 相同 seed 產生相同資料
- **WHEN** 用 `np.random.default_rng(42)` 兩次產生資料
- **THEN** 兩次產出的 DataFrame 完全相同
