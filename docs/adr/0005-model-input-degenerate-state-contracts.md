---
status: accepted
date: 2026-08-02
---

# `build_model_input` 一帶的退化狀態：哪些 fail-loud、哪些是合法契約

同一帶程式碼裡有三個退化狀態，我們給了三個不同的答案。看起來不一致，所以要把分界線寫下來。

**分界線是「這個狀態在生產是不是可能正常發生」**，不是「它看起來有多危險」。生產不可能發生的
狀態＝設定或程式出錯的訊號，要當場炸掉；生產會正常發生的狀態＝資料形狀的一部分，要寫成契約。

| 退化狀態 | 生產可能發生？ | 後果 | 答案 |
|---|---|---|---|
| 一、keys 不含 item | 不可能（`identity_columns` 是推導欄位） | 靜默列膨脹 ×N_products | fail-loud |
| 二、要了 dev、切出來卻是空的 | 不可能（生產樣本量下） | HPO 每個 trial 跑滿輪數、無訊息 | raise |
| 三、feature join miss | **可能，而且是結構性的** | 全 NULL 特徵列，LightGBM 自行處理 | 合法契約，維持 LEFT |

## 一、keys 不含 item → fail-loud

`build_model_input` 原本用一個三元式決定 label 的 join 鍵（`base_key ＝ [time] + entity`，不含
item，也就是一個 query group 的鍵）：

```python
label_join_key = base_key + [item_col] if item_col in keys.columns else base_key
```

keys 裡有 item 就照 item 粒度 join，沒有就退回只用 `(time, entity)`。而 label_table 在同一組
`(time, entity)` 底下有 N 個 item 的列，所以退回的那一路會把每一列 keys 乘開成 N 列，`item` 的值
還改由 label_table 帶進來——**靜默的列膨脹 ×N_products**，不會有任何錯誤訊息。

生產走不到這裡：

- `identity_columns` 是推導欄位（`core/schema.py:55`，恆為 `[time] + entity + [item]`），使用者
  無法讓它不含 item。
- 組裝只有一份實作（`build_model_input`，`pipelines/dataset/nodes.py`；`build_test_model_input`
  先把 keys 限縮到當次月份再呼叫它），合計註冊成五個 pipeline 節點（`pipelines/dataset/pipeline.py`
  的 train / train_dev / val / test / calibration，最後一個只在 `enable_calibration` 時註冊），
  五個餵進去的 keys 全是 identity。
- `pipelines/dataset/steps/model_input.py` 只被同套件內部引用，沒有「外部 API 彈性」需要保留。

→ **移除三元式**，改 `require_columns_present(keys.columns, base_key + [item_col], ...)`。這讓
「**keys 的 grain 就是 model_input 的 grain**」成為可依賴的不變量。

### 實作時的一處出入（2026-08-02，#141）

context 字串用 `"build_model_input keys"` 而非 `"build_model_input"`。同一個函式稍後還有一次
`require_columns_present(dataset.columns, ...)`，兩者若共用 context，測試的
`pytest.raises(match=...)` 會被另一條規則的訊息滿足——那正是 #140 列為禁止的假綠形式之一。

## 二、要了 `train_dev`、切出來卻是空的 → raise

生產樣本量下不可能發生。而 `train_dev` 是 HPO 每個 trial 的 early-stopping 驗證集
（`pipelines/training/nodes.py:626-637` 把 `train_dev_lgb_handle` 當 `val_dataset`，
`early_stopping_rounds` 預設 50，見 `:559`）。空的 dev ＝ early stopping 沒有訊號 ＝ 每個 trial
跑滿輪數，而且不會有任何錯誤訊息。

→ `split_train_keys` 在「設定要了 dev」且 dev 為空時 raise。

### 實作時的兩處出入（2026-08-02，#141）

- 條件寫成 `train_dev_ratio != 0`，不是直覺的 `> 0`。負值會讓 `ratio_to_threshold` 算出負的門檻，
  於是 `bucket < threshold` 恆空、`bucket >= threshold` 收全部——夠不到 `> 0` 的守衛，卻正是本節
  要殺掉的那個無聲狀態。`0` 仍是合法設定（不做 HPO），照樣放行。
- 這個判斷留在 `split_train_keys` 內部，沒有照 CLAUDE.md 的通則搬進 `core/consistency.py`。理由：
  A/B 系列不變量都在兩個既定閘門（Layer-1 CLI entry、Layer-2 dataset 首節點）求值，而這一條要看
  的是**抽樣之後**的 keys，兩個閘門都拿不到。同檔 `fit_preprocessor_metadata` 對缺 snap_date 的
  raise 是同一種形狀（node 後置條件，非一致性不變量），有前例可循。

### 還沒關掉的鏡像缺口：`train_dev_ratio > 1`

比值 > 1 會讓 dev 收全部、**train** 靜默變空——跟本節的失效模式對稱，只是方向相反。本輪不處理
（#141 的範圍只到 dev），但它值得一併收進 `train_dev_ratio` 的值域驗證；目前 `core/consistency.py`
對這個鍵零命中。

## 三、feature join miss → 合法契約，維持 LEFT

**這個在生產可能天天發生**，而且是結構性的——兩邊的客戶母體本來就來自不同上游：

- `sample_pool` 的客戶母體 ＝ `feature_store.dim_all_customer`
  （`conf/sql/etl/sample_pool/sample_pool.sql`）
- `feature_table` 的客戶母體 ＝ `feature_aum ∪ feature_sav ∪ feature_ccard ∪ feature_info` 的聯集
  （`conf/sql/etl/feature/feature_concat.sql` 的 `pool_cust`）

`sample_pool.sql` 自己對 `feature_table` 用的就已經是 `LEFT JOIN`。

→ 契約定為現行行為：**keys 有、`preprocessed_feature_table` 沒有的 `(time, entity)` 會產生全 NULL
特徵列並進入 model_input，由 LightGBM 自行處理 missing。** 這不是 bug，是這個資料形狀下的正常
輸出。

### 為什麼不改 INNER

改 INNER 會靜默改變 `model_input` 的列數，直接破掉第一項買來的 `out.count() == keys.count()`，也
讓 mAP 的候選集跟著變。**一個會靜默改變列數的修法，不能拿來修一個不會靜默出錯的狀態。**

### 為什麼現在不量 miss 率

真實 miss 率只有生產跑一次才知道，本機量不到，所以「先量再決定」在這一輪無法執行。

曾考慮在 `source_etl` 對 `sample_pool` 加 `max_null_ratio` 當代理值——**否決**。`sample_pool.sql`
的那個 LEFT JOIN 發生在 **ETL 當時**、對的是那個時點的 `feature_table`；而 `build_model_input`
join 的是後來可能被 backfill 或重建過的 `preprocessed_feature_table` 衍生物。兩者答的不是同一個
問題，前者只是個時間點對不上的代理值。

**日後若要量，量測點在 `build_model_input` 產出之後**，量的是實際進了 model_input 的東西。

## 這條 ADR 沒有解決的事

- 生產的實際 miss 率仍然未知。若日後量出來是 0，可以考慮把第三項收緊成 fail-loud；不是 0 就維持
  現狀並把數字寫進文件。
- 第二項的「dev 為空」判定需要一次 Spark action（`isEmpty` 級），成本評估未做。若證實在生產規模
  下不可忽略，退路是把判定移到 `train_dev_model_input` 已經物化之後。
