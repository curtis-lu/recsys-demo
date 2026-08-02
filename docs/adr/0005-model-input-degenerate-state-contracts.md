---
status: accepted
date: 2026-08-02
---

# `build_model_input` 一帶的退化狀態：哪些 fail-loud、哪些是合法契約

同一帶程式碼裡有三個退化狀態，我們給了三個不同的答案。看起來不一致，所以要把分界線寫下來。

**分界線是「這個狀態在生產是不是可能正常發生」**，不是「它看起來有多危險」。

## 一、keys 不含 item → fail-loud

`preprocessing/_spark.py:476` 的三元式在 keys 缺 item 時退回只用 `base_key` join label，
於是每個 `(time, entity)` 被 label_table 的產品數乘開，`item` 的值從 label_table 帶進來。

生產不可能發生：`identity_columns` 是推導欄位（`core/schema.py:55`，恆為
`[time] + entity + [item]`），使用者無法讓它不含 item；五個呼叫點餵的全是 identity
（`pipelines/dataset/pipeline.py:72/81/90/137`、`pipelines/dataset/nodes_spark.py:200`）。
`preprocessing/__init__.py` 不 export 任何東西，`_spark` 是私有模組，也沒有「外部 API 彈性」
需要保留。

而它的失效模式是**靜默列膨脹 ×N_products**。

→ 移除三元式，改 `_validate_columns(keys.columns, base_key + [item_col], "build_model_input")`。
這讓「**keys 的 grain 就是 model_input 的 grain**」成為可依賴的不變量。

## 二、`train_dev_ratio > 0` 但切出來的 dev 為空 → raise

生產樣本量下不可能發生。而 `train_dev` 是 HPO 每個 trial 的 early-stopping 驗證集
（`pipelines/training/nodes.py:626-637` 把 `train_dev_lgb_handle` 當 `val_dataset`，
`early_stopping_rounds` 預設 50，`:559`）。空的 dev ＝ early stopping 沒有訊號、每個 trial
跑滿輪數，而且不會有任何錯誤訊息。

→ `split_train_keys` 在 `train_dev_ratio > 0` 且 dev 為空時 raise。

## 三、feature join miss → 合法契約，維持 LEFT

**這個在生產可能天天發生**，而且是結構性的：

- `sample_pool` 的客戶母體 ＝ `feature_store.dim_all_customer`
  （`conf/sql/etl/sample_pool/sample_pool.sql`）
- `feature_table` 的客戶母體 ＝ `feature_aum ∪ feature_sav ∪ feature_ccard ∪ feature_info`
  的聯集（`conf/sql/etl/feature/feature_concat.sql` 的 `pool_cust`）

兩個不同上游。`sample_pool.sql` 自己對 `feature_table` 用的就已經是 `LEFT JOIN`。

→ 契約定為現行行為：**keys 有、`preprocessed_feature_table` 沒有的 `(time, entity)`
會產生全 NULL 特徵列並進入 model_input，由 LightGBM 自行處理 missing。**
這不是 bug，是這個資料形狀下的正常輸出。

## 為什麼不改 INNER

改 INNER 會靜默改變 `model_input` 的列數，直接破掉上面第一項買來的
`out.count() == keys.count()`，也讓 mAP 的候選集跟著變。**一個會靜默改變列數的修法，
不能拿來修一個不會靜默出錯的狀態。**

## 為什麼現在不量 miss 率

真實 miss 率只有生產跑一次才知道，本機量不到，所以「先量再決定」在這一輪無法執行。

曾考慮在 `source_etl` 對 `sample_pool` 加 `max_null_ratio` 當代理值——**否決**。
`sample_pool.sql` 的那個 LEFT JOIN 發生在 **ETL 當時**、對的是那個時點的 `feature_table`；
而 `build_model_input` join 的是後來可能被 backfill 或重建過的 `preprocessed_feature_table`
衍生物。兩者答的不是同一個問題，前者只是個時間點對不上的代理值。

**日後若要量，量測點在 `build_model_input` 產出之後**，量的是實際進了 model_input 的東西。

## 這條 ADR 沒有解決的事

- 生產的實際 miss 率仍然未知。若日後量出來是 0，可以考慮把第三項收緊成 fail-loud；
  不是 0 就維持現狀並把數字寫進文件。
- 第二項的「dev 為空」判定需要一次 Spark action（`isEmpty` 級），成本評估未做。若證實
  在生產規模下不可忽略，退路是把判定移到 `train_dev_model_input` 已經物化之後。
