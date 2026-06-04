# dataset pipeline

> 把三張來源表變成各 split 的訓練輸入：抽樣 → 前處理 → 組 `*_model_input`。
> DAG pipeline；節點接線與每張表的 schema 見 [`../data-lineage.html`](../data-lineage.html)。

## 用途

`dataset` 從 `sample_pool` 抽樣挑出各 split 的 key，對 `feature_table` 做一次前處理（編碼），再把 key ⋈ 特徵 ⋈ label 組成模型輸入。輸出供 `training` 讀。

```bash
python -m recsys_tfb dataset --env local
```

## 三層資料版本（`core/versioning.py`）

**為什麼分三層**：你常會反覆調 train / calibration 的抽樣做實驗。若所有產物共用一個版本，改抽樣就得連前處理、val / test 一起重算。分三層後，改抽樣只讓對應那層失效，base 層（前處理、val / test）原封不動，省去重算。

每層是一段設定的 hash；改了哪層的設定，只有那層（及其下游產物）需要重算：

| 版本層 | 由哪些設定決定 | 這層變了要重算的產物 |
|---|---|---|
| `base_dataset_version` | **非抽樣** dataset 設定 ＋ 完整 `schema` ＋ feature_table 欄位指紋（欄名＋型別的 hash） | `preprocessor`、`category_mappings`、`preprocessed_feature_table`、`val/test_keys`、`val/test_model_input` |
| `train_variant_id` | **train 抽樣**設定（`sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio`） | `sample_keys`、`train/train_dev_keys`、`train/train_dev_model_input` |
| `calibration_variant_id` | **calibration 抽樣**設定（`calibration_sample_ratio`、`calibration_sample_ratio_overrides` 等） | `calibration_keys`、`calibration_model_input` |

> 改 `schema` / 前處理 / `carry_columns` / feature_table 欄位 → bust `base_dataset_version`、整批重算。`model_version`（training 層）再把這三個 id 一起 hash 進去，所以資料版本一變、模型版本也跟著變。

## 節點流程

執行順序（calibration 節點僅在 `enable_calibration: true` 時加入）：

| 階段 | 節點 | 做什麼 |
|---|---|---|
| 資料閘 | `validate_data_consistency` | **第一個節點**：抽樣前先比對「設定宣告的 item」與「資料實際 item」，不符即 `DataConsistencyError`（見 README §4） |
| 抽樣 | `select_sample_keys` → `split_train_keys` | 對 train 期 `sample_pool` 分層抽樣得 `sample_keys`，再按 `cust_id` 以 `train_dev_ratio` 切成 `train_keys` / `train_dev_keys` |
| 抽樣 | `select_val_keys` / `select_test_keys` | val / test 期取全母體 identity（不 carry 分群欄；`val_sample_ratio` 可隨機縮減 cust，預設 1.0 ＝ 全母體） |
| 前處理 | `fit_preprocessor_metadata` | 在 **train 期** feature_table 上 fit 編碼字典（與抽樣解耦）→ `preprocessor`、`category_mappings` |
| 前處理 | `apply_preprocessor_to_features` | 對整張 feature_table 編碼**一次**，各 split 共用 → `preprocessed_feature_table` |
| 組裝 | `build_model_input`（×各 split） | keys **left join** `label_table`（on time＋entity＋item；join 不到的視為負例 label=0）再 **left join** `preprocessed_feature_table`（on time＋entity）→ `*_model_input` |
| 過濾 | `filter_groups_with_positives` | **只對 val / test**：丟掉沒有任何正例的查詢群組（排序指標對這種群組無意義）。train / train_dev / calibration 不過濾 |

> **query group** ＝ 同一個 (time, entity) 下所有候選 item（見 README §0）；排序與 mAP 都在組內進行。所以 `filter_groups_with_positives` 只丟「整組無正例」的 group（對排序指標無意義），且只作用於 val / test——train / train_dev / calibration 的 loss 會用到每一列、不丟。

## 抽樣（`conf/base/parameters_dataset.yaml`）

- `sample_group_keys`：分層維度（用 `|` 串成 key）。**為什麼分層**：確保各組（各 item / 各客群）在樣本中保有足夠正例，避免某些 query group 抽到完全沒有正例。
- `sample_ratio` ＋ `sample_ratio_overrides`：預設比例與各組覆寫。抽樣是**決定性 hash**（同 seed → 同結果）。
- `carry_columns`：要從 `sample_pool` 帶進 model_input 的非 identity 欄，供 `sample_weights` 分群。
  - 只帶進 **train / calibration**（這兩個 split 才做加權）；val / test 不帶。
  - 改它會 bust `base_dataset_version`（model_input 的 parquet schema 變了）。
- 各 split 日期：`train_snap_dates`、`val_snap_dates`、`test_snap_dates`、`calibration_snap_dates`；`train_dev_ratio` 控制 train / train_dev 切分。

> `sample_ratio_overrides` 與 `sample_weights` 的 key 通常**不是手填**，而是用 `scripts/sampling_overrides_editor.py` 從 `sample_pool` 推導；類別欄用 `scripts/suggest_categorical_cols.py`。見 README §2「設定怎麼來」。

## 前處理（fit / transform 解耦）

- **fit** 只看 train 期 feature_table（學編碼字典），**不**碰 val/test/未來資料 → 避免洩漏。
- **transform** 對整張 feature_table 套用一次，所有 split 共用同一份 `preprocessed_feature_table`。
- `categorical_columns` 指定的類別欄會 int 編碼；`drop_columns` 的欄不進特徵。`schema.item`（如 `prod_name`）一定是類別特徵。

## 重跑語意

- 改**抽樣**設定 → 只 bust 對應 variant 層；base（前處理、val/test）不動。
- 改 **schema / 前處理 / carry_columns / feature_table 欄位** → bust `base_dataset_version`，整批重算。
- **怎麼指定要用哪個版本**：`--base-dataset-version` / `--train-variant`（預設取最新）。各層的版本對齊由框架自動處理（manifest ＋ `latest` symlink）。

## 接下來

- 各表 schema / 版本層 / 範例 → [`../data-lineage.html`](../data-lineage.html)
- 一致性閘的所有錯誤訊息 → README §4
- 下一個 pipeline → [`training.md`](training.md)
