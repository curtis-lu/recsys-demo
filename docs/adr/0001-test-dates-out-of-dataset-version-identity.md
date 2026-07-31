---
status: accepted
date: 2026-07-31
---

# `test_snap_dates` 退出 dataset 版本身分

`dataset.test_snap_dates` 原本進 `compute_base_dataset_version` 的 hash payload，於是「多評估一個月」＝ `base_dataset_version` 翻號 ＝ 整條 dataset 重建，且因為 `model_version` 把 `base_dataset_version` 併進 hash（`core/versioning.py:148-171`），連帶必須重訓一個實質相同的模型。**我們把 `test_snap_dates` 從該 payload 剝除**：test 日期不再定義產物身分，只定義資料覆蓋範圍。

## 根本問題：一個 hash 兼任了三件事

| 關注點 | 該由誰決定 | 原本 |
|---|---|---|
| **產物身分**（這是哪一份 dataset／哪一個模型） | 影響產物內容的設定 | `base_dataset_version` |
| **覆蓋範圍**（表裡有哪些月份） | config 列出的日期 | 也是 `base_dataset_version` |
| **快取有效性**（本機 parquet cache 該不該重建） | 上游資料是否變動 | 也是 `base_dataset_version`（副作用地） |

三者過去由同一個 hash 兼任，所以任何一個變動都會付出全部三者的代價。本 ADR 拆開前兩者；第三者的接手見 [ADR-0002](0002-preprocessed-feature-table-incremental.md) 與 `pipelines/training/nodes.py` 的 cache 路徑分層。

## 為什麼 test 可以剝、train／val／calibration 不行

不對稱是有原因的，不是便宜行事：**train／val／calibration 是模型的輸入，test 是模型的觀眾。**

- `val_snap_dates` 的資料經 `val_parquet_handle` 進入 LightGBM 的 `valid_sets` 並驅動 `early_stopping`（`models/lightgbm_adapter.py:81-102`），直接決定 `num_iterations` ── 決定模型本身。
- `calibration_snap_dates` 決定校準後的模型輸出。
- `test_snap_dates` 不進任何模型擬合。而且這個原則在本 repo **早已被明文承認、只是沒有貫徹到版本層**：`fit_preprocessor_metadata` 刻意只吃 `train_snap_dates`，`nodes_shared.py:31-33` 的 docstring 寫著 "deliberately uses only train_snap_dates to prevent val/test leakage into the category-mapping fit"。

驗證過的支撐事實：

- 前處理的編碼是**純逐列** map lookup（`preprocessing/_spark.py:85-109`），查的是只在 train 上 fit 的 `category_mappings`；整個 `apply_preprocessor_to_features` 唯一的聚合是未知值計數，只餵給 `logger`，不進輸出。因此 test 日期不改變任何既有產物的**內容**。
- `HiveTableDataset` 以 `partitionOverwriteMode=dynamic` 寫入（`io/hive_table_dataset.py:172-177`），只覆蓋 DataFrame 裡出現的 partition。因此新增月份是 **append**，既有月份的產物與評估報表原封不動。
- 下游 `build_model_input` 一律以 keys 為驅動端 join features，而 keys 已按各 split 的日期過濾 ── 多出來的月份是惰性的，不會汙染任何 split。

## 考慮過但否決的選項

**新增第四層 `test_variant_id`**（比照 `train_variant_id` / `calibration_variant_id`）。形狀上最對稱，但要動 catalog 路徑、manifest、training／evaluation 的版本解析鏈。既然 test 產物的 `snap_date` 本來就是 dynamic partition、累積天然安全，這一層換不到對應的好處。保留為「哪天真的需要並存多組互斥 test 定義」時的後路。

**維持現狀，靠 pipeline slicing 省成本。** 行不通：切片的自動擴張只看 `catalog.exists()`、不驗新鮮度，而翻號後新版本目錄底下什麼都不存在，擴張等於全跑。

## 後果

- **上線即一次性翻號。** 用本 repo 當時的 `conf/base/` 設定實測，`base_dataset_version` `d89353a8 → f9f5e578`、`model_version` `aa107215 → 6a91a7b4`。（此組數值是在 `feature_table_fingerprint=None` 的條件下計算；實際執行時 `__main__.py:dataset()` 會傳入真實指紋，因此各環境看到的字面值不同 ── 這裡要記住的是「必然改變」與改變的傳染路徑，不是這四個字串本身。）既有產物的檔案還在舊路徑下、能繼續服務，但**不再能從當前 config 重現**。沒有「不翻號」的做法 ── 任何對 hash payload 的改動都會改變輸出。決定直接吃這筆成本，疊在一次本來就要做的重訓上。
- **`parameters_dataset.yaml` 不再是 dataset 內容的唯一真實來源。** 同一個 `base_dataset_version` 底下的 test 覆蓋範圍會隨時間累積；表裡實際有哪些月份要看 manifest 與 partition。這是接受累積語意的直接代價。
- **快取有效性失去了原本的免費保險。** 過去改 test 日期會翻號、連帶讓本機 parquet cache 必然 miss；剝除之後這個保險消失，責任移交給 cache 路徑本身（`pipelines/training/nodes.py` 的 `_CACHE_PATH_LAYOUT`）。**本 ADR 的實作若在 cache 路徑改動之前落地，會製造一個靜默 bug** ── 加了月份、cache 命中舊資料、新月份從未被 predict、且不報錯。因此兩者的落地順序是硬性的。
