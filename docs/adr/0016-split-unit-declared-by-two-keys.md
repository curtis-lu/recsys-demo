---
status: accepted
date: 2026-09-01
---

# 切分單位用兩把尺：`train_split_keys` 與 `val_sample_keys`

dataset pipeline 有兩個「以 entity 為單位」的操作：

- **train/dev 切分**（`pipelines/dataset/nodes.py::split_train_keys`）——同一個 entity 的所有 row 必須整批落在同一邊。
- **val 抽樣**（`::select_val_keys`）——抽中的 entity 整批保留，因為 mAP 是對一個 query group 算的，group 少了候選就是在回答另一個問題。

兩者原本都取 `schema.entity` 的**第一欄**。單欄設定下這與「取完整 entity」等價，所以從來沒出過事。多欄設定下它變成一個沒被承諾的行為：`train_dev_ratio: 0.1` 切的不是十分之一的 query，而是十分之一的第一欄值。

## 決定

新增**兩個** `dataset` 設定鍵，讓使用者自己宣告切分／抽樣的單位：

| 鍵 | 管哪個操作 | 登記進 `TRAIN_SAMPLING_KEYS` | 改了會翻 |
|---|---|---|---|
| `dataset.train_split_keys` | train/dev 切分 | **是** | `train_variant_id`（train／train_dev 重算） |
| `dataset.val_sample_keys` | val 抽樣 | **否** | `base_dataset_version`（preprocessor／val／test 重算，train 系列連坐） |

兩者預設值都是**完整 `schema.entity`**，合法值是 `entity` 的非空子集（不變量 A29，`core/consistency.py::entity_grouping_key_errors`，在 CLI 進入點 collect-all 一次報完）。解析共用 `core/schema.py::get_entity_grouping`。

## 為什麼是兩個鍵，不是一個

版本 ID 的機制決定了這件事，不是命名品味。

`core/versioning.py` 把登記在 `TRAIN_SAMPLING_KEYS` 的鍵從 `base_dataset_version` 的雜湊輸入中**剝掉**，改餵給 `train_variant_id`。而 catalog 裡：

- **val／test 產物只由 `base_dataset_version` 分割**；
- **train／train_dev 產物由 `base_dataset_version` + `train_variant_id` 分割**。

把三種寫法攤開對照，每一格填的是「這個操作的單位改了之後，對應的產物會不會重算」：

| 寫法 | 改 train 切分單位 | 改 val 抽樣單位 |
|---|---|---|
| **一個鍵，登記進 `TRAIN_SAMPLING_KEYS`** | train 重算 ✓、val/test 不動 ✓ | base 不翻 → **val 靜默沿用舊資料** ✗ |
| **一個鍵，不登記** | base 翻 → **train 被連坐，val/test 也被連坐重算** ✗ | val 重算 ✓ |
| **兩個鍵**（本決定） | `train_variant_id` 翻、base 不翻：train 重算、val/test 完全不動 ✓ | base 翻：val 重算 ✓ |

一個鍵的兩種寫法各自壞在不同的地方，而且都是**靜默**的：第一種寫法下，使用者改了 val 的抽樣單位、pipeline 照跑、讀到的是用舊單位抽出來的 val parquet，沒有任何訊息；第二種寫法下，使用者只想調 train 的切分，代價是整條 dataset 重算一遍。兩個鍵是唯一讓「重算範圍」對得上「真的改了什麼」的寫法。

## 為什麼不放進 `schema` 區塊

概念上這兩個鍵是在描述 entity 的粒度，放 `schema:` 比較乾淨。但 `core/schema.py` 的 `_DEFAULTS` 整份會進入 `get_schema_for_hash` 的雜湊輸入，也就是進入 `base_dataset_version`。實測：往 `_DEFAULTS` 加一個鍵，同一組參數的 `base_dataset_version` 就會變 —— **所有既有 dataset 產物與模型立即失效**，而使用者根本還沒用到多欄 entity。

所以兩個鍵住 `dataset:`，命名對齊既有的 `dataset.sample_group_keys`（同樣是欄名清單）。`get_entity_grouping` 放在 `core/schema.py` 但**不在** `_DEFAULTS` 裡，就是這個取捨的落點。

## 為什麼預設是完整 entity，不是第一欄

預設成第一欄等於把今天的缺陷追認成規格。預設成完整 entity 則與周邊設定一致：`entity` 已經宣告了「一筆排序請求屬於誰」。

單欄設定下兩者等價，所以這個選擇**不需要任何人遷移**——這正是本決定的驗收條件之一。

## 為什麼 `conf/base` 裡兩個鍵都是註解狀態

因為版本 ID 雜湊的是**設定的值，不是程式碼**。把鍵寫成實鍵——即使值等同預設——就會把它加進雜湊輸入，翻掉它所餵的版本 ID，重算一批行為完全沒變的產物。

同一個理由，A29 拒絕 `train_split_keys:` 這種寫了鍵但沒有值的寫法：它與「不寫」跑出來的切分一模一樣，但不是同一個產物。`tests/test_core/test_parameters_dataset_yaml.py` 機械地擋住這件事。

零遷移的實測（`conf/base`、`feature_table_fingerprint` 固定為 `deadbeef`）：

```
改動前（main @ be2d95a）      改動後
base_dataset_version: db0d7ca5   db0d7ca5
train_variant_id:     ca1c510d   ca1c510d
calibration_variant:  9f4592de   9f4592de
```

## 為什麼第一版不允許 `entity` 以外的欄

限制成 `entity` 的子集，這兩個鍵就只是在說「entity 有多粗」。放寬成任意欄（例如按 `region` 切）會讓它變成通用分組機制，而「這個分組與 query group 的關係」沒有任何地方在檢查。那是另一個功能，要做另開票。

## 代價

- 使用者要理解「兩個鍵不是重複」。這份 ADR 就是那個解釋的家。
- `train_split_keys` 進了 `TRAIN_SAMPLING_KEYS`，所以未來若有人把它搬出去，train 產物會靜默共用不同單位切出來的資料。`tests/test_core/test_versioning.py::TestSplitUnitKeysVersionRouting` 把兩個鍵的路由方向都釘住了。
