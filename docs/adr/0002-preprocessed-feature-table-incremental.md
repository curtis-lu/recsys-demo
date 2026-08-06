---
status: accepted
date: 2026-07-31
---

# `preprocessed_feature_table` 改為增量可擴充

dataset pipeline 原本把「config 列出的全部 snap_date」整批重算一次。[ADR-0001](0001-test-dates-out-of-dataset-version-identity.md) 讓新增 test 月份不再翻號之後，這個行為就變成純粹的浪費 ── 既有月份的 partition 會被重算成逐位元相同的內容。**改成只處理「config 列出的、但尚未落地的」月份**，差集由 metastore 的 partition 查詢（零掃描）得出，並提供 `--rebuild-dates` 作為強制重算的逃生口。

## 為什麼這是安全的

每個 `snap_date` partition 的內容 = f(該月 `feature_table` rows, `category_mappings`)，**與其他月份無關**：

- 編碼是純逐列的 map lookup（`preprocessing.py:68-92`），查表對象 `category_mappings` 只在 `train_snap_dates` 上 fit（`nodes_shared.py:31-33`、`pipelines/dataset/nodes_spark.py:269,288`）。
- `apply_preprocessor_to_features` 裡唯一的聚合是未知值計數，只餵給 `logger`，不進輸出。

所以日期過濾從來就不是產物身分的一部分，只是**工作量限制**（`feature_table` 可能存了十年，不能每次全編碼）。把它從「config 列出的全部」改成「尚未落地的」，不改變任何 partition 的內容，只改變這次要做多少工。

差集邏輯集中在 `pipelines/dataset/nodes_shared.py` 的單一 helper（`collect_dataset_snap_dates` 的鄰居），由 test 分支的四個 node 共同使用：`apply_preprocessor_to_features`、`select_test_keys`、`build_test_model_input`、`filter_test_model_input`。散在各 node 會讓「這次跳過了什麼」長出四種格式，也讓逃生口要穿過四個簽章。

> **本段的實作位置已過時（2026-08-03）**：差集邏輯（`plan_incremental_snap_dates`）與上述取捨不變，但**套用**方式改了——計畫由 CLI 算一次、以具名 dataset 進 catalog，節點把它當一般 input 收下，而不是各自從 `parameters` 重算；`filter_test_model_input` 的防禦性過濾已刪除（它守的情境進不來）。現行實作與理由見 [ADR-0007](0007-month-plans-travel-through-the-catalog.md)。

## 考慮過但否決的選項

**只在 `apply_preprocessor_to_features` 做差集。** 改動最小，但下游三個 node 仍按完整 `test_snap_dates` 重算 ── 結果冪等、正確，只是白做。代價是**加第 N 個月的成本 ∝ N 而非 ∝ 1**：累積到 12 個月時，每加一個月要重算 12 個月的 keys 與 model_input。而 `select_test_keys` 是 full population、沒有抽樣（`pipelines/dataset/nodes_spark.py:127-130`），這條鏈不便宜。半套的增量會在幾個月後被迫重做。

## 後果

- **這把 `exists() ≠ fresh` 制度化了。** 這是本 ADR 最需要誠實記錄的一件事。`docs/pipelines/evaluation.md` §7.4 已經為 pipeline slicing 警告過同一個陷阱 ── `catalog.exists()` 只能證明產物存在，不能證明它來自當前的設定與上游資料。本 ADR 把它從「切片時要小心」升級成**正常執行路徑的預設行為**：一旦 `feature_table` 對某個舊月份回補或修正資料，該月的 partition 將永遠不會更新，而且不報錯。
- **`--rebuild-dates` 是上述取捨的對價，不是可選的便利。** 沒有它，唯一的重算手段是讓版本翻號（＝重建整個 dataset），那正是本系列改動要消滅的行為。其值必須是設定中 `test_snap_dates` 的子集，否則靜默無效 ── 這條不變量放在 `core/consistency.py` 加 predicate，依 `CLAUDE.md` 的規範不得散落在 pipeline 裡。
- **一個會自己決定少做事的 pipeline，必須把它決定不做的事說出來。** 「本次處理了哪些月份、跳過了哪些」寫進 dataset 收尾的 structured log 與 manifest。這不是加碼觀測性，是本決策的必要配套：跳過的行為若不可見，上一條的靜默 stale 就無從察覺。
