---
status: accepted
date: 2026-08-10
---

# 增量掛在「執行」上，不掛在「產物」上：`can_load` 對月份感知

[ADR-0002](0002-preprocessed-feature-table-incremental.md) 讓 dataset 只處理「設定列出、但尚未落地」的月份，[ADR-0007](0007-month-plans-travel-through-the-catalog.md) 把那個決定搬上 pipeline 定義。兩者都只服務 **test 鏈**。加一個 `test_snap_dates` 月份時，其餘八個節點——`select_sample_keys`、`split_train_keys`、`select_val_keys`、`fit_preprocessor_metadata`、`build_train_model_input`、`build_train_dev_model_input`、`build_val_model_input`、`filter_val_model_input`——仍然全量重算，並把逐位元相同的內容覆寫回同一批 partition（`io/hive_table_dataset.py` 的 `save()` 沒有 skip 分支）。

那不是遺漏。issue #123 的 Out of Scope 明文寫著「train／val／calibration 分支的增量化：本次的差集只服務 test 分支」。本 ADR 記錄的是**現在補上它的決定，以及為什麼機制不是當初預期的那一種**。

## 決定

**增量是「這次執行要做什麼」的性質，不是「這個產物新不新」的性質。**

具體：切片的收邊條件 `_can_load`（`__main__.py`）對三個增量產物改問月份——名字在 `INCREMENTAL_DATASETS` 裡就去 catalog 取對應的 `<name>_month_plan`，`to_process` 非空即回 `False`。加上一個具名切片旗標 `--only-test-months`，內部等價於 `--only-node filter_test_model_input` 再加上資料閘。

於是 DAG 自己推出正確的節點集：`preprocessor` 是落地的 JSON、載得到 → `fit_preprocessor_metadata` 不進來；`test_keys` 與 `preprocessed_feature_table` 缺新月份 → 兩個生產者被拉回；train/val 的 build 不在上游閉包裡 → 不進來。

## 為什麼是執行層，不是產物層

產物層的做法（每個產物自己檢查「我那個版本的 partition 在不在」）看起來更自動。否決它的理由不是工程量，是**兩者的失誤方向相反**：

| | 忘記做某件事的後果 |
|---|---|
| 執行層（宣告式） | 忘記帶旗標 → **多跑**。浪費，但數字對 |
| 產物層（skip-if-exists） | 忘記帶逃生口 → **靜默沿用 stale**。數字錯，而且沒有錯誤訊息 |

ADR-0002 已經為三個產物付過第二種代價，並把它列為該決策「最需要誠實記錄的一件事」。把同樣的機制擴到九個產物，等於把那筆帳乘三——而換來的自動化，執行層用一個旗標就買到了。

還有一件事讓天平更斜：`compute_feature_table_fingerprint` 是 schema-only（只看欄名與 dtype），上游對既有月份**改值不改 schema** 不會 bump 版本。這個缺口已登記在 `docs/agents/deliberate-non-goals.md`。產物層的判準會直接踩在它上面；執行層不會，因為跳過永遠是使用者這一次顯式要的。

**因此不新增逃生口。**「不帶旗標＝跑完整 DAG」本身就是逃生口，而且比一個要記得帶的旗標更難忘記。

## 為什麼是 `_can_load`，不是 `catalog.exists()`

`exists()` 回答「這個產物在不在」，而切片需要的是「不跑生產者，消費者拿不拿得到它要的東西」。對增量產物，這兩個問題的答案不同。

不把月份感知下沉到 `HiveTableDataset.exists()`，因為那個方法還有別的呼叫點問的是原本那個問題：runner 開跑前的輸入驗證、`save()` 內部的 `_table_exists`。偷換語意會波及它們。切片的收邊條件只有一處（`core/pipeline.py` 的 `_slice_with_expansion`），改在那條路徑上的 `_can_load` 是最小切面。

## 這同時關掉一個靜默缺陷

現行 `--only-node build_test_model_input` 在加了新月份之後：`can_load("test_keys")` 因為表存在而回 `True` → `select_test_keys` 不被拉回 → 新月份的 keys 從未寫入 → 濾出 0 列 → `pipelines/dataset/steps/model_input.py` 沒有任何空值守衛 → **寫出空 partition、不報錯，而 `[months]` log 仍印 `processed=<新月份>`**。log 說做了，實際沒做。

ADR-0007 刪掉 `filter_test_model_input` 的防禦性月份過濾時，論證是「切片讀不到中間產物就會把上游拉回來」。那個論證對它檢查的那一跳（`test_model_input_unfiltered` 是 runner 自動建的 `MemoryDataset`）成立，但沒有再往上追一跳——`test_keys` 是持久化 Hive 表，`can_load` 對它回 `True`，鏈就在那裡斷了。本 ADR 補上那一跳；ADR-0007 的其餘結論不變。

修 `_can_load` 一處同時解掉浪費與這個缺陷，是本輪不等效能量測就動手的理由。

## 考慮過但否決的選項

**偵測「這次只有 `test_snap_dates` 變了」再分支。** 要記住上一次的設定才能 diff，而那份狀態不存在。且不必要：`base_dataset_version` 的定義**就是**「扣掉 `test_snap_dates` 的一切」（`core/versioning.py` 的 `COVERAGE_ONLY_KEYS`），這個條件每次啟動已經算好了。

**preset 硬寫 test 鏈的節點清單。** 日後鏈上多一個節點時，preset 會安靜地漏掉它。改成硬寫**一個終點**（`filter_test_model_input`）＋ DAG 反推，清單就不存在。該節點名在函式被刪之後仍保留可定址，ADR-0007 已確保。

**命名 `--incremental-test-month`。**「incremental」在本 repo 已是精確的既有詞（`INCREMENTAL_DATASETS`、`plan_incremental_snap_dates`），那三個產物**帶不帶旗標都是增量的**——旗標做的是縮小節點集，不是改變增量性。而且它與 `--rebuild-dates` 併用時字面打架（「增量新增」＋「重算既有」），而那是合法組合。`--only-*` 家族則自帶「這是切片、與 `--from-node`／`--only-node` 互斥」的提示。

## 後果

- **資料閘要顯式進 preset。** `validate_data_consistency` 的 `outputs=None`，而 `_slice_with_expansion` 的 producer map 只由 `node.outputs` 建——**沒有輸出的節點結構上永遠不會被自動拉回**。切片一旦成為加月份的建議動線，這個閘就從偶爾被跳過變成每次被跳過，而 B1（item 覆蓋）對新月份正是有意義的檢查。這是 preset 需要硬寫的第二個、也是最後一個節點名。相關：issue #157。
- **「test 鏈是哪些節點」成為單一常數。** preset 與 `_format_rebuild_slice_warning` 的觸發條件都要用到它。dataset 側原本只要「切片 ＋ `--rebuild-dates`」就無條件 WARN，而 preset 選中的就是整條鏈，那句警告會變成假警報、建議還相反（叫人拿掉旗標重跑）。改成條件式：鏈完整就不警告。
- **`scripts/rebuild_eval_month.sh` 改用新旗標。** 重算既有月份與新增月份需要的節點集完全相同，差別只在 month plan 把哪些月放進 `to_process`。讓兩條動線共用同一個節點集，`--rebuild-dates` 的語意回歸單一職責：**只決定哪些月份，不決定跑哪些節點**。
- **驗收看 partition 有沒有被寫，不看 wall-clock。** 本機 `local[*]` 與叢集的時間結構不同、會隨負載漂移，是不可重現的證據（`docs/operations/known-pitfalls.md` §4 記著同一個教訓）。判準：加月份後 `train_model_input` / `val_model_input` 的 partition mtime 與加之前完全相同，輔以 `[plan] running N of M nodes`。這與 PR #135 用過的證據形式相同。

## 為什麼是現在，不是 #123 那一輪

值得記下來，因為這個落差的形狀會重複發生。

#123 的 Problem Statement **認得**這件事，逐字寫著「`base_dataset_version` 翻號 → 整條 dataset 從頭重算（**含完全沒變的 train／val／calibration 產物**）」。那個因果在當時是對的：翻號＝新目錄，新目錄裡什麼都沒有，train/val 當然得重算。

**但修掉成因不等於修掉浪費。** 停止翻號之後，那份重算從「無可避免」變成「可以避免」——而沒有人接著去避免它，因為它的真正機制與版本無關：runner 無條件跑完 DAG。#123 拆開了 hash 的三個關注點，沒有動到第四個從未被命名的東西——**執行與身分是分開的**。

而 User Story 4 的判準（「加月份的成本正比於『新月份』而不是『累積的總月份數』」）**結構上量不到它**：全量重建是與累積月份數無關的常數項，∝ 表述對常數項是盲的。所以 #126 的 mutation check（「把差集那一步改回『處理全部設定月份』，跳過相關的測試必須轉紅」）通過、PR #135 的 real-run 證據（四輪執行、`base_dataset_version` 全程 `4093a8ea` 不變、partition mtime 逐月比對）精準達標、驗收全綠——而需求沒被滿足。

Out of Scope 那一行是六項中唯一沒附理由的，關票後就沉沒，也從未進過 `deliberate-non-goals.md`。制度上的修補（Out of Scope 每項附理由、關票時未解決項搬進地雷圖）已落在 `docs/agents/issue-tracker.md`。
