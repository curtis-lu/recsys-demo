---
status: accepted
date: 2026-08-10
---

# 增量掛在「執行」上，不掛在「產物」上：`can_load` 對月份感知

[ADR-0002](0002-preprocessed-feature-table-incremental.md) 讓 dataset 只處理「設定列出、但尚未落地」的月份，[ADR-0007](0007-month-plans-travel-through-the-catalog.md) 把那個決定搬上 pipeline 定義。兩者都只服務 **test 鏈**的三個節點。

`conf/base/parameters_dataset.yaml` 的 `enable_calibration: true` 是出廠設定，所以 pipeline 實際有 15 個節點（`tests/test_pipelines/test_dataset/test_pipeline.py:20`）。加一個 `test_snap_dates` 月份時，其中**十個**節點——`select_sample_keys`、`split_train_keys`、`select_val_keys`、`fit_preprocessor_metadata`、`build_train_model_input`、`build_train_dev_model_input`、`build_val_model_input`、`filter_val_model_input`、`select_calibration_keys`、`build_calibration_model_input`——全量重算，並把逐位元相同的內容覆寫回同一批 partition（`io/hive_table_dataset.py` 的 `save()` 沒有 skip 分支）。

那不是遺漏。issue #123 的 Out of Scope 明文寫著「train／val／calibration 分支的增量化：本次的差集只服務 test 分支」。本 ADR 記錄的是**現在補上它的決定，以及為什麼機制不是當初預期的那一種**。

## 決定

**增量是「這次執行要做什麼」的性質，不是「這個產物新不新」的性質。**

具體：切片的收邊條件 `_can_load`（`__main__.py`）對三個增量產物改問月份——名字在 `INCREMENTAL_DATASETS` 裡就去 catalog 取對應的 `<name>_month_plan`，`to_process` 非空即回 `False`。加上一個具名切片旗標 `--only-test-months`，內部等價於 `--only-node filter_test_model_input` 再加上資料閘。

於是 DAG 自己推出正確的節點集：`preprocessor` 是落地的 JSON、載得到 → `fit_preprocessor_metadata` 不進來；`test_keys` 與 `preprocessed_feature_table` 缺新月份 → 兩個生產者被拉回；train/val/calibration 的 build 不在上游閉包裡 → 不進來。

## 為什麼是執行層，不是產物層

產物層的做法（每個產物自己檢查「我那個版本的 partition 在不在」）看起來更自動。否決它的理由不是工程量，是**兩者要出錯所需的條件不同**：

| | 出錯需要什麼 |
|---|---|
| 執行層（宣告式） | 使用者**主動**打了縮小範圍的旗標，而世界剛好在他沒宣告的那一側變了 |
| 產物層（skip-if-exists） | 什麼都不必做。跳過是**預設**，只要上游悄悄變了就靜默沿用 stale |

差別不在「會不會出錯」——兩者都會（見下一節的反例）——而在**預設值站哪一邊**。ADR-0002 已經為三個產物付過產物層的代價，並把它列為該決策「最需要誠實記錄的一件事」。把預設跳過擴到全部產物，是把那筆帳從三個放大到整條 pipeline，而換來的自動化，執行層用一個旗標就買到了。

**因此不新增逃生口。**「不帶旗標＝跑完整 DAG」本身就是逃生口，而且比一個要記得帶的旗標更難忘記。

### 這個論證的已知反例

執行層**不是**沒有靜默 stale 的可能，只是要主動觸發。具體路徑：同一次編輯裡既加了 test 月、又改了 `sample_ratio`。後者只翻 `train_variant_id`（`core/versioning.py`），不翻 `base_dataset_version`，所以 `--only-test-months` 選中的節點不會建立新 variant 底下的 train 產物；而 `HiveTableDataset.exists()` 走的是 `SHOW TABLES`（`io/hive_table_dataset.py` 的 `_table_exists`），**表級判定、完全不看 `partition_filter`**，所以 `core/runner.py` 開跑前的輸入檢查也不會報錯。training 隨後讀到 0 列。

這條路徑成立，決定仍然維持——因為它需要使用者主動宣告縮小範圍，而產物層的對應失效不需要任何人做任何事。但**「方向相反」是過度宣稱，正確的說法是「主動觸發 vs 被動預設」**。

同理，`base_dataset_version` 的定義是「扣掉 `test_snap_dates` 的一切」（`core/versioning.py` 的 `COVERAGE_ONLY_KEYS`）這件事，**只覆蓋 base 層**；`train_variant_id` 與 `calibration_variant_id` 不在它的保護範圍內。

## 為什麼是 `_can_load`，不是 `catalog.exists()`

`exists()` 回答「這張表在不在」，而切片需要的是「不跑生產者，消費者拿不拿得到它要的東西」。對增量產物，這兩個問題的答案不同。

`catalog.exists` 全 repo 只有兩個呼叫點：`core/runner.py` 開跑前的輸入驗證，與切片用的 `_can_load`。（`save()` 呼叫的是 `_table_exists`，不經過 `exists()`，所以不受影響。）不把月份感知下沉到 `HiveTableDataset.exists()` 的理由因此不是「波及面大」，而是**兩個呼叫點問的是不同問題**：runner 問「這個輸入完全不存在嗎」，切片問「這次要的內容齊了嗎」。把後者塞進前者，會讓一張缺月份的表被判定成不存在而直接 raise。

切片的收邊條件只有一處（`core/pipeline.py` 的 `_slice_with_expansion`），改在那條路徑上的 `_can_load` 是最小切面。

## 這同時關掉一個靜默缺陷

在現行的月份盲 `can_load` 下，`--only-node filter_test_model_input`（**正是本 ADR 的 preset 目標節點**）加了新月份之後：中間產物 `test_model_input_unfiltered` 不在 `conf/base/catalog.yaml`、是 runner 自動建的 MemoryDataset → `build_test_model_input` 被拉回；但 `test_keys` 是持久化 Hive 表、`can_load` 回 `True` → `select_test_keys` **不**被拉回 → 新月份的 keys 從未寫入 → 濾出 0 列 → `pipelines/dataset/steps/model_input.py` 沒有任何空值守衛 → dynamic partition overwrite 對 0 列的 frame 決定不出任何 partition。

**結果是什麼都沒寫、沒有任何錯誤**，而 `[months]` log 早在 `__main__.py` 建月份計畫時（切片決策**之前**）就無條件印了 `processed=<新月份>`。症狀不是「寫錯」，是「什麼都沒做卻宣稱做了」。

ADR-0007 刪掉 `filter_test_model_input` 的防禦性月份過濾時，論證是「切片讀不到中間產物就會把上游拉回來」。那個論證對它檢查的那一跳（`test_model_input_unfiltered` 是 `MemoryDataset`）成立，但沒有再往上追一跳——`test_keys` 是持久化 Hive 表，鏈就在那裡斷了。本 ADR 補上那一跳；ADR-0007 的其餘結論不變。

修 `_can_load` 一處同時解掉浪費與這個缺陷，是本輪不等效能量測就動手的理由。

## 考慮過但否決的選項

**偵測「這次只有 `test_snap_dates` 變了」再分支。** 要記住上一次的設定才能 diff，而那份狀態不存在。且不必要：`base_dataset_version` 每次啟動已經算好，它的定義就蘊含這個條件（限於 base 層，見上）。

**preset 硬寫 test 鏈的節點清單。** 日後鏈上多一個節點時，preset 會安靜地漏掉它。改成硬寫**一個終點**（`filter_test_model_input`）＋ DAG 反推，清單就不存在。該節點名在函式被刪之後仍保留可定址，ADR-0007 已確保。

**命名 `--incremental-test-month`。**「incremental」在本 repo 已是精確的既有詞（`INCREMENTAL_DATASETS`、`plan_incremental_snap_dates`），那三個產物**帶不帶旗標都是增量的**——旗標做的是縮小節點集，不是改變增量性。而且它與 `--rebuild-dates` 併用時字面打架（「增量新增」＋「重算既有」），而那是合法組合。`--only-*` 家族則自帶「這是切片、與 `--from-node`／`--only-node` 互斥」的提示。

## 後果

- **資料閘要顯式進 preset。** `validate_data_consistency` 的 `outputs=None`，而 `_slice_with_expansion` 的 producer map 只由 `node.outputs` 建——**沒有輸出的節點結構上永遠不會被自動拉回**。切片一旦成為加月份的建議動線，這個閘就從偶爾被跳過變成每次被跳過，而 B1（item 覆蓋）對新月份正是有意義的檢查。這是 preset 需要硬寫的第二個、也是最後一個節點名。相關：issue #157。
- **「test 鏈是哪些節點」成為單一常數。** preset 與 `_format_rebuild_slice_warning` 的觸發條件都要用到它。dataset 側原本只要「切片 ＋ `--rebuild-dates`」就無條件 WARN，而 preset 選中的就是整條鏈，那句警告會變成假警報、建議還相反（叫人拿掉旗標重跑）。改成條件式：鏈完整就不警告。
- **`scripts/rebuild_eval_month.sh` 改用新旗標。** 重算既有月份與新增月份需要的節點集完全相同，差別只在 month plan 把哪些月放進 `to_process`。這會**移除該腳本目前附帶的一層自癒**（它現在跑完整 dataset，順帶重建 train/val/calibration）。可以接受，因為那層自癒與腳本的職責無關：不變量 A21 要求 `--rebuild-dates` 的值必須是 `test_snap_dates` 的子集，所以它重算的永遠是 test 月份，而 train/val/calibration 產物本來就不由 test 月份決定。改完之後 `--rebuild-dates` 的語意回歸單一職責：**只決定哪些月份，不決定跑哪些節點**。
- **驗收看 partition 有沒有被寫，不看 wall-clock。** 本機 `local[*]` 與叢集的時間結構不同、會隨負載漂移，是不可重現的證據（`docs/operations/known-pitfalls.md` §4 記著同一個教訓）。判準：加月份後 train／val／calibration 的 model_input partition mtime 與加之前完全相同，輔以 `[plan] running N of M nodes`。這與 PR #135 用過的證據形式相同。

## 為什麼是現在，不是 #123 那一輪

值得記下來，因為這個落差的形狀會重複發生。

#123 的 Problem Statement **認得**這件事，逐字寫著「`base_dataset_version` 翻號 → 整條 dataset 從頭重算（**含完全沒變的 train／val／calibration 產物**）」。那個因果在當時是對的：翻號＝新目錄，新目錄裡什麼都沒有，train/val/calibration 當然得重算。

**但修掉成因不等於修掉浪費。** 停止翻號之後，那份重算從「無可避免」變成「可以避免」——而沒有人接著去避免它，因為它的真正機制與版本無關：runner 無條件跑完 DAG。#123 拆開了 hash 的三個關注點，沒有動到第四個從未被命名的東西——**執行與身分是分開的**。

而 User Story 4 的判準（「加月份的成本正比於『新月份』而不是『累積的總月份數』」）**結構上量不到它**：全量重建是與累積月份數無關的常數項，∝ 表述對常數項是盲的。所以 #126 的 mutation check（「把差集那一步改回『處理全部設定月份』，跳過相關的測試必須轉紅」）通過、PR #135 的 real-run 證據（四輪執行、`base_dataset_version` 全程 `4093a8ea` 不變、partition mtime 逐月比對）精準達標、驗收全綠——而需求沒被滿足。

Out of Scope 那一行的問題**不是「沒附理由」**（該清單有數項同樣只是換句話說），而是**它排除的正好是 Problem Statement 已經指認為浪費的東西**，而票面沒有任何一處解釋這個前後不一致。關票之後它就沉沒，也從未進過 `deliberate-non-goals.md`。

制度上的修補（Out of Scope 每項附理由、關票時未解決項搬進地雷圖）寫在 `docs/agents/issue-tracker.md`，**PR #201 待合併**。
