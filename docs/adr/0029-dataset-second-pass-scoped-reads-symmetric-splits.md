---
status: accepted
date: 2026-09-25
---

# dataset pipeline 第二輪整理：讀取寫明月份、各 split 同一套做法、資料閘只寫判斷

> **判準不在這裡。** 形狀判準的唯一真實來源是 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)（本份稱「node 規則 N」），流程判準是 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)（「flow 規則 N」）。本份只記：把判準再套一次到 dataset 時，每條岔路選了哪一邊、為什麼。
>
> - **[ADR-0008](0008-dataset-modules-split-by-role.md) 仍然有效。** 它定的兩條判準與目錄形狀（node 在 `nodes.py`、只有本 pipeline 用的機制在 `steps/`）本份沒有改。本份補的是它之後長歪的地方，以及新定的規則。
> - **證據在另一份。** 下文的 `explain()` 實測、功能清單、23 處脫節的逐列清單，都在 [`docs/notes/2026-09-25-dataset-pipeline-inventory.md`](../notes/2026-09-25-dataset-pipeline-inventory.md)（與本份同一批進 main）。
> - 下文的「這次」都指「這一次執行 pipeline」；指這份文件時寫「本份」。
> - **這份會被修正。** 它寫在實作之前。實作時發現某條站不住，照 flow 規則 2 回頭改這裡，不能只寫在 commit message。
> - **行號會腐爛**，所以只給檔名與函式名。現況數字核對於 2026-09-25，main @ `2e99c573`。

---

## 這份在解什麼問題

ADR-0008 那一輪的結構搬移在 2026-08-08（PR #176）合併時，`pipelines/dataset/nodes.py` 是 671 行。之後加了 13 個功能（候選層級特徵表、多欄 item、item 清單從資料數、無正例 query group 的三個鍵、精度閘 B8、粒度閘 B10……），今天是 1733 行。

長度本身不是問題，node 規則 2 允許 node 長。問題是長出來的東西有五種形狀：

1. **讀取量跟著表的歷史長，不跟著這次的月份長。** 2026-09-25 對 train、train_dev、val、test 四個 build 跑 `explain()`（local[*]、合成資料 14 個月，train 10／val 1／test 1）：
   - `label_table`：四個 build 都讀全部 14 個月，沒有任何月份條件。
   - `preprocessed_feature_table`：有修剪，但靠的是 Spark 的 dynamic partition pruning（DPP，執行時依 join 對面的值自動跳過分區），程式碼裡沒寫。DPP 要 join 對面小到能整份廣播（broadcast）才會插入。生產的 entity 是百萬級，keys 超過 `spark.sql.autoBroadcastJoinThreshold`（預設 10MB，repo 沒有覆寫）時，這個修剪很可能靜默消失。
   - test 月份在同一個 `base_dataset_version` 下會累積（ADR-0001），所以沒修剪的讀取量逐月變大。
   - ⚠ 2026-09-25 更正：`preprocessed_feature_table` 那一列「有修剪，但靠 DPP」不成立，見決定 3 之後的〈實作後的更正〉第 1 條。實際上 train、val 的 build 讀它的全部分區。
2. **train 與 val／test 做同一件事，做法不同。**
   - 丟無正例的 query group：train 在 keys 上丟，val／test 在建好的 model_input 上丟。
   - 「整個單位一起留或一起丟」的抽樣有三份實作。
   - 去重：train 相信主鍵，val／test 各自 `dropDuplicates`。
   - entity 為 NULL 的列：train 丟掉並警告，val 抽樣時默默丟，val 不抽樣時與 test 則留著。
3. **資料閘佔 `nodes.py` 的 37%。** `validate_data_consistency`、`validate_numeric_precision`、`validate_model_input_grain` 連同它們的私有 helper 共 645 行，機制都內聯在 node 裡：巢狀函式、組報告的迴圈、候選層級表那一段又複製一份。「這次寫了哪些檔」有兩套實作。#406 第 2、3、5 項點名的就是這裡。
4. **選用輸入靠 CLI 注入。** 候選層級特徵表要穿過 7 個 node（這 7 個真的都要讀它），另外還有 3 份月份清單由 CLI 算好注入。`__main__.py` 的 `dataset()` 有 267 行，裝了指紋、版本雜湊、候選表偵測、manifest 等只有 dataset 懂的事。
5. **幾個設定面的靜默失效。**
   - `--only-test-months` 撞上抽樣設定改動時，training 讀到 0 列（#334）。
   - `preprocessor_on_disk` 的路徑寫錯時，B19 默默跳過。
   - 框架程式碼寫死了銀行示例的欄名。

另外有 23 處文件、ADR、docstring 與現況對不上，例如行號過期、引用已刪的函式、`docs/pipelines/dataset.md` 仍說切分只用 entity 的第一欄。逐列清單在盤點 note 第三節，收尾時一起處理。

---

## 決定總表

| # | 決定 | 會改到的東西 | 落地產物會不會變 |
|---|---|---|---|
| 1 | 每一步只讀這次要用的 time 值，而且寫在程式碼裡 | 四個 build 對三張右表加月份篩選；`require_months_present` 先篩再問 | 不變（逐列相同） |
| 2 | build 要讀哪些月份，由 node 自己算 | 拿掉 CLI 注入的 3 份候選表月份清單；只注入執行模式給精度閘 | 不變 |
| 3 | 月份篩選只留一種寫法 | 刪 `restrict_to_months`、`restrict_to_months_or_all` | time 欄是 DATE 時不變 |
| 4 | val／test 的丟組搬到 keys 上；B10 涵蓋四個 split | 新的 keys 層篩選 node；拿掉 `filter_val_model_input`、`filter_test_model_input` | r ＝ 0 或 1 時 model_input 逐列相同；0 < r < 1 時留下的無正例組會換一批；`val_keys`、`test_keys` 變成丟過組的 |
| 5 | 整單位抽樣只留一個逐列機制；entity 有 NULL 一律丟並警告 | `keep_entities_drawn_under_ratio`；各 split 選 key 那一步 | 沒有 NULL entity 時不變 |
| 6 | 各 split 都相信上游主鍵，不去重 | `select_val_keys`、`select_test_keys` | 沒有重複 identity 時不變 |
| 7 | 資料閘的 node 只寫「查什麼、什麼算失敗」，機制進 `steps/` | 三個資料閘；B8 與 B10 共用一個「這次寫了哪些檔」模組 | 報告內容不變（B10 多查 val、test 兩個 split） |
| 8 | `nodes.py` 不拆檔 | — | — |
| 9 | 改 `carry_columns` 只翻 `train_variant_id` | `TRAIN_SAMPLING_KEYS` | **版本 ID 變一次** |
| 10 | 刪 `category_mappings` 產物；框架不寫死示例欄名 | catalog、`fit_preprocessor_metadata`、`prepare_model_input_config` | 少一個 JSON 檔 |
| 11 | CLI 裡只有 dataset 懂的邏輯，搬進 dataset 根層的契約模組 | `__main__.py` 的 `dataset()` | 不變 |
| 12 | `--only-test-months` 要求 train 版本的表已經落地；跑完之後，train 版本的表在，才寫 `completed`、更新 `latest` | 同上 | 原本會靜默 0 列的執行改為被擋下 |
| 13 | `preprocessor_on_disk` 由程式從 `preprocessor` 推出 | dataset 與 evaluation 組 catalog 的地方 | 不變；部署的 catalog 可以少寫一條 |
| 14 | `pipeline-node-design.md` 加四條規則，登記一筆例外 | 該文件 | — |
| 15 | `base_dataset_version` 的雜湊加一個「dataset 產物格式版本」常數，本份把它加 1 | `core/versioning.py` | **每個部署的版本 ID 都變一次** |

**涵蓋的票**：
- #406：第 1 項由決定 3 解決；第 2、3、5 項由決定 7 解決；第 4 項的前提已被 ADR-0025 推翻（event 進了 identity，B10 的語意不變），關票時註明。
- #334：由決定 12 解決。

---

## 決定 1　每一步只讀這次要用的 time 值，而且寫在程式碼裡

**規則**：凡是讀來源表或落地中間表的地方，node 要在程式碼裡寫明這一步讀哪些 time 值，不能指望 Spark 從 join 對面推導出修剪。具體是：

- 四個 build（`build_model_input`，以及 `build_val_model_input`、`build_test_model_input`）先把 `label_table`、`preprocessed_feature_table` 篩到該 split 的月份，再做 join。候選層級特徵表今天已經這樣做。
- 丟無正例組時接的窄 `label_table`（今天的 `filter_train_keys`，以及決定 4 新增的 val／test 版本）同樣先篩月份。今天的 `filter_train_keys` 沒有篩。
- `require_months_present` 先篩到要檢查的月份，再問「哪些月份存在」，不再掃整張表的 time 欄。精度閘對候選層級表的做法（先篩月份，同一次掃描順便回答哪些月份存在）就是範本。

上面是 2026-09-25 盤點到的讀取點，不保證完整。實作時以「每一個 `.join`、每一個讀來源表的地方」逐一檢查，不以這份清單為準。

**為什麼結果不變**：label 與特徵的 join 鍵都含 time（identity 與 base key 都含 time），別的月份的列本來就接不上。篩選只改變成本，不改變答案。

**為什麼不是「靠 DPP 就好」**：DPP 是 optimizer 盡力而為的最佳化，觸發條件在生產規模很可能不成立，而且它消失時沒有任何訊號。`base_dataset_version` 的修剪是 `load()` 下 WHERE 的硬保證，月份修剪也要是同一個等級。

**為什麼不是先訂一個秒數目標**：`pipeline-performance-work.md` 規則 11 要求先有目標數字才開始優化。repo 裡沒有任何 dataset 在生產環境的耗時紀錄，唯一的量測（2026-09-06）又早於之後的六次結構改動。所以本份把效率目標定成一條結構原則，驗收看 plan 裡有沒有月份條件、讀了幾次，不看秒數（flow 規則 8：成本驗收量次數，不量秒數）。

**和已否決的假設不衝突**：2026-09-06 否決的是「用 cache 或 bucketing 讓特徵表只洗一次」（`docs/notes/2026-09-06-dataset-pipeline-profiling.md` §6.2）與「換 join 順序」（§7.1）。兩者都是在固定 13 個月的資料上量的。「按月份修剪」沒有被測過，而且它的量級會隨累積的月份數成長。

## 決定 2　build 要讀哪些月份，由 node 自己算

**規則**：
- train 與 train_dev 的 build 讀 `dataset.train_snap_dates`，val 的 build 讀 `dataset.val_snap_dates`。
- test 的 build 讀它本來就收到的 `test_model_input` month plan 裡的 `to_process`。
- 同一個答案同時用在 `label_table`、`preprocessed_feature_table` 與候選層級表。
- `month_plans.py` 的 `candidate_feature_table_months`，以及它注入的 3 個 DAG 輸入（名字由 `candidate_months_input` 產生），一起刪掉。

**為什麼**：這些答案不是一行設定值，就是 node 本來就收到的輸入。繞道 CLI，DAG 就多 3 個輸入，`month_plans.py` 裡還多住一個自己承認「不是 month plan」的函式。讀 build 的人應該當場看到月份從哪來（node 規則 2）。

**唯一需要注入的**：精度閘（B8）檢查候選層級表時，讀的是「這次各 build 要讀的月份」的聯集；用 `--only-test-months` 時，這個聯集只有 test 月份。精度閘自己看不出這次是不是 `--only-test-months`，所以**只注入執行模式**（是或不是 `--only-test-months`），聯集由精度閘用同一套規則自己算。不注入任何月份清單。執行模式是 node 看不到、開跑前就決定的事實，照新規則 15（見決定 14）可以注入。

> **實作註記（2026-09-25，#460）**：
> - 精度閘要照 test build 的規則算 test 那一份，所以它除了執行模式（catalog 名 `only_test_months`），還多收 `test_model_input_month_plan`。那份計畫本來就由 CLI 注入給 `build_test_model_input`，不是新注入的東西。
> - train 與 train_dev 的 build 改跑新的 node 函式 `build_train_model_input`（node 名不變）；`build_model_input` 不再直接註冊成 node，而是三個 build 共用的組裝，`months` 是沒有預設值的 keyword 參數。沒有預設值，是因為忘了傳的 build 會讀到別的 split 的月份，每個 key 都接不到 label 與特徵，而且不報錯。

**沒宣告候選表時的 `None` 維持現狀**：沒宣告時，CLI 注入 `None`，node 內部分支處理。
- 框架沒有一等的「選用輸入」：`core/runner.py` 按位置綁定輸入。
- 另一條路是依設定改變 DAG 的長相（先例如 evaluation `pipeline.py` 的 `if compare_source is not None:`）。那樣做，node 內的分支還是在，而且不同部署的 `--list-nodes` 會長得不一樣，什麼也沒省到。

所以這部分只整理寫法：每個 node 裡的候選表分支縮成一行具名步驟的呼叫。

## 決定 3　月份篩選只留一種寫法

**規則**：dataset 裡所有按 time 值篩選的地方，全部改用 `months_filter_as_date`（先 `to_date` 再比），刪掉 `restrict_to_months` 與 `restrict_to_months_or_all`。除了在 node 裡直接呼叫這兩個函式的地方，還有兩處容易漏掉：`validate_data_consistency` 裡內聯的 `F.col(time_col).isin(...)`，以及 `steps/categoricals.py` 的 `count_items_in_months`（它呼叫 `restrict_to_months`）。

**為什麼**：
- `restrict_to_months` 直接拿 `pd.Timestamp` 去比。time 欄是 STRING 時（Hive 分區欄讀回來就是 STRING），它一列都對不到，而且不報錯。
- 今天 `fit_preprocessor_metadata` 對 `feature_table` 用這一種，`apply_preprocessor_to_features` 對同一張表用另一種。ADR-0008 登記過「哪一邊才對，還沒有人判定」。**本決定的判定是：先轉日期那一邊。**
- 2026-09-25 的 explain 顯示，兩種寫法的 `PartitionFilters` 一樣能修剪，改寫法沒有效能代價。
- `restrict_to_months_or_all` 的「空清單＝整池」分支，早就被 A23（`train_snap_dates` 必填）封死了（#406 第 1 項）。

## 決定 1–3 實作後的更正（2026-09-25，#460）

#460 的審查（一個視角專找「本份寫錯在哪」）用 local[*] 實跑查出下面幾件。決定本身都不變，要改的是理由與範圍的說法。實驗腳本與輸出沒進 repo；做法寫在各條裡，照著在 local[*] 上重做即可。

1. **「有修剪，但靠 DPP」不成立：train、val 的 build 今天就讀整張 `preprocessed_feature_table`。**
   - 量法：先 `collect()` 真的執行，再讀執行後計畫裡每個 scan 的 `numPartitions`。
   - 結果：`keys LEFT JOIN 右表`（build 的形狀）讀了 12 個分區中的 12 個，AQE 開或關都一樣。兩個正對照：把右表放左邊的 inner join、或設 `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false`，都只讀 2 個——所以這個量法抓得到真的修剪。
   - 原因是結構，不是規模：這個設定預設是 `true`，DPP 只能重用 keys 那一側的 broadcast；而 LEFT join 只能 broadcast 右表。所以 keys 再小都不會修剪。
   - 盤點 note §1 當證據的 `dynamicpruningexpression(... IN dynamicpruning)`，是執行**前**的 `explain()` 印的占位字串。有沒有真的修剪，那一行長得一模一樣。
   - 對決定 1 的影響：理由更強，不是更弱——原本以為「規模大了才會消失」的修剪，其實從來沒有。唯一的例外是 test build：它的 keys 本來就篩過月份，optimizer 能從 join 條件推出右表的月份條件（約束推導，不是 DPP）。
   - 對決定 14 規則 14 的驗證方法：「看 `explain()` 的 `PartitionFilters`」要補一句——執行前計畫裡的 `dynamicpruning` 不算修剪的證據，要看執行後讀了幾個分區。
2. **明寫篩選，只有依 time 分區的表才省掉掃描。**
   - 依 time 分區的表（本框架寫的 `preprocessed_feature_table`，以及 time 是分區欄的來源表）：`PartitionFilters` 帶月份條件，只讀那幾個月的分區。
   - 沒依 time 分區的來源表：照樣整張掃。time 欄是 DATE 時，月份條件會下推成 parquet 的 row group 篩選，能跳多少看檔案怎麼排；time 欄是 STRING 或 TIMESTAMP 時連這個都沒有。省下的是進 join、進 shuffle 的列。
   - 所以決定 1「月份修剪也要是同一個等級」的準確說法是：**進 join 的只剩這次的月份，寫在程式裡、不靠 optimizer**；讀取本身省多少，看來源表怎麼分區，那是部署的事。本機合成資料的 `label_table` 沒分區，e2e 看到的會是 Filter，不是 PartitionFilter。
3. **決定 3 會改變落地內容的情況，比總表列的多。** 舊寫法拿 `pd.Timestamp` 直接比，下面這些情況一個月份都對不到，新寫法對得到：
   - time 欄是 STRING（總表已列）。原因是 `STRING IN (TIMESTAMP)` 被轉成字串比對 `"2025-01-31 00:00:00"`。
   - time 欄是 TIMESTAMP 且帶時分（例如 08:00）。
   - 字串沒補零（`2025-1-31`）。
   - 執行 Python 的行程時區與 Spark session 時區不同時，連 DATE 欄都對不到（本機 Asia/Seoul 對 Asia/Taipei 的那個坑）。新寫法之後，dataset 的月份篩選不再依賴 `TZ`。
   這些情況在舊寫法下都是讀到 0 列，生產上不會有人在這種狀態下交付；但〈版本與順序的約束〉第 2 條要照這份清單讀，不只 STRING。
4. **`require_months_present` 也照日期比。** 決定 1 點名精度閘的做法是範本，範本是先轉日期再問有哪些月份；#460 讓它照做（收回來的是 `to_date` 之後的值）。結果：TIMESTAMP 帶時分的月份不再被誤報為缺月；寫成 `2025/01/31`、`20250131` 這種格式的字串，以前存在檢查放行、篩選卻讀到 0 列（靜默編出空月份），現在改成在存在檢查就報錯，訊息會提醒字串要寫成 `YYYY-MM-DD`。

---

## 決定 4　val／test 的丟組搬到 keys 上；B10 涵蓋四個 split

下文的 r 是各 split 無正例組的保留比例：`dataset.train_zero_positive_group_ratio`、`dataset.val_zero_positive_group_ratio`、`dataset.test_zero_positive_group_ratio`（ADR-0025 決定 3）。

**規則**：
- val／test 比照 train 的 `filter_train_keys`：在 keys 上先接一次只帶 identity 與 label 的窄 `label_table`，判斷每個 query group 有沒有正例，把要丟的組從 keys 裡丟掉，然後照常 build。
- r > 0 時，權重欄（`ZERO_POSITIVE_GROUP_WEIGHT_COL`）在這一步加到 keys 上，由 build 帶進 model_input（`steps/model_input.py` 的 `model_input_columns` 本來就會帶上 keys 多出的欄）。
- `filter_val_model_input`、`filter_test_model_input` 拿掉，中間物 `val_model_input_unfiltered`、`test_model_input_unfiltered` 也跟著拿掉。
- B10 改成四個 split 都配對檢查：train、train_dev、val、test。
  - train、train_dev、val 不是增量表，照今天的做法整份比（目前版本底下的全部檔案）。`--only-test-months` 時它們沒有重建，再比一次也只是讀檔尾，不讀資料。
  - test 是增量的，比的是 test month plan 裡 `to_process` 的月份：`test_model_input` 這些月份的檔，對上 `test_keys` 的同一批月份，不是整張 `test_keys`。`to_process` 是空的時候，test 那一對不檢查，在報告與 log 裡寫明「本次未寫入」，不當成失敗。B10 因此要收 `test_model_input_month_plan` 當輸入——就是 test 的 build 用的那一份，不是 `test_keys` 那一份（上次執行中途失敗時，兩份的 `to_process` 可能不同）。它本來就是 DAG 上的現成輸入，不是新的注入。
  - `to_process` 裡某個月份丟組之後一列都沒寫（例如 r ＝ 0、那個月的 label 還沒進來）：keys 與 model_input 那個月都沒有檔，視為 0 ＝ 0，照實寫進報告，不報錯。今天 B10 對「有檔但範圍內零個檔」的前置檢查不適用這種情況，改寫時要分開。
  - `validate_model_input_grain` 與新的 test keys 篩選 node 都要加進 `ONLY_TEST_MONTHS_NODES`。前者不加，「涵蓋 test」會在加 test 月份的主要路徑上落空；後者不加，`test_keys` 的新月份不會落地。B10 的輸出沒有下游讀者，防漂移測試從終點往回推導推不到它，所以要跟精度閘一樣在測試裡明列。
  - `core/consistency.py` 的 B10 說明裡「只涵蓋兩個 split 是限制、不是疏忽」那一段（理由是 val／test 在建表之後才丟組）會被推翻，要跟著改寫。

**r ＝ 0 或 1 時結果不變**：判斷用的仍是 `label_table` 的 label，看到的仍是同一批 keys。ADR-0025 決定 3 的補記（2026-09-21）對 train 做過同一個搬遷，並寫明留下的組與列跟建表之後再丟逐列相同。val／test 的 r 預設是 0。

**0 < r < 1 時留下的無正例組會換一批**：
- 無正例組去留靠 `spark_bucket` 對 query group 欄算桶號，而 query group 含 time。`spark_bucket` 對 DATE／TIMESTAMP 欄先格式化成 `yyyy-MM-dd HH:mm:ss`，對 STRING 欄照原樣轉字串（`utils/hashing.py`）。
- 今天 val／test 的丟組跑在「從 Hive 讀回來的 `val_keys`／`test_keys`」建成的表上，那時 time 是 STRING 分區欄。搬到 keys 上之後，丟組發生在落地之前，keys 直接來自 `sample_pool`；在框架的 `source_etl` 與兩份示例裡，`sample_pool` 的 time 是 DATE。字串不同，桶號就不同。（使用者自備、time 本來就是 STRING 的 `sample_pool`，桶號不會換。）
- train 當年搬遷時沒有這個問題，因為它的 keys 在丟組之前就已經落地一次（`sample_keys`），前後看到的都是 STRING。
- 換一批不是錯：仍然是決定性的，同一份設定重跑得到同一批，留下的比例仍接近 r。而且決定 15 會讓版本 ID 翻一次，新舊兩批不會混在同一個 ID 下。`examples/ad` 設的是 r ＝ 0.5，所以它的 digest 會變，這是預期中的變化，不是實作錯誤。
- 順帶記下這個陷阱：同一份 key 在落地前後的桶號不同，因為 time 的型別變了，而且不會報錯。凡是「對同一批 key 抽樣要可重現」的地方，都要在同一個形態（落地前或落地後）上算。

**為什麼今天是兩種做法，而那個理由不成立**：`filter_val_model_input` 的 docstring 說，沒有東西把 val 的列數釘在它的 keys 上（B10 只涵蓋 train／train_dev），所以建表之後再丟不會破壞什麼。但 B10 之所以不涵蓋 val／test，正是因為丟組放在建表之後——這個理由指向它自己造成的結果。後果是：val／test 的 join 如果因為右表有重複的鍵而把列數放大，今天沒有任何東西擋得住。

**#429 的限制仍然守住**：
- #429 要求「有沒有正例」要看 `label_table` 的 label，不看 `sample_pool` 自帶的 label 副本。那欄是使用者 SQL 抄過來的，框架不保證兩者一致；看錯的話，會把真正有正例的組整組刪掉，而且粒度閘比對不出來。
- #429 列為不做的，是「用 `sample_pool` 的 label 提前丟組」，因為那得先加一道 label 一致性閘。本決定自己接窄的 `label_table`，不需要那道閘。

**代價**：
- val／test 的 r 預設是 0，所以這一步每次都會跑，多一次窄 join。換掉的是在寬表（帶全部特徵欄）上做的 window。
- 0 < r < 1 時，組數計數改在窄表上算，不再把整個 build 重跑一次。
- `val_keys`、`test_keys` 這兩張落地表的內容變成「丟過組的」，r > 0 時還會多一欄權重（catalog 是 `columns: "auto"`，支援加欄）。查過，dataset 以外沒有讀者。
- `keep_zero_positive_groups_drawn_under_ratio` 裡有一道執行期的後備檢查：輸入如果已經帶著權重欄就報錯（B12 的後備）。搬到 keys 上之後，輸入是剛從 `sample_pool` 選出的 keys，這道檢查永遠不會觸發，要重新想它該守在哪。真的撞名時，build 會因為欄名重複而大聲失敗，不會靜默。
- node 名 `filter_val_model_input`、`filter_test_model_input` 消失，`--only-node`／`--from-node` 指到它們會報錯；`pipeline.py` 的 `ONLY_TEST_MONTHS_NODES`（見上一段）、防漂移測試 `tests/test_pipelines/test_dataset/test_pipeline.py`、`tests/test_pipelines/test_resume_contracts.py` 的接續契約都要跟著改。文件與腳本裡引用這兩個 node 名的地方，實作時全 repo grep 一次（flow 規則 10）。ADR-0007 當初刻意保留 `filter_test_model_input` 這個 node 名；這次是有意拿掉。

> **實作註記（2026-09-25，#461）**：
> - 新 node 叫 `filter_val_keys`、`filter_test_keys`。三個整組抽樣 node 共用同一個「把抽中的組接回 keys」的機制（`steps/model_input.py` 的 `keep_keys_of_drawn_groups`）：先把抽樣結果縮成一組一列，再接回 keys，所以 keys 只會變少、不會變多。train 不帶權重，接法是 semi join；val／test 帶權重，用 inner join 把那一欄接回來。
> - `filter_test_keys` 吃 `test_keys_month_plan`（選 key 用的那一份），`label_table` 篩到它的 `to_process`；B10 吃 `test_model_input_month_plan`（組裝用的那一份），理由見上文。
> - B12 的執行期後備檢查改由組裝負責。權重在 keys 上，特徵表若有同名的欄，組裝最後那一步選欄時會因為兩欄同名而由 Spark 報錯（`Reference ... is ambiguous`），跟 B7 的後備是同一種：訊息裡沒有 B12 代號，但不會靜默覆寫。抽樣 helper 原本那道「已經有這一欄就報錯」的檢查留著，當成 helper 自己「不覆寫」的保證；它現在只看得到 identity 與 label，擋不到特徵。決定寫在 `core/consistency.py` 的 B12 說明與該 helper 的 docstring。
> - 接續成本有一項上文沒寫：從 `fit_preprocessor_metadata` 接續，會多補跑 `select_val_keys`、`select_test_keys`。四個整組抽樣 node 都排在 fit 之後，它們的輸入又不落地。反過來，從 `build_train_model_input` 接續不再補跑 val／test 的 build。接續契約已照改。
> - `--only-test-months` 的 node 從 6 個變 7 個：少 `filter_test_model_input`，多 `filter_test_keys` 與 `validate_model_input_grain`。
> - B10 報告的 test 那一段多一個 `months`，逐月列出 keys 與 model_input 的列數，「整個月的組都被丟了」的 0 ＝ 0 看得到。
> - 審查後補的三件，上文沒想到（本份原本只把 test 當成「這次的月份」一整批）：
>   - **test 逐月比，不比加總。** 一個月放大、一個月短少，加總會互相抵銷。
>   - **B10 擋下的 test 月份已經落地。** 直接重跑，月份計畫會把它當成已落地而跳過，壞資料留著。所以 test 的錯誤訊息寫出月份，要求用 `--rebuild-dates` 重算；用 `--rebuild-dates` 重算過的月份，若某個 item 分區（或整個月）這次變成空的，舊分區會留著（dynamic partition overwrite），訊息也把這個可能寫出來。
>   - **`test_keys` 要重做的月份，包含 `test_model_input` 這次要組的月份**（`month_plans.py` 的 `build_month_plans`）。上文說兩份計畫的 `to_process` 可能不同；搬到 keys 上之後，keys 帶著丟組的判斷、而判斷看 label，上次跑到一半留下的 keys 若是在 label 回補之前判斷的，接上新的 label 會對不上，而且列數相等、B10 看不出來。現在 keys 的計畫一定涵蓋組裝的計畫。

## 決定 5　整單位抽樣只留一個逐列機制；entity 有 NULL 一律丟並警告

**規則**：
- 「整個單位落在同一邊」的機制只保留一個：逐列對單位欄算 `spark_bucket`，跟門檻比。三個呼叫端共用它：
  - `keep_entities_drawn_under_ratio`（val 抽樣）：今天是先 `distinct` 出單位、再 inner join 回來，改成這個形狀。
  - `split_train_keys`（train／train_dev 切分）：今天在 node 裡內聯算 bucket 與門檻，改成用同一個機制。它要的是門檻兩側各一份，不是只留一側。
  - 無正例組的整組抽樣（`steps/model_input.py` 的 `_kept_under_ratio`）：本來就是這個形狀。
- entity 任一欄為 NULL 的列，在每個 split 選 key 時丟掉。
  - 丟之前先問一次「有沒有要丟的」（`isEmpty`），有就警告，寫明是哪個 split、丟了哪些欄為 NULL 的列。train 在 `split_train_keys` 做（今天就在那裡做，看的是已經抽樣、落地的 `sample_keys`，範圍最小）；val、test 在 `select_val_keys`、`select_test_keys` 做，而且要在 val 的抽樣之前。
  - **代價**：資料乾淨時 `isEmpty` 找不到東西，會把該 split 的月份窄掃一遍（只讀 entity 欄）。train 的成本跟今天一樣；val、test 各多一次，各自只掃自己的月份（預設各 1 個月）。
  - **為什麼不放進資料閘順便數**：資料閘本來就會掃一次 `sample_pool`，放進去可以省掉 val、test 那兩次。但 ADR-0006 規定資料閘只管「設定與資料的矛盾」，不做資料品質稽核；NULL entity 是資料品質問題。為了省兩次窄掃描去開 ADR-0006 的例外，不划算。

**和今天的差別**：
- 今天只有 train 的切分會丟 NULL 並警告（`warn_dropped_null_split_unit`），而且看的是**切分單位**（`train_split_keys`），不是整個 entity。切分單位依 A29 是 entity 的子集，所以新規則對 train 更嚴：切分單位以外的 entity 欄是 NULL 的列，今天會留下，之後會被丟掉。
- `split_train_keys` 今天的 `isEmpty` 看的是切分單位；改成看整個 entity。它的錯誤訊息（切完 train_dev 是空的時候）靠這個結果判斷原因，改寫時要保住這個判斷。

**為什麼結果不變**：`spark_bucket` 只看欄位值，對 distinct 後的單位算和逐列算，會得到同一個 bucket。唯一的差別在 NULL：inner join 會丟掉 NULL 單位（NULL 不等於 NULL），逐列算則不會（`concat_ws` 會略過 NULL）。這正是 NULL 規則必須先處理的原因。沒有 NULL entity 的資料，結果逐列相同。

**為什麼**：
- 2026-09-06 在 `split_train_keys` 上實測過同一個改寫：耗時少了 47%，而且把「同一個 entity 落在同一邊」的保證，從「distinct 加 join 湊出來」升級成「bucket 只看欄位值，必然如此」（`pipeline-performance-work.md` 規則 2 的最高證據等級）。
- `dataset.val_sample_ratio` 在 `conf/base` 是 0.5，所以 val 預設就走舊的、要洗三次的路徑。
- entity 為 NULL 的列不屬於任何 entity，接不到特徵，也接不到 label。今天四條路徑有三種處理方式，其中一種是默默丟掉。

**為什麼是警告而不是報錯中止**：沿用 `split_train_keys` 的理由。ADR-0006 把資料品質檢查放在上游的 `source_etl`（`primary_key_not_null`），在這裡中止的話，略髒的來源表會整條跑不動。

## 決定 6　各 split 都相信上游主鍵，不去重

**規則**：拿掉 `select_val_keys`、`select_test_keys` 的 `dropDuplicates`，跟 `select_train_keys`（node 名 `select_sample_keys`）一致。

**保證從哪來、漏洞在哪**：
- `sample_pool` 的 identity 唯一性由 `source_etl` 的 `max_duplicate_key_ratio` 檢查負責（`conf/base/parameters_sample_pool_etl.yaml` 有宣告，失敗會中止）。
- 使用者如果自己準備 `sample_pool`、不經過 `source_etl`，就沒有人檢查。ADR-0006〈這條 ADR 沒有解決的事〉對自備的 `feature_table` 記過同一個漏洞，對 `sample_pool` 一樣成立。這時重複的 identity 會變成重複的列。
- 這個風險今天 train 就有。本決定讓三個 split 承擔同一個風險，而不是只有 train 承擔。

**為什麼不是全部都去重**：train 是最大的一份（全部 train 月份乘上全部候選）。全部去重，等於在最大的表上多洗一次；而資料品質歸上游，是 ADR-0006 的既有決定。B10 擋不到這種重複，因為 keys 與 model_input 會一樣重複，列數還是相等。

> **實作註記（2026-09-25，#462，決定 5、6）**：
> - 共用的機制叫 `unit_drawn_under_ratio`（`steps/sampling.py`），回傳一個 Column：「這一列的單位抽到門檻以下」。三個呼叫端：
>   - `keep_entities_drawn_under_ratio`（val 抽樣）只拿它做一次 filter，不 join、不去重。
>   - `split_train_keys` 用它本身取 train_dev，用它的否定取 train。bucket 不會是 NULL（`concat_ws` 不回 NULL），所以否定是精確的補集。
>   - `steps/model_input.py` 的 `_kept_under_ratio`（無正例組抽樣）。
> - 丟 NULL entity 也是一個共用步驟 `drop_rows_with_null_entity`，回傳「丟完的 keys」與「有沒有丟」。第二個值只有 `split_train_keys` 用：切完 train_dev 是空的時候，靠它分辨「根本沒有列進來」與「列都因 NULL 被丟了」。警告那一行以 `<split> keys:` 開頭（`train/train_dev`、`val`、`test`）。
> - 上文「和今天的差別」少寫兩件：
>   - 「val 抽樣時默默丟」只對抽樣單位（`val_sample_keys`）的欄成立：抽樣是對那幾欄 inner join，其他 entity 欄是 NULL 的列會留下。只有多欄 entity 又宣告了較粗的 `val_sample_keys` 時才碰得到。
>   - 在選 key 那一步留下的 NULL entity 列（val 不抽樣時、val 抽樣但 NULL 在抽樣單位以外、test），到了丟無正例組那一步（`filter_val_keys`／`filter_test_keys`）接不到 label，算無正例組。所以：
>     - r ＝ 0（預設）時，它們本來就在那一步被默默丟掉，落地內容不含這些列。本票對 r ＝ 0 的改變只是「改在選 key 時丟，而且警告」，落地內容不變。
>     - r > 0 時，今天它們可能被抽中、帶著權重 1／r 留進 model_input；本票之後一律丟掉。〈版本與順序的約束〉第 2 條說的「有 NULL entity 時落地內容會變」，指的是這種情況與 train 的切分單位以外欄為 NULL 的情況。
> - 決定 6 的「B10 擋不到」也寫進了 `select_val_keys` 的 docstring；使用者文件在 `docs/pipelines/dataset.md` §3.1 與 §9。
> - 審查補的一件，決定 6 照原樣實作、沒有改：「三個 split 承擔同一個風險」的後果其實不一樣。
>   - train 有重複列，效果大約等於那些列權重加倍。
>   - test 有重複列，壞的是對外報告的指標：重複列原樣進預測表，evaluation 只查 `label_table` 有沒有重複鍵（`prepare_eval_data`），不查預測表。組內排名用 `row_number`，同一個正例的兩份拷貝各佔一個名次，average precision 會偏高，而且不報錯。
>   - 以前擋住這件事的，是 `select_test_keys` 的 `dropDuplicates`。
>   - 不改的理由：決定 6 否決「全部去重」的第二個理由（資料品質歸上游，ADR-0006）對 val／test 一樣成立，而且本份原本就接受「自備 `sample_pool` 沒人檢查」。要在下游擋，比較一致的做法是 evaluation 對預測表的重複鍵報錯（它對 `label_table` 就是這樣做），不是 dataset 靜默去重。這件事沒有開票。

## 決定 7　資料閘的 node 只寫「查什麼、什麼算失敗」，機制進 `steps/`

**規則**：
- node body 只寫兩件事：
  1. 這次要查哪些表、哪些月份（這是決策）。
  2. 把收集到的事實交給 `core/consistency.py` 的 predicate，由它判斷什麼算失敗。
- 收集事實（讀 parquet 檔尾、找出這次寫入的檔、Spark 聚合）與組報告，屬於機制，進 `steps/`，每個函式具名、只做一件事。
- B8 與 B10 共用一個「這次寫了哪些檔、檔尾記了什麼」的模組。今天有兩套：一套是 `steps/precision.py` 的 `landed_partition_files`（月份用日期比），另一套是 `utils/parquet_stats.py` 加上 `nodes.py` 的 `_footer_rows`（用字串比）。合併時保留按版本篩檔：`steps/precision.py` 的 `landed_partition_files` 與 `nodes.py` 的 `validate_model_input_grain` 兩處 docstring 都說 `inputFiles()` 會回傳整個 relation、不管 catalog 載入時的版本條件，而 2026-09-25 的實測（兩個版本並存時，對 `load()` 的結果呼叫 `inputFiles()`，只回傳本版本的檔）與它相反。哪一邊對沒有定論，版本篩選很便宜，留著並用測試釘住。
- `validate_data_consistency` 裡的三個巢狀函式改成具名步驟（node 規則 9）。候選層級表的精度檢查不再複製一份組報告的程式。
- 帶不變量代號的錯誤訊息（B8 找不到檔、B10 範圍內零個檔）由 predicate 產出，不在 node 裡組字串。

**為什麼**：這是把 node 規則 4、11 套到資料閘上的結果。今天沒有寫成明文，所以長歪了兩次（B8、B10 各一次），加候選層級表時又各複製了一份。本份把它寫成新規則 17（見決定 14）。

**#406 第 5 項提的兩個選項都不選**：那兩個選項是「新開 `steps/model_input_grain.py`」和「併入 `steps/precision.py`」。本決定改成依 concern 切：共用的「檔尾事實」一個模組，各個閘自己的報告組裝各自一個模組。檔名由實作規格決定。

## 決定 8　`nodes.py` 不拆檔

- 照決定 4、7 做完，`nodes.py` 估計會從 1733 行降到 1200–1300 行，其中約四成是 docstring 與註解。
- 對照其他 pipeline：evaluation 2106 行、training 1587 行，全部是單一個 `nodes.py`。
- 拆成 `nodes/` 套件要改架構約束 S1，而且只有 dataset 這樣做的話，會變成四條 pipeline 裡唯一的例外。要拆，應該四條一起決定，那不在本份範圍內。

這是品味題，信心中等。

## 決定 9　改 `carry_columns` 只翻 `train_variant_id`

**規則**：把 `dataset.carry_columns` 登記進 `core/versioning.py` 的 `TRAIN_SAMPLING_KEYS`。

**為什麼**：
- carry 欄只進 train／train_dev 的 keys 與 model_input。val／test 的 keys 只選 identity，所以它們的 model_input 沒有 carry 欄（ADR-0004 的推導式）；training 與 evaluation 也不從 val／test 讀 carry 欄。
- 今天改 carry 會翻 `base_dataset_version`，前處理器、val、test 整批重算，而它們一欄都沒變。
- 唯一找得到的理由，是 `docs/superpowers/specs/2026-05-18-sampling-overrides-editor-design.md` 裡的一句話：「parquet 的欄位變了，所以翻 base 是對的」。但欄位會變的只有 train 那兩張表，而它們本來就按 variant 分區，而且是 `columns: "auto"`，支援加欄。

**後果**：`carry_columns` 離開 base 的雜湊。設定檔裡寫了 `carry_columns` 的部署（`conf/base` 與 `examples/ad` 都有寫），`base_dataset_version` 會因此變一次；沒寫的部署不會因為這個決定而變。那些部署的 `train_variant_id` 也會變：`carry_columns` 進了 variant 的雜湊。讓每個部署的 base 都翻一次的是決定 15。

## 決定 10　刪 `category_mappings` 產物；框架不寫死示例欄名

- **刪 `category_mappings`**（catalog 條目，以及 `fit_preprocessor_metadata` 的第二個輸出）：
  - 沒有任何 node 讀它。
  - 它跟 `preprocessor.json` 裡的 `category_mappings` 鍵內容相同：是同一個 dict，序列化到兩個檔。
  - #382 的交接包設計（該票的留言）寫明要讀 `preprocessor.json`。
  - 刪掉之後，兩個真實來源只剩一個。
  - 要跟著改的非 node 讀者：`examples/ad/digest.py`（`DATASET_JSON` 會對這個檔取指紋）、`examples/ad/baseline_digest.json`、`examples/ad/conf/base/catalog.yaml` 的同名條目。實作時全 repo grep `category_mappings.json` 與 catalog 條目名。
  - #382 的驗收條件寫「與 `preprocessor`／`category_mappings` 逐值一致」，要改寫成只對 `preprocessor.json`。
- **`prepare_model_input_config` 的 `drop_columns` 預設只留 schema 角色**（time、entity、label）：
  - 今天的預設還寫死了 `apply_start_date`、`apply_end_date`、`cust_segment_typ`，違反 ADR-0017 的框架字彙界線。
  - `conf/base/parameters_dataset.yaml` 已經明寫 `drop_columns`，所以參考設定的行為與版本 ID 都不變。
- `CONTEXT.md`「前處理器」詞條裡的產物名，在刪檔的同一個 PR 更新。

## 決定 11　CLI 裡只有 dataset 懂的邏輯，搬進 dataset 根層的契約模組

**規則**：`__main__.py` 的 `dataset()` 裡，只有 dataset 懂的事，搬進 `pipelines/dataset/` 根層的一個模組，由 CLI 呼叫。這些事包括：
- 特徵表與候選層級表的指紋
- 兩層版本 ID
- month plan 的組裝
- 偵測候選層級表有沒有宣告、沒宣告時注入 `None`（inference 另外也讀同一個 catalog 名稱做 A47，那一段不搬）
- 決定 12 的前提檢查

CLI 只留通用的部分：寫 manifest、執行 pipeline。

**為什麼放根層**：它的呼叫端（`__main__.py`）在 pipeline 之外，照 node 規則 8 放根層，不放 `steps/`。先例是 training 的 `cache_sources.py`（ADR-0014），那個模組的 docstring 自稱是 `month_plans.py` 的 training 對應版。好處是：決定 12 的前提檢查和「這次要處理什麼」住在一起，可以不經過 CLI 直接測試。

**和決定 12 的先後**：決定 12 是行為變更，要先做（flow 規則 3）；它的檢查可以先放進這個新模組（模組裡一開始只有這一項）。其餘項目的搬移不改行為，放在最後。

**不搬的**：`plan_incremental_snap_dates` 也被 evaluation 用，所以留在 `month_plans.py`（ADR-0018 決定 1 已登記理由）。

## 決定 12　`--only-test-months` 要求 train 版本的表已經落地

**問題（#334）**：
1. 使用者改了抽樣鍵，同時用 `--only-test-months` 跑。
2. `train_variant_id` 因此變了，但這個模式不包含 train 的 build。
3. CLI 卻照樣在新 variant 的目錄寫 manifest 草稿（`_write_manifest_stub`），跑完再寫成 `status: completed`（`_write_pipeline_manifest`），並把 `train_variants/latest` 指過去。
4. 之後 training 用新 variant 讀 `train_model_input`，拿到 0 列，全程不報錯。

同一個假 `completed` 也會從別的門進來：改了抽樣鍵之後，用 `--only-node`／`--from-node` 跑一段不碰 train 的切片，CLI 一樣會把新 variant 標成 `completed`。所以 manifest 的狀態不能拿來當「train 的表已經建好」的證據。

**規則**：
- `--only-test-months` 開跑前，向 metastore 查 `train_model_input` 在目前這組 `base_dataset_version`／`train_variant_id` 底下有沒有分區（ADR-0009 已經用 metastore 回答「寫了哪些分區」）。這只是一次 metastore 查詢，不跑 Spark job；CLI 這時本來就已經起了 Spark session（算特徵表指紋要讀 schema）。沒有分區就停下來，訊息分兩種：
  - 這個 `base_dataset_version` 底下一個分區都沒有：這個 base 版本從來沒建過，請跑完整的 dataset。可能的原因：第一次跑；base 層的設定或特徵表的 schema 變了；框架升級時把決定 15 的常數加了 1。
  - base 有、只是這個 `train_variant_id` 沒有：train 的抽樣設定變了，這個 train 版本還沒建，請跑完整的 dataset（不加 `--only-test-months`）。
- **跑完之後，`completed` 與 `train_variants/latest` 看表在不在，不看這次跑了哪些 node。** 向 metastore 確認目前設定算出的 variant 在 `train_model_input` 與 `train_dev_model_input` 底下都有分區：
  - 有：把 variant 的 manifest 寫成 `completed`，把 `latest` 指過去。
  - 沒有（例如改了抽樣鍵之後，跑了一段不碰 train、或只碰一部分 train 的切片）：兩樣都不動，並警告「目前設定的 train 版本還沒建，training 會繼續讀 `latest` 指的那一個」。
  - 執行前的 manifest 草稿（`status: running`）照舊寫。沒有任何程式讀 variant 的 manifest 來做決定（`latest` 的唯一讀者 `resolve_train_variant_id` 只看 symlink 指到的目錄名），草稿留著不會誤導任何人。

**為什麼是「看表在不在」**：training 讀哪個 variant，完全由 `latest` 決定（`core/versioning.py` 的 `resolve_train_variant_id`），不會拿自己的設定重算一次。所以 `latest` 有兩種錯法，都不報錯：
- 指向一個沒有表的 variant：training 讀到 0 列（#334）。
- 目前設定的 variant 明明有表，`latest` 卻還指著別的：training 用的資料跟設定不符。例如先後用抽樣設定 A、B 各建過一次，再把設定改回 A 跑 `--only-test-months`。
「這次跑了哪些 node」擋得住其中一種、擋不住另一種；「表在不在」兩種都擋得住。

**為什麼擋在 dataset 開頭，而不是等 training 讀到 0 列再報錯**：擋在出問題的那一步，訊息講得出原因。training 端的 0 列檢查只看得到症狀，還要等 training 起來、表讀完才知道。

**為什麼查 metastore，不查 manifest**：今天 manifest 會被切片執行寫成 `completed`（見上），而且舊版本的 manifest 不一定有 `status` 欄。分區在不在，是「表建好了沒」最直接的證據。開跑前的檢查與跑完後的 `latest` 規則用的是同一個證據。

**誰來判斷**：照 node 規則 11，這個檢查寫成 `core/consistency.py` 的 predicate；事實（兩層版本各有沒有分區）由決定 11 的模組收集後傳進去。

**和 ADR-0012 的關係**：ADR-0012 當初不補攔截的理由是「要使用者主動縮小範圍才會踩到」；#334 開票時（2026-09-12）決定要補。ADR-0012〈後果〉第一條不變：`scripts/rebuild_eval_month.sh` 不會順手重建 train／val。本決定是擋下來，不是自動補建。

> **實作註記（2026-09-25，#463）**：
> - 不變量代號 A55，predicate 是 `core/consistency.py` 的 `train_version_landed_errors`。
> - 決定 11 的根層模組叫 `pipelines/dataset/run_contract.py`。目前只放本決定的兩個事實：
>   - `train_version_landing`（開跑前）：`train_model_input` 在這個 base 底下、在這個 variant 底下，各有沒有分區。
>   - `unlanded_train_tables`（跑完之後）：兩張 train 表裡，哪幾張在這個 variant 底下沒有分區。
> - base 那一層的問法：把 catalog 條目 `partition_filter` 裡的 `train_variant_id` 拿掉，另建一個 dataset 物件，問它有哪些分區。開跑前、跑完之後各兩次 `SHOW PARTITIONS`，都只讀 metastore。
> - 上文沒寫、實作時定的三件：
>   - **列不出分區的條目算「沒落地」**，並警告（不是 Hive 表，或 catalog 沒有這一條）。開跑前會擋下，跑完不動 `latest`。反方向是發布一個沒人看過表的 variant。兩份示例 catalog 都是 Hive 表，只有測試用的 catalog 走得到這裡。
>   - **`--dry-run`、`--list-nodes` 也檢查**：預覽不該承諾一個會被擋下的執行。
>   - **不發布時印兩行 `[train_variant]` 警告**，寫出 `latest` 目前指的 variant；還沒有 `latest` 時，寫明 training 會找不到 train 版本而報錯。
> - base 那一層的 manifest 與 `data/dataset/latest` 照舊每次成功執行都更新。本決定只管 train variant 那一層。
> - 順帶更正：`docs/operations/user-guides/pipeline-slicing.md` 原本寫「config 變了 → filter 指向不存在的 partition → `exists()` 自然為假，這一層防呆是有效的」。這與 `_table_exists` 的表級判定相反（ADR-0012 已記），而那正是切片會把沒建好的 variant 標成 `completed` 的原因。已改寫，並在同一段補上本決定的兩道規則。

## 決定 13　`preprocessor_on_disk` 由程式從 `preprocessor` 推出

**背景**：
- item 清單從資料數出來時，`fit_preprocessor_metadata` 要在覆寫 `preprocessor.json` 之前，先讀磁碟上的舊檔，比對 item 清單有沒有變（B19）。
- 一個 node 不能讀寫同一個 catalog 名字（`docs/agents/architecture-constraints.md` 的 A6，不是 `core/consistency.py` 的同名代號），所以 catalog 替同一個檔取了第二個名字 `preprocessor_on_disk`。
- 每個部署都要手寫這一條，路徑必須和 `preprocessor` 一字不差。evaluation 的 `prepare_eval_data` 也讀它。

**問題**：這個條目是選用的，因為第一次跑時檔案本來就不存在。所以路徑寫錯時，`load()` 對不存在的檔回傳 `None`；B19 以為這是第一次跑，就默默跳過。

**規則**：dataset 與 evaluation 組 catalog 時，從 `preprocessor` 條目推出 `preprocessor_on_disk`：同一個 filepath，選用。
- 部署的 catalog 沒寫這一條：自動補上。
- 部署的 catalog 寫了，而且路徑相同：照常。
- 部署的 catalog 寫了，但路徑不同：開跑前報錯，不再默默跳過。

## 決定 14　`pipeline-node-design.md` 加四條規則，登記一筆例外

每一條規則都要照該文件的體例寫四件事：規則、不照做會怎樣、為什麼不是另一種做法、誰擋得住。實際的 code 對照組在實作完成之後再從 repo 撈。

| 新規則 | 來自 | 誰擋得住 |
|---|---|---|
| **14**　讀來源表或落地中間表時，在程式碼裡寫明讀哪些 time 值，不靠 optimizer 從 join 推導修剪 | 決定 1 | 沒人，只能人看（驗證方法：看 `explain()` 的 `PartitionFilters`／`Filter`） |
| **15**　node 自己算得出來的值，不從 CLI 注入。只有開跑前才知道、node 又看不到的事實，才注入（例如切片與接續要用的 month plan、執行模式） | 決定 2 | 沒人，只能人看 |
| **16**　同一件事，各 split 用同一個機制。真要不同，docstring 要寫理由，而且理由不能是這個不對稱自己造成的結果 | 決定 4、5、6 | 沒人，只能人看 |
| **17**　資料閘的 node 只寫「查什麼」與「交給哪個 predicate 判斷」；收集事實與組報告進 `steps/` | 決定 7 | 沒人，只能人看 |

**登記的例外**（使用者 2026-09-25 同意登記）：`split_train_keys` 一個 node 產出兩樣東西（`train_keys_unfiltered`、`train_dev_keys_unfiltered`），分別被兩個下游消費，違反規則 1 的反方向。理由是：兩邊用同一個門檻互補切開（`bucket >= threshold` 與 `bucket < threshold`）。拆成兩個 node，就得各算一次 bucket，互補這件事就從「同一個式子的正反兩面」退化成「兩處程式碼碰巧一致」。

另外，把該文件裡 6 處過期的行號改成寫函式名。

## 決定 15　`base_dataset_version` 加一個「dataset 產物格式版本」常數

**問題**：`base_dataset_version` 是拿設定算出來的雜湊，程式碼改了它不會變。所以程式改了落地內容、設定卻沒動時，新舊兩版程式寫出的表會掛在同一個版本 ID 下。test 是增量的，這時同一張 `test_keys`、同一個版本 ID 底下，舊月份是舊程式寫的（沒丟過組），新月份是新程式寫的（丟過組）。決定 9 只會翻「設定檔裡寫了 `carry_columns`」的部署，蓋不住這件事。

**規則**：
- `core/versioning.py` 加一個整數常數（名字由實作規格決定），放進 `base_dataset_version` 的雜湊輸入。
- 本份把它加 1：每個部署的 `base_dataset_version` 都變一次，全部產物在新 ID 下重建。決定 15 本身不改 `train_variant_id` 的雜湊（決定 9 會改），但 variant 的目錄在 base 底下，所以跟著搬。
- 以後程式改了 dataset 的落地內容、設定卻沒動時，要把它再加 1。

**誰擋得住**：沒人，只能人看。「這個改動會不會改變落地內容」是判斷題；實作規格與 PR 審查要逐張問。

**代價**：
- 每個部署重建一次 dataset、重訓一次。這是本份一開始就接受的一次翻版（見「版本與順序的約束」第 2 條）；本決定只是讓它對每個部署都成立，而不是只對寫了 `carry_columns` 的部署成立。
- `model_version` 與 HPO 的 `search_id` 都含 base，所以都會變，HPO 從頭搜。
- 常數加 1 之後、重訓之前，`docs/operations/user-guides/adding-an-eval-month.md` 那套「不重訓、只加評估月份」的流程對現役模型失效：新 base 還沒建，`--only-test-months` 會被決定 12 擋下；training 算出的 `model_version` 也不是現役模型的。以後每加一次 1 都會再發生一次，所以加 1 的時機要跟重訓排在一起。已經部署的推論與評估讀的是模型 manifest 裡記的 base，不受影響。
- 寫死版本雜湊值的測試與 `examples/ad` 的 baseline 版本號會跟著變，這是預期中的變化。

**為什麼不是「接受同一個 ID 下內容改變」**：版本 ID 的用處是「同一個 ID 就是同一份資料」。讓它在部分部署失效，比多重建一次更難察覺——test 月份新舊混在一起時，沒有任何東西會報錯。

---

## 版本與順序的約束

這一節給寫實作規格與開票的人。PR 怎麼切由那一步決定（flow 規則 4：看每一半有沒有比「測試綠」更強的證據），但以下幾件是硬約束：

1. **本份先進 main，而且不含任何程式碼**（flow 規則 1）。
2. **版本只翻一次。** 會改變落地內容的決定——3（time 欄是 STRING 的部署）、4（`val_keys`／`test_keys` 變成丟過組的）、5（有 NULL entity 時）、6（有重複 identity 時）、9、10（靠預設 `drop_columns` 的部署）——必須和決定 15 的那一次加 1 **在同一次部署生效**。這樣新內容全部落在新的版本 ID 下，生產只重建一次 dataset、只重訓一次。任何一個晚於那次部署才上線的內容變更，都要再把常數加 1。
3. **會改行為的先做，純搬移放最後**（flow 規則 3）。決定 11 的搬移、23 處文件脫節的修正、決定 14 的規則文字，放在最後。決定 12 是行為變更，要先做；它的檢查先放進決定 11 的新模組（見決定 11〈和決定 12 的先後〉）。
4. **每個「不變」的宣稱都要有證據：**
   - 決定 1、2、3：各落地產物的 digest 與 main 相同，而且 `explain()` 看得到月份條件。
   - 決定 4：r ＝ 0 與 r ＝ 1 時 model_input 的 digest 與 main 相同；0 < r < 1 時改驗性質：有正例的組一組不少、每組列數不變、無正例組整組去留、留下比例接近 r、同一份設定重跑得到同一批。
   - 決定 5、6：在沒有 NULL、沒有重複的示例資料上 digest 相同；另外用刻意含 NULL、含重複的測試資料，驗證新規則。
   - 決定 7：B8、B10 的 JSON 報告與 main 相同（B10 多出的 val、test 兩對除外），並用刻意放大列數的右表確認 val、test 的配對會擋下。
   - 決定 10：參考設定與 `examples/ad` 的 `preprocessor.json` 內容不變（版本 ID 會因決定 9、15 改變，不拿它當證據）。這兩份設定都明寫了 `drop_columns`，走不到被改的預設值，所以另外要有一條單元測試直接釘住新的預設值。
   - 決定 12：測試要重現 #334（改抽樣鍵後跑 `--only-test-months`）與「第一次跑就用 `--only-test-months`」兩種情況，各自得到對的訊息；再確認改抽樣鍵後跑不碰 train 的切片，`latest` 與 `completed` 都不動而且有警告；以及「設定改回一個已建過的 variant 再跑 `--only-test-months`」時，`latest` 會指回它。
   - 決定 13：刻意把 `preprocessor_on_disk` 寫成別的路徑，確認開跑前報錯。

---

## 考慮過、沒選的做法

- **先訂秒數目標再優化**（決定 1）：repo 裡沒有生產耗時，沒有停止條件。
- **靠 DPP 就好**（決定 1）：生產規模很可能不觸發，而且消失時沒有訊號。
- **CLI 繼續注入月份清單，三張右表共用**（決定 2）：答案本來就是一行設定值，繞道只多出 DAG 輸入。
- **依設定改變 DAG 的長相**（決定 2）：node 內的分支還在，而且各部署的 DAG 會不一樣。
- **val／test 維持建表後再丟組，只改 docstring**（決定 4）：B10 會一直照不到 val／test。
- **全部 split 都去重**（決定 6）：在最大的那份表上多洗一次，而且違反 ADR-0006 的分工。
- **只修 #406 點名的幾項**（決定 7）：同一種形狀會在下一個資料閘再長一次。
- **把 `nodes.py` 拆成 `nodes/` 套件**（決定 8）：dataset 會變成唯一的例外。
- **等 training 讀到 0 列再報錯**（決定 12）：只看得到症狀，而且要等 Spark 起來。
- **用 manifest 的 `completed` 判斷 train 版本建好了沒**（決定 12）：切片執行也會把它寫成 `completed`。
- **接受部分部署在同一個版本 ID 下內容改變**（決定 15）：test 月份新舊混在一起時沒有任何東西會報錯。
- **把 `preprocessor.json` 的類別編號改寫成 `{值: 編號}`**：
  - JSON 的 key 只能是字串，而類別欄可以是整數或布林（B5 的 `CATEGORICAL_DTYPES`）。
  - 整數 `1` 存進去會變成 `"1"`，型別在檔案裡就丟了。每個讀這個檔的地方都得知道要轉回原型別；有一處忘了轉，查表就對不上，值會被當成沒看過的類別，而且不報錯。
  - 今天的清單格式（值在清單裡的位置就是它的編號）保留了型別，本身就是完整的編碼。
  - 要給框架以外的系統看的明確編號，歸 #382 的交接包處理。
- **修 `test_model_input` 寫死的分區欄 `prod_name`**：寫錯的話，存檔當下就會報錯，不會默默出事，所以不處理。

---

## 對既有 ADR 的修訂

照 repo 慣例，ADR 正文不改，只在文末加一段標了日期的修訂，指向本份：

| ADR | 修訂內容 |
|---|---|
| 0007 | `filter_test_model_input` 會再次拿掉（決定 4） |
| 0008 | 「fit 與 apply 的月份篩選不一致，哪邊對還沒判定」已經判定（決定 3）；`nodes.py` 的長度與不拆檔（決定 8） |
| 0012 | #334 的攔截（決定 12） |
| 0016 | `carry_columns` 的版本路由（決定 9） |
| 0025 | 決定 3 的 val／test 丟組位置（決定 4） |
| 0026 | 候選層級表的月份清單不再由 CLI 注入（決定 2）；〈後果〉裡 fit 月份檢查的成本描述更正（決定 1） |
| 0006 | B10 不再只涵蓋 train／train_dev（決定 4） |
| 0013 | `ONLY_TEST_MONTHS_NODES` 會少 `filter_test_model_input`，多新的 test keys 篩選 node 與 `validate_model_input_grain`（決定 4）；這個模式的前提檢查與 manifest 規則（決定 12） |
