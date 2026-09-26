# dataset pipeline 第二輪整理前的盤點（2026-09-25）

[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 的證據。與 ADR-0029 同一批進 main。這是**當天的快照**：行號核對於 main @ `2e99c573`，之後會過期，不隨程式碼更新。

路徑簡寫：`nodes.py`、`pipeline.py`、`month_plans.py` 都在 `src/recsys_tfb/pipelines/dataset/`；`dataset.md` ＝ `docs/pipelines/dataset.md`；`node-design` ＝ `docs/agents/pipeline-node-design.md`；`arch` ＝ `docs/agents/architecture-constraints.md`；`consistency.py` ＝ `src/recsys_tfb/core/consistency.py`。

---

## 一、四個 build 有沒有按月份修剪（`explain()` 實測）

**環境**：local[*]，合成資料。來源表 14 個月（2025-01 到 2026-02），train 10 個月、val 1 個月（2025-12）、test 1 個月（2026-01）。先跑一次完整的 dataset，再照 `pipeline.py` 的接線載入各 node 的輸入、直接呼叫 node 函式、對回傳的 DataFrame 跑 `explain(mode="formatted")`。量測腳本沒有進 repo（它是一次性的）；要重現，照這段描述在 local[*] 上重做即可。

**結果**：

| build | keys | `label_table` | `preprocessed_feature_table` |
|---|---|---|---|
| train／train_dev | 只有版本分區條件（keys 本來就只有 train 月份） | **沒有月份條件，讀全部 14 個月** | 有修剪，但靠 DPP |
| val | 只有版本分區條件 | **沒有月份條件** | 有修剪，但靠 DPP |
| test | 版本＋`cast(snap_date as date) = 2026-01-31` | **沒有月份條件** | 約束推導＋DPP |

val build 的 plan 節錄：

```
(3) Scan parquet ml_recsys.label_table
Location: InMemoryFileIndex [.../ml_recsys.db/label_table]
PushedFilters: [IsNotNull(snap_date), IsNotNull(cust_id), IsNotNull(prod_name)]

(8) Scan parquet ml_recsys.recsys_prod_preprocessed_feature_table
PartitionFilters: [isnotnull(base_dataset_version#83), (base_dataset_version#83 = e5820aa5),
  isnotnull(snap_date#84), dynamicpruningexpression(snap_date#84 IN dynamicpruning#693)]
```

> ⚠ **2026-09-25 更正（#460 審查）**：上表與下段「有修剪，但靠 DPP」「keys 很小，所以觸發了」讀錯了證據。`dynamicpruningexpression(... IN dynamicpruning)` 是執行前 `explain()` 印的占位字串；真的執行後，build 形狀（keys LEFT JOIN 右表）讀了全部分區，DPP 在這個 join 方向永遠不會修剪。細節與量法見 ADR-0029〈決定 1–3 實作後的更正〉第 1 條。這份快照其餘內容不動。

**DPP（dynamic partition pruning）是什麼、為什麼不能靠它**：Spark 在執行時，拿 join 對面實際出現的 time 值去跳過這一邊的分區。程式碼裡沒寫，是 optimizer 自己加的。它要 join 對面能整份廣播（broadcast）才會插入（`spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly` 預設 true）。合成資料的 keys 很小，所以觸發了；生產的 entity 是百萬級，keys 超過 `spark.sql.autoBroadcastJoinThreshold`（預設 10MB，repo 沒有覆寫）時，這個修剪很可能不會出現，而且消失時沒有任何訊號。這一點在合成資料上驗不出來。

**其他發現**：

- `months_filter_as_date`（`to_date(欄) IN (…)`）與直接用字串比（`欄 IN ('…')`），在 `test_keys` 與 `preprocessed_feature_table` 上產生的 `PartitionFilters` 等價，兩者都能修剪。
- `require_months_present` 對 `feature_table` 的 `select(time).distinct()` 沒有任何篩選，掃整張表的 time 欄。
- 合成資料的 `label_table`、`feature_table` 沒有宣告分區欄；生產是否分區，由產出它們的上游決定。
- `base_dataset_version` 的修剪是 `load()` 下 WHERE 的硬保證：同一張表有兩個版本時，對 `load()` 的結果呼叫 `inputFiles()`，只回傳本版本的檔案（`preprocessed_feature_table` 12 個、`train_model_input` 10 個，另一個版本 0 個）。這與 `steps/precision.py` 的 docstring（「`inputFiles()` 回傳整個 relation」）相反；只量了這一種情況，哪一邊對沒有定論。

---

## 二、上次重構之後加進 dataset 的功能

ADR-0008 那一輪的結構搬移在 2026-08-08（PR #176）合併時，`nodes.py` 是 671 行；2026-09-25 是 1733 行。這段期間加進來的功能（commit SHA；檔數是全 repo 的改動檔數；最後一欄是從 hunk 標頭推的約略值）：

| 功能 | commit | 全 repo 檔數（+／−） | src／conf 檔 | `nodes.py` 動到的函式（依 hunk 標頭，約略） |
|---|---|---|---|---|
| `--only-test-months` 模式 | `987ffc19` | 9（+543／−33） | `__main__`、`pipeline.py` | 0 |
| 切分單位 `train_split_keys`／`val_sample_keys` | `e7c8f399` | 15（+638／−36） | 6 | split、select_val |
| B8 精度閘 ＋ 報告 | `66618bf8`、`47e39cd2` | 21＋11 | 8＋4 | 新 node＋apply |
| 數值型別收斂 | `42547679` | 13 | 5（含 inference） | build |
| split 逐列 | `10ed9a60` | 3 | 2 | split |
| B10 粒度閘 | `d0245bb1` | 12 | 5 | 新 node＋build |
| 移除 calibration split | `2630fe0f` | **36（+644／−803）** | 13 | select_train、grain、filter |
| query group／base key／identity 集中到 `get_schema` | `11ac379e` | 29 | 14 | apply、build、filter |
| event 角色（B11） | `543e5654`、`b7bc8a8a` | 13＋4 | 10＋3 | 只動資料閘與 grain 訊息 |
| 零正例組三個鍵 | `ee323c1e`＋`bf9dd387`＋`e975b567` | 37＋6＋17 | 12 | 新 filter_train_keys ×2、filter_val／filter_test 拆開、資料閘 B12 |
| 候選層級特徵表 | `26fe13a5`＋`0256a3c3`＋`0a1b93e5` | 16＋12＋19 | 9 | 6 個 node（資料閘 16 個 hunk、精度閘 10、fit 8、build 5、build_test 2、apply 2），之後兩筆又回頭改 build、精度閘、fit |
| 多欄 item | `6f25b2c7`＋`e49b0643` | 15＋13 | 4＋5 | 7 個 node：資料閘、3 個 select、fit、build、filter_train |
| item 清單從資料數 | `0bd91411` | **48（+1789／−179）** | 19（dataset＋training＋inference＋evaluation＋catalog） | fit、新增 wrapper build_val／build_test、資料閘 |

最寬的兩次是「item 清單從資料數」（跨四條 pipeline）與「移除 calibration split」。會一改 N 處的形狀有三種：選用來源表、新增一個 split、新增一個資料閘。

---

## 三、文件、ADR、docstring 與現況對不上的 23 處

ADR 原文照 repo 慣例不改、只加修訂，所以 ADR 那幾列是預期中的腐爛，但讀者照樣會被誤導。收尾時逐列處理：活文件與程式 docstring 直接改；ADR 加修訂；行號改成寫函式名。

> 2026-09-26：逐列的處理結果記在 ADR-0029 決定 14 的實作註記（#467）。下表維持當天的快照。

| # | 文件那一邊 | 程式碼那一邊 | 類型 |
|---|---|---|---|
| 1 | `dataset.md:776`「train/train-dev 切分與 val entity sampling 目前只使用 `schema.entity` 的第一個欄位」 | `nodes.py:442`、`:564` 用 `get_entity_grouping`（ADR-0016／#264） | **活文件錯誤** |
| 2 | `dataset.md:245` 引用 `candidate_frame_columns` | 不存在；實際是 `encoded_frame_columns`（`nodes.py:1310`） | 活文件錯誤 |
| 3 | `dataset.md:569`「三層 version ID」 | 只剩兩層（`core/versioning.py:3-14`） | 活文件錯誤 |
| 4 | ADR-0026:58、`dataset.md:787`「fit 另外讀 train 月份兩次（月份檢查與詞表）」 | 月份檢查掃整張候選表（`nodes.py:754-758` → `steps/scoping.py:96-99`） | 成本描述不符 |
| 5 | `node-design:89`（`nodes.py:207–239`）、`:171`（`:202-`）、`:184`（`:383-`）、`:307`（`:466`）、`:314`（`:574`）、`:370`（`:315`） | 實際位置依序是 `:369–401`、`:339`、`:523`、`:744`、`:936`、`:472` | 行號過期（6 處） |
| 6 | ADR-0008:417-418「今日仍在：`nodes.py:480`…`:574`」 | 實際是 `:778`、`:936` | 行號過期（標為「今日」） |
| 7 | ADR-0001:38 `nodes.py:471`；ADR-0002:9 `month_plans.py:62`、:12 `consistency.py:116`；ADR-0011:64 `feature_columns.py:109` | 實際是 `:769`；`:61`；A21 的 legend 在 `:137`、predicate 在 `:4981`；`:127` | 行號過期 |
| 8 | `pipeline.py:7-9`「the other ten recompute」 | 17 − 6 ＝ 11 | 程式註解過期 |
| 9 | `pipeline.py:94-97` 資料閘註解只列 B1／B5／B6／B7 | 實際是 B1、B5、B6、B7、B11–B17（`nodes.py:144-145`） | 程式註解過期 |
| 10 | `pipeline.py:193-196`「it and the build_model_input nodes…share preprocessed_feature_table as their last unmet input」 | #429 之後 train／train_dev 的 build 最後一個未滿足輸入是 train_keys（實際拓撲序：`filter_train_keys` 是 #8，精度閘 #10）；順序仍由 `tests/test_pipelines/test_dataset/test_pipeline.py:287-298` 釘住 | 部分過期 |
| 11 | `nodes.py:5-7` 模組 docstring 列出的 steps 模組 | 漏了 `precision` | 程式 docstring 過期 |
| 12 | `nodes.py:22-25`「the two month-presence pre-checks」 | 現在有 3 個，另有 6 個被包的區塊 | 程式 docstring 過期 |
| 13 | `month_plans.py:168`「train ∪ cal ∪ val ∪ test」；`:37-39`「Used by apply_preprocessor_to_features」 | calibration 已移除；呼叫者是資料閘 | 程式 docstring 過期 |
| 14 | `consistency.py:979-983` 說 dataset 的 test 分支 node 讀 `REBUILD_SNAP_DATES_KEY` | dataset node 不讀；實際讀者是 training（`training/nodes.py:465`、`:1193`）與 inference（`inference/nodes.py:358`） | 程式 docstring 錯誤 |
| 15 | ADR-0012:21-24（標為「現在」）「固定 15 個節點…全量重算的是八個節點」 | 17 個節點；非 test 鏈的有 11 個 | ADR 的「現況」註記過期 |
| 16 | ADR-0013:21-27 的 `ONLY_TEST_MONTHS_NODES` 列 5 個 | 現在 6 個（多了 `validate_numeric_precision`，`pipeline.py:21-28`） | ADR 原文慣例 |
| 17 | ADR-0007:51「`filter_test_model_input` 這個函式刪除」 | #429 又把它加回來（`nodes.py:1686`），ADR-0007 沒有修訂 | ADR 與現況不符 |
| 18 | ADR-0006:71、:76、:191；ADR-0021:21 `nodes.py::filter_groups_with_positives` | 函式已不存在（改為 `filter_val_model_input`／`filter_test_model_input` 加 `keep_zero_positive_groups_drawn_under_ratio`） | 已刪函式仍被引用（ADR 原文慣例） |
| 19 | ADR-0008:73「nodes.py 11 個 node 函式」；檔案清單沒有 `precision.py` | 15 個 node 函式，註冊成 17 個 node；有 `steps/precision.py` | ADR 原文慣例 |
| 20 | `arch:160-163`（F7：consistency 2036、versioning 425、schema 232 行） | 5757、514、674 | 數字過期 |
| 21 | `arch:233`（F10：catalog 46 個條目，2026-08-31） | 54 個（`yaml.safe_load` 實算） | 數字過期（有標日期） |
| 22 | `arch:381`「evaluation 尚未依判準重整」；`docs/agents/pipeline-refactor-process.md:7`「還沒…只剩 evaluation」；ADR-0008:362-363、:393-395「只剩 `evaluation/nodes_spark.py`」 | evaluation 已是 `nodes.py`＋`steps/`（ADR-0019）；`node-design`〈已登記的例外〉只剩 1 筆 | 鄰近文件過期 |
| 23 | issue #406 項目 1、5 引用 `select_calibration_keys`、`calibration_variant_id`，行號是舊版（`nodes.py:230`、`:810`、`:962`…）；項目 4 的前提「加 event 會讓 B10 誤報」 | calibration 已刪；ADR-0025「更正 ADR-0021」已裁定宣告 event 後 B10 語意不變（identity 跟著加寬） | 票面過期 |
