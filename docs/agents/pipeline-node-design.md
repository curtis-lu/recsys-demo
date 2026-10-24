# Pipeline node 設計判準

改或加一個 pipeline node 之前照這份做：邊界畫在哪、node body 長什麼樣、機制與驗證放哪個檔。

- 位置合不合法看 [`architecture-constraints.md`](architecture-constraints.md)，那份有 `tests/test_core/test_architecture_constraints.py` 把關。這份管內容，多數沒有機械檢查：測試全綠不代表符合這份。兩份都要守。
- 重整一整條 pipeline 的流程（排順序、切 PR、選驗證手段）看 [`pipeline-refactor-process.md`](pipeline-refactor-process.md)。
- 詞彙（query group、候選層級特徵表、month plan、資料閘）在 [`CONTEXT.md`](../../CONTEXT.md)；不變量代號（A 系列、B 系列）的意思在 `src/recsys_tfb/core/consistency.py` 的模組 docstring。

## 全部規則一覽

動手前對著這張表逐條打勾。

| # | 規則 | 怎麼確認 | 誰擋得住 |
|---|---|---|---|
| **換個框架也成立** ||||
| [1](#1-node-的邊界要落在撈得出來看的產物上) | node 邊界要落在「撈得出來看」的產物上 | 中間物有沒有 catalog 條目／測試／log？一個都沒有就合併 | 沒人，只能人看 |
| [2](#2-打開-node-就讀得完它做了哪些決定不必跳檔) | 打開 node 就讀得完它做了哪些決定，不必跳檔 | 從上到下讀一遍，中途需要開別的檔嗎 | 沒人，只能人看 |
| [3](#3-一個-helper-最多裝一個決策) | 一個 helper 最多裝一個決策 | 每個新 helper 講得出「它決定了哪一件事」嗎 | 沒人，只能人看 |
| [4](#4-決策要浮到-node-body機制才能沉進-helper) | 決策要浮到 node body，機制才能沉進 helper | 把 helper 名字換成純機械名，重讀 node body 還講得完嗎 | 沒人，只能人看 |
| [5](#5-決策重複寫兩份機制才共用) | 決策重複寫兩份，機制才共用 | 抽出去的東西換成機械名會不會少講一件事 | 沒人，只能人看 |
| [6](#6-名字要說出選錯不會報錯的那個差別) | 名字要說出「選錯不會報錯」的那個差別 | 兩個相近的函式，選錯會有錯誤訊息嗎？沒有就要寫進名字 | 沒人，只能人看 |
| [7](#7-產物落不落地是接續成本的決定) | 產物落不落地，是接續成本的決定 | 跑 `--list-nodes` 看接續成本；跑 `test_resume_contracts.py` | **部分**：`RESUME_CONTRACTS` |
| **這個 repo 的形狀** ||||
| [8](#8-模組放根層還是-steps看-src-側呼叫端在不在本-pipeline-內) | 模組放根層還是 `steps/`，看 src 側呼叫端在不在本 pipeline 內 | 這個模組有沒有本 pipeline 以外的 src 側消費者 | **部分**：S3 擋得住「外部伸進 `steps/`」這一個方向 |
| [9](#9-步驟寫在註解與-helper-名上不寫在-log_step-上) | 步驟寫在註解與 helper 名上，不寫在 `log_step` 上 | 每個具名呼叫上面有沒有一段 `# Decision —` | 沒人，只能人看 |
| [10](#10-log_step-只包會觸發-spark-action-的區塊) | `log_step` 只包會觸發 Spark action 的區塊 | 區塊裡的具名呼叫，追一層進去找得到 action 嗎 | 沒人，只能人看 |
| [11](#11-驗證放哪由它需要看到什麼決定) | 驗證放哪，由「它需要看到什麼」決定 | 只看 config → 進 `core/consistency.py`；留在 node 的要標「前置檢查」或「後置條件」 | 沒人，只能人看 |
| [12](#12-命名底線模組私有守衛叫-require_不得有-backend-後綴) | 命名：底線＝模組私有、守衛叫 `require_*`、不得有 backend 後綴 | 跨模組呼叫得到的函式有沒有底線；有沒有 `_spark`／`_pandas` 後綴 | 沒人，只能人看 |
| [13](#13-docstring-講為什麼是這個答案不講簽章已經說過的事) | docstring 講「為什麼是這個答案」，不講簽章已經說過的事 | 有沒有寫「為什麼不是另一種做法」「選錯會不會報錯」；docstring 與 `docs/` 有沒有寫死程式數得出來的數量 | 沒人，只能人看 |
| [14](#14-讀來源表或增量產物時在程式碼裡寫明讀哪些-time-值) | 讀來源表或增量產物時，在程式碼裡寫明讀哪些 time 值 | 讀來源表或增量產物的地方，join 之前有沒有月份篩選；新的讀取點在 `test_month_scoped_reads.py` 加一個案例 | **部分**：`test_month_scoped_reads.py` 釘住今天的讀取點，新加的沒人擋 |
| [15](#15-node-自己算得出來的值不從-cli-注入) | node 自己算得出來的值，不從 CLI 注入 | 注入的每一項，node 真的算不出來嗎？一行設定值或 node 已經收到的輸入算得出來，就不注入 | 沒人，只能人看 |
| [16](#16-同一件事各-split-用同一個機制不對稱要寫理由而且理由不能是這個不對稱自己造成的) | 同一件事，各 split 用同一個機制；不對稱要寫理由，而且理由不能是這個不對稱自己造成的 | 假設這個不對稱不存在，docstring 寫的理由還成立嗎 | 沒人，只能人看 |
| [17](#17-資料閘的-node-只寫查什麼與交給誰判斷收集事實與組報告進-steps) | 資料閘的 node 只寫「查什麼」與「交給誰判斷」；收集事實與組報告進 `steps/` | node body 裡有沒有巢狀函式、數列數、組報告，或自己判斷有沒有違反不變量；決策（查什麼、政策、前置檢查）要留在 node | 沒人，只能人看 |
| [18](#18-程式改了落地的內容某個部署的設定卻沒動時把對應的格式版本加-1) | 程式改了落地的內容、某個部署的設定卻沒動時，把對應的格式版本加 1（dataset 產物、training 模型、training 預測、`.bin` 快取，四個常數） | 有沒有哪一份合法的設定與資料，改動前後寫出來的內容不一樣？包括 import 的模組；改 `.bin` 內容多半也改模型，兩個都要加 | 沒人，只能人看 |

體例：**程式碼註解與 docstring 一律英文**，**`docs/` 一律繁體中文**。

「沒人」的規則靠這張表與 code review。想替某條加機械檢查，先確認它擋得住真的會發生的失效；這裡多數規則是語意判斷（「這個名字說出決策了嗎」），寫成正規表示式會抓不到重點。

## 1. node 的邊界要落在「撈得出來看」的產物上

「撈得出來看」＝這個產物有 catalog 條目、或有測試讀它、或有 log 提到它，至少一個。

判定問題：**出事的時候，能不能把這條邊界上的東西單獨撈出來看？**

- 三者都沒有的中間結果 → 跟相鄰的 node 合併。切開只多一個框要理解，不多一個觀察點（實例：[ADR-0010](../adr/0010-inference-chunked-scoring-shape.md) §4 合併 inference 的 `build_scoring_dataset` 與 `apply_preprocessor`）。
- 反方向：一個 node 產出兩樣東西、各被不同的下游消費 → 那是兩個 node，切開。登記過的例外見〈已登記的例外〉。

## 2. 打開 node 就讀得完它做了哪些決定，不必跳檔

node 是**一串決策**，不是一個呼叫。打開它，當場讀得完這個節點對資料做了哪些決定。

照這條寫的 node 函式會比一般的長，這是刻意的：這裡的「只做一件事」＝講完這條 pipeline 在這一段的 ML 故事。

## 3. 一個 helper 最多裝一個決策

兩個以上的決策，拆成多個具名步驟，由 node body 依序呼叫。

- **數的是裝了幾個決策，不是被幾個 node 呼叫。** `compute_feature_columns`（`pipelines/dataset/steps/feature_columns.py`）被兩個 node 呼叫，只裝一個決策、名字說出來了：合法。只有一個呼叫端、裝了四個決策的 helper：照樣違規。
- **「兩三個緊密相關的決策可以打包」不成立。** 放寬之後，這條要擋的形狀就合規了（[ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 拒絕過這個放寬）。
- **要擋的形狀**：短短的轉手 node，加一個裝著多個決策、名字一個都沒說的 helper。它通過 `architecture-constraints.md` S1 的位置檢查，node 也短得好看，但讀者得打開另一個檔逐行反推。實例：ADR-0008 §1 盤點的 `select_train_keys`（12 行）→ `select_keys`（98 行、四個決策）。

## 4. 決策要浮到 node body，機制才能沉進 helper

- **決策＝會改變模型看到的資料的選擇。** 哪些列有資格、哪一列被留下、缺值算 0 還是算沒有、詞彙表從哪來。
- **機制＝語意定了之後，怎麼在引擎上算出來。** 用哪種 join、broadcast 還是 shuffle、常數與型別細節（未知類別的哨兵值、float32 的 cast 實作）。

步驟的名字負責說出決策，值不必上浮。

**判定程序**：把 helper 的名字換成純機械的名字（`_encode_via_map_literal`、`_bucket_by_crc32`），重讀 node body。

- 仍講得完整個 ML 故事 → 過。
- 讀完會問「這一步到底決定了什麼」 → 決策漏進 helper 了，拆出來。

過的例子（`src/recsys_tfb/pipelines/dataset/nodes.py:401`）：把 `months_filter_as_date` 換成 `_filter_isin_dates`，註解還在，node body 仍講得出「train 只能從設定的月份抽」。

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test.
pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
```

不過的形狀：`pool = prepare_train_pool(sample_pool, parameters)`。換成 `_apply_pool_prep` 之後只知道 pool 被處理過，「只抽 train 月份」掉進 helper 裡了。

## 5. 決策重複寫兩份，機制才共用

兩個 node 做同樣的決策時，決策在兩個 node 的 body 裡各寫一份，不抽成共用 helper：抽出去就是規則 3 的形狀，而各寫一份才讓每個 node 各自讀得懂。

**機制要共用**：log 的格式字串、欄名推導、join 寫法。各寫一份會漂移。

分界同規則 4：抽出去的東西換成機械名不損失資訊 → 抽；抽出去之後 node 少講了一件事 → 不抽。

這條主要擋的是**題目重疊、答案各異**的一組 node 被包成一個帶旗標的 helper：每個旗標都是一個被搬離 node body 的決策，選錯旗標不會報錯。

活的例子：`select_sample_keys`、`select_val_keys`、`select_test_keys`（`src/recsys_tfb/pipelines/dataset/nodes.py`）。三個 node 都回答「哪些月份有資格」「要不要抽、抽掉誰」，答案各不相同，各自寫在自己的 `# Decision —` 底下；共用的是各裝一件事的機制（例如 `dataset/steps/sampling.py`、`dataset/steps/scoping.py` 裡的函式、`core/schema.py` 的 `get_entity_grouping`）。沒有 `_select_keys(split_name, parameters)`：包起來會長出「抽列還是抽 entity」「帶不帶 carry 欄」兩個旗標，而 val 抽錯單位不會報錯，只會讓 mAP 回答另一個問題（`select_val_keys` 的 docstring、ADR-0016）。

## 6. 名字要說出「選錯不會報錯」的那個差別

兩個函式若只差一個會**靜默**改變結果的細節，名字要說出那個細節。名字沒說，兩者在呼叫端看起來可以互換，選錯也沒有錯誤訊息。

名字只指向存在的東西：不指向已廢棄的雙軌制（backend 後綴）、不存在的 config 鍵，也不把裡面零個 node 的模組叫「共用的 node」。名字指錯，讀者就無法靠檔名或函式名決定該打開哪一個，這比名字難看嚴重得多。

## 7. 產物落不落地，是接續成本的決定

規則 1 問「出事時撈不撈得出來」；這條問「**下次要從這裡接續，得付多少**」，決定邊界上的產物要不要進 catalog 落地。

判準＝「是不是某個宣告接續點的必要輸入」×「重算貴不貴」：

- **便宜的留 memory-only**（view、handle、cheap transform）：切片的自動擴張會把生產者拉回來重跑，代價可接受。
- **貴的落地**（例如 HPO 輸出）：否則 `--from-node finalize_model` 會一路補跑回 `tune_hyperparameters`，等於重訓一次。

新增 node 會默默把接續點弄貴，不會有人提醒。改完做兩件事：

1. 跑 `python -m recsys_tfb <pipeline> --list-nodes`，看各 node 的接續成本。
2. 跑 `tests/test_pipelines/test_resume_contracts.py`。它的 `RESUME_CONTRACTS` 釘住各 pipeline 承諾的接續點與允許補跑的集合。紅了：給新產物補 catalog 條目，或改契約並在 PR 說明為什麼接受變貴。

切片機制見 [`pipeline-slicing.md`](../operations/user-guides/pipeline-slicing.md)。

## 8. 模組放根層還是 `steps/`，看 src 側呼叫端在不在本 pipeline 內

```
pipelines/<name>/
  __init__.py     re-export create_pipeline
  pipeline.py     接線：DAG 拓撲，以及「為什麼這樣接」的註解
  nodes.py        node 函式：這條 pipeline 的 ML 故事唯一的家
  <contract>.py   有本 pipeline 以外的 src 側消費者的模組（對外契約）
  steps/          只有 nodes.py 呼叫的機制模組，一模組一個 concern
```

src 側呼叫端全在本 pipeline 內 → `steps/`；有外部消費者 → 根層。這樣讀者看一次目錄列表就分得出對外契約與內部步驟。

- **測試不算呼叫端。** 測試直接 import `steps/` 不改變任何模組的位置。
- **純度不決定位置。** `dataset/steps/feature_columns.py` 零 pyspark，照樣在 `steps/`。要釘純度，用 AST 測試釘那一個模組。現有兩種掛法：`month_plans.py` 由 `architecture-constraints.md` S2 釘在 `tests/test_core/test_architecture_constraints.py`（直接掃描＋可達性，兩個測試缺一不可）；`chunk_plans.py` 由 `tests/test_pipelines/test_inference/test_chunk_plans.py` 的 `test_no_pyspark_import`、`test_no_project_import` 釘。
- **根層可以沒有任何契約模組**，那代表這條 pipeline 開跑前沒有對外契約。現有的契約模組與它們的外部呼叫端：
  - dataset：`run_contract.py`（`__main__.py` 開跑前要版本、月份計畫、要注入的東西，跑完問 train 版本落地沒，ADR-0029 決定 11）；`month_plans.py`（evaluation 指令用它的月份計畫工具、inference 指令用它的候選層級表名稱，ADR-0007）。
  - training：`run_contract.py`（`__main__.py` 問設定檢查、版本、模式與要併進 manifest 的報告，ADR-0030 決定 13）；`cache_sources.py`（各快取 node 從哪張 Hive 表複製，ADR-0014）。
  - evaluation：`compare_tables.py`（`__main__.py` 開跑前讀出比較來源各是哪張 Hive 表）。
  - inference：沒有，根層只有 `pipeline.py` 與 `nodes.py`。
- **`steps/__init__.py` 只放 docstring，不 re-export。** `nodes.py` 逐模組 import，import 那一行就說出這個步驟來自哪個 concern。

誰擋得住，只有一個方向：

- **S3**（`architecture-constraints.md`）：本 pipeline 以外的 `src/` 模組 import `steps/` 就轉紅，也擋 `steps/__init__.py` 出現 re-export（`test_steps_packages_re_export_nothing`）。
- **反方向 S3 看不到**：只有本 pipeline 在呼叫、卻留在根層的模組。新增或改動模組時，自己查它的 src 側呼叫端。
- S1 只管 `pipelines/dataset/` 的 node 定義位置；純度只有 `month_plans.py` 與 `chunk_plans.py` 有測試。其餘靠 code review。

## 9. 步驟寫在註解與 helper 名上，不寫在 `log_step` 上

node body 裡的一個步驟＝一行具名呼叫，上面一段 `# Decision —` 說出決策**與選錯的後果**。形狀就是規則 4 那段例子。

步驟寫成這個形狀；不寫成 `with log_step(...)` 區塊、巢狀私有函式或註解分隔線。

## 10. `log_step` 只包會觸發 Spark action 的區塊

Spark 的 join、filter、select、withColumn、cast 都是 lazy，真正的計算發生在後面的 action（多半是 `catalog.save()`）。lazy 區塊包進 `log_step`，計時一定印出 ~0.00s，讀起來跟「這一步很快」一模一樣，兩種零就分不開了。

`log_step` 包的是會把資料收回 driver 或觸發寫入的區塊：`collect()`、`count()`、`isEmpty()`、`toPandas()`、`save()`。例（`src/recsys_tfb/pipelines/dataset/nodes.py:845`，`require_months_present` 會 collect）：

```python
with log_step(logger, "require_months_present(train_snap_dates)"):
    require_months_present(
        feature_table, time_col, train_months, "train_snap_dates",
    )
```

- node 的時間花在哪，由 Runner 的 `load`／`func`／`save` 三段拆分回答（`core/runner.py`），不靠 node 內的計時標籤。
- Runner 已經記了 `node_started`／`node_completed`／`node_failed`（`architecture-constraints.md` F2）。node 內只記業務判斷：跳過了什麼、選了哪條分支、處理了幾列。

**怎麼查：追一層。** 規則 9 讓區塊裡是具名步驟，所以 action 多半在步驟的函式體裡，grep 區塊那幾行找不到。區塊裡每個具名呼叫，打開它的定義找 `collect`／`count`／`isEmpty`／`toPandas`／`save`；追到專案外的 helper，看它的 docstring 有沒有說會 collect。追一層都找不到 action → 這個區塊拆掉。

追一層之後仍要判斷的兩種，留著計時，並在該處寫明理由：

- **metastore 往返**（例如 `existing_partition_values()`）：不在 action 名單裡，但真的往返，時間隨分區數長。
- **空輸入**讓平常有 action 的區塊那一次變成零 action（例如全部續跑時 `populated_buckets` 拿到空月份清單）：不算違例。

## 11. 驗證放哪，由「它需要看到什麼」決定

| 需要看到 | 放哪 |
|---|---|
| 只有 config | A 系列 predicate（`core/consistency.py`，CLI entry 執行） |
| 來源表的 metadata 或廉價 distinct；本 pipeline 剛落地的 parquet 表的列數（只讀檔尾的 `count()`） | B 系列資料閘。零掃描：廉價 distinct 之外，I/O 只讀 metadata／檔尾、不讀欄位資料（[ADR-0006](../adr/0006-data-quality-checks-belong-upstream.md) 與它的修訂） |
| node 執行期才存在的中間資料 | 留在 node，**docstring 標明是前置檢查或後置條件** |

- **前置檢查**＝檢查**輸入**（來源表有沒有這些月份、這些欄）。失敗＝上游沒準備好。
- **後置條件**＝檢查**自己算出來的結果**（切完的 dev 是不是空的、join 完欄位齊不齊）。失敗＝這個 node 的邏輯或設定有問題。

兩種失敗要找的人不同，所以 docstring 要說是哪一種。

node 裡的 `raise` 本身合法：`core/consistency.py` 的 legend 登記了「runtime backstop」這個模式。要做到的是登記、標種類。新增一致性不變量，在 `core/consistency.py` 加 predicate，不在各 pipeline 裡 ad-hoc 寫。

既有不變量各有測試（`tests/test_core/test_consistency.py`），但沒有測試擋「在 pipeline 裡 ad-hoc 加 `raise`、不進 `core/consistency.py`」，也沒有測試查留在 node 的 `raise` 有沒有標種類。驗證分層的實例論證：[ADR-0011](../adr/0011-inference-validation-two-layers.md) §3。

## 12. 命名：底線＝模組私有、守衛叫 `require_*`、不得有 backend 後綴

- **底線前綴＝只有本模組呼叫。** `nodes.py`（或任何跨模組的呼叫端）呼叫得到的，一律無底線：node body 逐行呼叫它，就是模組外在用。
- **不用 backend 後綴**（`_spark`／`_pandas`）。雙軌制只剩 Spark，後綴指向不存在的東西（規則 6）。純 driver 端的處理不算雙軌，那是機制，用 concern 命名。
- **守衛叫 `require_*`**（`require_months_present`、`require_columns_present`、`require_item_is_a_feature`、`require_base_key_columns`）。只警告不 raise 的叫 `warn_*`。
- **模組用 concern 命名**，不用「helper」「shared」「common」這類不指向任何東西的字。

## 13. docstring 講「為什麼是這個答案」，不講簽章已經說過的事

所有註解與 docstring 都適用的規則（寫程式碼看不出來的事、每句 durable、一件事只講一次、超過 10 行逐句檢查）在 [`code-comments.md`](code-comments.md)。node 的 docstring 另外要寫簽章說不出來的三件事：

1. **為什麼不是另一種做法**：「LEFT + COALESCE，never INNER：INNER 會靜默丟掉 miss，而 miss 是這個 frame 的大多數」。
2. **選錯的後果，以及會不會有錯誤訊息。** 會靜默出錯的，一律寫出來。
3. **成本量級**（會觸發 action 的函式）：「一次 `distinct().collect()`，落到 driver 的東西以月份數為界、不以列數為界」。

例（`src/recsys_tfb/pipelines/dataset/nodes.py:497`，`split_train_keys`；第 2 點，最後一句是第 3 點）：

```python
# An empty train_dev is invisible downstream: it is the early-stopping
# validation set for every HPO trial (training's steps/hpo_scoring.py
# passes it to the adapter as valid_data), so an empty one means each trial
# silently runs its full round budget with early stopping never firing —
# no error, no warning, just worse models and a longer search. Costs one
# Spark action; see ADR-0005 for the fallback if that ever matters at scale.
```

docstring 與 `docs/` 的文字都不寫程式數得出來的數量（「四個消費者」「62 個 node 裡 59 個」）：程式一改就變假話，沒有測試會紅。改成「去哪裡查」（例如「node 清單看 `--list-nodes`」），或只寫在有測試核對的地方（例如 `test_static_coverage_floor` 的斷言）。ADR 要拿數字當證據時，標明是哪個 commit 的快照並附重現指令（ADR-0031 附錄二的做法）。

## 14. 讀來源表或增量產物時，在程式碼裡寫明讀哪些 time 值

node 讀一張可能裝著這一步用不到的月份的表時，在那一步寫月份篩選，說出「這一步只讀這幾個 time 值」，不靠 Spark 從 join 的另一邊推出來。哪些表算：

- **來源表**（`feature_table`、`sample_pool`、`label_table`、候選層級特徵表）：一律算。月份由上游決定，每個月都在長。
- **增量產物**（每次只寫新月份、舊月份留著；同一版本底下月份一直累積，ADR-0001）：算。清單在 `src/recsys_tfb/pipelines/dataset/month_plans.py` 的 `INCREMENTAL_DATASETS`。
- **其他落地表**（`sample_keys`、`train_keys`、`val_keys` 這類）：不算，train、val 的月份在版本 ID 的雜湊裡。但在註解寫一句為什麼不篩，範本是 `build_val_model_input` 的 docstring。

沒寫篩選的讀取，量跟著表的歷史長，不跟著這次要用的月份長；結果逐列相同，沒有任何症狀。「只是為了回答一個問題」的讀取也算：`require_months_present` 先篩到要檢查的月份，再問哪些月份在。

例（`src/recsys_tfb/pipelines/dataset/nodes.py:1112`，`build_model_input`，中間省略）：

```python
# Decision — every table the keys are joined against is read for this
# split's months only, and the filter is written here rather than left to
# the optimizer (ADR-0029 decision 1). ...
in_months = months_filter_as_date(schema["time"], months)
label_table = label_table.filter(in_months)
preprocessed_feature_table = preprocessed_feature_table.filter(in_months)
```

- **dataset 裡用 `months_filter_as_date`**（`dataset/steps/scoping.py`，先轉成日期再比）。拿 Python 日期物件直接跟字串 time 欄比，一列都對不到，也不報錯。別的 pipeline 不能 import dataset 的 `steps/`（規則 8），用自己的比對方式，一樣寫在程式裡。
- **不靠動態分區修剪（DPP）。** 它是 optimizer 盡力而為的最佳化，有沒有發生，程式碼裡看不到，消失時也沒有訊號。keys LEFT JOIN 右表時只能廣播右表，而預設設定下 DPP 只能借 keys 那側的廣播，所以這類 join 實際上從不修剪（ADR-0029 決定 1）。
- 這條保證的是「進 join 的只有這次的月份，而且寫在程式裡」。讀取本身省多少看表怎麼分區：依 time 分區的只讀那幾個月的分區，沒分區的照樣整張掃。那是部署的事。

**怎麼確認寫了**：看 analyzed plan，這張表在 join 之前有月份篩選。optimized plan 不算（Spark 碰巧推得出的條件也會出現在那裡）；執行前 `explain()` 印出的 `dynamicpruning` 也不算，有沒有真的修剪那一行都長一樣。

**要量省了多少**：先 `collect()` 真的執行，再看執行後計畫裡每個 scan 的 `numPartitions`。分區表看 `PartitionFilters`，沒分區的表看 `Filter`。

**新的讀取點**：在 `tests/test_pipelines/test_dataset/test_month_scoped_reads.py` 加一個案例。它在 analyzed plan 上釘住今天的讀取點（四個 build 的每張右表、各個丟無正例組的 node 讀的 `label_table`、月份存在檢查只問要檢查的月份），新加的沒人提醒。

## 15. node 自己算得出來的值，不從 CLI 注入

注入＝CLI（`__main__.py`）開跑前算好、登記進 DAG 的輸入（catalog 沒有的）。**只注入同時符合兩個條件的事實：開跑前才知道，而且 node 自己看不到**（例如 month plan：要列出 Hive 分區才知道，還受 `--rebuild-dates` 影響）。一行設定值算得出來的、node 本來就收到的輸入算得出來的，都由 node 自己算。

注入有兩條路，判準相同：

- **DAG 輸入**：dataset 的在 `src/recsys_tfb/pipelines/dataset/run_contract.py` 的 `pipeline_inputs`——month plan、執行模式（`--only-test-months`）、沒宣告候選層級特徵表時的 `None`（Runner 會載入每一個宣告的輸入，所以那個名字要有東西）。
- **併進 `parameters` 的執行期值**：在 `src/recsys_tfb/__main__.py` 組 `runtime_params` 的地方。真的值是版本 ID（任何 node 跑之前就要拿它解析 catalog 路徑）與 `--rebuild-dates`（使用者在命令列給的）；`model_version`、`snap_date` 只是讓 catalog 樣板解析得了的佔位值，不算違規。
- 例：資料閘需要知道這次是不是 `--only-test-months`，它自己看不出來，所以注入這個模式；月份它照各 build 的同一套規則自己算，不注入月份清單（兩邊一致由 `test_month_scoped_reads.py` 的 `TestTheDataGateAsksForWhatTheBuildsRead` 釘著）。
- 「CLI 算一次、大家共用」不成立：省下一行 list comprehension，付出的是規則 2，讀 node 的人要去另一個檔找答案。
- 沒宣告候選層級特徵表時注入 `None`，不把讀它的 node 拔出 DAG：node 裡的分支照樣要給有宣告的部署走，拔掉只會讓不同部署的 `--list-nodes` 長得不一樣，什麼也沒省到。

例（`src/recsys_tfb/pipelines/dataset/nodes.py:679`，`build_train_model_input` 自己算要讀的月份）：

```python
# Decision — the months this build reads: the train months, the ones its
# keys were drawn from. train_dev is split off that draw, so its build
# reads the same list (ADR-0029 decision 2).
train_months = [
    pd.Timestamp(d) for d in parameters["dataset"]["train_snap_dates"]
]
```

`tests/test_cli.py` 經指令釘住今天注入哪些名字，但擋不住新增一項 node 自己算得出來的注入：連測試一起改就過了。

## 16. 同一件事，各 split 用同一個機制；不對稱要寫理由，而且理由不能是這個不對稱自己造成的

train、train_dev、val、test 回答同一個問題時，用同一個機制，在 pipeline 的同一個位置做（例如都在 keys 上、建表之前）。這類問題例如：沒有正例的 query group 丟不丟、在哪裡丟（丟組）；整個單位一起抽的抽樣怎麼抽；要不要去重；entity 有 NULL 怎麼辦；讀哪些月份。

真的要不同，在 docstring 寫理由，理由要過兩關：

1. **假設這個不對稱不存在，理由還成立嗎？** 不成立，理由就是不對稱自己造成的，不算數。
2. **理由講的是這個 split 本身的性質嗎**（它的指標怎麼算、它的月份怎麼累積）？「當初先寫的是這樣」不算。

站得住的例子（`src/recsys_tfb/pipelines/dataset/nodes.py:607`，`select_val_keys`：val 整個 entity 一起抽，train 逐列抽）。假設 train 也整個 entity 一起抽，mAP 照樣按 query group 算，理由還在：

```python
# Decision — when val is sampled, it is sampled per *entity*, never per row:
# mAP is computed over a query group, so a group must keep all of its
# candidates or the metric answers a different question.
```

不算數的例子（ADR-0029 決定 4 改掉的）：「val 在建好的 model_input 上丟組，因為沒有東西把 val 的列數釘在 keys 上」。釘列數的是粒度閘 B10：它比對每個 split 的 keys 與 model_input 列數是否相等。B10 當時不涵蓋 val，正是因為丟組放在建表之後——丟過組的 model_input 列數本來就對不上 keys，沒辦法配對。拿掉不對稱，理由就沒了。後果是 val／test 的 join 放大列數沒人擋，而 test 的指標是對外報告的數字。

每個 split 各自選「最適合」的做法，單獨看都說得通，合起來的代價是：讀者看一個 split 推不出其他三個；同一個資料問題在各條路徑結果不同；出事的那條，比對檢查也可能照不到（上面的 B10）。想做「只給某個 split 的捷徑」時，拿這段過第 2 關。

和規則 5 的分工：規則 5 擋題目重疊、答案各異的決策被包成一個帶旗標的 helper；這條擋答案本該相同、卻因各自長出來而不同。

粒度閘（B10）現在四個 split 都配對，擋得住「join 放大列數」這一種後果，擋不住不對稱本身。

## 17. 資料閘的 node 只寫「查什麼」與「交給誰判斷」；收集事實與組報告進 `steps/`

適用於判斷寫在 `core/consistency.py` B 系列不變量（有代號、有 predicate）的資料閘，例如 dataset 的 `validate_data_consistency`、`validate_model_input_grain`。判斷不在 `core/consistency.py` 的檢查（例如 inference 的 `validate_predictions`）不歸這條管。要新增有代號的檢查，照規則 11 先加 predicate，這條就跟著適用。

這是規則 4 與規則 11 套在資料閘上。node body 寫：

1. **查什麼**：哪些表、哪些月份、哪些欄、用什麼單位比（例如粒度閘逐月配對，不比加總）。
2. **交給 `core/consistency.py` 的 predicate 判斷**：由它判斷有沒有違反不變量，並產出每一條失敗訊息（含代號）。node 只給這批訊息一行總標題，交給 `collect_all_message` 排成一則。
3. **這個閘的政策**：找到就中止，還是只警告。
4. **前置檢查**，照規則 11 標種類。例：`validate_model_input_grain` 先確認這次的版本範圍底下有檔，免得兩邊都讀到 0 的比對被當成通過；前置檢查的錯誤與比對的錯誤收齊後一次中止，不提早 return。它數列數之前的執行計畫檢查是例外：計畫不是只讀 parquet 檔尾，就連同前面的錯誤一起中止、一列都不數，因為為了收齊錯誤去數，正是它要擋的成本。

這四件以外的決策也留在 node（例如粒度閘的「兩邊都沒有檔的月份算 0＝0，不算失敗」）。政策與前置檢查同樣留下：搬進 helper，讀 node 的人就看不到找到問題之後停不停（規則 4）。

進 `steps/` 的只有機制：數列數與讀出它的執行計畫、找出這次寫了哪些檔、Spark 聚合、組報告、把報告排成 log 行。每個函式具名、只做一件事；不寫成 node 裡的巢狀函式（那樣讀者要逐行讀完才知道閘查了什麼，同一套機制也會在各閘各長一份、寫法不一；實例是 ADR-0029 動工前的 `validate_data_consistency`，`2e99c573`）。

例（`src/recsys_tfb/pipelines/dataset/nodes.py:1305`，前置檢查留在 node，檔數由 `dataset/steps/footer_facts.py` 的 `plan_table_count` 算）：

```python
# Pre-check (this node's own inputs) — a scope that matches none of a
# table's files would make the comparison below pass on two zeroes,
# having looked at nothing; a table with no files at all is an empty
# split and passes (model_input_grain_scope_errors says why).
for side, count in whole[split].items():
    errors += model_input_grain_scope_errors(
        split, side, files_in_table=count.files_in_table,
        files_in_scope=count.files_in_scope, scope=scope,
    )
```

## 18. 程式改了落地的內容、某個部署的設定卻沒動時，把對應的格式版本加 1

版本 ID 與快取路徑是拿設定算出來的雜湊，程式改了它們不會變。

- **部署**：一份設定（`conf/`）跑起來的一個實例。repo 裡的銀行示例、廣告示例各是一個，生產上的每一份設定也各是一個。
- **落地內容**：版本 ID 或快取路徑底下、之後會被讀的表與檔（dataset 各 split 的 keys 與 model_input、`preprocessor.json`；training 的模型、test 預測、`.bin` 快取）。指資料本身（有哪些列、每列的值），不含列的順序或檔案怎麼切；`.bin` 例外，見下方。

程式改了下表某一列的內容、某個部署的設定沒動時，把那一列的常數加 1，**跟那個改動同一次發布上線**。

| 常數 | 在哪 | 什麼改了要加 1 | 加 1 之後 |
|---|---|---|---|
| `DATASET_ARTIFACT_FORMAT_VERSION` | `core/versioning.py` | dataset 落地的內容：各 split 的 keys 與 model_input、`preprocessor.json`、`preprocessed_feature_table` | 每個部署的 `base_dataset_version` 換一個，dataset 全部重建；`model_version`、HPO 的 `search_id` 都含它，所以每個部署都重訓 |
| `TRAINING_MODEL_FORMAT_VERSION` | `core/versioning.py` | HPO 一個 trial 的分數，或 training 訓練出來的模型 | `model_version` 與 `search_id` 換一個：HPO 從頭搜、重訓、test 預測全部重寫。它也是 `.bin` 快取路徑的一段，所以 `.bin` 也重建 |
| `TRAINING_PREDICTION_FORMAT_VERSION` | `core/versioning.py` | training 寫出的 test 預測，模型卻不變——只有評分會跑的程式，例如 `score_output.py` 的組表與檢查 | 不換任何版本 ID、不重訓。predict 在 `predict_manifest.json` 替每個月記了這個值，下次跑時記的值不同，所有月份的 test 預測重寫 |
| `TRAIN_DATA_CACHE_FORMAT_VERSION` | `pipelines/training/steps/train_data_cache.py` | `.bin` 快取目錄裡的東西：哪些列、列的順序、欄怎麼編碼、sidecar 記了什麼 | 只換快取路徑：第一次跑重建一次 `.bin` |

要問的是：**有沒有哪一份合法的設定與資料，改動前後寫出來的內容不一樣？** 有一份就加，只在某種資料才會變的也算（例如只在 entity 有 NULL 時才不同）。兩個容易漏的範圍：

- **新增設定鍵、預設值跟舊行為不同**：沒寫那個鍵的部署，雜湊輸入一個字都沒變，內容卻變了。
- **「某條 pipeline 的程式」包括它 import 的模組**，不只 `pipelines/<那一條>/`。例如多欄 item 的分隔字元 `ITEM_SEPARATOR` 在 `core/schema.py`，改它，dataset 的 keys、model_input 與 `preprocessor.json` 都會變；training 的評分入口在 `models/`、寫出前的組表在 `score_output.py`，inference 也用它們。

一個改動常常要加不只一個：

- **改了 `.bin` 的內容，多半也改了模型**：只把 query group 裡的列換個順序，lambdarank 的模型就會變（`TRAIN_DATA_CACHE_FORMAT_VERSION` 的 docstring 記了實測），所以 `.bin` 的列順序算內容。兩個都加。只加快取那個，`search_id` 不變，接續的搜尋會把新舊檔案上的 trial 混在一起；只加模型那個不會出錯，`.bin` 會跟著重建。
- **改了模型**：只加模型那個。它換了 `model_version`，預測本來就會重寫。
- **只改了預測、模型不變**：只加預測那個。加成模型那個結果也對，但每個部署都要從頭跑 HPO（生產上是小時級）。
- **「評分的程式」不等於「只改預測」**：問改到的程式在 HPO 評 trial、建 `.bin`、訓練時會不會也跑到。會的話就是模型那個，即使是為了 inference 改的。例如 `ModelAdapter.score` 用 `io/extract.py` 的 `pdf_to_X` 編矩陣，訓練與 HPO 走同一個批次編碼（`_write_batch_features`）：改那段，模型就變了。

不加的後果：新舊程式寫的內容掛在同一個版本 ID 或快取路徑底下，而且不報錯。增量的 test 月份只寫新的，舊月份是舊程式寫的；HPO 續跑接上舊 trial；predict 跳過已寫過的月份；`.bin` 路徑有 `_SUCCESS` 就直接用。

**同一次發布只加一次**（生產只在發布時重建）。動手前比對最新的發布 tag（`git tag -l 'v*'`）裡這個常數的值（沒有這個常數算 0）：main 上的值已經比它大，這次發布本來就會重建，不再加；一樣大，就加 1。四個常數各比各的。維護分支（`release/*`）上的修正若改了落地內容，取一個 main 從沒用過的值，否則兩邊各自加到同一個數，內容不同、ID 卻相同。

加 1 的代價：dataset 或模型那個加了之後，每個部署都要重建或從頭跑 HPO、重訓，而重訓之前「不重訓、只加評估月份」（[`adding-an-eval-month.md`](../operations/user-guides/adding-an-eval-month.md)）對正在用的模型失效；預測那個只多一次全月份預測；`.bin` 那個只多建一次 `.bin`，舊目錄留在磁碟上要手動刪。

常數在 `src/recsys_tfb/core/versioning.py:166`、`:171`、`:176` 與 `src/recsys_tfb/pipelines/training/steps/train_data_cache.py:57`。`core/versioning.py` 的模組 docstring 與 `TRAIN_DATA_CACHE_FORMAT_VERSION` 的 docstring 用英文寫了同一條規則；兩邊有出入時以 docstring 為準（`docs/agents/domain.md` 對版本 ID 的慣例）。為什麼分成四個常數：[ADR-0030](../adr/0030-training-second-pass-honest-adapter-shared-scoring.md) 決定 9、10。

# 已登記的例外

下表的違例各有登記過的理由，不代表判準可以放寬。**要新增一筆，先問使用者**（同 `architecture-constraints.md` 節三的例外登記規則）。

| 違例 | 違反哪條 | 為什麼還在 | 誰核准 |
|---|---|---|---|
| dataset 的 `split_train_keys` 一個 node 產出兩樣東西，分別給兩個下游 | 1（反方向） | 兩樣東西是同一個式子的正反兩面，拆開就只剩「兩處程式碼碰巧一致」 | 使用者 2026-09-25，ADR-0029 決定 14 |
| dataset 的三個 build node 把組裝交給共用的 `build_model_input`，它裝了好幾個決策 | 3、4、5 | 四個 split 組 model_input 的決策完全相同，只差讀哪幾個月；拆成四份，一份先改了，四個 split 就不對稱了（規則 16） | 使用者 2026-09-25，#467 的留言 |
| evaluation 的 `compute_compare_metrics` 一個 node 產出四樣東西（A′、B′ 各自的列的事實與指標），分別給兩個下游 | 1（反方向） | 四樣都落地、各有 catalog 條目，拆開不多一個能撈出來看的地方，也不多一個用得到的接續點 | 使用者 2026-10-23，#536 審查 |

## `split_train_keys`：一個 node，兩個輸出

產出 `train_keys_unfiltered` 與 `train_dev_keys_unfiltered`，分別給 train 與 train_dev 的丟組 node。留著的理由在這幾行（`src/recsys_tfb/pipelines/dataset/nodes.py:491`）：

```python
to_dev = unit_drawn_under_ratio(
    keys, split_cols, train_dev_ratio, seed, site="split_train_dev",
)
train_keys = keys.filter(~to_dev)
train_dev_keys = keys.filter(to_dev)
```

train 取 train_dev 那個式子的否定，每一列必定落在剛好一邊。拆成兩個 node 就得各算一次抽樣；哪天只改了一邊，就會有列同時在兩邊或兩邊都不在，而且不報錯。

## `compute_compare_metrics`：一個 node，四個輸出

對兩邊（這次的模型 A′、比較對象 B′）各做同一件事：先數列的事實，再用它算指標。產出 `compare_row_facts_a`、`compare_metrics_a`、`compare_row_facts_b`、`compare_metrics_b`；兩份指標給 `compare_to_source`，A′ 的列的事實給 `generate_comparison_report`。主線同樣的兩步是拆開的（`compute_row_facts`、`compute_metrics`）。

為什麼留：

- 四樣都有 catalog 條目、都落地，已經各自撈得出來；拆開不多一個觀察點。
- 主線拆開，是為了讓設定改了之後有共同的重跑起點（`COMPUTED_KEYS` 指向 `compute_row_facts`）。比較這側的重跑建議（換了比較對象、`--compare-only` 下改了算的設定）都從這個 node 開始（`COMPARISON_RERUN_NODE`），`--compare` 下改了算的設定則從主線的 `COMPUTED_KEYS` 那一列開始；拆出列的事實那一半，沒有人會從它接續。

代價：四個輸出照位置對應 `pipeline.py` 的 outputs，對調了不會報錯，比較報表會把兩邊標反。有一條測試在對調時轉紅（斷言經 outputs 名字拿到的 A′ 帶分群欄、B′ 沒有）。

這筆只登記這一個 node。比較這側以後多算一種結果（例如決定 16 的兩個 AP），要不要另開 node，照規則 1 重新判斷。

## `build_model_input`：一個 helper，好幾個決策

三個 build node（`build_train_model_input`，註冊成 train 與 train_dev 兩個 node；`build_val_model_input`；`build_test_model_input`）各自只決定「這個 split 讀哪幾個月」（val／test 另有新 item 的警告），然後交給共用的 `build_model_input`。它不是 node，裝了好幾個決策：兩個 LEFT join（沒有 label 的 key 算負例、沒有特徵的 key 留 NULL）、候選層級表的編碼、數值欄的 cast。照規則 3、4、5 的字面都違規。

為什麼留：

- 四個 split 組 model_input 的決策完全相同，只差讀哪幾個月。規則 5 要擋的是「題目重疊、答案各異」被包成帶旗標的 helper；這裡答案一題都沒有不同，也沒有旗標。
- 拆成四份，就是四份一模一樣的決策各自維護；哪一份先改了，四個 split 就不對稱了（規則 16）。
- 規則 3、4 擔心的是讀者得打開另一個檔逐行反推。`build_model_input` 跟呼叫者在同一個檔（`nodes.py`），每個決策都掛著 `# Decision —`，不必反推。這是它跟規則 3 的反例 `select_keys`（在別的檔、名字沒說出任何決策）真正的差別。

這筆只登記這一組 node。下一組長得像的，先問使用者。

`months` 是沒有預設值的 keyword-only 參數（`src/recsys_tfb/pipelines/dataset/nodes.py:1061`），不寫成「沒傳月份就讀 train 月份」：那樣 val／test 忘了傳月份時不會報錯，只會悄悄讀到 train 的月份，每個 key 都接不到 label 與特徵。
