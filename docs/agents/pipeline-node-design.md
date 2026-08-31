# Pipeline node 設計判準

改或加一個 pipeline node 之前先讀這份。它回答三個問題：**邊界畫在哪**、**node 內部長什麼樣**、**機制與驗證放哪個檔**。

**這份不管位置合不合法。** 那是 [`architecture-constraints.md`](architecture-constraints.md) 的事，那份的每一條都有 `tests/test_core/test_architecture_constraints.py` 把關。這份管的是內容，而內容**幾乎沒有機械檢查**——測試全綠不代表符合這份，理由見〈這些規則大多沒人擋得住〉。兩份都要讀。

**規則從哪來**：第一節與第二節多數條目是 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) §2、§3 在 dataset pipeline 上裁決的結果，這裡把它們一般化成跨 pipeline 的判準，並成為判準本身的唯一真實來源；ADR-0008 保留為 2026-08-05 那次決策的完整論證。規則 10 與規則 13 標了「首次寫下」，那是從既有程式碼讀出來、之前只活在某個模組 docstring 裡的慣例。

---

## 全部規則一覽

動手前對著這張表打勾。編號只是指路用，規則本身寫在標題裡。

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
| [13](#13-docstring-講為什麼是這個答案不講簽章已經說過的事) | docstring 講「為什麼是這個答案」，不講簽章已經說過的事 | 有沒有寫「為什麼不是另一種做法」「選錯會不會報錯」 | 沒人，只能人看 |

外加一條體例：**程式碼註解與 docstring 一律英文**（對齊既有全部模組），**`docs/` 一律繁體中文**。

---

# 一 · 換個框架也成立的規則

這一節不依賴本 repo 的任何設施。專案形狀在第二節。

## 1. node 的邊界要落在「撈得出來看」的產物上

「撈得出來看」的意思是：這個產物有 catalog 條目、或有測試讀它、或有 log 提到它，三者至少一個。

三者都沒有的中間結果，切成獨立 node 只增加拓撲、不增加資訊——讀者多一個框要理解，卻沒有多一個地方可以下手觀察。這種邊界要合併。

反方向同樣成立：一個 node 若產出兩樣東西、被兩個不同的下游各自消費，那是兩個 node 被塞在一起，切開。

判定問題只有一句：**「這條邊界上的東西，出事的時候我能不能單獨把它撈出來看？」** 撈不出來就不是邊界。

**為什麼不是「切得越細越好」**：[ADR-0010](../adr/0010-inference-chunked-scoring-shape.md) §4 有實例。inference 曾把 `build_scoring_dataset` 與 `apply_preprocessor` 切成兩個 node，中間物沒有 catalog 條目、沒人讀，合併之後少一個框而資訊不減。

**誰擋得住**：沒有機械檢查。

## 2. 打開 node 就讀得完它做了哪些決定，不必跳檔

一個 node 是**一串決策**，不是一個呼叫。打開它，讀者應該當場讀得完這個節點對資料做了什麼決定。

**代價要先講清楚**：照這條寫出來的 node 函式會比一般的長。這是刻意的。行數本來就只是「這個函式只做一件事」的粗略代理，而這裡的「一件事」＝「講完這條 pipeline 在這一段的 ML 故事」。

實例：`predict_and_write_scores` 在 #197 那次改動後從 206 行長到 253 行，因為機制搬進了 `steps/`、決策留在 node body（記在 `architecture-constraints.md` F8）。

**誰擋得住**：沒有機械檢查。

## 3. 一個 helper 最多裝一個決策

兩個以上的決策必須拆成多個具名步驟，由 node body 依序呼叫。

**不照做會長成這樣**：一個 12 行的轉手 node，加一個裝著四個決策的 helper。這個形狀的危險在於它看起來很好——位置檢查全綠、node 短得漂亮——但讀者要知道這個節點做了什麼決定，得打開另一個檔、再逐行反推。

### 實際長什麼樣

**這個 repo 真的長過那個樣子。** 2026-08-05 重構前（[ADR-0008](../adr/0008-dataset-modules-split-by-role.md) §1 的盤點）：

```
pipelines/dataset/nodes_spark.py     select_train_keys    12 行   ← node
pipelines/dataset/helpers_spark.py   select_keys          98 行   ← 整個抽樣設計都在這
```

那 98 行裡有**四個決策**，函式名一個都沒說：

1. 月份過濾（哪些列有資格被抽）
2. 抽樣率覆寫的優先序（per-group override 蓋過 split 預設）
3. 依 identity key 決定去留（誰活下來）
4. 輸出欄＝identity ＋ carry

node 那 12 行讀起來乾淨漂亮。但要知道 train split 是怎麼選出來的，得打開另一個檔、逐行反推那 98 行。

**今天的樣子**：四個決策變成 `nodes.py:207`–`239` 裡四個具名步驟，各自帶一段 `# Decision —`；`steps/sampling.py` 只剩「每一步在 Spark 上怎麼算」。那個模組的 docstring 自己記著這件事：

```python
"""Sampling mechanics for key selection: ...

Each function here carries at most one mechanism. The four decisions ADR-0008 §1
counted inside the old ``select_keys`` ... are now named steps in the
key-selecting nodes (``nodes.py``); this module holds only how each step is
computed on Spark.
"""
```

### 「被很多 node 呼叫」不是違規訊號

判準是**裝了幾個決策**，不是**被幾個人用**。

`compute_feature_columns`（`steps/feature_columns.py`）被兩個 node 呼叫，仍然合法：它只裝一個決策，而且名字說出來了。反過來，`select_keys` 只有一個呼叫端，照樣非法。

### 為什麼不放寬成「兩三個緊密相關的決策可以打包」

ADR-0008 明確拒絕過這個放寬，理由很直接：**放寬之後 `select_keys` 當年那個形狀就合規了**，這條判準就擋不住它唯一要擋的東西。

**誰擋得住**：沒有機械檢查。而且要注意，上面那個 12 行 node ＋ 98 行 helper 的形態**完全滿足** `architecture-constraints.md` 的 S1（node 必須 `def` 在 `nodes.py`）——ADR-0008 就是拿當年的 `select_train_keys` → `select_keys` 論證這件事的。位置對了不代表內容對了。

## 4. 決策要浮到 node body，機制才能沉進 helper

先定義兩個詞：

- **決策 ＝ 會改變模型看到的資料的選擇。** 哪些列有資格、哪一列被留下、缺值算 0 還是算沒有、詞彙表從哪來。
- **機制 ＝ 語意定了之後，怎麼在引擎上算出來。** 用哪種 join、broadcast 還是 shuffle、常數與型別細節（未知類別的哨兵值、float32 的 cast 實作）。

**步驟的名字負責說出決策，值不必上浮。**

### 判定程序

把 helper 的名字換成純機械的名字（`_encode_via_map_literal`、`_bucket_by_crc32`），重讀 node body：

- 仍講得完整個 ML 故事 → 過。
- 讀完會問「這一步到底決定了什麼」 → 決策漏進 helper 了，拆。

### 實際長什麼樣

照做的（`pipelines/dataset/nodes.py:207`）：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test / calibration.
pool = restrict_to_months_or_all(sample_pool, time_col, train_months)
```

把它換成機械名試試——`pool = _filter_isin_dates(sample_pool, time_col, train_months)`。上面那段註解還在，所以 node body 仍然講得完「train 只能從設定的月份抽」這件事。**過。**

沒照做會寫成這樣：

```python
pool = prepare_train_pool(sample_pool, parameters)
```

換成機械名之後（`pool = _apply_pool_prep(sample_pool, parameters)`），讀者只知道「pool 被處理過」，不知道處理掉了什麼。「只抽 train 月份」這個決策掉進 helper 裡了。**不過，拆。**

**誰擋得住**：沒有機械檢查。這一條靠 code review 執行。

## 5. 決策重複寫兩份，機制才共用

兩個 node 做同樣四個決策時，**把四個決策各自寫在兩個 node 裡**，不要抽成一個共用 helper——那正是規則 3 禁止的形狀，而重複才是讓每個 node 各自讀得懂的原因。

但**機制要共用**：log 的格式字串、欄名推導、join 寫法。兩份會漂移。

分界線與規則 4 同一條：抽出去的東西若能換成機械名而不損失資訊，就該抽；抽出去之後 node 少講了一件事，就不該抽。

### 實際長什麼樣

`select_train_keys`（`nodes.py:183`）與 `select_calibration_keys`（`nodes.py:242`）做的是同樣四個決策：有資格的月份、每組留多少、誰活下來、輸出哪些欄。兩個 node 各自把四個決策逐條寫了一遍：

```python
# select_train_keys
pool = restrict_to_months_or_all(sample_pool, time_col, train_months)
keys = with_effective_sample_ratio(keys, group_keys, sample_ratio, overrides)
keys = keep_rows_drawn_under_ratio(keys, identity_key, seed, site="sample_keys")

# select_calibration_keys
pool = restrict_to_months_or_all(sample_pool, time_col, cal_months)
keys = with_effective_sample_ratio(keys, group_keys, cal_ratio, cal_overrides)
keys = keep_rows_drawn_under_ratio(keys, identity_key, seed, site="calibration_keys")
```

被共用的是**機制**：`restrict_to_months_or_all`、`with_effective_sample_ratio`、`keep_rows_drawn_under_ratio` 都在 `steps/` 裡，各自只裝一件事。沒有一個 `_select_keys(split_name, parameters)` 把四個決策包起來。

而 `select_calibration_keys` 的 docstring 只需要說出那**唯一的差別**：兩個 split 共用 `random_seed`，所以抽樣 `site` 不同名，calibration 才不會抽到跟 train 完全相同的列（#140）。

**誰擋得住**：沒有機械檢查。

## 6. 名字要說出「選錯不會報錯」的那個差別

兩個函式若差別只在一個會**靜默**改變結果的細節，名字必須把那個細節說出來。名字丟掉這個區別，兩者在呼叫端就看起來可以互換，而選錯的兩種後果**都沒有錯誤訊息**。

反面同樣重要：**名字不得指向不存在的東西**。指向已廢棄的雙軌制（backend 後綴）、指向不存在的 config 鍵、宣稱是「共用的 node」而裡面零個 node——讀者無法用檔名或函式名決定該打開哪一個。這比命名醜陋嚴重得多。

**誰擋得住**：沒有機械檢查。

## 7. 產物落不落地，是接續成本的決定

規則 1 決定「這條邊界該不該存在」；這一條決定**邊界上的產物要不要進 catalog 落地**。兩個問題不同：規則 1 問「出事時撈不撈得出來」，這一條問「**下次要從這裡接續，得付多少**」。

判準＝「是不是某個宣告接續點的必要輸入」×「重算貴不貴」：

- **便宜的留 memory-only**（view、handle、cheap transform）——切片的自動擴張會把生產者拉回來重跑，代價可接受。
- **貴的落地**（HPO 輸出）——否則 `--from-node finalize_model` 會一路補跑回 `tune_hyperparameters`，等於重訓一次。

**接續點品質是會被新增 node 默默破壞的契約。** 這是這一條跟其他條不同的地方：新增一個 node 不會有人跳出來說「你把接續點弄貴了」，除非有東西釘住它。

改完跑 `python -m recsys_tfb <pipeline> --list-nodes` 肉眼確認各 node 的接續成本。切片機制本身見 [`pipeline-slicing.md`](../operations/user-guides/pipeline-slicing.md)。

**誰擋得住**：**部分擋得住。** `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS` 釘住各 pipeline（含 calibration-enabled training 變體）承諾的接續點與允許補跑集合。改壞會紅燈——要嘛給新產物補 catalog 條目，要嘛修改契約並在 PR 說明為什麼接受變貴。

---

# 二 · 這個 repo 的形狀

## 8. 模組放根層還是 `steps/`，看 src 側呼叫端在不在本 pipeline 內

一條 pipeline 的目錄有四種角色：

```
pipelines/<name>/
  __init__.py     re-export create_pipeline
  pipeline.py     接線：DAG 拓撲，以及「為什麼這樣接」的註解
  nodes.py        node 函式 ← 這條 pipeline 的 ML 故事唯一的家
  <contract>.py   有本 pipeline 以外的 src 側消費者的模組（對外契約）
  steps/          只有 nodes.py 呼叫的機制模組，一模組一個 concern
```

**根層 vs `steps/` 的判準只有一句：src 側呼叫端是否全在本 pipeline 內。** 全在內 → `steps/`；有外部消費者 → 根層。

**測試不算。** 測試直接 import `steps/` 底下的東西不改變任何模組的位置。判準管的是生產端呼叫者，因為它買到的是「讀者看一次目錄列表就分得出對外契約與內部步驟」。

兩個容易誤用的推論：

- **「純模組（零 pyspark）」不是根層的判準。** `pipelines/dataset/steps/feature_columns.py` 是零 pyspark 的純模組，而它在 `steps/` 裡。純度是**模組層級**的性質、跟位置無關，要釘就用 AST 測試釘那一個模組。現況兩個純模組用了兩種掛法：`month_plans.py` 由 `architecture-constraints.md` S2 釘在 `tests/test_core/test_architecture_constraints.py`（直接掃描 ＋ 可達性，兩個測試缺一不可），`chunk_plans.py` 由自己的測試檔 `tests/test_pipelines/test_inference/test_chunk_plans.py` 釘（`test_no_pyspark_import` ＋ `test_no_project_import`）。
- **一條 pipeline 根層可以沒有任何契約模組。** 那代表它沒有 pipeline 開跑前的對外契約，是資訊不是缺陷。dataset 有 `month_plans.py` 在根層，是因為 `__main__.py` 在 pipeline 開跑前就要算好月份計畫再注進 catalog（[ADR-0007](../adr/0007-month-plans-travel-through-the-catalog.md)）；training 有 `cache_sources.py`，同一個理由、同一個呼叫端（[ADR-0014](../adr/0014-training-modules-split-by-role.md)）；inference 的分塊計畫發生在 node 內，所以它根層只有 `pipeline.py` 與 `nodes.py`。

`steps/__init__.py` **不 re-export 任何東西**：`nodes.py` 逐模組 import，import 那一行就說出這個步驟來自哪個 concern。這條自 #234 起由 S3 的 `test_steps_packages_re_export_nothing` 擋著——三個 `steps/__init__.py` 只能有 docstring。

**誰擋得住**：**部分擋得住，而且只有一個方向。**

- 擋得住的那個方向是 **S3**（`architecture-constraints.md`，#234 加入）：本 pipeline 以外的 `src/` 模組 import 了 `steps/` 底下的東西就轉紅——也就是「該放根層的被藏進 `steps/`」。同一條 S3 也擋住 `steps/__init__.py` 出現 re-export。
- **擋不住反過來那個方向**：一個模組明明只有本 pipeline 在呼叫、卻留在根層，S3 看不到——沒有外部呼叫端可以觸發它。`search_space.py` 與 `hpo_resume.py` 在根層待到 #234 才被搬走，正是這個盲點的實例，發現它的是人不是測試。
- S1 只管 `pipelines/dataset/` 的 node 定義位置；純度只有 `month_plans.py` 與 `chunk_plans.py` 兩個登記過的模組有測試。其餘靠 code review。

## 9. 步驟寫在註解與 helper 名上，不寫在 `log_step` 上

一個步驟在 node body 裡的樣子是：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test / calibration.
pool = restrict_to_months_or_all(sample_pool, time_col, train_months)
```

一行具名呼叫，上面一段說出決策**與選錯的後果**。

不是 `with log_step(...)` 區塊、不是巢狀私有函式、不是註解分隔線。

**誰擋得住**：沒有機械檢查。

## 10. `log_step` 只包會觸發 Spark action 的區塊

**這一條本檔首次寫下**（出處：`pipelines/dataset/nodes.py` 模組 docstring）。

Spark 的 join、filter、select、withColumn、cast 全都是 lazy：它們在微秒內回傳一份計畫，真正的計算發生在後面某個 action 裡（多半是 `catalog.save()`）。

**不照做會怎樣**：把 lazy 區塊包進 `log_step`，計時**保證**印出 ~0.00s，而那行讀起來跟「這一步很快」一模一樣。兩種零混在同一個事件名下，就沒有人分得出哪個零是哪個。

**留在 `log_step` 裡的，是會把資料收回 driver 或觸發寫入的區塊**：`collect()`、`count()`、`isEmpty()`、`toPandas()`、`save()`。

實例（`nodes.py:466`）——包住的：

```python
with log_step(logger, "require_months_present(train_snap_dates)"):
    require_months_present(feature_table, time_col, train_months, "train_snap_dates")
```

沒包的（`nodes.py:574`，純 lazy）：

```python
result = feature_table.filter(months_filter_as_date(time_col, months)).select(
    *encoded_frame_columns(base_key, feature_columns, feature_table.columns)
)
```

**這條不是效能潔癖，是觀測誠實。** node 的時間到底花在哪，是 Runner 的 `load`／`func`／`save` 三段拆分要回答的問題（`core/runner.py`），不是 node 內部的計時標籤。

順帶一提：Runner 已經記了每個 node 的 `node_started`／`node_completed`／`node_failed`（`architecture-constraints.md` F2），**node 內不需要再寫「開始了／完成了」**。node 該記的是業務判斷——跳過了什麼、選了哪條分支、處理了幾列。

**誰擋得住**：沒有機械檢查，而且**不能查成一次 grep**——查法見〈這些規則大多沒人擋得住〉。

## 11. 驗證放哪，由「它需要看到什麼」決定

| 需要看到 | 家 |
|---|---|
| 只有 config | Layer-1 A 系列 predicate（`core/consistency.py`，CLI entry 執行） |
| 來源表的 metadata 或廉價 distinct | Layer-2 B 系列（資料閘，零掃描，[ADR-0006](../adr/0006-data-quality-checks-belong-upstream.md)） |
| node 執行期才存在的中間資料 | 留在 node，**且必須在 docstring 標明是前置檢查或後置條件** |

**兩個詞不可混用**：

- **前置檢查**＝檢查**輸入**（來源表有沒有這些月份、這些欄）。失敗表示**上游沒準備好**。
- **後置條件**＝檢查**自己算出來的結果**（切完的 dev 是不是空的、join 完欄位齊不齊）。失敗表示**這個 node 的邏輯或設定有問題**。

兩種錯誤要找的人不同，所以 docstring 要說是哪一種。

「node 裡有 `raise`」本身不違規——`core/consistency.py` 的 legend 已經有「runtime backstop」這個登記過的模式。**違規的是沒登記、沒標種類。**

新增一致性不變量**必須**在 `core/consistency.py` 加 predicate，不得在各 pipeline ad-hoc 散落。

**誰擋得住**：沒有機械檢查。既有的每個不變量本身有測試（`tests/test_core/test_consistency.py`），但**沒有任何測試擋得住「有人在某條 pipeline 裡 ad-hoc 加一個 `raise` 而不進 `core/consistency.py`」**，也沒有測試檢查留在 node 的 `raise` 有沒有在 docstring 標明種類。這兩件事都只能靠 code review。

## 12. 命名：底線＝模組私有、守衛叫 `require_*`、不得有 backend 後綴

- **底線前綴 ＝ 只有本模組呼叫。** `nodes.py`（或任何跨模組的呼叫端）呼叫得到的一律無底線——底線在 Python 的意思是「模組外不要用」，而 node body 逐行呼叫它就是模組外在用。
- **不得有 backend 後綴**（`_spark`／`_pandas`）。pandas／Spark 雙軌制已經只剩 Spark 一條，後綴指向不存在的東西（違反規則 6 的反面）。純 driver 端的處理不算雙軌——那是機制，用 concern 命名。
- **守衛用 `require_*`**：`require_months_present`／`require_columns_present`／`require_item_is_a_feature`／`require_base_key_columns`。只警告不 raise 的用 `warn_*`。
- **模組用 concern 命名**，不用「helper」「shared」「common」這類指向不存在的東西的字。

**誰擋得住**：沒有機械檢查。

## 13. docstring 講「為什麼是這個答案」，不講簽章已經說過的事

**這一條本檔首次寫下**（出處：`pipelines/dataset/` 全部模組的一致慣例，之前沒有寫下來過）。

函式簽章已經說了它做什麼。docstring 要說的是簽章說不出來的三件事：

1. **為什麼不是另一種做法**——「LEFT + COALESCE，never INNER：INNER 會靜默丟掉 miss，而 miss 是這個 frame 的大多數」。
2. **選錯的後果，以及會不會有錯誤訊息**——這是本 repo 最貫穿的主題。凡是「會靜默出錯」的，docstring 必須說出來。
3. **成本量級**（會觸發 action 的函式）——「一次 `distinct().collect()`，落到 driver 的東西以月份數為界、不以列數為界」。

**不要寫**「這個函式接受 X 回傳 Y」——那是簽章的工作。

實例（`nodes.py:315`，第 2 點）：

```python
# An empty train_dev is invisible downstream: it is the early-stopping
# validation set for every HPO trial (training/nodes.py passes
# train_dev_lgb_handle as val_dataset), so an empty one means each trial
# silently runs its full round budget with early stopping never firing —
# no error, no warning, just worse models and a longer search.
```

**誰擋得住**：沒有機械檢查。

---

# 這些規則大多沒人擋得住

**不要把「測試綠」讀成「符合這份文件」。**

`architecture-constraints.md` 的 S1（node 必須 `def` 在 `nodes.py`）與 S2（登記在冊的模組零 pyspark）**只管 `pipelines/dataset/`**，而且它們只擋得住**位置與純度**：

> 一個 12 行的轉手 node 加一個裝著四個決策的 helper，**完全滿足 S1**。

第一、二節十三條規則裡，只有規則 7（`RESUME_CONTRACTS`）與規則 8（S1／S2 ＋ `chunk_plans.py` 自己的測試）有**部分**機械檢查，其餘十一條全部靠開頭那張表 ＋ code review。這是已知的殘留風險，不是疏漏——[ADR-0008](../adr/0008-dataset-modules-split-by-role.md) §2 在裁決當下就記載了同一件事。

**為什麼不補上機械檢查**：這裡每一條規則都需要判斷「這個名字說出決策了嗎」「這個註解講的是為什麼嗎」，那是語意題。能機械化的部分（node 定義位置、模組純度、`steps/` 不外流）已經在 `architecture-constraints.md`，或評估後決定不加。要加新的一條之前，先確認它擋得住的是**真的會發生的失效**，而不是把一條判斷題寫成一個抓不到重點的正規表示式。

## 規則 10 是唯一「幾乎」可機械檢查的一條

判斷一個呼叫是不是 Spark action 需要一份 action 名單，而名單會腐爛（新版 PySpark 加方法、專案 helper 內部藏 action）。所以它留在 review 層。

**但別把它查成一次 grep。** 規則 9 要求區塊裡是具名步驟，所以 action 多半**不在區塊那幾行裡**——它在那個步驟的函式體。在 `pipelines/inference/nodes.py` 上實測：8 個 `log_step` 區塊只有 3 個當場 grep 得到 action，其餘 5 個要追進 `steps/` 才看得到。

**查法是「追一層」**：區塊裡每個具名呼叫，打開它的定義找 `collect`／`count`／`isEmpty`／`toPandas`／`save`；追到專案外的 helper 就看那個 helper 的 docstring 有沒有說它會 collect。一層都追不到 action ＝ 這個區塊該拆掉。

兩個追一層之後仍要人判斷的形態，都出現在 inference 上：

- **metastore 往返**（`existing_partition_values()`）不在 action 名單裡，但它是真的往返、時間隨分區數長，所以留著計時是對的。
- **空輸入**（全部續跑時 `populated_buckets` 拿到空月份清單）會讓一個正常有 action 的區塊那一次退化成零 action，那不是違例。

兩者都要在該處寫明理由，否則下一個 reviewer 只會看到「grep 不到」。

---

# 已登記的例外（3 筆）

**看到這些不必以為判準是裝飾。** 它們各有登記過的理由：

| 違例 | 違反哪條 | 為什麼還在 |
|---|---|---|
| `nodes.py` 從 `recsys_tfb.preprocessing` import 兩個底線函式（`_encode_categoricals`、`_cast_feature_floats_to_float32`） | 12 | 該模組被 dataset 與 inference 共用，rename 要同時改兩條 pipeline 的呼叫點。登記在 ADR-0008「這條 ADR 沒有解決的事」與 `deliberate-non-goals.md` |
| `pipelines/evaluation/nodes_spark.py` 帶 backend 後綴 | 12 | evaluation pipeline 尚未依本檔重整；`pipeline.py` 同時從 `nodes_spark.py` 與 `comparison_nodes.py` 取 node，還有一個動態 `importlib.import_module` |
| `pipelines/training/` 的部分 node `def` 在 `recsys_tfb.diagnosis.model` 底下 | 8 | #222 重整 training 時**刻意不搬**（ADR-0014 決定 6）：搬進來會生出 7 個違反規則 3 的薄殼，而且這 7 個 node 未來要搬去 evaluation，現在搬等於白做。這也是 S1 無法一般化到所有 pipeline 的原因 |

要新增一筆到這張表，**必須先問使用者**（同 `architecture-constraints.md` 節三的例外登記規則）。

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 可機械檢查的約束、框架事實、例外登記 | [`architecture-constraints.md`](architecture-constraints.md) |
| 重整一整條 pipeline 的**流程**（排順序、切 PR、選驗證手段） | [`pipeline-refactor-process.md`](pipeline-refactor-process.md) |
| dataset 那次裁決的完整論證與當時的盤點 | [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) |
| node 邊界該不該合併的實例論證 | [ADR-0010](../adr/0010-inference-chunked-scoring-shape.md) §4 |
| 驗證分層（chunk 層 vs batch 層）的實例論證 | [ADR-0011](../adr/0011-inference-validation-two-layers.md) §3 |
| 不變量代號 A 系列／B 系列的意義 | `src/recsys_tfb/core/consistency.py` 模組 docstring |
| 程式碼現在長什麼樣 | `graphify-out/GRAPH_REPORT.md` |
