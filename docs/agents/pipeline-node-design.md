# Pipeline node 設計判準

改或加一個 pipeline node 之前先讀這份。它回答三個問題：**邊界畫在哪**、**node 內部長什麼樣**、**機制與驗證放哪個檔**。

**這份不管位置合不合法。** 那是 [`architecture-constraints.md`](architecture-constraints.md) 的事，那份的每一條都有 `tests/test_core/test_architecture_constraints.py` 把關。這份管的是內容，而內容**幾乎沒有機械檢查**——測試全綠不代表符合這份，理由見〈這些規則大多沒人擋得住〉。兩份都要讀。

**規則從哪來**：第一節與第二節多數條目是 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) §2、§3 在 dataset pipeline 上裁決的結果，這裡把它們一般化成跨 pipeline 的判準，並成為判準本身的唯一真實來源；ADR-0008 保留為 2026-08-05 那次決策的完整論證。規則 10 與規則 13 標了「首次寫下」，那是從既有程式碼讀出來、之前只活在某個模組 docstring 裡的慣例。規則 14–18 來自 dataset 的第二輪整理（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md)）：ADR-0008 之後，dataset 加了十幾個功能，有幾種形狀一再長歪，這五條就是把它們寫成明文。

**詞彙**：query group、候選層級特徵表、month plan、資料閘、不變量代號（A 系列、B 系列）這些詞的意思，在 repo 根目錄的 [`CONTEXT.md`](../../CONTEXT.md)。

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
| [14](#14-讀來源表或落地的中間表時在程式碼裡寫明讀哪些-time-值) | 讀來源表或落地的中間表時，在程式碼裡寫明讀哪些 time 值 | 讀來源表或增量產物的地方，join 之前有沒有月份篩選；新的讀取點在 `test_month_scoped_reads.py` 加一個案例 | **部分**：`test_month_scoped_reads.py` 釘住今天的讀取點，新加的沒人擋 |
| [15](#15-node-自己算得出來的值不從-cli-注入) | node 自己算得出來的值，不從 CLI 注入 | 注入的每一項，node 真的算不出來嗎？一行設定值或 node 已經收到的輸入算得出來，就不注入 | 沒人，只能人看 |
| [16](#16-同一件事各-split-用同一個機制不對稱要寫理由而且理由不能是這個不對稱自己造成的) | 同一件事，各 split 用同一個機制；不對稱要寫理由，而且理由不能是這個不對稱自己造成的 | 假設這個不對稱不存在，docstring 寫的理由還成立嗎 | 沒人，只能人看 |
| [17](#17-資料閘的-node-只寫查什麼與交給誰判斷收集事實與組報告進-steps) | 資料閘的 node 只寫「查什麼」與「交給誰判斷」；收集事實與組報告進 `steps/` | node body 裡有沒有巢狀函式、讀檔尾、組報告，或自己判斷什麼算失敗；政策與前置檢查要留在 node | 沒人，只能人看 |
| [18](#18-程式改了-dataset-落地的內容某個部署的設定卻沒動時把-dataset_artifact_format_version-加-1) | 程式改了 dataset 落地的內容、某個部署的設定卻沒動時，把 `DATASET_ARTIFACT_FORMAT_VERSION` 加 1 | 有沒有哪一份合法的設定與資料，改動前後寫出來的內容不一樣？包括 dataset import 的模組 | 沒人，只能人看 |

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

**今天的樣子**：四個決策變成 `select_sample_keys`（`nodes.py`）裡四個具名步驟，各自帶一段 `# Decision —`；`steps/sampling.py` 只剩「每一步在 Spark 上怎麼算」。那個模組的 docstring 自己記著這件事：

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

照做的（`pipelines/dataset/nodes.py` 的 `select_sample_keys`）：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test.
pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
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

`select_sample_keys` 與 `select_val_keys`（`src/recsys_tfb/pipelines/dataset/nodes.py`）
是活的一對。兩個 node 的決策**部分重疊**：都要回答「哪些月份有資格」「要不要抽、抽掉
誰」，而 train 還多一個「輸出哪些欄」（它帶 carry 欄，val 不帶）。重疊的那幾題，**兩邊
的答案沒有一題相同**。

（2026-09-25 更新，#460：「哪些月份有資格」原本兩邊各走一個機制——train 的
`restrict_to_months_or_all` 在月份清單為空時退回整池，val 的 `restrict_to_months` 不退——
所以舊版這裡還列了「要不要退回整池」這個旗標。ADR-0029 決定 3 刪掉這兩個函式，兩邊都改走
`months_filter_as_date`，差別只剩月份清單本身。）

（2026-09-25 更新，#462：val 原本「不靠主鍵、自己 `dropDuplicates`」，train 靠主鍵——這一題
兩邊答案曾經不同。ADR-0029 決定 6 讓三個 split 都靠主鍵，下面的節錄已照改；決定 5 另在 val
抽樣之前加了「entity 有 NULL 就丟並警告」一步。上面列的重疊題目兩邊答案仍然不同。）

每一個決策都寫在它自己的 node body 裡，前面掛一行 `# Decision —`（英文原文，可以直接
grep）。以下是逐字節錄，各留第一行：

```python
# select_sample_keys（nodes.py）
# Decision — eligibility: only rows in the configured train months can be
#   drawn. ...
pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
if draw_can_drop_rows(sample_ratio, overrides):        # 比例滿且無 override 就整段跳過
    # Decision — how much of each stratum to keep: a per-group override
    #   outranks the split's default ratio; ...
    keys = with_effective_sample_ratio(keys, group_keys, sample_ratio, overrides)
    # Decision — who survives: the draw is on the identity key, so the same
    #   key is kept or dropped identically on every rerun.
    keys = keep_rows_drawn_under_ratio(keys, identity_key, seed, site="sample_keys")
# Decision — what a split's keys are: the identity key, plus the carry ...

# select_val_keys（nodes.py）
# Decision — eligibility: only the configured val months.
val_pool = sample_pool.filter(months_filter_as_date(time_col, val_dates))
# Decision — the val population is every key in those months, trusting
#   sample_pool's primary key as train does rather than de-duplicating it.
all_keys = val_pool.select(*identity_key)
# Decision — a row whose entity is NULL in any column is dropped, out loud,
#   before the draw (ADR-0029 decision 5). ...
all_keys, _ = drop_rows_with_null_entity(all_keys, schema["entity"], split="val")
if val_sample_ratio >= 1.0:
    return all_keys                                    # 預設路徑：整個母體，不抽
# Decision — when val is sampled, it is sampled per *entity*, never per row:
#   mAP is computed over a query group, so a group must keep all of its
#   candidates or the metric answers a different question.
# Decision — the draw unit: what the user declared, else the whole entity.
sample_cols = get_entity_grouping(parameters, "val_sample_keys")
sampled = keep_entities_drawn_under_ratio(
    all_keys, sample_cols, val_sample_ratio, seed, site="val_keys",
)
```

被共用的是**機制**：`months_filter_as_date`
（`dataset/steps/scoping.py`）、`keep_rows_drawn_under_ratio` /
`keep_entities_drawn_under_ratio` / `with_effective_sample_ratio` /
`drop_rows_with_null_entity`
（`dataset/steps/sampling.py`）、`get_entity_grouping`（`core/schema.py`），
各自只裝一件事。**沒有**一個 `_select_keys(split_name, parameters)` 把決策包起來。

正是因為重疊那幾題的答案都不同，包起來才會壞：那個 helper 會長出「抽列還是抽 entity」
「要不要帶 carry 欄」兩個旗標，而每個旗標都是一個從 node 本體被
搬走的決策。讀 `select_val_keys` 的人會看到 `_select_keys("val", …)`，然後得去讀 helper
才知道 val 是抽 entity 的——**而抽錯單位不會報錯**，只會讓 mAP 回答另一個問題
（`select_val_keys` 的 docstring 與 ADR-0016 記的就是這件事）。

第三個同族的 `select_test_keys` 也在同一支檔案裡，它連抽樣都沒有——三個 node 攤開來，
差別一眼看得到；包成一個 helper 就看不到了。

（`select_sample_keys` 的 docstring 記著「#414 移除 `select_calibration_keys` 之後沒有第
二個 node 給出**同樣的四個答案**」。那說的是孿生 node，今天確實沒有了；這條規則要擋的
是上面這種**題目重疊、答案各異**的形狀，兩者不衝突。）

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

**誰擋得住**：**部分擋得住。** `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS` 釘住各 pipeline 承諾的接續點與允許補跑集合。改壞會紅燈——要嘛給新產物補 catalog 條目，要嘛修改契約並在 PR 說明為什麼接受變貴。

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
- **一條 pipeline 根層可以沒有任何契約模組。** 那代表它沒有 pipeline 開跑前的對外契約，是資訊不是缺陷。dataset 根層有兩個：`run_contract.py`，`__main__.py` 在 pipeline 開跑前向它要版本、月份計畫與要注進 catalog 的東西，跑完再問它 train 版本落地了沒（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 11）；`month_plans.py`，evaluation 指令也直接用它的月份計畫工具，inference 指令用它定義的候選層級表名稱（[ADR-0007](../adr/0007-month-plans-travel-through-the-catalog.md)）。training 有 `cache_sources.py`，理由同 `run_contract.py`、同一個呼叫端（[ADR-0014](../adr/0014-training-modules-split-by-role.md)）；inference 的分塊計畫發生在 node 內，所以它根層只有 `pipeline.py` 與 `nodes.py`。

`steps/__init__.py` **不 re-export 任何東西**：`nodes.py` 逐模組 import，import 那一行就說出這個步驟來自哪個 concern。這條自 #234 起由 S3 的 `test_steps_packages_re_export_nothing` 擋著——四個 `steps/__init__.py` 只能有 docstring。

**誰擋得住**：**部分擋得住，而且只有一個方向。**

- 擋得住的那個方向是 **S3**（`architecture-constraints.md`，#234 加入）：本 pipeline 以外的 `src/` 模組 import 了 `steps/` 底下的東西就轉紅——也就是「該放根層的被藏進 `steps/`」。同一條 S3 也擋住 `steps/__init__.py` 出現 re-export。
- **擋不住反過來那個方向**：一個模組明明只有本 pipeline 在呼叫、卻留在根層，S3 看不到——沒有外部呼叫端可以觸發它。`search_space.py` 與 `hpo_resume.py` 在根層待到 #234 才被搬走，正是這個盲點的實例，發現它的是人不是測試。
- S1 只管 `pipelines/dataset/` 的 node 定義位置；純度只有 `month_plans.py` 與 `chunk_plans.py` 兩個登記過的模組有測試。其餘靠 code review。

## 9. 步驟寫在註解與 helper 名上，不寫在 `log_step` 上

一個步驟在 node body 裡的樣子是：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test.
pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
```

一行具名呼叫，上面一段說出決策**與選錯的後果**。

不是 `with log_step(...)` 區塊、不是巢狀私有函式、不是註解分隔線。

**誰擋得住**：沒有機械檢查。

## 10. `log_step` 只包會觸發 Spark action 的區塊

**這一條本檔首次寫下**（出處：`pipelines/dataset/nodes.py` 模組 docstring）。

Spark 的 join、filter、select、withColumn、cast 全都是 lazy：它們在微秒內回傳一份計畫，真正的計算發生在後面某個 action 裡（多半是 `catalog.save()`）。

**不照做會怎樣**：把 lazy 區塊包進 `log_step`，計時**保證**印出 ~0.00s，而那行讀起來跟「這一步很快」一模一樣。兩種零混在同一個事件名下，就沒有人分得出哪個零是哪個。

**留在 `log_step` 裡的，是會把資料收回 driver 或觸發寫入的區塊**：`collect()`、`count()`、`isEmpty()`、`toPandas()`、`save()`。

實例（`fit_preprocessor_metadata`）——包住的：

```python
with log_step(logger, "require_months_present(train_snap_dates)"):
    require_months_present(
        feature_table, time_col, train_months, "train_snap_dates",
    )
```

沒包的（`apply_preprocessor_to_features`，純 lazy）：

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

實例（`split_train_keys`，第 2 點）：

```python
# An empty train_dev is invisible downstream: it is the early-stopping
# validation set for every HPO trial (training/nodes.py passes
# train_dev_lgb_handle as val_dataset), so an empty one means each trial
# silently runs its full round budget with early stopping never firing —
# no error, no warning, just worse models and a longer search.
```

**誰擋得住**：沒有機械檢查。

## 14. 讀來源表或落地的中間表時，在程式碼裡寫明讀哪些 time 值

node 讀一張**可能裝著這一步用不到的月份**的表時，就在那一步寫一個月份篩選，說出「這一步只讀這幾個 time 值」。不要指望 Spark 從 join 的另一邊自己推出來。哪些表算：

- **來源表**（`feature_table`、`sample_pool`、`label_table`、候選層級特徵表）：一律算。它們的月份由上游決定，每個月都在長。
- **增量產物**：本框架落地、每次只寫新月份、舊月份留著的表。清單在 `month_plans.py` 的 `INCREMENTAL_DATASETS`（`preprocessed_feature_table`、`test_keys`、`test_model_input`）。算：同一個版本底下的月份會一直累積（ADR-0001）。
- **其他落地表**（`sample_keys`、`train_keys`、`val_keys` 這類）：不算。train、val 的月份在版本 ID 的雜湊裡，讀回來的剛好就是這一步要的月份。但要在註解寫一句為什麼不篩，`build_val_model_input` 的 docstring 就是這樣寫的。

為什麼要寫：沒寫篩選的讀取，量跟著表的歷史長，不跟著這次要用的月份長。

### 不照做會長成這樣

ADR-0029 動工前的 `build_model_input`（當時的 main，`2e99c573`）。keys 只有這個 split 的月份，`label_table` 與 `preprocessed_feature_table` 卻整張拿去 join（候選層級特徵表那時已經有篩選，月份清單由 CLI 注入，見規則 15）：

```python
# Decision — a key with no label row is a negative, not a gap. ...
dataset = join_labels_missing_as_negative(
    keys, label_table, label_join_key, label_col,
)
# Decision — a key with no feature row keeps its row and gets an all-NULL ...
dataset = join_features_missing_as_null(
    dataset, preprocessed_feature_table, feature_join_key,
)
```

答案是對的：join 鍵含 time，別的月份的列本來就接不上。錯的是成本。2026-09-25 在合成資料上看 plan：四個 build 讀 `label_table` 都沒有月份條件，14 個月全讀。

`preprocessed_feature_table` 更容易看走眼。執行前印的 `explain()` 裡有 `dynamicpruningexpression(...)`，看起來 Spark 有自動跳過別的月份。真的執行之後去數，12 個分區讀了 12 個。原因是結構：keys LEFT JOIN 右表時，Spark 只能廣播（broadcast）右表；而預設設定下，動態分區修剪（dynamic partition pruning，DPP）只能借用 keys 那一側的廣播。所以 keys 再小都不會修剪。

### 今天的樣子

`build_model_input`（`pipelines/dataset/nodes.py`）：

```python
# Decision — every table the keys are joined against is read for this
# split's months only, and the filter is written here rather than left to
# the optimizer (ADR-0029 decision 1). ...
in_months = months_filter_as_date(schema["time"], months)
label_table = label_table.filter(in_months)
preprocessed_feature_table = preprocessed_feature_table.filter(in_months)
```

同一條也管「只是為了回答一個問題」的讀取。`require_months_present`（`steps/scoping.py`）以前對整張表的 time 欄做 `distinct()`；現在先篩到要檢查的月份，再問哪些月份在。

dataset 裡的篩選一律用 `months_filter_as_date`（`steps/scoping.py`，先轉成日期再比）。它取代的 `restrict_to_months` 拿 Python 的日期物件直接比；time 欄是字串時，一列都對不到，也不報錯。別的 pipeline 不能 import dataset 的 `steps/`（規則 8），用自己的比對方式，但一樣要寫在程式裡。

**寫了之後省多少，看表怎麼分區。** 依 time 分區的表，只讀那幾個月的分區。沒依 time 分區的表照樣整張掃，但進 join、進 shuffle 的只剩這次的月份。所以這條保證的是「進 join 的只有這次的月份，而且寫在程式裡」；讀取本身省多少，是部署的事。

### 為什麼不是「靠 Spark 自己修剪就好」

DPP 是 optimizer 盡力而為的最佳化：執行時拿 join 另一邊實際出現的 time 值，跳過這一邊的分區。它有沒有發生，程式碼裡看不到；它消失時，沒有任何訊號。上面那個例子就是：它看起來有，實際上從來沒有。本框架對版本的修剪是 `load()` 下 WHERE 的硬保證，月份也要是同一個等級。

**怎麼確認寫了**：看 analyzed plan（程式寫了什麼），確認這張表在 join 之前有月份篩選。不要看 optimized plan：Spark 碰巧推得出月份條件的地方，它也會有條件，那正是這條規則不肯依賴的東西。**執行前的 `explain()` 印出來的 `dynamicpruning` 也不算證據**，有沒有真的修剪，那一行都長一樣。

**要量省了多少**：先 `collect()` 真的執行，再看執行後計畫裡每個 scan 的 `numPartitions`。分區表看 `PartitionFilters`，沒分區的表看 `Filter`。

**誰擋得住**：**部分擋得住。** `tests/test_pipelines/test_dataset/test_month_scoped_reads.py` 在 analyzed plan 上斷言今天這些讀取點：四個 build 的每張右表、各丟組 node 的 `label_table`，join 之前都有月份篩選；月份存在檢查只問要檢查的月份。新加一個讀取點，沒人會提醒你；要自己在那個檔加一個案例。這很要緊，因為違反這條不會有任何症狀：結果逐列相同，只有成本不同。

## 15. node 自己算得出來的值，不從 CLI 注入

CLI（`__main__.py`）開跑前可以往 DAG 裡塞東西：catalog 沒有的輸入，由 CLI 算好登記進去，這叫注入。**只有同時符合兩個條件的事實才注入**：開跑前才知道，而且 node 自己看不到。dataset 今天注入三種：

- **month plan**：每個增量產物哪些月份已經落地、這次要處理哪些。要列出 Hive 的分區才知道，還受 `--rebuild-dates` 影響。
- **執行模式**：這次是不是 `--only-test-months`。
- **沒宣告候選層級特徵表時的 `None`**：框架沒有「選用輸入」，Runner 照位置把輸入交給 node，所以要有東西佔住那個位置。

一行設定值算得出來的，或是 node 本來就收到的輸入算得出來的，都不注入，node 自己算。

CLI 併進 `parameters` 的執行期值也算注入，判準相同。今天有兩種：版本 ID（任何 node 跑之前就要拿它解析 catalog 路徑）與 `--rebuild-dates`（使用者在命令列給的）。

### 不照做會長成這樣

ADR-0029 動工前，候選層級特徵表要讀哪些月份，是 CLI 算好注入的。`month_plans.py` 有一個 `candidate_feature_table_months`，替三個 build 各算一份月份清單，注入成 DAG 的三個輸入。它的 docstring 自己承認不是 month plan：

```python
"""``{split: months}`` — the months of the candidate-level feature table
each build reads (``train``, ``val``, ``test``).

Not an incremental plan — that table is never landed, so nothing is
subtracted for having been written before. ...
"""
```

可是答案不過是 `dataset.train_snap_dates`、`dataset.val_snap_dates`，以及 test build 本來就收到的 month plan。繞道 CLI 的代價：DAG 多三個輸入；`month_plans.py` 多住一個不屬於它的函式；讀 build 的人看不到月份從哪來，要跳到另一個檔（違反規則 2）。

### 今天的樣子

build 自己算（`build_train_model_input`）：

```python
# Decision — the months this build reads: the train months, the ones its
# keys were drawn from. train_dev is split off that draw, so its build
# reads the same list (ADR-0029 decision 2).
train_months = [
    pd.Timestamp(d) for d in parameters["dataset"]["train_snap_dates"]
]
```

注入的東西集中在一處（`pipelines/dataset/run_contract.py` 的 `pipeline_inputs`），每一項都寫了為什麼 node 自己算不出來：

```python
return {
    **{
        month_plan_input(name): plan
        for name, plan in month_plans.items()
    },
    # The run mode, for the one node that cannot see it: ...
    "only_test_months": only_test_months,
    # `None` is how a node learns none is declared. ...
    **({} if candidate_declared else {CANDIDATE_FEATURE_TABLE: None}),
}
```

執行模式只注入給數值精度閘（B8，檢查數值特徵存成較窄的型別時會不會失真）。它要檢查這次各 build 讀的月份，而 `--only-test-months` 時只有 test 的 build 在跑，這件事它自己看不出來。所以只注入「是不是這個模式」，月份由它照各 build 的同一套規則自己算，不注入月份清單。

### 為什麼不是另一種做法

- **「CLI 算一次，大家共用」**：共用省下的是一行 list comprehension，付出的是規則 2：讀 node 的人要去另一個檔找答案。
- **「沒宣告候選表時，乾脆不把讀它的 node 接進 DAG」**：node 裡的分支照樣在（有宣告的部署還是要走），而且不同部署跑 `--list-nodes` 會看到不一樣的 DAG，什麼也沒省到。所以 `None` 留著，由 node 裡的分支處理。

**誰擋得住**：沒有機械檢查。`tests/test_cli.py` 經指令釘住了今天注入哪些名字（要緊的是 node 用哪個名字讀回它），但它擋不住新增一項 node 自己算得出來的注入：連測試一起改就過了。

## 16. 同一件事，各 split 用同一個機制；不對稱要寫理由，而且理由不能是這個不對稱自己造成的

train、train_dev、val、test 四個 split 回答同一個問題時，用同一個機制，而且在 pipeline 的同一個位置做（例如都在 keys 上、建表之前）。這類問題例如：沒有正例的 query group 丟不丟、在哪裡丟（下文簡稱「丟組」）；整個單位一起抽的抽樣怎麼抽；要不要去重；entity 有 NULL 怎麼辦；讀哪些月份。

真的要不同，在 docstring 寫理由。理由要過一個檢查：**假設這個不對稱不存在，理由還成立嗎？** 不成立，那個理由就是不對稱自己造成的，不算數。

這個檢查只擋得住最難看出來的那一種。過了檢查，還要問：理由講的是不是這個 split 本身的性質，例如它的指標怎麼算、它的月份怎麼累積？「當初先寫的是這樣」不是理由。

### 不照做會長成這樣

ADR-0029 動工前盤點時，四個 split 在四件事上各做各的。例如 entity 為 NULL 的列：train 丟掉並警告；val 抽樣時默默丟；val 不抽樣時與 test 則留著。同一個資料問題，四條路徑三種結果，其中一種是默默丟掉。

最難看出來的一種，是理由指向自己。ADR-0029 動工前的 `filter_val_model_input`，說明為什麼 val 在建好的 model_input 上丟組，而不是跟 train 一樣在 keys 上丟：

```python
"""...
Applied to the built model_input rather than to the keys (the train side's
choice): nothing pins val's row count to its keys (B10 covers train and
train_dev only), so there is nothing a later drop could break, and the
label is already joined.
"""
```

理由是「沒有東西把 val 的列數釘在 keys 上」。釘住列數的是粒度閘（B10）：它比對每個 split 的 keys 與 model_input 列數是否相等。它不涵蓋 val／test，正是因為丟組放在建表之後：丟過組的 model_input，列數本來就對不上 keys，沒辦法配對。把不對稱拿掉，這個理由就不見了。

後果：val／test 的 join 如果因為右表有重複的鍵，把列數放大了，沒有任何東西擋得住。而 test 的指標就是對外報告的數字。

### 今天的樣子

三個丟組 node 函式（`filter_train_keys`、`filter_val_keys`、`filter_test_keys`；train 那個註冊成 train 與 train_dev 兩個 node）都在 keys 上、build 之前丟，共用同一個機制把抽中的組接回 keys。`filter_val_keys` 的 docstring 寫明了這件事：

```python
"""...
**On the keys, before the build, as train does it** (ADR-0029 decision 4).
...
"""
# Decision — the keys keep their own rows and gain only the weight: ...
kept = keep_keys_of_drawn_groups(
    keys, drawn, group_cols, weight_col=weight_col,
)
```

所以粒度閘現在四個 split 都能配對。

**站得住的不對稱長這樣**：val 抽樣時整個 entity 一起抽，train 逐列抽（`select_val_keys`）：

```python
# Decision — when val is sampled, it is sampled per *entity*, never per row:
#   mAP is computed over a query group, so a group must keep all of its
#   candidates or the metric answers a different question.
```

假設 train 也整個 entity 一起抽，這個理由還在：mAP 照樣是按 query group 算。所以它算數。

### 和規則 5 的關係

兩條不衝突，擋的東西不同：

- 規則 5 擋的是：題目重疊、答案各異的決策，被包成一個帶旗標的 helper。
- 這條擋的是：答案本來該相同，卻因為各自長出來而不同。

### 為什麼不是「每個 split 各自選最適合的做法」

各自選的時候，每個選擇單獨看都說得通。問題在整體：讀者看到一個 split 的做法，推不出另外三個；同一個資料問題在各條路徑上結果不同；而且哪一條出事，比對的檢查也可能跟著照不到（上面那個例子）。

**誰擋得住**：沒有機械檢查。粒度閘現在四個 split 都配對，擋得住「join 放大列數」這一種後果，但擋不住不對稱本身。

## 17. 資料閘的 node 只寫「查什麼」與「交給誰判斷」；收集事實與組報告進 `steps/`

資料閘是只做檢查、不產出資料的 node。這條管的是規則 11 那張表第二列的閘：判斷寫在 `core/consistency.py` 的 B 系列不變量。dataset 今天有三個：`validate_data_consistency`（設定與來源表的矛盾）、`validate_numeric_precision`（數值精度閘，B8）、`validate_model_input_grain`（粒度閘，B10）。node 在執行期檢查自己算出來的東西（例如 inference 的 `validate_predictions` 檢查預測表），是規則 11 第三列，不歸這條管。

這條是規則 4（決策留在 node body）與規則 11（驗證放哪）套在資料閘上的結果。node body 寫這幾件事：

1. **這次要查哪些表、哪些月份。** 這是決策。
2. **把收集到的事實交給 `core/consistency.py` 的 predicate。** 什麼算失敗、每一條失敗的訊息（包括帶不變量代號的那些），由 predicate 產出。node 只給這一批訊息一行總標題，交給 `collect_all_message` 排成一則。
3. **這個閘自己的政策。** 例如數值精度閘：設定是 `block` 就中止，是 `truncate` 就只警告。政策決定「找到問題之後停不停」，是決策，留在 node。
4. **前置檢查。** 例如 `require_months_in`：這次要讀的月份在不在。照規則 11 標明種類。

進 `steps/` 的是機制：讀 parquet 檔尾、找出這次寫了哪些檔、Spark 聚合、組報告、把報告排成 log 行。每個函式具名、只做一件事。

### 不照做會長成這樣

ADR-0029 動工前的 `validate_data_consistency`（`2e99c573`），機制寫成 node 裡的巢狀函式：

```python
def _raise_if_any(errors: list[str]) -> None:
    if errors:
        raise DataConsistencyError(
            "Data consistency check failed ("
            ...

def _item_combinations(df: DataFrame) -> list[tuple[tuple, object]]:
    ...
    rows = (
        df.filter(F.col(time_col).isin(windows))
        .select(*item_sources, *combined)
        .distinct()
        .collect()
    )
```

讀者要逐行讀完巢狀函式，才知道這個閘查了什麼。藏在裡面的東西也沒人看見：`_item_combinations` 用的月份篩選是規則 14 那段說的舊寫法，要統一寫法時，得特地點名它才不會漏掉。

同一種形狀長歪了不只一次：數值精度閘、粒度閘各有一份「這次寫了哪些檔」（一份在 `steps/precision.py`，一份是 `nodes.py` 裡的私有 helper），兩份比月份的方法還不一樣，一份轉成日期比，一份用字串比；組報告的迴圈直接寫在 node body 裡，加候選層級特徵表時又被複製了一份。

### 今天的樣子

`validate_data_consistency` 查 item 值的那一段：

```python
# Decision — the item values judged are the ones in the dataset windows,
# the months some split will read; one distinct per table serves both B1
# and B15.
sample_pool_items = item_combinations_in_months(sample_pool, schema, windows)
label_items = item_combinations_in_months(label_table, schema, windows)
```

`validate_numeric_precision` 的結尾，政策留在 node：

```python
# Decision — the policy key is what turns a finding into a stop. `block`
# is the default because the failure it describes is silent everywhere
# else; `truncate` exists so an operator who has read the finding can accept
# the loss without editing the gate out.
if policy == "block":
    raise DataConsistencyError(message)
logger.warning("%s", message)
return report
```

「這次寫了哪些檔」只剩一套，在 `steps/footer_facts.py`，兩個閘共用；各閘自己的報告組裝各一個模組（`steps/precision.py`、`steps/model_input_grain.py`）。

### 為什麼不是另一種做法

- **「只修被點名的那幾個函式」**：同一種形狀會在下一個資料閘再長一次。上面就是：先長在數值精度閘，再長在粒度閘，再長在候選層級表那一段。
- **「node 裡只留查什麼和交給 predicate，其他全搬走」**：政策與前置檢查也會被搬進 helper。那正是規則 4 說的：決策漏進 helper，讀 node 的人看不到「找到問題之後會不會停」。

**誰擋得住**：沒有機械檢查。

## 18. 程式改了 dataset 落地的內容、某個部署的設定卻沒動時，把 `DATASET_ARTIFACT_FORMAT_VERSION` 加 1

dataset 的版本 ID（`base_dataset_version`）是拿設定算出來的雜湊。程式碼改了，它不會變。

先定兩個詞：

- **部署**：一份設定（`conf/`）在生產上跑的一個實例。銀行示例與廣告示例各是一個部署。
- **落地內容**：版本 ID 底下、下游（training、evaluation、inference）會讀的表與檔，例如各 split 的 keys 與 model_input、`preprocessor.json`、`preprocessed_feature_table`。

**規則**：程式改了 dataset pipeline 的落地內容，而某個部署的設定沒動時，把 `core/versioning.py` 的 `DATASET_ARTIFACT_FORMAT_VERSION` 加 1。它是雜湊的輸入之一，加 1 就讓每個部署的版本 ID 都換一個。**要跟那個改動同一次發布上線。**

要問的不是「這個改動有沒有動到設定」，而是：**有沒有哪一份合法的設定與資料，改動前後寫出來的內容不一樣？** 有一份就要加。只在某種資料才會變的也算，例如只在 entity 有 NULL 時才不同。兩個容易漏的範圍：

- **新增一個設定鍵、預設值又跟舊行為不同**：這個改動「動了設定」，但沒寫那個鍵的部署，雜湊的輸入一個字都沒變，內容卻變了。
- **「dataset 的程式」包括它 import 的模組**，不只 `pipelines/dataset/`。例如多欄 item 的分隔字元 `ITEM_SEPARATOR` 在 `core/schema.py`；改它，keys、model_input 與 `preprocessor.json` 都會變。

### 不照做會怎樣

新舊程式寫的內容，掛在同一個版本 ID 底下。test 月份是累積的：每次只寫新的月份，舊的不重寫。所以同一張 `test_keys`、同一個版本 ID 底下，舊月份是舊程式寫的，新月份是新程式寫的，而且不報錯。

實例：ADR-0029 決定 4 讓 `test_keys` 變成「丟過無正例組的」。如果沒有加 1，同一個 ID 底下，舊月份沒丟過組、新月份丟過組。

### 今天的樣子

`core/versioning.py`：

```python
DATASET_ARTIFACT_FORMAT_VERSION: int = 1
```

它和設定一起進 `base_dataset_version` 的雜湊（鍵名 `dataset_artifact_format_version`）。什麼時候要加 1，那個模組的 docstring 用英文寫了同一條規則。

**代價要先講清楚**：加 1 之後，每個部署的版本 ID 都變，dataset 全部重建。模型版本（`model_version`）與 HPO 的搜尋 ID 都含這個 ID，所以每個部署都要重訓。重訓之前，「不重訓、只加評估月份」的流程（[`adding-an-eval-month.md`](../operations/user-guides/adding-an-eval-month.md)）對正在用的模型失效。所以加 1 的時機要跟重訓排在一起，一批改動只加一次。

### 為什麼不是「接受同一個 ID 下內容改變」

版本 ID 的用處是「同一個 ID 就是同一份資料」。讓它在部分部署失效，比多重建一次更難察覺：新舊月份混在一起時，沒有任何東西會報錯。

**誰擋得住**：沒人，只能人看。「這個改動會不會改變落地內容」是判斷題，實作規格與 PR 審查要逐張問。有人提過一個部分擋法（PR #472 的審查）：廣告示例的端到端腳本（`examples/ad/run_e2e.sh`）會把各產物的指紋（digest）和存好的基準（`baseline_digest.json`）比對；可以再加一條，dataset 那一層的指紋變了、base 版本號卻沒變，就擋下。沒有做。

---

# 這些規則大多沒人擋得住

**不要把「測試綠」讀成「符合這份文件」。**

`architecture-constraints.md` 的 S1（node 必須 `def` 在 `nodes.py`）與 S2（登記在冊的模組零 pyspark）**只管 `pipelines/dataset/`**，而且它們只擋得住**位置與純度**：

> 一個 12 行的轉手 node 加一個裝著四個決策的 helper，**完全滿足 S1**。

第一、二節十八條規則裡，只有規則 7（`RESUME_CONTRACTS`）、規則 8（S1／S2 ＋ `chunk_plans.py` 自己的測試）與規則 14（`test_month_scoped_reads.py` 釘住今天的讀取點）有**部分**機械檢查，其餘十五條全部靠開頭那張表 ＋ code review。這是已知的殘留風險，不是疏漏——[ADR-0008](../adr/0008-dataset-modules-split-by-role.md) §2 在裁決當下就記載了同一件事。

**為什麼不補上機械檢查**：這裡每一條規則都需要判斷「這個名字說出決策了嗎」「這個註解講的是為什麼嗎」，那是語意題。能機械化的部分（node 定義位置、模組純度、`steps/` 不外流）已經在 `architecture-constraints.md`，或評估後決定不加。要加新的一條之前，先確認它擋得住的是**真的會發生的失效**，而不是把一條判斷題寫成一個抓不到重點的正規表示式。

## 沒有測試的規則裡，規則 10 是唯一「幾乎」可機械檢查的一條

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
| `pipelines/training/` 的部分 node `def` 在 `recsys_tfb.diagnosis.model` 底下 | 8 | #222 重整 training 時**刻意不搬**（ADR-0014 決定 6）：搬進來會生出 7 個違反規則 3 的薄殼，而且這 7 個 node 未來要搬去 evaluation，現在搬等於白做。這也是 S1 無法一般化到所有 pipeline 的原因 |
| dataset 的 `split_train_keys` 一個 node 產出兩樣東西，分別給兩個下游 | 1（反方向） | 兩樣東西是同一個式子的正反兩面，拆開就只剩「兩處程式碼碰巧一致」。見下方 |
| dataset 的三個 build node 把組裝交給共用的 `build_model_input`，它裝了好幾個決策 | 3、5 | 四個 split 組 model_input 的決策完全相同，只差讀哪幾個月；拆成四份，一份先改了，四個 split 就不對稱了（規則 16）。見下方 |

要新增一筆到這張表，**必須先問使用者**（同 `architecture-constraints.md` 節三的例外登記規則）。後兩筆使用者 2026-09-25 同意登記：`split_train_keys` 見 ADR-0029 決定 14，`build_model_input` 見 #467 的留言。

## `split_train_keys`：一個 node，兩個輸出

它產出 `train_keys_unfiltered` 與 `train_dev_keys_unfiltered`，分別被 train 與 train_dev 的丟組 node 消費。照規則 1 的反方向，這是兩個 node 被塞在一起。

留著的理由在這四行：

```python
to_dev = unit_drawn_under_ratio(
    keys, split_cols, train_dev_ratio, seed, site="split_train_dev",
)
train_keys = keys.filter(~to_dev)
train_dev_keys = keys.filter(to_dev)
```

train_dev 取「抽到門檻以下」，train 取它的否定。同一個式子的正反兩面，所以每一列必定落在剛好一邊。拆成兩個 node，就得各算一次抽樣，這個保證就從「同一個式子的正反兩面」退化成「兩處程式碼碰巧一致」。哪天只改了一邊，就會有列同時在兩邊、或兩邊都不在，而且不報錯。

## `build_model_input`：一個 helper，好幾個決策

三個 build node 都很短：`build_train_model_input`（註冊成 train 與 train_dev 兩個 node）、`build_val_model_input`、`build_test_model_input`。各自只決定「這個 split 讀哪幾個月」（val／test 另有新 item 的警告），然後交給共用的 `build_model_input`。

`build_model_input` 不是 node，裡面有好幾個決策：兩個 LEFT join（沒有 label 的 key 算負例、沒有特徵的 key 留 NULL）、候選層級表的編碼、數值欄的 cast。照規則 3 的字面，這是「一個 helper 裝多個決策」；照規則 5 的字面，這幾個決策應該在每個 build 裡各寫一份。

**為什麼留**：

- 四個 split 組 model_input 的決策完全相同，只差讀哪幾個月。規則 5 要擋的是「題目重疊、答案各異」被包成一個帶旗標的 helper；這裡的答案一題都沒有不同，也沒有旗標。
- 拆成四份，就是四份一模一樣的決策各自維護。哪一份先改了，四個 split 就不對稱了，那正是規則 16 要擋的結果。
- 規則 3 擔心的是讀者得打開另一個檔、逐行反推。`build_model_input` 跟它的呼叫者在同一個檔（`nodes.py`），每個決策都掛著 `# Decision —`，讀的人不必反推。這是它跟規則 3 那個反例（在別的檔裡、名字沒說出任何一個決策的 `select_keys`）真正的差別。

這三點都成立，才是這筆例外。下一組 node 只要其中一點不成立（例如各 split 的答案有一題不同，像三個丟組 node 函式：train 不帶權重、val／test 帶），就照規則 5 各寫各的。

test 從 dataset 的增量化（`1d3f740c`）起就是這個形狀，val 從 #379 起，#460 讓 train／train_dev 也照做。

**沒選的做法**：讓 train 直接跑 `build_model_input`，「沒傳月份就讀 train 月份」當預設值。缺點是 val／test 忘了傳月份時不會報錯，只會悄悄讀到 train 的月份，每個 key 都接不到 label 與特徵。所以 `months` 是沒有預設值的 keyword 參數：

```python
def build_model_input(
    keys: DataFrame,
    ...
    candidate_feature_table: DataFrame | None = None,
    *,
    months: list,
) -> DataFrame:
```

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
