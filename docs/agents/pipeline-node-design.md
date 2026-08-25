# Pipeline node 設計判準

給在這個 repo 動 pipeline node 的 AI coding agent。本檔回答三個問題：**一個 node 的邊界該畫在哪**、**node 內部該長什麼樣**、**機制與驗證該放哪個檔**。

**與 `architecture-constraints.md` 的分工**：那份收「可機械檢查的約束」與「框架事實」，每條都有 `tests/test_core/test_architecture_constraints.py` 把關；本檔收的全部是 **code review 判準**，沒有一條有機械檢查（理由與後果見第四節）。兩份都要讀——那份擋得住位置，擋不住內容。

**這些判準的來源**：第一節與第二節的多數條目是 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 第二、三節在 dataset pipeline 上裁決的結果，本檔把它們一般化成跨 pipeline 的判準，並成為判準本身的唯一真實來源；ADR-0008 保留為 2026-08-05 那次決策的記錄與完整論證。少數條目標了「**本檔首次寫下**」，那是從既有程式碼讀出來、之前只活在某個模組 docstring 裡的慣例。

---

# 節一 · 通用原理

這一節不依賴本 repo 的任何設施，換一個框架也成立。專案形狀在節二。

## G1. node 的邊界 ＝ 一個會被落地或被觀察的產物

一個中間結果若**沒有 catalog 條目、沒有測試讀它、沒有 log 提到它**，把它切成獨立 node 只增加拓撲、不增加資訊——讀者多一個框要理解，卻沒有多一個地方可以下手觀察。這種邊界要合併。

反方向同樣成立：一個 node 若產出兩樣東西、被兩個不同的下游各自消費，那是兩個 node 被塞在一起，切開。

判定問題只有一句：**「這條邊界上的東西，出事的時候我能不能單獨把它撈出來看？」** 撈不出來就不是邊界。

> 這條的來源是 [ADR-0010](../adr/0010-inference-chunked-scoring-shape.md) 第四節——inference 曾把 `build_scoring_dataset` 與 `apply_preprocessor` 切成兩個 node，中間物沒有 catalog 條目、沒人讀，合併後少一個框而資訊不減。

## G2. node body ＝ 具名步驟的組合，每個步驟名就是一個決策

打開 node 就讀得完這個節點對資料做了什麼決定，**不必跳檔**。

這條的代價要講清楚：照做的 node 函式會比一般的長。這是刻意的——node 是**一串決策**，不是一個呼叫。行數本來就只是「這個函式只做一件事」的粗略代理，而這裡的「一件事」是「講完這條 pipeline 在這一段的 ML 故事」。

## G3. 一個 helper 至多承載一個決策

兩個以上必須拆成多個具名步驟，由 node body 依序呼叫。

失效形態是**一個 12 行的轉手 node 加一個裝著四個決策的 helper**：位置檢查全綠、node 短得漂亮，而讀者要理解這個節點做了什麼決定，得打開另一個檔、再逐行反推。

## G4. 決策與機制的分界線

- **決策 ＝ 會改變模型看到的資料的選擇。** 哪些列有資格、哪一列被留下、缺值算 0 還是算沒有、詞彙表從哪來。
- **機制 ＝ 語意定了之後，怎麼在引擎上算出來。** 用哪種 join、broadcast 還是 shuffle、常數與型別細節（未知類別的哨兵值、float32 的 cast 實作）。

**步驟的名字負責說出決策，值不必上浮。**

### 判定程序（機械檢查給不了，靠 code review 執行）

把 helper 的名字換成純機械的名字（`_encode_via_map_literal`、`_bucket_by_crc32`），重讀 node body：

- 仍講得完整個 ML 故事 → 過。
- 讀完會問「這一步到底決定了什麼」 → 決策漏進 helper 了，拆。

## G5. 決策重複，機制共用

兩個 node 做同樣四個決策時，**把四個決策各自寫在兩個 node 裡**，不要抽成一個共用 helper——那正是 G3 禁止的形狀，而重複才是讓每個 node 讀得懂的原因。

但**機制要共用**：log 的格式字串、欄名推導、join 寫法，兩份會漂移。

分界線與 G4 同一條：抽出去的東西若能換成機械名而不損失資訊，就該抽；抽出去之後 node 少講了一件事，就不該抽。

## G6. 名字要承載「選錯不會有錯誤訊息」的區別

兩個函式若差別只在一個會**靜默**改變結果的細節，名字必須把那個細節說出來。名字丟掉這個區別，兩者在呼叫端就看起來可以互換，而選錯的兩種後果都沒有錯誤訊息。

反面判準同樣重要：**名字不得指向不存在的東西**。指向已廢棄的雙軌制（backend 後綴）、指向不存在的 config 鍵、宣稱是「共用的 node」而裡面零個 node——讀者無法用檔名或函式名決定該打開哪一個，這比命名醜陋嚴重得多。

---

## G7. 落地還是 memory-only ＝ 接續成本的決定

G1 決定「這條邊界該不該存在」；這一條決定**邊界上的產物要不要進 catalog 落地**。兩個問題不同：G1 問「出事時撈不撈得出來」，這條問「**下次要從這裡接續，得付多少**」。

判準＝「是不是某個宣告接續點的必要輸入」×「重算貴不貴」：

- **便宜的留 memory-only**（view、handle、cheap transform）——切片的自動擴張會把生產者拉回來重跑，代價可接受。
- **貴的落地**（HPO 輸出）——否則 `--from-node finalize_model` 會一路補跑回 `tune_hyperparameters`，等於重訓一次。

**接續點品質是會被新增 node 默默破壞的契約。** `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS` 釘住各 pipeline（含 calibration-enabled training 變體）承諾的接續點與允許補跑集合。改壞會紅燈——要嘛給新產物補 catalog 條目，要嘛修改契約並在 PR 說明為什麼接受變貴。

改完跑 `python -m recsys_tfb <pipeline> --list-nodes` 肉眼確認各 node 的接續成本。切片機制本身見 [`../operations/user-guides/pipeline-slicing.md`](../operations/user-guides/pipeline-slicing.md)。

# 節二 · 在這個 repo 的形狀

## S-A. 檔案角色四分

```
pipelines/<name>/
  __init__.py     re-export create_pipeline
  pipeline.py     接線：DAG 拓撲，以及「為什麼這樣接」的註解
  nodes.py        node 函式 ← 這條 pipeline 的 ML 故事唯一的家
  <contract>.py   有本 pipeline 以外的 src 側消費者的模組（對外契約）
  steps/          只有 nodes.py 呼叫的機制模組，一模組一個 concern
```

**根層 vs `steps/` 的判準只有一句：src 側呼叫端是否全在本 pipeline 內。** 全在內 → `steps/`；有外部消費者 → 根層。**測試不算**——測試直接 import `steps/` 底下的東西不改變任何模組的位置，判準管的是生產端呼叫者，因為它買到的是「讀者看一次目錄列表就分得出對外契約與內部步驟」。

兩個容易誤用的推論：

- **「純模組（零 pyspark）」不是根層的判準。** `pipelines/dataset/steps/feature_columns.py` 是零 pyspark 的純模組而它在 `steps/` 裡。純度是**模組層級**的性質、跟位置無關，要釘就用 AST 測試釘那一個模組。現況兩個純模組用了兩種掛法：`month_plans.py` 由 S2 釘在 `tests/test_core/test_architecture_constraints.py`（直接掃描 ＋ 可達性，兩個測試缺一不可），`chunk_plans.py` 由自己的測試檔 `tests/test_pipelines/test_inference/test_chunk_plans.py` 釘（`test_no_pyspark_import` ＋ `test_no_project_import`）。
- **一條 pipeline 根層可以沒有任何契約模組。** 那代表它沒有 pipeline 開跑前的對外契約，是資訊不是缺陷。dataset 有 `month_plans.py` 在根層，是因為 `__main__.py` 在 pipeline 開跑前就要算好月份計畫再注進 catalog（[ADR-0007](../adr/0007-month-plans-travel-through-the-catalog.md)）；inference 的分塊計畫發生在 node 內，所以它根層只有 `pipeline.py` 與 `nodes.py`。

`steps/__init__.py` **不 re-export 任何東西**：`nodes.py` 逐模組 import，import 那一行就說出這個步驟來自哪個 concern。

## S-B. 步驟寫在註解與 helper 名上，不寫在 `log_step` 上

一個步驟在 node body 裡的樣子是：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test / calibration.
pool = restrict_to_months_or_all(sample_pool, time_col, train_months)
```

一行具名呼叫，上面一段說出決策**與選錯的後果**。不是 `with log_step(...)` 區塊、不是巢狀私有函式、不是註解分隔線。

## S-C. `log_step` 只包會觸發 Spark action 的區塊

**本檔首次寫下**（出處：`pipelines/dataset/nodes.py` 模組 docstring）。

Spark 的 join、filter、select、withColumn、cast 全都是 lazy：它們在微秒內回傳一份計畫，真正的計算發生在後面某個 action 裡（多半是 `catalog.save()`）。把 lazy 區塊包進 `log_step`，計時**保證**印出 ~0.00s，而那行讀起來跟「這一步很快」一模一樣——兩種零混在同一個事件名下，就沒有人分得出哪個零是哪個。

**留在 `log_step` 裡的，是會把資料收回 driver 或觸發寫入的區塊**：`collect()`、`count()`、`isEmpty()`、`toPandas()`、`save()`。

**這條不是效能潔癖，是觀測誠實。** node 的時間到底花在哪，是 Runner 的 `load`／`func`／`save` 三段拆分要回答的問題（`core/runner.py`），不是 node 內部的計時標籤。

> 順帶一提 F2：Runner 已經記了每個 node 的 `node_started`／`node_completed`／`node_failed`，**node 內不需要再寫「開始了／完成了」**。node 該記的是業務判斷——跳過了什麼、選了哪條分支、處理了幾列。

## S-D. 驗證歸屬：由「它需要看到什麼」決定

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

## S-E. 命名

- **底線前綴 ＝ 只有本模組呼叫。** `nodes.py`（或任何跨模組的呼叫端）呼叫得到的一律無底線——底線在 Python 的意思是「模組外不要用」，而 node body 逐行呼叫它就是模組外在用。
- **不得有 backend 後綴**（`_spark`／`_pandas`）。pandas／Spark 雙軌制已經只剩 Spark 一條，後綴指向不存在的東西（G6 反面）。純 driver 端的處理不算雙軌——那是機制，用 concern 命名。
- **守衛用 `require_*`**：`require_months_present`／`require_columns_present`／`require_item_is_a_feature`／`require_base_key_columns`。只警告不 raise 的用 `warn_*`。
- **模組用 concern 命名**，不用「helper」「shared」「common」這類指向不存在的東西的字。

## S-F. docstring 講「為什麼是這個答案」

**本檔首次寫下**（出處：`pipelines/dataset/` 全部模組的一致慣例，之前沒有寫下來過）。

函式簽章已經說了它做什麼。docstring 要說的是簽章說不出來的三件事：

1. **為什麼不是另一種做法**——「LEFT + COALESCE，never INNER：INNER 會靜默丟掉 miss，而 miss 是這個 frame 的大多數」。
2. **選錯的後果，以及會不會有錯誤訊息**——這是本 repo 最貫穿的主題。凡是「會靜默出錯」的，docstring 必須說出來。
3. **成本量級**（會觸發 action 的函式）——「一次 `distinct().collect()`，落到 driver 的東西以月份數為界、不以列數為界」。

**不要寫**「這個函式接受 X 回傳 Y」——那是簽章的工作。

---

# 節三 · 動手前的 checklist

改或加一個 node 之前，逐條打勾：

- [ ] **邊界**：這個 node 的產物撈得出來看嗎？（catalog 條目／測試／log 至少一個）中間物撈不出來 → 合併（G1）。
- [ ] **接續成本**：新產物該落地還是 memory-only？貴的落地、便宜的讓自動擴張補跑；改完跑 `--list-nodes` 確認，並看 `RESUME_CONTRACTS` 有沒有紅（G7）。
- [ ] **步驟**：node body 從上到下讀，每一行具名呼叫上面有一段 `# Decision —` 說出決策與選錯的後果（G2、S-B）。
- [ ] **判定程序**：把每個 helper 名字換成機械名，重讀 node body。還講得完 ML 故事嗎（G4）？
- [ ] **helper**：每個新 helper 只承載一個決策或一個機制（G3）。
- [ ] **重複**：跟隔壁 node 重複的決策，逐字重複寫；重複的機制，抽出去共用（G5）。
- [ ] **位置**：新模組的 src 側呼叫端全在本 pipeline 內 → `steps/`；有外部消費者 → 根層（S-A）。
- [ ] **`log_step`**：每個 `log_step` 區塊裡至少有一個 action（`collect`／`count`／`isEmpty`／`toPandas`／`save`）。沒有就拆掉（S-C）。
- [ ] **驗證**：新的 `raise` 是 config-only 嗎 → 進 `core/consistency.py`。留在 node 的，docstring 標了「前置檢查」還是「後置條件」嗎（S-D）？
- [ ] **命名**：無 backend 後綴；跨模組呼叫得到的無底線；守衛叫 `require_*`（S-E）。
- [ ] **docstring**：說了為什麼不是另一種做法、選錯會不會有錯誤訊息（S-F）？
- [ ] **繁中／英文**：程式碼註解與 docstring 一律英文（對齊既有全部模組）；`docs/` 一律繁體中文。

---

# 節四 · 這份判準沒有機械檢查

**誠實說明，不要把「測試綠」讀成「符合本檔」。**

`architecture-constraints.md` 的 S1（node 必須 `def` 在 `nodes.py`）與 S2（登記在冊的模組零 pyspark）**只管 `pipelines/dataset/`**，而且它們只擋得住**位置與純度**：

> 一個 12 行的轉手 node 加一個裝著四個決策的 helper，**完全滿足 S1**。

本檔第一、二節每一條都沒有機械檢查，全部靠節三的 checklist ＋ code review。這是已知的殘留風險，不是疏漏——[ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 第二節在裁決當下就記載了同一件事。

**為什麼不補上機械檢查**：本檔的每一條判準都需要判斷「這個名字說出決策了嗎」「這個註解講的是為什麼嗎」，那是語意題。能機械化的部分（node 定義位置、模組純度、`steps/` 不外流）已經在 `architecture-constraints.md`，或評估後決定不加。要加新的一條之前，先確認它擋得住的是**真的會發生的失效**，而不是把一條判斷題寫成一個抓不到重點的正規表示式。

**S-C 是本檔唯一「幾乎」可機械檢查的一條**：判斷一個呼叫是不是 Spark action 需要一份 action 名單，而名單會腐爛（新版 PySpark 加方法、專案 helper 內部藏 action）。所以它留在 review 層。

**但別把它查成一次 grep。** S-B 要求區塊裡是具名步驟，所以 action 多半**不在區塊那幾行裡**——它在那個步驟的函式體。在 `pipelines/inference/nodes.py` 上實測：8 個 `log_step` 區塊只有 3 個當場 grep 得到 action，其餘 5 個要追進 `steps/` 才看得到。**查法是「追一層」**：區塊裡每個具名呼叫，打開它的定義找 `collect`／`count`／`isEmpty`／`toPandas`／`save`；追到專案外的 helper 就看那個 helper 的 docstring 有沒有說它會 collect。一層都追不到 action ＝ 這個區塊該拆掉。

兩個追一層之後仍要人判斷的形態，都出現在 inference 上：**metastore 往返**（`existing_partition_values()`）不在 action 名單裡，但它是真的往返、時間隨分區數長，所以留著計時是對的；**空輸入**（全部續跑時 `populated_buckets` 拿到空月份清單）會讓一個正常有 action 的區塊那一次退化成零 action，那不是違例。兩者都要在該處寫明理由，否則下一個 reviewer 只會看到「grep 不到」。

---

# 節五 · 已知的界外違例

**看到這些不必以為判準是裝飾。** 它們各有登記過的理由：

| 違例 | 判準 | 為什麼還在 |
|---|---|---|
| `nodes.py` 從 `recsys_tfb.preprocessing` import 兩個底線函式（`_encode_categoricals`、`_cast_feature_floats_to_float32`） | S-E | 該模組被 dataset 與 inference 共用，rename 要同時改兩條 pipeline 的呼叫點。登記在 ADR-0008「這條 ADR 沒有解決的事」與 `deliberate-non-goals.md` |
| `pipelines/evaluation/nodes_spark.py` 帶 backend 後綴 | S-E | evaluation pipeline 尚未依本檔重整；`pipeline.py` 同時從 `nodes_spark.py` 與 `comparison_nodes.py` 取 node，還有一個動態 `importlib.import_module` |
| `pipelines/training/` 的部分 node `def` 在 `recsys_tfb.diagnosis.model` 底下 | S-A | 同上，training 尚未依本檔重整。這也是 S1 無法一般化到所有 pipeline 的原因 |

要新增一筆到這張表，**必須先問使用者**（同 `architecture-constraints.md` 節三的例外登記規則）。

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 可機械檢查的約束、框架事實、例外登記 | `docs/agents/architecture-constraints.md` |
| dataset 那次裁決的完整論證與當時的盤點 | [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) |
| node 邊界該不該合併的實例論證 | [ADR-0010](../adr/0010-inference-chunked-scoring-shape.md) 第四節 |
| 驗證分層（chunk 層 vs batch 層）的實例論證 | [ADR-0011](../adr/0011-inference-validation-two-layers.md) 第三節 |
| 不變量代號 A 系列／B 系列的意義 | `src/recsys_tfb/core/consistency.py` 模組 docstring |
| 程式碼現在長什麼樣 | `graphify-out/GRAPH_REPORT.md` |
