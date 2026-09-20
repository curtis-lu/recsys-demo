---
status: accepted
date: 2026-09-13
---

# evaluation pipeline 依角色切模組：node 講評估決策，只有這條 pipeline 用的機制進 `steps/`

> **要照著做，讀 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)；要知道 evaluation 為什麼這樣切，讀這份。**
>
> - **判準不在這裡。** 13 條形狀判準的唯一真實來源是 `pipeline-node-design.md`（本份稱「node 規則 N」），流程判準是 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)（「flow 規則 N」）。兩份各自從 1 編號，所以本份每次引用都帶前綴。本份只記「把那兩份套到 evaluation 時，判準給不出答案的岔路，以及各自選了哪一邊」。
> - **行為改動不在這裡。** 物化、job 收斂、NDCG 停算、監控模式的 node 清單是 [ADR-0018](0018-evaluation-materialize-at-producer.md) 的事，而且要**先**做完；本份的搬移排在最後一張，才拿得到「零行為改動」那兩樣免費證據（flow 規則 8）。
> - **稽核與複核不在這裡。** 逐條記錄在 [`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)（與本份同一批進 main）。本份只引用改變了決定的幾條。
> - **這份會被修正。** 它寫在 bug 修正那一輪之前，實作時發現某條站不住，照 flow 規則 2 回頭改這裡，不能只寫在 commit message。
> - **行號會腐爛**，所以只給檔名與函式名。少數行號標了核對日期（2026-09-13，main @ `6898179`）。

---

## 這份在解什麼問題

`src/recsys_tfb/pipelines/evaluation/` 是四條 modeling pipeline 裡最後一條還沒依形狀判準整理的。三個數字（2026-09-13 全部重數）：

```
                      # Decision   node 定義在幾個檔   backend 後綴
  dataset                  29              1                無
  inference                22              1                無
  training                 34              1（＋7 個在 diagnosis/model/，ADR-0014 決定 6）  無
  evaluation                0              2                有   ← 這裡
```

（`grep -rc '# Decision' src/recsys_tfb/pipelines/<name>/` 逐檔加總。）

**核心問題跟 training 那次一樣：不是 node 太長，是 node 裡沒有故事。** `prepare_eval_data` 143 行，是 `nodes_spark.py` 最長的 node，裡面做了五個決定（哪個 model_version、哪個月、label 怎麼補、rank 缺了怎麼辦、segment 從哪來），每個決定上面都有一段註解——但沒有一段用 `# Decision —` 標出來，讀者分不出哪些是決定、哪些是實作細節。

現況規模（2026-09-13 核對）：

| 檔案 | 行數 | 內容 |
|---|---|---|
| `nodes_spark.py` | 617 | 8 個 node ＋ `make_diagnosis_node` 工廠（產生各 registry 診斷的 node）＋ 3 個私有 helper |
| `comparison_nodes.py` | 161 | 5 個 node（compare 兩種模式用） |
| `pipeline.py` | 181 | 4 種模式（預設／`--post-training`／`--compare`／`--compare-only`） |
| `src/recsys_tfb/evaluation/` | 15 個 `.py`（含 2 個 `__init__.py`） | 指標、baseline、分群、報表、比較——**共用庫與只有這條 pipeline 用的機制混在同一層** |

三件登記在案的違例，都指向同一個根因：

1. **`nodes_spark.py` 帶 backend 後綴**——`pipeline-node-design.md`〈已登記的例外〉第一筆。pandas 雙軌制早就拆掉了，後綴指向不存在的東西（node 規則 6 的反面、規則 12）。
2. **`comparison_nodes.py` 不符合架構稽核的 `nodes*.py` glob**——這正是 #163 那個盲點。稽核以為 evaluation 的 node 只在一個檔。
3. **`pipeline.py` 同時從兩個檔取 node**，還有一個動態 `importlib.import_module`（那一個是合法的：它組的是 `recsys_tfb.diagnosis.metric.{name}`，不指向 `steps/`，`architecture-constraints.md` 已登記）。

另外還有沒登記、但判準管得到的：

- **7 處 `raise` 沒標種類**（node 規則 11 要求標明是前置檢查還是後置條件）：`prepare_eval_data` 3 處、`compute_metric_ci` 1 處、`make_diagnosis_node` 2 處、`validate_enriched_eval_predictions_present` 1 處。
- **13 處函式體內 import**（`nodes_spark.py`；其中 10 處在 node 函式體內、3 處在私有 helper 內），例如 `compute_metrics` 裡才 `from recsys_tfb.evaluation.metrics_spark import compute_all_metrics`。ADR-0014 在 training 收了 21 處到模組層。
- **零個 `log_step`**——node 規則 10 沒有東西要查，順便記下：node 的時間拆分靠 Runner 的 load／func／save 三段，不靠 node 內計時。

---

## 重構後長什麼樣

這是本份唯一的品味題，兩個方案並列，**建議 α，信心中等**。node 規則 8 只管 `pipelines/<name>/` 底下「根層 vs `steps/`」，對 `src/recsys_tfb/evaluation/` 這個第三個位置沒有明文；兩案都不違規，差的是讀者看一次目錄列表能不能分出「對外契約」與「內部步驟」。

### 呼叫端事實（兩案共同的前提，2026-09-13 核對）

`src/recsys_tfb/evaluation/` 的 13 個非 `__init__` 模組，依「pipeline 以外的 src 側有沒有人 import」分兩堆：

| 有外部呼叫端（**必須留在原地**） | 誰在用 |
|---|---|
| `metrics.py` | `training/steps/hpo_scoring.py`、`diagnosis/metric/*` 三個模組、5 支 `scripts/` |
| `metrics_spark.py` | `training/nodes.py`（測試集評分走同一個 `compute_all_metrics`） |
| `report_builder.py` | `scripts/render_diagnosis.py`、`scripts/migrate_evaluation_results_keys.py` |
| `report.py`、`compare.py`、`diagnostics_spark.py`、`distributions.py` | 被 `report_builder.py` import——它是共用庫，所以它的依賴也是。S3 禁止 pipeline 以外的模組 import `steps/`，這四個搬進去 `report_builder` 就違規 |
| `comparison/report.py` | 只有 `comparison_nodes.py` 用，但它是報表組裝器、import `report_builder`，跟後者同類 |

| 只有這條 pipeline 用（**候選搬進 `steps/`**） | 唯一呼叫端 |
|---|---|
| `baselines.py`（**不搬**，見本節末的更正） | `nodes_spark.py::compute_baseline_metrics` |
| `segments.py` | `nodes_spark.py::prepare_eval_data` |
| `comparison/sources.py` | `comparison_nodes.py::load_compare_predictions`（`core/consistency.py` 有一段註解說 A11 是它 `MODEL_VERSION_SOURCES` 的鏡像——是註解，不是 import；搬了要改註解） |
| `comparison/restrict.py` ＋ `comparison/alignment.py` | `comparison_nodes.py::restrict_to_common`（restrict 內部 import alignment） |
| `config_fingerprint.py`（本表寫的時候還不存在，見本節末的更正） | `nodes_spark.py`（import 四個公開名） |

**更正（實作時發現，2026-09-14，#365）**：上面兩張表寫在 Phase 0／1 之前，實作時有兩處對不上。

- **`baselines.py` 不搬，留在 `evaluation/`。** Phase 0 之後 `report_builder.py` 在模組層 import `baselines.resolve_lookback_months`（ADR-0020 bug 1 把 lookback 的預設值收成一份）。`report_builder` 是共用庫，`baselines` 搬進 `steps/` 就違反 S3，所以照第一張表自己的判準，它屬於「被共用庫拉住」那一堆。
  - 沒走的路：照 #344 拆 `segment_keys.py` 的做法，把純設定讀取的 `resolve_lookback_months`（14 行）拆到留在 `evaluation/` 的新模組，其餘 Spark 部分照搬進 `steps/`。代價是為 14 行多一個模組、多一個要解釋的檔。
  - 走的路的代價：`evaluation/` 裡留下一個主要讀者是 `nodes.py` 的 Spark 模組，本份說的「混合層」沒有完全消失。〈方案 α〉「`evaluation/` 剩下的每一個檔都有 pipeline 以外的讀者」這句，對 `baselines.py` 只在「被 `report_builder` 拉住」的意義上成立。信心中等。
- **`config_fingerprint.py` 搬進 `steps/`（使用者 2026-09-14 決定）。** 它是 Phase 0 才加的，所以本表沒分到它。`src/` 裡唯一 import 它的是 node 模組；`__main__.py` 與 `diagnosis/metric/contract.py` 只在註解裡提到它的 `COMPUTED_KEYS`。照本份的規則（只有這條 pipeline 用的機制進 `steps/`）搬。

### 方案 α（建議）：只有這條 pipeline 用的機制搬進 `steps/`

```
src/recsys_tfb/pipelines/evaluation/
  __init__.py              re-export create_pipeline
  pipeline.py              四種模式的接線 ＋「為什麼這樣接」的註解
  nodes.py                 ← nodes_spark.py ＋ comparison_nodes.py 合併：全部 node、make_diagnosis_node 工廠、
                             ADR-0018 新增的 no_diagnosis_pages
  steps/
    __init__.py            只有 docstring（S3 擋 re-export）
    snap_date_scope.py     restrict_to_eval_snap_date（ADR-0018 決定 1 新增的機制）
    config_fingerprint.py  ← evaluation/config_fingerprint.py（#365 加入，見〈呼叫端事實〉的更正）
    segments.py            ← evaluation/segments.py
    compare_sources.py     ← evaluation/comparison/sources.py
    compare_universe.py    ← evaluation/comparison/alignment.py ＋ restrict.py

src/recsys_tfb/evaluation/   共用庫：有 training／diagnosis／scripts 呼叫端，或被它們的依賴拉住
  metrics.py  metrics_spark.py  report_builder.py  report.py  compare.py
  diagnostics_spark.py  distributions.py   （決定 5 原寫的 report_tables.py 不開，見決定 5 的更正）
  baselines.py      （原列在 steps/，留在這裡，見〈呼叫端事實〉的更正）
  segment_keys.py   （#344 從 segments.py 拆出；report_builder、metrics_spark 在用）
  comparison/report.py
```

**買到的**：目錄列表就是 node 規則 8 的答案——`evaluation/` 剩下的每一個檔都有 pipeline 以外的讀者，`steps/` 裡的每一個檔都只有 `nodes.py` 在用。`architecture-constraints.md` S3 的掃描是 `SRC.rglob("*.py")` ＋ `PIPELINES.glob("*/steps/__init__.py")`，新目錄**自動納入**，不用改稽核（flow 規則 7 的盲點在這裡不成立，但實作 PR 仍要用 `from .steps.x import y` 的形式 mutation 一次，flow 規則 6）。

**付出的**：4 個模組搬家、`tests/test_evaluation/` 對應 5 個測試檔改 import 路徑（測試 import `steps/` 合法，node 規則 8 明說「測試不算」）、`consistency.py` 一段註解、`LITERAL_COLUMN_EXCEPTIONS` 登記表的路徑重指（見〈實作前的閘門〉）、graphify 重建。純搬移可以用 AST 逐函式比對證明（flow 規則 8 的免費證據）。

### 方案 β：既有模組一個都不搬

`nodes_spark.py` ＋ `comparison_nodes.py` → `nodes.py`；`steps/` 只裝**新**的機制（`snap_date_scope.py`，以及從 node body 抽出來的東西，目前沒有）；`src/recsys_tfb/evaluation/` 維持現狀。

**買到的**：零搬家、零 import 路徑變更、測試一行不動；AST 比對只剩合併那一份；`LITERAL_COLUMN_EXCEPTIONS` 只需重指合併進 `nodes.py` 的三筆（`nodes_spark.py` 兩筆、`comparison_nodes.py` 一筆），`comparison/sources.py` 那筆不動。
**付出的**：`evaluation/` 繼續是共用庫與 pipeline 私有機制的混合層。下一個要加機制的人分不出該放哪——`baselines.py` 與 `metrics_spark.py` 並排，看起來同級，實際一個只有一個讀者、一個被 training 拉住。

### 為什麼建議 α

node 規則 8 存在的理由是「讀者看一次目錄列表就分得出對外契約與內部步驟」。β 把這條規則的效益留在 `pipelines/evaluation/` 這一層，但那一層在 β 底下只有 `snap_date_scope.py` 一個 step——訊號等於沒有。α 的代價是機械搬移，可以用 AST 比對證明，屬於 flow 規則 8 表格裡「秒級」那一格。

**信心中等的原因**：ADR-0014 在 training 遇到同型問題（7 個 diagnosis node 在 `diagnosis/model/`）時選了**不搬**（決定 6），理由是搬會製造薄殼、而且那些 node 未來要搬去別處。evaluation 這四個模組不會製造薄殼（它們本來就是機制，不是 node），也沒有「未來要搬去別處」的計畫，所以 ADR-0014 的理由在這裡不成立——但那是推論，不是實證。**衝突時以本 ADR 為準；實作時發現 α 站不住，改這裡。**

**實作後審查提出的風險（2026-09-14，#365；記下來，本張沒處理）**：

- **import `steps/` 模組會帶進整條 pipeline。** `pipelines/evaluation/__init__.py` re-export `create_pipeline`，所以 import `steps/config_fingerprint.py` 會先載入 `pipeline.py`，連帶載入 pyspark 與 mlflow；它在 `evaluation/` 時不會。它的 docstring 原本寫「離線工具 import 它不必拖進 Spark」，這句已經改掉。目前 `src/` 與 `scripts/` 沒有這種讀者；四條 pipeline 的 `steps/` 都有同一個性質。
- **`config_fingerprint.py` 比較像契約，不像內部步驟。** `__main__.py` 為了讓 `COMPUTED_KEYS` 讀得到而把 `post_training` 放進 `parameters`；`diagnosis/metric/contract.py` 的 `EXTRA_CONFIG_KEYS` 是指紋的另一半。兩者今天都不 import 它。哪天 pipeline 以外有人要 import `COMPUTED_KEYS`，S3 會逼它搬回 `evaluation/`。
- **node 與 step 同名。** `nodes.py::restrict_to_common` 先留評估月份、第三個回傳值是 coverage dict；它呼叫的 `steps/compare_universe.py::restrict_to_common`（以 `_restrict` 別名 import）不留月份、第三個回傳值是 `CommonUniverse`。兩者參數個數相同，讀者從名字分不出來，import 錯一個不一定當場報錯。改名會動 AST，不在純結構票裡做。

---

# 實作前的閘門

依 `pipeline-refactor-process.md`：

1. **ADR-0018 的五個決定全部進 main**，且 bug 那一輪做完。本份的搬移 PR 必須能宣稱「零行為改動」。
2. **baseline 先建**：同一份 `data/` 跑 `tests/test_evaluation/` 與 `tests/test_pipelines/test_evaluation/`，記下既有 fail（`known-pitfalls.md` §5）。
3. **AST 逐函式比對腳本備好**：搬移前後每個 `def`／`class` 的 AST dump 逐字相同（允許的差異只有 import 行與新增的 `# Decision —` 註解）。#173、#174、#198 都是這樣證明的。
4. **`conf/` 對本 PR 的 base commit byte-identical**——本份不動任何設定（決定 6 那一步改 `conf/`，所以它不在這張 PR，見該決定）。基準是「本 PR 的 base」而不是 main，因為 ADR-0018 的 Phase 1 已經改過 `conf/`。`pipeline.py` 的 diff 只准是 import 路徑那幾行，`Node(...)` 建構逐字不變——這句只管 `pipeline.py`；閘門 5 那張登記表在 `tests/` 底下，它的變更不受這句限制。
5. **`LITERAL_COLUMN_EXCEPTIONS` 的變更先拿給使用者簽**。那張表在 `tests/test_core/test_architecture_constraints.py`，以 `(路徑, 函式名)` 登記允許出現 `snap_date` 字面值的地方，表頭寫「Adding one needs the user's sign-off」。本份與 ADR-0018 一起會動到它：α 底下既有 4 筆路徑作廢要重指（`nodes_spark.py` 兩筆、`comparison_nodes.py` 一筆、`comparison/sources.py` 一筆），ADR-0018 新增 1 筆（`steps/snap_date_scope.py`）。**重指不是新增，但同一張表、同一個簽核規則，一次拿去簽。**

**更正（實作時發現，2026-09-14，#365）**：

- **閘門 5 的「既有 4 筆」是 3 筆。** `comparison_nodes.py` 那筆 #352 就刪了（它改成跟 `steps/snap_date_scope.py::eval_snap_date` 拿評估月份）。重指的是 `nodes_spark.py` 兩筆（→ `nodes.py`）與 `comparison/sources.py` 一筆（→ `steps/compare_sources.py`），使用者 2026-09-14 核准。〈方案 β〉那段「`comparison_nodes.py` 一筆」同樣不成立。
- **閘門 3 的 AST 比對擴成模組 body 的每一個 top-level 節點**，含模組層賦值與常數，不只 `def`／`class`：只比函式的話，常數少抄一個元素照樣放行。允許的差異是 import 陳述（含從函式體內移出的）、docstring、註解；另加一道 import 綁定比對，確認移出來的 import 綁到同一個 dotted 目標（比的是字串，不是物件）。`# Decision —` 是註解，本來就不進 AST。
- **測試檔的位置跟〈方案 α〉的「付出的」寫的不一樣。** 那段寫測試留在 `tests/test_evaluation/`、只改 import 路徑。實作照 dataset／inference 搬完 `steps/` 後的慣例，把 step 測試 `git mv` 到 `tests/test_pipelines/test_evaluation/`：`test_segments.py`、`test_config_fingerprint.py` 同名搬過去，`test_comparison_sources.py` 改名 `test_compare_sources.py`，`test_comparison_alignment.py` ＋ `test_comparison_restrict.py` 併成 `test_compare_universe.py`；node 測試 `test_nodes_spark.py` 改名 `test_nodes.py`。

---

# 有取捨的決定

## 決定 1：`comparison_nodes.py` 併進 `nodes.py`，不留第二個 node 檔

node 規則 8：`nodes.py` 是「這條 pipeline 的 ML 故事唯一的家」。compare 的五個 node 是同一條 pipeline 的三分之一，分開放的唯一理由是歷史。併進去之後 #163 那個 `nodes*.py` glob 盲點對 evaluation 自然消失——**但 #163 本身不關**，它是通用問題，關不關由使用者裁。

**否決的替代**：留兩個檔、把第二個改名成 `nodes_compare.py` 讓 glob 抓到。這是「靠改檔名迴避稽核」，而不放寬 glob 是使用者 2026-09-19 的裁決（#163，見 `docs/agents/architecture-constraints.md` A1）——改檔名是繞過那個裁決，不是遵守它。

## 決定 2：`make_diagnosis_node` 工廠留在 `nodes.py`

各 registry 診斷的 node 由 `make_diagnosis_node(name)` 動態產生（回傳閉包 `_run`），`pipeline.py` 迭代 `DIAGNOSES` 各建一個 `Node`。它不是 `def` 在 `nodes.py` 頂層的 node 函式，架構稽核的 AST 掃描看不到這些診斷 node 的「定義」。

**留著，不展開成手寫 node。** `pipeline.py` 的註解已經說明理由：手寫會產生 N 份只差模組名的複製品，各自漂移。這跟 S1「node 必須 `def` 在 `nodes.py`」的精神一致（工廠就在那個檔），只是形式上稽核抓不到。**在 `nodes.py` 模組 docstring 記一句**，讓下一個看稽核報告的人知道診斷 node 在哪。

## 決定 3：`prepare_eval_data` 不切，五個決定各標一段 `# Decision —`

143 行、五個決定，每個決定的機制都已經是具名呼叫（`rank_within_query`、`join_segment_sources`、`fillna`）。切成多個 node 的話中間物沒有 catalog 條目、沒人單獨讀——node 規則 1 說那不是邊界。

改的只有：五段既有註解改成 `# Decision —` 開頭、每段補「選錯的後果」（node 規則 9、13）；加 ADR-0018 決定 1 之後它的 `outputs` 是 Hive 表這件事的說明。

三處 `raise` 標種類：`model_version` 缺 → **前置檢查**（CLI 沒解析）；`evaluation.snap_date` 空 → **前置檢查**（設定沒給）；該月零列 → **前置檢查**（上游沒產出這個月）。

## 決定 4：函式體內的 13 處 import，能收到模組層的收，收不了的寫理由

ADR-0014 在 training 收了 21 處。evaluation 的 13 處全在 `nodes_spark.py`，而它是葉節點（只有 `pipeline.py` 在 `create_pipeline()` 裡延後 import 它），循環 import 的可能性低，多半是習慣。但同一條路上 `report_builder.py` 另有兩處函式內 import `diagnosis.metric.contract.DIAGNOSES`，而 `diagnosis/metric/*` 反過來 import `evaluation.metrics`——那兩處才可能真的是繞循環。**先跑一次 `python -c "import recsys_tfb.pipelines.evaluation.nodes"` 把每一處提到模組層試，炸的那幾處留在函式內、上面寫一行「循環：A → B → A」。** 沒炸的全部收。

不寫成「全部收」的理由：ADR-0014 決定 6 的教訓——沒查清楚就搬，會製造薄殼或循環，然後被下一個 PR 改回去。

**實作註（2026-09-14，#365）**：13 處全部收到模組層，沒有一處炸。除了 `python -c "import recsys_tfb.pipelines.evaluation.nodes"`，也逐一先 import 每個被收的模組、再 import `nodes`，一樣沒炸：`diagnosis/metric/*` 反過來 import 的是 `evaluation.metrics`，不是 node 模組，所以不成環。收了之後有 7 處測試的 `patch` 目標原本打在來源模組（`recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample` 等），靠的是函式內 import 的晚綁定；改成打 `recsys_tfb.pipelines.evaluation.nodes.<name>`，逐處改回舊目標的結果記在 PR 說明。`report_builder.py` 的兩處函式內 import 不在本張範圍。

## 決定 5：報表層的兩把小刀，不做兩層渲染器

稽核 C（兩份渲染器、兩份數字格式器已漂移）與 D（比較報表 import 私有函式）都成立，但複核發現稽核的建議形狀踩不下去：

- **C 真正擋住合併的不是 `-0`，是 HTML 跳脫規則相反**。`report/pages.py` 對 `section.description` 做 `_escape()`；`evaluation/report.py` 不做。而 `report_builder.py::build_diagnosis_links_section` **依賴不跳脫**——它把 `<a href="diagnosis/index.html">` 放在 `description` 裡。照稽核的「Page ＋ 殼兩層」合併而沒先處理這條，主報表的診斷入口會退化成字面文字，沒有錯誤訊息，現有測試也不會紅（它們斷言的是 `_render_section_extras` 的 formula／bullets，不是 description 的 HTML）。

決定只做可以單獨驗收的一半：

- **C-(a)**：`_fmt_cell` 收成一份、以 `evaluation/report.py` 的版本為準（它多修了 `-0`，兩份在其他輸入上逐行比對輸出相同），放 `report/fmt.py` 並改成公開名 `fmt_cell`——`report/__init__.py` 定義 `fmt` 是「按量的語意決定格式的數字格式器」，per-cell 格式器屬於它。`_render_table` 產的是 HTML，**不放 `fmt.py`**（放了那個模組的 docstring 當場變假，正是決定 8 在修的同一種病），留在 `report/pages.py` 改成公開名 `render_table`，`evaluation/report.py` import 它。**兩個都要改成公開名**，否則 `evaluation/report.py` 會生出一條跟 C-(b) 要消滅的同形狀跨套件私有 import。
- **C-(b)**：`_render_section_extras` 從 `report/pages.py` 提成公開名字，消掉跨套件私有 import（node 規則 12）。
- **跳脫契約原樣保留**：`evaluation/report.py` 的 `description` 繼續不跳脫，並在該處寫一行「為什麼」（`build_diagnosis_links_section` 靠它）。兩層渲染器不做。
- **D-1**：`comparison/report.py` 從 `report_builder` import 的底線函式裡，**真的要搬的是三個**：`_per_item_metric_compare_table`、`_resolve_display_k`、`_n_items`，提到公開的 `evaluation/report_tables.py`。另外兩個不搬：`_visible_metric_keys` 由 ADR-0018 決定 4 刪除（NDCG 停算後沒有東西要藏），`_k_to_lookup` 是死 import、刪掉。放 `evaluation/` 不放 `report/`，因為它們帶著 metrics dict 的鍵名知識，屬於 evaluation 不屬於中性呈現層（node 規則 8：兩個呼叫端都在 evaluation 內）。
- **D-2 不做**：`report.display.guardrail_recall_k` 在主報表是**死的**（`report_builder.py` 讀了它算出 `rec_ks`，之後沒有任何使用點），活的只有比較報表那一邊（`comparison/report.py` 讀同一個鍵，鍵缺席時預設 `[1,3,5]`）。「統一預設值」等於悄悄改比較報表的欄數，而且沒有人提出需求（2026-09-13 更正：原寫「撞報表凍結」，凍結已依 [ADR-0020](0020-evaluation-bug-round-intended-behaviours.md) 解除，理由改成沒有需求）。所以：**鍵留著**（比較報表在讀），主報表那段死碼（`rec_ks`）刪掉，`docs/pipelines/evaluation.md` 寫明這個鍵只影響比較報表。

**更正（實作時發現，2026-09-14，#364）**：上面有兩處照寫會出問題，實作改成下面這樣。

- **C-(a) 的格式器不放 `report/fmt.py`、不改公開名**，留在 `report/pages.py` 當模組私有的 `_fmt_cell`（連同它用的 `_fmt_no_sci`）。統一之後，它在 `src/` 唯一的呼叫端是同模組的 `render_table`；`evaluation/report.py` 改 import `render_table`，不再直接碰格式器。底線＝只有本模組呼叫（node 規則 12），所以不需要公開名——「兩個都要改成公開名」那句的前提是 `evaluation/report.py` 要 import 格式器，這個前提不成立。放進 `fmt.py` 還會讓那個模組的 docstring 當場變假：它寫「按量的語意決定格式」與「壞值一律回空字串」，而 `_fmt_cell` 按 Python 型別分派，`inf` 回 `'inf'`、字串與 list 原樣回傳。這跟上面否決把 `render_table` 放進 `fmt.py` 是同一把尺。
- **D-1 不開 `evaluation/report_tables.py`，改成在 `report_builder.py` 原地改公開名**：`per_item_metric_compare_table`、`resolve_display_k`、`count_items`。三個函式依賴 `report_builder.py` 裡另外 7 個名字，而 `report_builder.py` 自己留下的函式又反過來用其中 4 個（`_k_to_lookup`、`_MACRO_LABEL`、`_dataset_overview`、`_k_exceeds_item_count`）。開新模組、閉包一起搬，`report_builder.py` 就得回頭 import 這 4 個私有名，正是 D-1 要消滅的同一種病。上面只解釋了「放 `evaluation/` 不放 `report/`」，沒解釋為什麼要新模組；`comparison/report.py` 本來就從 `report_builder` import 三個公開名，這條模組耦合已經存在、也被接受。
- **`_n_items` 的公開名是 `count_items`，不是 `n_items`**：`report_builder.py` 與 `comparison/report.py` 都有 `n_items = _n_items(metrics…)` 這種行，改成同名就變成 `n_items = n_items(…)`，Python 把 `n_items` 當區域變數，執行時 `UnboundLocalError`。
- **連帶影響〈收尾要 grep 的兩份清單〉**：`_fmt_cell` 不會歸零（它合法地留在 `report/pages.py`）；`_k_to_lookup` 與 `rec_ks` 也不歸零——`_k_to_lookup` 在 `report_builder.py` 還有十幾個使用點，`rec_ks` 在 `comparison/report.py` 是活的區域變數。只刪 `comparison/report.py` 的 `_k_to_lookup` 死 import 與 `report_builder.py` 的 `rec_ks` 死賦值。

## 決定 6：`report.sections` 的死開關刪掉，加一條**雙向**一致性不變量，常數住在 `core/`

稽核 E：`conf/base/parameters_evaluation.yaml` 宣告 8 個 `report.sections` 開關，程式用 `_section_on(parameters, name)` 讀的只有 `dataset_overview`、`primary_map`、`diagnostics`、`baseline` 四個；`guardrail_recall`／`per_item_attr`／`category`／`per_segment` 這四個名字沒有任何 `_section_on(…)` 在問（`category`／`per_segment` 這兩個字串作為 metrics dict 的鍵仍大量在用，**別誤刪**），`display.recall_colorscale` 全樹零命中，而 `docs/pipelines/evaluation.md` 的排錯表還叫使用者去關 `per_segment`。

**審查補了稽核漏掉的第五個**：`_section_on` 有五個呼叫點，第五個是 `diagnosis_links`，而它**不在** YAML 宣告的 8 個裡；`_section_on` 對缺席的鍵回 `True`，所以主報表的診斷入口永遠開著、使用者沒有關掉它的設定。這是跟 E 同一家族、方向相反的漂移。

**複核否決了稽核的修法**：「`_section_on` 對未知鍵 fail loud」擋不住這個病——它只在程式問某個鍵時執行，永遠看不到「conf 裡有一個沒人問的鍵」。

決定：**刪鍵 ＋ 補宣告 ＋ 雙向不變量**。

- 刪四個死開關與 `display.recall_colorscale`（`display.guardrail_recall_k` **不刪**，比較報表在讀，見決定 5）；**補宣告 `diagnosis_links`**（預設 `true`，行為不變）；改 `docs/pipelines/evaluation.md` §3.5 與 §8 那一列；改 `tests/test_evaluation/test_parameters_evaluation_yaml.py`。
- **程式讀取的集合搬成一個常數，住在 `core/consistency.py`**（暫名 `EVALUATION_REPORT_SECTIONS`，一個 frozenset），`report_builder.py::_section_on` 從 `core` import 它、名字不在集合裡就 raise（前置檢查）。方向是 evaluation → core，跟 repo 現有的依賴方向一致；`core/` **不** import `report_builder`（`consistency.py` 自己寫著 core 不得對上層有 import-time 依賴，A15 對 `diagnosis` 用的是函式內延遲 import，而 `report_builder` 拖著 pandas 與 plotly 鏈，連延遲 import 都嫌重）。
- 新 predicate（A 系列）：**`evaluation.report.sections` 宣告的鍵集合 ＝ `EVALUATION_REPORT_SECTIONS`**，雙向相等，不是單向包含——單向「宣告 ⊆ 讀取」正好放行 `diagnosis_links` 這種「讀了沒宣告」。`CLAUDE.md` 明文：新增一致性不變量必須在該模組加 predicate。
- 這一步**改 `conf/`**，所以它不在最後那張純結構 PR 裡，排在 ADR-0018 的 Phase 1。

單獨刪鍵不夠的理由：症狀掃掉了，下次再長出來沒人擋。

**實作註（2026-09-13，#351）**：不變量代號是 **A34**，predicate 是 `core/consistency.py::report_section_key_errors`，接在 evaluation 指令的入口（跟 A22 同一處、起 Spark 之前），不進 `validate_config_consistency`——那道閘每個指令都跑，而只有 evaluation 讀這些鍵，舊設定留著死開關不該擋住 dataset、training、inference（A24 的理由，#158）；`_section_on` 的前置檢查 raise `ValueError`。本份沒寫到的一個邊界這樣定：`evaluation.report.sections` 一個鍵都沒宣告（整塊不在、是 `null`、或是空的 `{}`）時不檢查。一個都不宣告是明顯的「全部用預設」，不是某個鍵悄悄漂走；只要宣告了任何一個鍵，就逐鍵雙向比對。另外補一條原始碼掃描測試（`tests/test_evaluation/test_report_builder.py::test_every_listed_section_is_read_by_the_report`）：常數裡的每個名字都要有一個 `_section_on` 呼叫在讀。少了它，刪掉某個 `_section_on` 呼叫、常數與 YAML 沒跟著刪，A34 與前置檢查都照樣綠，死開關就回來了。

## 決定 7：稽核 G 升格為 bug，本份只登記機制去重

`restrict_to_common`（`comparison_nodes.py`）自己做兩次 item collect ＋ 兩次 count ＋ 一次 intersect count，而 `comparison/alignment.py::common_universe` 內部**再 collect 一次**同樣的 item 集合。複核發現更重的事：兩處對「共同母體」的**定義不同**——node 用 `[time]+entity` 做 `intersect()`，`alignment.py` 用 `entity` 做 `left_semi`，而 `alignment.py` 自己的註解逐字說明為什麼 `intersect` 是錯的（NULL 的語意）。印進 `report_comparison.html` coverage 段的數字，跟實際被保留的母體不是同一個量。

**「數字對不上」那一半是 bug 14，進 bug 那一輪定行為。** 本份只登記機制那一半：`common_universe` 把它已經算出來的 `a_items`／`b_items` 一起回傳，node 不再自己 collect。定義要不要統一、統一成哪個，由 bug 那輪決定；在那之前至少把「兩處語意不同」寫進 node 註解。

**2026-09-13 更正（#345 已做完兩半）**：

- 機制去重：`common_universe` 回傳 `CommonUniverse`（`common_entities`、`common_items`、`a_items`、`b_items`），node 不再自己 collect。
- 定義統一：coverage 的 common 改數「裁切後兩側都還在的 query group」，不再用原始 frame 的 `intersect`（見 ADR-0020 bug 14 的實作註記）。

結構搬移那一輪不用再做這一條。

## 決定 8（附帶清掃）：`report/__init__.py` 的 docstring 改成真的

它說「目前唯一的消費者是 `evaluation/report.py`，`diagnosis/` 與 `report_builder.py` 都沒有 import 本套件」。實際 `report_builder.py` import `Page` 與 `write_pages`，各 registry 診斷全部 import `ScopeNote` 與 `figures`。同檔寫的「目標狀態」早就達成了。稽核 B、C 的推論都建立在「誰依賴 `report/`」這個判斷上，而最明顯的來源寫錯——這是 node 規則 6 反面的文件版。跟搬移同一張 PR 改；它不在原始需求裡，是搬移時順手對齊真實識別字。

---

# 照判準直接推出的（沒有取捨）

- **`nodes_spark.py` → `nodes.py`**；`pipeline-node-design.md`〈已登記的例外〉那一筆刪除（刪不用問，加才要問）。`architecture-constraints.md` 提到 `evaluation/nodes_spark.py` 的四處跟著改：「最長的五個 node」清單（該清單本身已過期，`validate_model_input_grain` 沒列進去）、`importlib.import_module` 六處清單、S6 登記表說明裡的兩列。
- **7 處 `raise` 標種類**（決定 3 標了 3 處；`compute_metric_ci` 的 `n_boot` 檢查 → 前置檢查；`make_diagnosis_node` 的 arity 與 `None` 檢查 → 前置檢查；B4 → 前置檢查）。ADR-0018 決定 1 新增的 `n_snap_dates == 1` → **後置條件**。
- **`steps/__init__.py` 只有 docstring**（S3 的 `test_steps_packages_re_export_nothing`）。
- **`nodes.py` 逐模組 import `steps/`**，import 那一行就說出步驟來自哪個 concern。
- **命名**：搬進 `steps/` 的模組用 concern 命名（`snap_date_scope`、`segments`、`compare_sources`、`compare_universe`、`config_fingerprint`；原寫的 `baselines` 不搬，見〈呼叫端事實〉的更正），不用「helper」「common」；跨模組呼叫得到的函式無底線（node 規則 12）。
- **graphify 重建**；`docs/diagrams/evaluation-pipeline.mmd` 若只改了檔名指涉就跟著改。

**更正（實作時發現，2026-09-14，#365）**：合併後 `nodes.py` 的 `raise` 是 12 處：上面列的 7 處加 `n_snap_dates == 1` 是 8 處，多出來的 4 處都在本份之後才加，而且 docstring 已經標了種類：`prepare_eval_data` 的 label 重複鍵檢查（ADR-0020 bug 10，前置檢查）、`render_diagnosis_pages` 的三處接線檢查（ADR-0020 bug 9，前置檢查）。`compute_metric_ci` 裡已經沒有 `n_boot` 檢查；它剩下的 raise 是「CI 開著、樣本卻是 `None`」，標**前置檢查**。

---

# 收尾要 grep 的兩份清單（flow 規則 10 的九種形態）

**模組路徑**：`pipelines.evaluation.nodes_spark`、`pipelines.evaluation.comparison_nodes`、`evaluation.baselines`、`evaluation.segments`、`evaluation.comparison.sources`、`evaluation.comparison.restrict`、`evaluation.comparison.alignment`。
**符號名**：`persist_eval_predictions`（ADR-0018 刪掉的）、`_fmt_cell`、`_render_table`、`_render_section_extras`、`_k_to_lookup`、`rec_ks`、`_HIDDEN_METRIC_PREFIXES`、`_visible_metric_keys`（後兩個 ADR-0018 決定 4 刪掉的）。`validate_enriched_eval_predictions_present` 不改名（ADR-0018 決定 1 只改它的語意）。
**登記表與稽核**：`tests/test_core/test_architecture_constraints.py` 的 `LITERAL_COLUMN_EXCEPTIONS`（4 筆路徑重指，見閘門 5）；`architecture-constraints.md` S6 那一節的登記表說明跟著改——表列的四筆路徑，以及表頭寫死的筆數（「14 筆／11 筆」那種數字，重指之後要重數）；同檔 S4 那一節拿 `comparison_nodes.py`、`comparison/restrict.py` 當實例的三處。
**文件**：`docs/pipelines/evaluation.md`、`docs/agents/pipeline-node-design.md`（例外表）、`docs/agents/architecture-constraints.md`（上述四處）、`docs/operations/known-pitfalls.md` §12（varargs 那段若提到 `nodes_spark`）、`docs/adr/0015-compare-population-counted-in-query-groups.md`（提到 `comparison_nodes.py` 的那一處）、`docs/diagrams/`、`conf/base/catalog.yaml` 的註解（「由 `comparison_nodes.py::persist_eval_predictions` 寫入」那句）、`core/consistency.py` 的鏡像註解。
**log 介面**：`nodes_spark` 這個 logger 名會變成 `nodes`；#198 記過「以 logger 名過濾的監控會靜默失效」，PR 說明要列出來。

**更正（2026-09-14，#365）**：`evaluation.baselines` 不歸零（不搬，見〈呼叫端事實〉的更正）；模組路徑清單補 `evaluation.config_fingerprint`；登記表是 3 筆重指（見〈實作前的閘門〉的更正）。

---

# 這一輪刻意沒做的事

| 沒做 | 為什麼 |
|---|---|
| 兩層渲染器（稽核 C 的建議形狀） | 跳脫契約沒解，硬合會讓診斷入口靜默退化。見決定 5 |
| 統一 `guardrail_recall_k` 預設值（D-2） | 會改比較報表欄數，且沒有需求。見決定 5（報表凍結已解除，ADR-0020） |
| 主報表診斷區改成第 N＋1 項 registry 診斷（稽核 B） | 搬報表的一塊，屬結構重整，另開一票（報表凍結已解除，ADR-0020） |
| compare 模式「算」與「畫」分開、比較數字落 JSON（稽核 F） | 沒有效能證據、沒有讀者要那份數字 |
| 指標家族參數化（稽核 J-2） | 跨三模組的重構，另一輪；停算那一半在 ADR-0018（報表凍結已解除，ADR-0020） |
| 刪 `evaluation/statistics.py`、`calibration.py`（稽核 I） | 已在 `c88746a` 修掉，無事可做 |
| training 的 7 個 diagnosis node 搬來 evaluation | 跟 `overall_map` 跨月合併同一個接縫，使用者要求另開一輪討論；本份不預留位置 |
| `metric.k` 九處讀取收成一份（稽核 H） | 行為題不是結構題，歸 bug 輪（ADR-0020〈設計 H〉） |
| 關 #163 | 通用問題，由使用者裁 |

---

## 出處

- 形狀判準：[`pipeline-node-design.md`](../agents/pipeline-node-design.md)；流程：[`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)。
- 同型前例：[ADR-0008](0008-dataset-modules-split-by-role.md)（dataset）、[ADR-0014](0014-training-modules-split-by-role.md)（training，含「不搬」的先例）。
- 稽核與複核：[`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)。
- 行為改動：[ADR-0018](0018-evaluation-materialize-at-producer.md)。
- 本份第一版被兩輪 fresh-context 審查改掉的地方（D-1 五→三、決定 6 四→五與 predicate 方向、`_render_table` 不進 `fmt.py`、數字重數）：2026-09-13，已寫進各決定正文。
