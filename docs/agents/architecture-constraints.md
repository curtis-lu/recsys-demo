# 架構約束與框架事實

這個框架**長什麼樣**（節一），以及新東西**該長什麼樣**（節二、節三）。

程式碼現況請讀 `graphify-out/GRAPH_REPORT.md`；設計取捨的白話背景請讀 `docs/design-principles.md`（那份給人讀，本檔給 agent 讀）。

## 你要做什麼 → 先讀哪幾條

| 你要做的事 | 必讀 |
|---|---|
| 動 `core/`（Runner、Node、Catalog、io） | 節一全部（F1–F10） |
| 新增或修改一個 node | A1、A2、A5、A6、A7 ＋ F4、F5 ＋ [`pipeline-node-design.md`](pipeline-node-design.md) |
| 動 `pipelines/dataset/` 的 node | 上一列 ＋ S1、S2 |
| 新增 catalog 條目 | A1 ＋ F10 |
| 想破例（寫檔、零輸出 node、`writes=`） | 節三——**要加一筆必須先問使用者** |
| 覺得「測試綠了應該就沒問題」 | 每條約束底下的「**這個檢查看不到**」 |

## 三類條目，拘束力不同

| 類型 | 意義 | 你該怎麼用 |
|---|---|---|
| **事實**（F 系列） | 這個框架有什麼、沒有什麼 | 不是規則，沒有「違反」可言。但它決定哪些路走得通——別去嘗試框架不支援的做法 |
| **約束**（A、S 系列） | 可機械檢查的規則 | 違反即錯。每條都附檢查方式，由 `tests/test_core/test_architecture_constraints.py` 自動驗證 |
| **例外登記**（R 系列） | 已核准的破例清單 | 清單內的既有案例合法。**要新增一筆必須先取得使用者同意**，不得自行擴充 |

## ⚠ 兩套 A 系列不是同一套編號

- **本檔的 A1–A7** ＝ 結構約束（node 與 catalog 該長什麼樣，AST 稽核，測試期抓）
- **`core/consistency.py` 的 A1–A28** ＝ 設定不變量 predicate（config 值彼此矛不矛盾，執行期 raise）

兩邊的 **A5、A7 已經在撞車**，意思完全不同。**本 repo 不重編號**——重編號會讓既有文件與 commit message 的引用全部指錯（理由同 A16/A17/A18 退休不回填，見 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 第四節）。

**引用時連模組一起寫**：「consistency 的 A5」／「本檔的 A5」。`S` 系列（structure）是為了不再增加撞車面而另起的前綴。

> 本框架是**手刻的 Kedro 風格實作**（`src/recsys_tfb/core/` 自製 DataCatalog／Node／Pipeline／Runner，`src/recsys_tfb/io/*` 仿 kedro.io），**無 kedro 套件依賴**。
> 下文凡引用 Kedro 官方立場者，基準為 **Kedro 1.5.0**，出處整理在 `docs/notes/2026-08-03-kedro-official-design-rationale.md`。
> 引用 Kedro 是為了說明我們為什麼一樣或不一樣，**不代表我們有義務跟它一致**。

---

## 10 條約束一覽

`tests/test_core/test_architecture_constraints.py` 執行，**20 個測試，1.4–1.7 秒**（2026-08-31 連跑三次；#234 加入 S3 的三個測試後）。⚠ **次秒級的數字本來就抖，別當精確值引用**——同一台機器上 main 的 17 個測試量到 1.25 秒，而本檔原本寫 0.62 秒。要引用就自己重跑一次。

| # | 規則 | 管到哪 | 這個檢查看不到 |
|---|---|---|---|
| [A1](#a1-資料流產物一律經-catalognode-不得自己讀寫它們) | 資料流產物一律經 catalog；node 不得自己讀寫它們 | `pipelines/` 底下的 node 函式與 `Node(...)` | 間接寫入（經專案 helper）；`steps/` 底下的程式碼 |
| [A2](#a2-node-函式不得依賴可變全域狀態) | node 函式不得依賴可變全域狀態 | 同上 | `core/`、`utils/` 不在掃描範圍（另由 R2 盯著） |
| [A3](#a3-不得用-print) | 不得用 `print()` | 整個 `src/recsys_tfb/` | — |
| [A4](#a4-src-不得-import-notebooks) | `src/` 不得 import `notebooks/` | 整個 `src/recsys_tfb/` | 「把探索性程式碼搬進 `src/`」抓不到 |
| [A5](#a5-每個-node-至少要有一個-input一個-output或一個-writes) | 每個 node 至少要有一個 input、一個 output，或一個 `writes` | `pipelines/` 底下的 `Node(...)` | 動態組出來的參數（58 個裡有 4 個） |
| [A6](#a6-同一-node-的-inputwrites-名不得與-output-名相同) | 同一 node 的 `input`／`writes` 名不得與 `output` 名相同 | 同上 | 同上 |
| [A7](#a7-零輸出的-side-effect-node-必須登記) | 零輸出的 side-effect node 必須登記 | 同上 | — |
| [S1](#s1-dataset-的每個-node-必須定義在-pipelinesdatasetnodespy) | dataset 的每個 node 必須**定義**在 `pipelines/dataset/nodes.py` | 只管 `pipelines/dataset/` | **內容**——12 行轉手 node ＋ 四決策 helper 完全合規 |
| [S2](#s2-pipelinesdatasetmonth_planspy-不得-import-pyspark) | `pipelines/dataset/month_plans.py` 不得 import pyspark | 只管那一個模組 | `pyspark` 仍會進 `sys.modules`（刻意不驗，見該條） |
| [S3](#s3-pipeline-以外的-src-模組不得-import-該-pipeline-的-steps) | pipeline 以外的 `src/` 模組不得 import 該 pipeline 的 `steps/` | 三條 pipeline 的 `steps/`，掃整個 `src/`（測試刻意不掃） | 先 import 套件再走屬性（`training.steps.hpo_resume`）；現況零命中 |

**CLI 層（`__main__.py`）、`core/`、`io/` 不在 A1／A2 管轄內**——那幾層本來就負責 I/O 與程序級資源。

還有一個不屬於任何編號的守衛：`test_audits_the_tree_it_was_shipped_with`。它確認測試掃的是自己所在的那棵樹，防止在 worktree 裡跑 pytest 卻掃到 main 的 `src/` 而全綠。

---

# 節一 · 框架事實（動 `core/` 之前先讀）

| # | 一句話 |
|---|---|
| [F1](#f1-沒有-hooks-機制) | 沒有 hooks 機制 |
| [F2](#f2-observability-是強制的不是可選的) | Observability 是強制的，不是可選的 |
| [F3](#f3-只有-sequential-runner) | 只有 sequential runner |
| [F4](#f4-node-極薄沒有-namespace沒有-tags) | Node 極薄：沒有 namespace、沒有 tags |
| [F5](#f5-切片語意單一起點自動上游擴張跳過零輸出-node) | 切片語意：單一起點、自動上游擴張、**跳過零輸出 node** |
| [F6](#f6---env-的覆蓋層語意會靜默退化) | `--env` 的覆蓋層語意會靜默退化 |
| [F7](#f7-我們有kedro-沒有的抽象) | 我們有、Kedro 沒有的抽象 |
| [F8](#f8-node-函式大小的現況分佈) | Node 函式大小的現況分佈 |
| [F9](#f9-測試的三個層次) | 測試的三個層次 |
| [F10](#f10-中介產物命名) | 中介產物命名 |

## F1. 沒有 hooks 機制

`core/` 與 `io/` 對 `hook` 零命中。橫切關注點（logging、計時、錯誤處理）**直接實作在 `core/runner.py`**（7 處 `logger.` 呼叫）。

連帶後果：

- **不要試圖「加一個 hook」來做任何事**。要加橫切行為，只能改 `core/runner.py`。這是有意識的取捨——換來的是行為固定可預期，代價是擴充點只有一個。
- `io/base.py` 的 `load`／`save`／`exists` 是**公開** `@abstractmethod`。Kedro 用私有 `_load`／`_save` 加公開 wrapper，因為 wrapper 是它掛 hook 與 versioning 的地方；我們沒有要掛的東西，所以攤平沒有損失。**不要「為了對齊 Kedro」把它改成私有方法加 wrapper**——那會憑空增加一層而沒有對應的用途。
- Kedro 一整組 hook 相關約束（hook 參數不得有預設值、不得依賴 hook 執行順序、hook 間共享狀態須唯讀……）在本 repo **不適用**。

## F2. Observability 是強制的，不是可選的

Kedro 把 observability 當成 hook 的一種**使用場景**，也就是可以不裝。本框架把它做進 Runner：每個 node 執行時必定記錄 `node_started`／`node_completed`／`node_failed` 結構化事件（`core/runner.py:95-238`），失敗時帶 `exc_info=True`。

所以：**新增 node 時不需要自己寫「開始了／完成了」的 log**，Runner 已經記了。你該記的是 node 內部的業務判斷——跳過了什麼、選了哪條分支、處理了幾列。

另有兩層執行中繼資料：

- `core/logging.py` 的 `RunContext`（run_id／pipeline／env／dataset_version／model_version）掛在每筆結構化 log 上。
- `core/versioning.py:330-377` 的 `build_manifest_metadata` 把 version／pipeline／created_at／git_commit／parameters 落成 `manifest.json`。

## F3. 只有 sequential runner

`core/runner.py` 只有一個 `Runner.run()`，循序執行。沒有 `ParallelRunner`、沒有多行程。

連帶後果：Kedro 為多行程而設的一整組約束（dataset 與 node 必須可 pickle、不得用 lambda／巢狀函式／closure、不能並用多行程的 dataset 要標記屬性）在本 repo **不適用**。

**但這是「現在不適用」，不是「永遠不必管」。** 若未來要加平行執行，第一個擋路的**不是**「可不可 pickle」，而是**贏家模型只活在 driver 的記憶體裡**：`pipelines/training/steps/hpo_scoring.py` 的 `TrialScorer.best["model"]` 存的是訓練好的 `ModelAdapter` 物件本身（LightGBM booster 掛在它的 `.booster` 上）。多行程時每個 worker 刷新的是自己那一份，主行程那一份始終是 `None`，於是 `tune_hyperparameters` 的 last-resort 分支會靜靜地把 `study.best_params` 重訓一次——**每跑一次就白付一輪完整訓練，而平行化的目的正是省時間**，而且沒有任何錯誤訊息。

而 Optuna 的兩種平行化**都不需要**把 objective 送過行程邊界：`study.optimize(..., n_jobs=N)` 用執行緒、共用記憶體（但 `TrialScorer.best` 的「比大小再寫回」不是原子操作，那條路要自己加鎖）；多行程做法是各行程自己建 objective、共用一個 storage——本 repo 已經在用後者的基礎設施（`pipelines/training/steps/hpo_resume.py` 的 `JournalStorage` ＋ `JournalFileBackend`）。

**方向是「跑完從磁碟讀回贏家」**，不是「讓 callable 可 pickle」：`hpo_resume.write_checkpoint` 每次刷新最佳成績就已經把模型存到磁碟。⚠ **但現況的 checkpoint 不能直接多行程用**——模型（`model.txt`）與 meta（`best_meta.json`）是兩個獨立檔案、兩次獨立的 `os.replace`，中間沒有跨檔鎖、也沒有版本指標，多個 writer 同時刷新時讀者可能拿到 A worker 的模型配 B worker 的 score／params（`JournalStorage` 保護的是 study 的 trial 記錄，不保護這對檔案）。啟用平行前必須先解決其一：單一 writer、檔案鎖，或版本化的 checkpoint 目錄 ＋ 一個原子寫入的 manifest 指向當前贏家。

**目前沒有任何票或需求要求平行 HPO**（`deliberate-non-goals.md`），這條記的是「真要做的時候該解哪個問題」，不是待辦。**這一段在 2026-08-30（#229）改寫過**：舊版寫的是「內嵌的 Optuna 閉包不可 pickle，是第一個擋路的東西」，兩層都錯——pickle 送過去的是程式碼不是模型，而且兩種平行化模式都不需要 pickle objective。

## F4. Node 極薄：沒有 namespace、沒有 tags

`core/node.py` 全長 31 行，`Node` 只有 `func`／`inputs`／`outputs`／`writes`／`name` 五個屬性，**沒有任何驗證邏輯**（零 `raise`）。

`writes` 宣告「這個 node 自己會寫哪些 dataset」，語意對應 Kedro 的 `confirms`；機制與已核准清單見 A1 與 R1。

連帶後果：Kedro 靠 namespace 與 tag 做的事（分組執行、模組化 pipeline、`.` 分隔的命名空間規則）在本 repo 都不存在。dataset 名稱可以自由使用 `.`，因為沒有東西保留它。

**A5／A6 兩條約束就是為了補上 Node 沒做的驗證**——它們由稽核測試在測試期把關，不是由 `Node.__init__` 在建構期擋。

## F5. 切片語意：單一起點、自動上游擴張、**跳過零輸出 node**

- `--from-node` 與 `--only-node` 各只吃**一個** node 名稱，且**互斥**（`__main__.py` 的 `_slice_pipeline`）。沒有多條件過濾，也沒有「只跑缺漏輸出」這種功能。
- 切片會透過 `can_load` 詢問 catalog：缺少的輸入會**自動把上游生產者拉回來執行**（`core/pipeline.py` 的 `_slice_with_expansion`），並在 `SlicePlan.auto_included` 回報。
- **零輸出的 side-effect node 會被跳過**，並印出 `[plan] skipped side-effect nodes (outputs=None, not re-validated)`（`__main__.py` 的 `_format_slice_plan`）。

最後一條與 Kedro 相反——Kedro 的「只跑缺漏輸出」明文規定無 output 的 node **永遠跑**，因為沒有輸出可檢查、無法判斷 side effect 是否發生過。

**這件事有現實後果**：`dataset` pipeline 的第一個節點 `validate_data_consistency`（`pipelines/dataset/pipeline.py`）就是零輸出 node，它是 Layer-2 資料一致性閘。用 `--from-node` 接續 dataset pipeline 時，**這道閘不會跑**。框架會印警告，但不會阻止。

**擴張永遠救不了它**：producer map 由 `node.outputs` 建，零輸出的 node 不在裡面，所以沒有任何缺料能把它拉回來。**唯一的進場方式是被明確點名**——而點名只有「模式」做得到，切片做不到（[ADR-0013](../adr/0013-pipeline-modes-and-slicing-are-separate.md)）。`--only-test-months` 的 `ONLY_TEST_MONTHS_NODES` 把它列為清單第一個成員就是為此（#203、#157）。新增模式時這是必須各自處理的一件事，不是預設會繼承的行為。

## F6. `--env` 的覆蓋層語意會靜默退化

`ConfigLoader._load()`（`core/config.py:140-147`）讀 `conf/base` 再深度合併 `conf/<env>`。而 `_load_yaml_dir` 在目錄不存在時**靜默回傳空 dict**（`core/config.py:131-132`），`--env` 本身是自由字串、沒有任何驗證——8 個 CLI command 各自宣告 `env: str = typer.Option("local", "--env", "-e", …)`，沒有一個帶 enum 或事後檢查（`grep -n 'env: str = typer.Option' src/recsys_tfb/__main__.py`）。

所以現況是：

- `conf/` 底下只有 `base`／`spark-local`／`sql`，**沒有 `conf/local/`**。`--env local` 讀的是不存在的 `conf/local/`，實際效果等同「只用 base」。
- **打錯環境名不會有任何錯誤訊息**，只會拿到 base 設定。被交代「用 `--env <某環境>` 跑」時，不要把「跑完沒報錯」當成「讀到那個環境的設定」。
- `conf/spark-local/` **不是**環境覆蓋層。它只有 `spark-defaults.conf` 與 `spark-env.sh`，是給 Spark 自己讀的 `SPARK_CONF_DIR`。名字跟 Kedro 的 `conf/local`（使用者專屬、不進版控）很像但語意相反——**本 repo 的 `conf/spark-local` 是進版控的**。

> 這個靜默退化是已知缺陷，修法（在 `core/consistency.py` 加一條 A 系列 predicate 檢查 `conf/<env>` 存在）已另開 **issue #153**，不在本檔範圍。

## F7. 我們有、Kedro 沒有的抽象

| 模組 | 行數 | 解決什麼 |
|---|---|---|
| `core/consistency.py` | 1485 | 不變量 predicate 的**唯一真實來源**（A 系列 config-static／B 系列資料閘） |
| `core/versioning.py` | 415 | 三層 hash 版本 ID |
| `core/logging.py` | 323 | `RunContext` 與結構化日誌 |
| `core/schema.py` | 189 | 欄位角色集中定義 |
| `core/safe_eval.py` | 141 | HPO 宣告式搜尋空間的受限求值（stdlib `ast`，無額外套件） |

其中 `consistency.py` 值得單獨講：**本框架的正確性重心不在 node 契約，而在集中式 predicate**。量體對比很直白——`consistency.py` 1485 行，`node.py` 31 行。Kedro 把正確性押在「node 是純函式且輸入輸出宣告清楚」，我們押在「所有不變量集中成可測試的 predicate」。

新增一致性不變量**必須**在 `core/consistency.py` 加 predicate，不得在各 pipeline 散落。細節見該模組 docstring。

版本化也不是同一件事：Kedro 的 dataset versioning 管的是「檔案的哪一版」，我們的三層 hash 管的是「哪一組設定產生的產物」。

## F8. Node 函式大小的現況分佈

`pipelines/**/*nodes*.py` 的頂層 `def`（不含巢狀），2026-08-30 量測共 52 個。

**這個數字每次改 node 都會變，所以別引用它，重量一次**：

```bash
.venv/bin/python -c "
import ast, pathlib
from collections import Counter
b = Counter(); n = 0
for p in pathlib.Path('src/recsys_tfb/pipelines').rglob('*nodes*.py'):
    for f in ast.parse(p.read_text()).body:
        if isinstance(f, ast.FunctionDef):
            L = f.end_lineno - f.lineno + 1; n += 1
            b['<=40' if L<=40 else '41-80' if L<=80 else '81-120' if L<=120 else '121-160' if L<=160 else '>160'] += 1
print(n, dict(b))"
```

| 行數 | 個數 |
|---|---|
| ≤ 40 | 27 |
| 41–80 | 12 |
| 81–120 | 6 |
| 121–160 | 4 |
| > 160 | 3 |

最長的五個：`predict_and_write_scores` 263 行（`inference/nodes.py`）、`tune_hyperparameters` 185 行（`training/nodes.py`）、`predict_and_write_test_predictions` 179 行（`training/nodes.py`）、`validate_predictions` 146 行（`inference/nodes.py`）、`prepare_eval_data` 143 行（`evaluation/nodes_spark.py`）。

> **⚠ 上面那組數字是三張票合併後重量的結果，不是任何一張單獨的結果。** #229、#232、#230 三次改動都從 57／56 那個基準分支出去，各自在自己的 PR 裡量過一次；**下面三段引言記的是各自的 delta，把它們相加會得到錯的數字**。合併後的真值只有一個來源：重跑上面那段指令。這正是「別引用它，重量一次」在同一天內就被驗證了一次的實例。
>
> **2026-08-30 第三次重量**（#230，合併 #232 之後）。`_materialize_parquet_handle`（75 行）、`_resolve_cache_path`、`_populate_cache_from_hive` 三個 helper 從 `training/nodes.py` 消失——前者拆掉、後兩者搬進 `pipelines/training/steps/local_cache.py`，而那個掃描只看 `*nodes*.py`。單獨看本票是 56 → 53；**與 #232 一起落地後的合併真值是 52**。
> 桶子怎麼動的（以本票單獨計）：≤40 掉 4 個 ＝ 走掉兩個 helper ＋ 兩個 cache node 從 3–5 行長到 40 行以上；41–80 淨值不變 ＝ 走掉一個 helper、`cache_test_model_input` 升上去，再由那兩個 cache node 補回；81–120 多 1 個就是升上去的 `cache_test_model_input`。
> **這是規則 2 的又一個實例**：五個 cache node 從各 3–5 行的轉手函式，長成各自寫完四個決策的 37–96 行——行數增加的地方，正是決策從 helper 浮回 node body 的地方。
> **順手修掉一處腐爛**：最長的五個裡 `predict_and_write_test_predictions` 原寫 171，實為 **179**——#246 把它加長了但沒回頭重量。

> **2026-08-30 重量的原委，以及腐爛了什麼**（#229）。該次改動把 `_hpo_score` 與 HPO trial 的評分搬進 `pipelines/training/steps/hpo_scoring.py`，所以總數 57 → 56、≤40 少一個，`tune_hyperparameters` 228 → 185、`finalize_model` 156 → 155。**重量時發現另外三個數字早就腐爛了，跟這次改動無關**：41–80 原寫 12（當時實為 11）、`predict_and_write_scores` 原寫 253（實為 263）、第五名原寫 `prepare_eval_data` 143（實為 `validate_predictions` 146）。三者都是 2026-08-09 之後沒人回頭重量。**這正是上面那句「別引用它，重量一次」的實例**——引用了就會像這樣把三個過期的數字一起帶下去。

> **2026-08-30 第二次重量**（#232）。sample_weight／refit／MLflow 三處機制搬進 `pipelines/training/steps/`，所以總數 56 → 55（`resolve_weight_diagnostics` 併回它唯一的呼叫端）、`finalize_model` 155 → 133 而掉出前五名，`prepare_eval_data` 143 遞補第五。≤40 少兩個、41–80 多一個，是因為 `persist_sample_weight_report` 吸收了那些決策而從 21 行長到 59 行——**這正是規則 2 說的「照判準寫的 node 會比較長」**，決策從 helper 浮回 node body，行數就記在 node 上。

**這是事實不是規則**——本檔不訂函式長度門檻。記錄它是為了讓你知道常態（六成的 node 函式在 40 行內），下次寫一個新的時心裡有個尺。行數本來就只是「這個函式只做一件事」的粗略代理，60 行可以混五種職責，130 行也可能只是一段長而平的轉換。

**兩件跟數字有關的事**：

- 總數從 #197 之前的 65 掉到 57，是因為 inference 的 8 個模組私有機制與 2 個公開非 node 函式搬進 `pipelines/inference/steps/`，而那個掃描只看 `*nodes*.py`。同一次改動讓 `predict_and_write_scores` 從 206 行長到 253——[`pipeline-node-design.md`](pipeline-node-design.md) 規則 2 明說照判準寫的 node 會比一般長，因為 node 是一串決策而不是一個呼叫。
- 本表在 #188 之前寫「61 個、43/8/5/3/2」，但用上面那段量出來是「60 個、36/12/7/3/2」。**舊數字的量法沒被記下來，所以無法判斷是量法不同還是已經腐爛。** 這次把指令一起寫上，就是為了下一個人不必再猜。

## F9. 測試的三個層次

| 層次 | 驗什麼 | 範例 |
|---|---|---|
| 單元 | 單一 node 函式的輸入輸出 | `tests/test_pipelines/test_training/test_nodes.py` |
| 整合 | pipeline 組起來跑得動、node 相依正確 | `tests/test_pipelines/test_training/test_pipeline.py` |
| 一致性 | `core/consistency.py` 的 predicate 本身 | `tests/test_core/test_consistency.py` |

改一個 node 函式的行為 → 單元層。改 pipeline 組裝或 node 相依 → 加整合層。加或改不變量 → 一定要動一致性層。

（測試怎麼跑得快、哪些是已知 failing，見 `CLAUDE.md` 與 `docs/operations/known-pitfalls.md`；本檔只講分層。）

## F10. 中介產物命名

`conf/base/catalog.yaml` 目前 46 個條目（2026-08-31 量：`yaml.safe_load` 後的 top-level key 數），命名全部帶業務語意，零 `tmp`／`_v2`／`_final`／`_new` 這類殘留。

新增 catalog 條目時對齊既有命名感覺即可——**這是事實不是約束**，因為禁用字清單只抓得到最粗糙的一類命名問題，真正的「有業務語意」是判斷題，機械檢查給不了。

---

# 節二 · 約束（新增或修改 node、catalog 條目之前先讀）

每條的結構都一樣：規則 → 為什麼 → **檢查**（測試實際做什麼）→ **這個檢查看不到**。

## A1. 資料流產物一律經 catalog；node 不得自己讀寫它們

pipeline 各節點之間傳遞的資料（會被下游 node 消費的東西）一律由 catalog 條目宣告，node 只做輸入到輸出的轉換。

### 例外一：`Node(writes=[...])`（資料流產物）

`writes` 列出的名稱，Runner 交給 node 的是 catalog **dataset 物件本身**而非載入後的資料（`core/runner.py:121-128`）。拿到它的 node 可以自行管理**這個 dataset 的分區寫入生命週期**——包含 `.save()` 寫入，以及查詢哪些分區已存在（`existing_partition_values()`）。**不含**把它當一般資料來源整批讀取（那該用普通 input）。

- 交出去的是**完整的 dataset 物件，不是 write-only proxy**：續跑要能反問「已經有哪些分區」，包裝成只能寫的東西就答不出來。
- **寫入目標必須是已註冊的 catalog 條目**（Runner 在啟動時檢查），光是「某個 node 的 output」不算。理由：`writes` 不建立拓撲相依邊，而啟動驗證是順序盲的——放行「由某個 node 生產」會讓生產者還沒跑時 `get_dataset()` 回 `None`，node 靜默拿到 `None`。擋在啟動階段，這條路就不存在。
- **`inputs` 位置對應、`writes` 以 keyword 綁定**：node 函式的參數名必須**逐字等於** dataset 名。這不是風格選擇——本 repo 的慣例是「新的可選 input 加在最後」（見 `pipelines/training/pipeline.py:203-206` 的 `log_experiment` 註解），位置綁定下照著做會把 dataset 物件擠到可選參數的槽位，而尾端的 `=None` 正好把 arity 錯誤吃掉，**不報錯**。keyword 綁定讓同一個錯誤在 node 執行前就 raise。（寫入目標的參數仍須排在所有 input 參數之後，因為 input 是位置填進去的。）
- 這個例外**必須寫在 pipeline 定義的 `writes` 參數裡**，不得在函式體內自己取得 catalog。理由與 Kedro 的 `confirms` 一致：**side effect 要用宣告的，不能藏在函式體裡**。`writes` 是獨立參數而非 `inputs` 裡的前綴字串，就是為了讓這件事在讀 pipeline 定義時一眼可見（issue #186）。

已核准清單：**R1（2 筆）**。

### 例外二：診斷副產物

有 2 個 node 會自己寫診斷／稽核檔（HPO 診斷、MLflow artifact）。這些**不是資料流產物**——沒有任何 node 消費它們，它們寫進 model version 目錄或 MLflow。這是既有慣例。

已核准清單：**R4（2 筆）**。

**兩類例外要新增一筆，都必須先問使用者。**

### 檢查（4 個測試）

| # | 做什麼 |
|---|---|
| (a) | `Node(...)` 的 `writes` 內容必須與 R1 登記相符 |
| (b) | `Node(...)` 的 `inputs` 中不得再出現 `"@…"` 字串。舊 sigil 已移除，殘留的話 Runner 會把它當成一個不存在的 dataset 名而在驗證階段報錯；這條讓它在測試期就指出檔案與行號 |
| (c) | node 模組不得出現 `DataCatalog` 或 `catalog.load()`／`catalog.save()`。**AST 比對，不是文字比對**——這些字眼在註解裡合法出現 |
| (d) | node 模組裡有直接寫檔呼叫的函式必須與 R4 登記相符 |

### 這個檢查看不到

- **間接寫入。** 寫檔掃描只看得到**直接呼叫**（`open`／`mkdir`／`log_artifacts`…）。經由專案 helper 的寫入它看不到——`tune_hyperparameters` 正是這種，靠人工登記在 R4。

  **另一個掃不到的寫入，刻意不進 R4**：`pipelines/training/steps/local_cache.py` 的 `populate_cache_from_hive`，經 `utils/hdfs.copy_hdfs_to_local` 把 Hive 分區複製到 driver 本機。不登記的理由是 R4 收的是「**診斷副產物**」，而一份 Hive 表的本機複本不是診斷副產物（使用者 2026-08-30 裁決，ADR-0014 閘門 G2）。記在這一段，是因為「掃描看不到它」這件事仍然為真——它只是該被記在這裡，而不是被塞進一張語意不合的表。
- **`steps/` 底下的程式碼。** (c) 與 (d) 只讀 `pipelines/**/nodes*.py`（`test_architecture_constraints.py:172`、`:190` 的 `rglob("nodes*.py")`）。所以搬進 `steps/` 的程式碼不在稽核範圍內——`pipelines/dataset/steps/` 自 #176 起、`pipelines/inference/steps/` 自 #197 起（約 500 行）都是。

  **這不是豁免**：`steps/` 裡出現 `catalog.load`／`catalog.save` 一樣違反 A1，只是**沒有測試會發現**，靠 code review。#197 當下實查過 `pipelines/inference/steps/` 零命中。要不要把 glob 放寬到 `pipelines/**/*.py` 是一張獨立的票（放寬會一併把 `dataset/steps/` 納入，需先確認那邊也乾淨）。

## A2. node 函式不得依賴可變全域狀態

`pipelines/` 底下不得出現 `global` 宣告。

這條的界線是**層**而不是檔案：`core/` 與 `utils/` 管理程序級資源（一個 JVM、一份 run context），pipeline node 不管。

**檢查**：`src/recsys_tfb/pipelines/` 底下 `global` 宣告數必須為 0。

### 這個檢查看不到

`core/` 與 `utils/` 完全不在掃描範圍內。那裡有 5 處 `global`，**不是 A2 的豁免例外，是根本不在 A2 的掃描範圍**。它們另外由 **R2** 登記盯著——那份登記的作用是「框架層的全域狀態不會悄悄變多」，不是「A2 允許 5 個」。

## A3. 不得用 `print()`

`src/recsys_tfb/` 底下一律用 `logging.getLogger(__name__)`。

理由見 F2——結構化日誌是這個框架的觀測基礎，`print` 出來的東西不帶 `RunContext`、進不了 log 聚合、也不會出現在失敗追蹤裡。

**檢查**：`src/recsys_tfb/` 底下 `print(` 呼叫數必須為 0（逐行正規表示式）。

## A4. `src/` 不得 import `notebooks/`

生產程式碼與探索程式碼單向隔離：notebook 可以 import `src/`，反過來不行。

**檢查**：`src/recsys_tfb/` 底下不得出現 `import notebooks` 或 `from notebooks`。

### 這個檢查看不到

**誠實說明**：`notebooks/` 目前只有兩個 `.ipynb`、沒有任何 `.py`、也沒有 `__init__.py`，所以 `import notebooks` 現在**根本不可能成立**。這條是預防性守衛（成本近乎零），不是在擋一個現存風險。

真正該注意的是**別把探索性程式碼搬進 `src/`**，那個機械檢查抓不到。

## A5. 每個 node 至少要有一個 input、一個 output，或一個 `writes`

三者皆空的 node 在這個框架裡不可能有作用——它拿不到資料，也不會被任何東西消費。

`writes` 算數：只宣告寫入目標的 node 拿得到 dataset 物件，也確實會產生效果，所以它不屬於「不可能有作用」那一類。

`core/node.py` 不做這個檢查（見 F4），由稽核測試把關。

**檢查**：AST 掃描 `pipelines/` 底下**所有** `.py` 的 `Node(...)` 定義（`rglob`，不只 `pipeline.py`）。

### 這個檢查看不到

- **動態組出來的 `inputs`／`outputs`／`writes`。** 讀不出字面值就跳過。目前 58 個 `Node` 中有 4 個是這種。`test_static_coverage_floor` 把「58 個裡有 54 個可判定」釘死，**所以這個盲區不會悄悄變大**——變大了測試就紅。
- **`pipelines/` 以外的 `Node(...)`。** 掃描範圍止於該目錄（現況為零）。

## A6. 同一 node 的 `input`／`writes` 名不得與 `output` 名相同

Runner 先載入全部 inputs 再執行、再存 outputs（`core/runner.py:127-142`）。名稱相同代表你打算原地覆寫一個 dataset，而執行順序讓這件事的語意不明確。

要覆寫就用不同的 catalog 條目名，或明確走 `writes`（見 A1）。

**檢查**：同 A5 的 AST 掃描；`inputs` 與 `writes` 合起來跟 `outputs` 比對。

### 這個檢查看不到

與 A5 相同的兩個盲區（動態參數、`pipelines/` 以外）。

## A7. 零輸出的 side-effect node 必須登記

因為切片會跳過它們（見 F5），新增一個零輸出 node 等於新增一個「接續執行時會靜默不跑」的東西。這需要是有意識的決定。

**檢查**：`pipelines/` 底下所有 `.py` 中 `outputs=None`（或根本沒寫 `outputs`）的 `Node` 定義，必須與 **R3** 登記清單相符（Counter 比對）。

## S1. dataset 的每個 node 必須**定義**在 `pipelines/dataset/nodes.py`

`pipelines/dataset/pipeline.py` 中每個 `Node(...)` 的第一參數，必須是 `pipelines/dataset/nodes.py` 裡以 `def` 定義的名稱。

**是「def 定義」而不是「從 nodes.py import」**：後者有 re-export 漏洞——`nodes.py` 加一行 `from .steps.sampling import some_step`，`pipeline.py` 照樣「來自 nodes.py」、檢查全綠，而函式定義在別的檔，正是 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 要消滅的形狀。

**檢查**：AST 取 `nodes.py` 的 `FunctionDef` 名稱集合（不含 import），比對 `pipeline.py` 中所有 `Node(...)` 的第一參數。

### 這個檢查看不到

**內容。** S1 只擋位置。一個 12 行的轉手 node 加一個裝著四個決策的 helper **完全滿足 S1**。

內容那一半由 [`pipeline-node-design.md`](pipeline-node-design.md) 定義：node 邊界、node body 的形狀、決策與機制的分界、`log_step` 的範圍、`steps/` 與根層的判準、命名與 docstring。那份十三條裡只有兩條有部分機械檢查，其餘靠該檔開頭那張規則總表 ＋ code review。這是 ADR-0008 已知的最大殘留風險，不是疏漏。

那份是判準的唯一真實來源，適用於**每一條** pipeline（S1 只管 dataset）；ADR-0008 保留為 dataset 那次裁決的記錄與完整論證。**已知的界外違例登記在該檔的〈已登記的例外〉**（evaluation 尚未依判準重整、training 的 7 個 diagnosis node 刻意不搬、`recsys_tfb.preprocessing` 的兩個底線名），看到它們不必以為判準是裝飾。

## S2. `pipelines/dataset/month_plans.py` 不得 import pyspark

含函式體內的延遲 import。

守的不是風格，是**檔案切分的承重前提**：`steps/scoping.py` 之所以不能併進 `month_plans.py`，唯一理由就是這條純度；而該模組 436 行的測試（`tests/test_pipelines/test_dataset/test_month_plans.py`）不需要 SparkSession 也是靠它——這個 repo 的 Spark cold start 是 2–4 分鐘。

一句沒有機制強制的「必須」會漂移（ADR-0002:21 那段三天就過時的補釘是前例）。

**檢查**：三個測試，缺一不可。

1. **直接掃描**——AST 掃該模組所有 `Import`／`ImportFrom`（用 `ast.walk`，所以**函式體內的延遲 import** 一樣掃得到），root package 不得為 `pyspark`。
2. **可達性**——沿 `pipelines/dataset/` 的 import 遞迴一跳以上，任何路徑都不得抵達 pyspark。缺了這條，`from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date` 會被第 1 條讀成「import 了 `recsys_tfb`」而放行，但該模組已經是 Spark-typed 了——而這正是本 repo 實際在用的 import 寫法。
3. **可達性真的跨得進子套件**——`test_reachability_crosses_into_a_subpackage`。**模組路徑必須按 `.` 展開成子路徑**（`...dataset.steps.scoping` → `dataset/steps/scoping.py`）：只取第一段會找到不存在的 `dataset/steps.py`，遞迴回傳「查無」，於是靜默放行——**失效方向剛好是它要守的那一個**。這條釘住四種 import 寫法，見 ADR-0008 第四節 2026-08-07 那段。

### 這個檢查看不到（刻意的）

**`pyspark` 有沒有進 `sys.modules`。** 那才是真正想要的性質，但它因為與本模組無關的理由不可能成立：`pipelines/__init__.py` → `core` → `io` → `models` → `mlflow`，終點是 `mlflow/types/schema.py` 自己那行 `import pyspark`。

S2 買到的是**結構**邊界——month_plans 不碰 Spark 型別，所以它的測試不需要 SparkSession，而 2–4 分鐘的成本是 session 不是 import。

## S3. pipeline 以外的 `src/` 模組不得 import 該 pipeline 的 `steps/`

`pipelines/<name>/steps/` 底下的模組，只有 `pipelines/<name>/` 內的 src 模組可以 import。

守的是 [`pipeline-node-design.md`](pipeline-node-design.md) 規則 8 買到的那一件事：**讀一次目錄列表就分得出對外契約與內部步驟**。根層放的是別人會呼叫的（`pipelines/dataset/month_plans.py`、`pipelines/training/cache_sources.py`），`steps/` 放的是這條 pipeline 自己呼叫的。外部模組一旦越過根層直接伸進 `steps/`，那個列表就開始說謊——而且是靜靜地說謊，沒有任何東西會壞。

**測試不在掃描範圍內，這是刻意的。** 規則 8 明說測試 import 不移動任何模組的位置：判準管的是生產端呼叫圖。`tests/test_pipelines/test_training/test_hpo_resume.py` 直接 import `steps.hpo_resume` 是對的，必須維持合法。

**檢查**：兩個測試。

1. **掃描**——AST 走過 `src/recsys_tfb/**/*.py` 的每個 import，把相對 import 依所在套件還原成絕對路徑，並且 `from X import Y` 的**兩半都算**（`X` 與 `X.Y`）；模組路徑形如 `recsys_tfb.pipelines.<name>.steps...` 而檔案不在 `pipelines/<name>/` 底下就是違例。
2. **掃描抓得到每一種寫法**——`test_the_scan_sees_every_spelling_of_the_import`，在 `tmp_path` 上建五種 import 寫法並確認五種都被抓。`steps` 藏的位置各不相同，解析器的三個零件各自只被其中一種撐住：

   | 寫法 | `steps` 藏在哪 | 少了什麼就漏掉 |
   |---|---|---|
   | `from ...training.steps.hpo_resume import open_study` | `node.module` | — |
   | `from ...training.steps import hpo_resume` | `node.module` | — |
   | `import recsys_tfb.pipelines.training.steps.hpo_resume` | `alias.name` | `ast.Import` 那一支 |
   | `from recsys_tfb.pipelines.training import steps` | `alias.name` | `node.names` 那一半 |
   | `from .pipelines.training import steps` | `alias.name`（相對） | 上面兩者都要 |

   所以只讀 `ImportFrom` 的 `node.module` 會漏掉其中**三種**而永遠全綠。三個變異各自實測：拿掉 `node.names` 那一半 → 第四種轉紅；拿掉 `ast.Import` 那一支 → 第三種轉紅；拿掉相對還原 → 第五種轉紅。

   前三種**是 #234 才寫得出來的**——在那之前 `hpo_resume` 在根層，沒有任何寫法能把它掛在 `steps` 底下。後兩種指的是套件而不是模組，本來就寫得出來（`steps/` 早就有 `hpo_scoring` 與 `local_cache`）。

附帶一條同源的檢查：`steps/__init__.py` **不得有任何 import、不得有 `__all__`**（`test_steps_packages_re_export_nothing`）。`nodes.py` 那行 import 之所以有資訊量，是因為它指名了 concern；一個 re-export 會把它抹掉——`from .steps import build_trial_params` 編得過，而讀者什麼也沒學到。

### 這個檢查看不到

它讀的是 AST 裡的 import 語句，所以兩類東西走得過去：

- **先 import 套件、再走屬性。** 一個模組 import `recsys_tfb.pipelines.training`，之後讀 `training.steps.hpo_resume`。
- **字串組出來的 import**（`importlib.import_module("recsys_tfb.pipelines.training.steps.hpo_resume")`）。任何靜態掃描都看不到，不是這條的特例。⚠ `importlib.import_module` 在 `src/` 有六處**確實在用**，只是沒有一處指向 `steps/`：`pipelines/__init__.py` 的 `_REGISTRY` 查表（指向 pipeline 套件），以及 `evaluation/pipeline.py`、`evaluation/nodes_spark.py`、`evaluation/report_builder.py` 的 `recsys_tfb.diagnosis.metric.{name}`。

現況全樹兩類都沒有指向 `steps/` 的命中。值得釘的是 import 語句那個形式——會被順手寫出來的是它，另外兩種要刻意才寫得出來。

---

# 節三 · 例外登記

**清單內的既有案例合法。要新增任何一筆，必須先取得使用者同意——不得自行擴充。**

登記表怎麼比對：測試比的是 **Counter**（目錄 ＋ 名字），所以「多一個同名站點」會被抓到；但**不釘行號**，所以站點上方的一般編輯不會誤報。

## R1. `Node(writes=[...])`（A1 的例外一）── 2 筆

| 位置 | dataset | 理由 |
|---|---|---|
| `pipelines/training/pipeline.py`（`predict_and_write_test_predictions` 的 `writes=` 那一行） | `training_eval_predictions` | 逐 partition 存檔：每次 `.save()` 恰好寫一個 partition 的列，讓 dynamic-partition overwrite 乾淨覆蓋單一分區，避免整表重寫。改成由 catalog 統一寫入就得先把所有 partition 物化在記憶體裡，正好抵銷這個設計要省的東西。消費端在 `pipelines/training/nodes.py` 的同名函式 |
| `pipelines/inference/pipeline.py`（`predict_and_write_scores` 的 `writes=` 那一行） | `unranked_predictions` | 同一個理由，但這裡是**正確性**而不只是省記憶體：生產母體單一 `(time, item)` 塊要把約 60 GB 拉進 driver，所以塊必須按 entity 切；而 `save()` 的 dynamic overwrite 會把 frame 裡出現的分區整個替換，所以塊的邊界必須等於分區的邊界。由 catalog 統一寫入等於先把整批預測物化在 driver 上——那正是本設計要消掉的東西（ADR-0010 §3 約束 C、#188） |

**這兩個 dataset 物件上被用到的方法（不只 `.save()`）**：

| 方法 | 用途 |
|---|---|
| `.save(pdf)` | 寫入單一 partition（training 逐月份，inference 逐 `(桶, item)`） |
| `.existing_partition_values()` | 查詢哪些 partition 已寫過，供續跑計畫判斷跳過哪些單位（training 經 `steps/predict_months.py` 的 `written_prediction_partitions`，inference 經 `_written_score_partitions`）；方法定義在 `io/hive_table_dataset.py` |

兩者都屬於「這個 node 自行管理這個 dataset 的分區寫入生命週期」，是同一個例外的範圍內。**把它當一般資料來源整批讀取不在此列**——那該用普通 input 讓 catalog 載入。

第二個方法也是**交出完整 dataset 物件而非 write-only proxy 的理由**：續跑必須問得出「已經有哪些分區」。

> **歷史**：這個例外原本寫成 `inputs` 清單裡的 `"@training_eval_predictions"`，靠一個 sigil 辨識，在 node 定義上看起來像個輸入。issue #186 把它換成 `Node` 上的獨立參數 `writes=[...]`（比照 Kedro 的 `confirms`），`@` 前綴已從 `core/runner.py`／`core/pipeline.py` 移除，A1 的檢查 (b) 會擋住殘留寫法。

## R2. 框架層可變全域狀態（**不在 A2 掃描範圍內**，非豁免）── 5 筆

| 位置 | 變數 | 用途 |
|---|---|---|
| `core/logging.py:159` | `_current_context` | run context 的程序級單例 |
| `utils/spark.py:49` | `_canonical_configs`, `_canonical_enable_hive`, `_last_app_id`, `_last_alive_ts` | SparkSession 生命週期 |
| `utils/spark.py:87` | `_canonical_configs`, `_canonical_enable_hive` | 同上 |
| `utils/spark.py:161` | `_last_alive_ts` | 同上 |
| `utils/spark.py:232` | `_last_app_id`, `_last_alive_ts` | 同上 |

這些管理的是程序級資源（一個 JVM、一份 run context），本質上就是單例。pipeline node 沒有這種需求。

（測試比對的是「哪個檔、幾個 `global` 陳述、涉及哪些變數名」：`logging.py` 1 個、`spark.py` 4 個。行號只是給人看的。）

## R3. 零輸出 side-effect node（A7 的例外）── 2 筆

| 位置 | node | 被切片跳過的後果 |
|---|---|---|
| `pipelines/dataset/pipeline.py` | `validate_data_consistency` | **Layer-2 資料一致性閘不會跑**。用 `--from-node` 接續 dataset pipeline 時，資料層不變量未經檢查。**例外：被模式明確點名時會跑**——`--only-test-months` 就是這樣把它留在清單裡的（見 F5） |
| `pipelines/training/pipeline.py` | `log_experiment` | MLflow 實驗記錄不會寫。不影響產物正確性，影響可追溯性 |

**位置只給檔案、不給行號**：兩個 node 名都是 `Node(name=...)` 或函式名的字面值，grep 得到；而行號會被同檔任何一次增刪默默弄錯——本檔原本寫 `pipeline.py:28` 與 `pipeline.py:202`，前者被 #203 加的模組級常數推到 92、後者早就差了一行，而 A7 的稽核測試只比對 node 名的 Counter、抓不到行號腐爛。F5 同理。

## R4. 自己寫診斷副產物的 node（A1 的例外二）── 2 筆

這些寫的**不是資料流產物**——沒有任何 node 消費它們，所以不經 catalog。它們落在 model version 目錄或 MLflow。

| 函式 | 寫什麼 | 檢查看得到嗎 |
|---|---|---|
| `log_experiment`（`pipelines/training/nodes.py`） | MLflow artifacts——整個 `diagnostics_dir` 上傳（搜 `mlflow.log_artifacts`）。params／metrics 的欄位名自 #232 起在 `pipelines/training/steps/experiment_log.py`，**但那個上傳呼叫刻意留在 node 裡**：測試 (d) 只掃 `nodes*.py`，寫檔一搬進 `steps/` 就掉出登記（實測會讓該測試轉紅） | ✅ 直接呼叫，掃得到 |
| `tune_hyperparameters`（`pipelines/training/nodes.py`） | HPO 搜尋診斷進 `diagnostics_dir/hpo/`，經 `recsys_tfb.diagnosis.hpo.write_hpo_diagnostics`（搜 `write_hpo_diagnostics`） | ❌ **間接寫入，掃描看不到**——靠這份登記人工盯著 |

**位置只給檔案與要搜的字串、不給行號**，理由同 R3：本表原本寫 `nodes.py:1256`／`:1339`／`:520`／`:739`，光是 #226 從同檔刪掉 4 行就讓四個數字同時失準；而 (d) 只比對函式名的 Counter、抓不到行號腐爛。

**離開這張表的**：`persist_sample_weight_report`（2026-08-30，ADR-0014 決定 2；G1 簽核記錄在 issue #222 的切票留言與 #226 票面）。`sample_weight_report` 拿到 catalog 條目之後，node 只 `return diag`，寫檔由 catalog 負責——它不再是「自己寫診斷副產物的 node」，也同步離開下面那組測試釘的集合。登記**縮小**不需要新例外。

> ⚠ **這張表的 2 筆，跟測試釘的 6 筆不是同一組。**
> 這張表列的是「**會寫診斷副產物的 node**」。測試 (d) 釘的是「**掃描看得到直接寫檔的函式**」＝ `log_experiment` ＋ 5 個 cache node（`cache_train_model_input`、`cache_train_dev_model_input`、`cache_val_model_input`、`cache_test_model_input`、`cache_calibration_model_input`，都在 `pipelines/training/nodes.py`，搜 `shutil.rmtree` 可見）。
> 差別在兩端：`tune_hyperparameters` 在表上、不在測試裡（間接寫入，掃不到）；5 個 cache node 在測試裡、不在表上（它們刪的是本機 parquet cache，不是診斷副產物）。
>
> **2026-08-30 起測試那一組由 2 筆變 6 筆**（ADR-0014 決定 1，使用者已批准）。原本的第 2 筆是 `_materialize_parquet_handle`——一個 helper 裝著 5 個 cache node 的全部四個決策，所以讀任何一個 cache node 都讀不出這個 cache 決定了什麼。決策上浮到各 node 之後，`shutil.rmtree` 也跟著回到各 node 的 body：5 個 node 真的各自會刪檔，登記變大是誠實的。
> 機制（路徑計算、複製、HDFS 拉取）進了 `pipelines/training/steps/local_cache.py`，**但刪檔沒有跟著搬**——(d) 只掃 `pipelines/**/nodes*.py`，搬進 `steps/` 就沒有任何測試看得到它。這是時序問題不是規則問題：等把 glob 放寬到 `pipelines/**/*.py` 那張票（#163 一帶）做完，這個決定該重新檢討。

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 程式碼現在長什麼樣（模組關係、god node） | `graphify-out/GRAPH_REPORT.md` |
| node 內容該長什麼樣（S1 擋不住的那一半） | [`pipeline-node-design.md`](pipeline-node-design.md) |
| 設計取捨的白話背景（給人讀） | `docs/design-principles.md` |
| 不變量代號 A 系列／B 系列的意義 | `src/recsys_tfb/core/consistency.py` 模組 docstring |
| Kedro 官方立場的出處與原文 | `docs/notes/2026-08-03-kedro-official-design-rationale.md` |
| 本檔各條目怎麼推導出來的 | `docs/notes/2026-08-03-kedro-vs-handrolled-gap-table.md` |
