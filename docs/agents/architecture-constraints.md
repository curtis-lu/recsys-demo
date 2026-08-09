# 架構約束與框架事實

給在這個 repo 動手的 AI coding agent。本檔回答兩個問題：**這個框架長什麼樣**（節一），以及**新東西該長什麼樣**（節二）。
現況長什麼樣請讀 `graphify-out/GRAPH_REPORT.md`；設計取捨的白話背景請讀 `docs/design-principles.md`（那份給人讀，本檔給 agent 讀）。

本檔的條目分三類，**類型決定它對你的拘束力**：

| 類型 | 意義 | 你該怎麼用 |
|---|---|---|
| **事實** | 這個框架有什麼、沒有什麼 | 不是規則，沒有「違反」可言。但它決定哪些路走得通——別去嘗試框架不支援的做法 |
| **約束** | 可機械檢查的規則 | 違反即錯。每條都附檢查方式，由 `tests/test_core/test_architecture_constraints.py` 自動驗證 |
| **例外登記** | 已核准的破例清單 | 清單內的既有案例合法。**要新增一筆必須先取得使用者同意**，不得自行擴充 |

> 本框架是**手刻的 Kedro 風格實作**（`src/recsys_tfb/core/` 自製 DataCatalog／Node／Pipeline／Runner，`src/recsys_tfb/io/*` 仿 kedro.io），**無 kedro 套件依賴**。
> 下文凡引用 Kedro 官方立場者，基準為 **Kedro 1.5.0**，出處整理在 `docs/notes/2026-08-03-kedro-official-design-rationale.md`。
> 引用 Kedro 是為了說明我們為什麼一樣或不一樣，**不代表我們有義務跟它一致**。

---

# 節一 · 框架事實（動 `core/` 之前先讀）

## F1. 沒有 hooks 機制

`core/` 與 `io/` 對 `hook` 零命中。橫切關注點（logging、計時、錯誤處理）**直接實作在 `core/runner.py`**（7 處 `logger.` 呼叫）。

連帶後果：

- **不要試圖「加一個 hook」來做任何事**。要加橫切行為，只能改 `core/runner.py`，這是有意識的取捨——換來的是行為固定可預期，代價是擴充點只有一個。
- `io/base.py` 的 `load`／`save`／`exists` 是**公開** `@abstractmethod`。Kedro 用私有 `_load`／`_save` 加公開 wrapper，因為 wrapper 是它掛 hook 與 versioning 的地方；我們沒有要掛的東西，所以攤平沒有損失。**不要「為了對齊 Kedro」把它改成私有方法加 wrapper**——那會憑空增加一層而沒有對應的用途。
- Kedro 一整組 hook 相關約束（hook 參數不得有預設值、不得依賴 hook 執行順序、hook 間共享狀態須唯讀……）在本 repo **不適用**。

## F2. Observability 是強制的，不是可選的

Kedro 把 observability 當成 hook 的一種**使用場景**，也就是可以不裝。本框架把它做進 Runner：每個 node 執行時必定記錄 `node_started`／`node_completed`／`node_failed` 結構化事件（`core/runner.py:95-238`），失敗時帶 `exc_info=True`。

所以：**新增 node 時不需要自己寫「開始了／完成了」的 log**，Runner 已經記了。你該記的是 node 內部的業務判斷（跳過了什麼、選了哪條分支、處理了幾列）。

另有兩層執行中繼資料：`core/logging.py` 的 `RunContext`（run_id／pipeline／env／dataset_version／model_version）掛在每筆結構化 log 上；`core/versioning.py:330-377` 的 `build_manifest_metadata` 把 version／pipeline／created_at／git_commit／parameters 落成 `manifest.json`。

## F3. 只有 sequential runner

`core/runner.py` 只有一個 `Runner.run()`，循序執行。沒有 `ParallelRunner`、沒有多行程。

連帶後果：Kedro 為多行程而設的一整組約束（dataset 與 node 必須可 pickle、不得用 lambda／巢狀函式／closure、不能並用多行程的 dataset 要標記屬性）在本 repo **不適用**。

**但這是「現在不適用」，不是「永遠不必管」。** 若未來要加平行執行，`tune_hyperparameters`（`pipelines/training/nodes.py:520`）內嵌的 Optuna 閉包會是第一個擋路的東西——它不可 pickle。

## F4. Node 極薄：沒有 namespace、沒有 tags

`core/node.py` 全長 31 行，`Node` 只有 `func`／`inputs`／`outputs`／`writes`／`name` 五個屬性，**沒有任何驗證邏輯**（零 `raise`）。

`writes` 宣告「這個 node 自己會寫哪些 dataset」，語意對應 Kedro 的 `confirms`；機制與已核准清單見 A1 與 R1。

連帶後果：Kedro 靠 namespace 與 tag 做的事（分組執行、模組化 pipeline、`.` 分隔的命名空間規則）在本 repo 都不存在。dataset 名稱可以自由使用 `.`，因為沒有東西保留它。

節二的 A5／A6 兩條約束就是為了補上 Node 沒做的驗證——它們由稽核測試在測試期把關，不是由 `Node.__init__` 在建構期擋。

## F5. 切片語意：單一起點、自動上游擴張、**跳過零輸出 node**

- `--from-node` 與 `--only-node` 各只吃**一個** node 名稱，且**互斥**（`__main__.py:116`）。沒有多條件過濾，也沒有「只跑缺漏輸出」這種功能。
- 切片會透過 `can_load` 詢問 catalog：缺少的輸入會**自動把上游生產者拉回來執行**（`core/pipeline.py` 的 `_slice_with_expansion`），並在 `SlicePlan.auto_included` 回報。
- **零輸出的 side-effect node 會被跳過**，並印出 `[plan] skipped side-effect nodes (outputs=None, not re-validated)`（`__main__.py:137-141`）。

最後一條與 Kedro 相反——Kedro 的「只跑缺漏輸出」明文規定無 output 的 node **永遠跑**，因為沒有輸出可檢查、無法判斷 side effect 是否發生過。

**這件事有現實後果**：`dataset` pipeline 的第一個節點 `validate_data_consistency`（`pipelines/dataset/pipeline.py:28`）就是零輸出 node，它是 Layer-2 資料一致性閘。用 `--from-node` 接續 dataset pipeline 時，**這道閘不會跑**。框架會印警告，但不會阻止。

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

`pipelines/*/nodes*.py` 共 61 個頂層函式：

| 行數 | 個數 |
|---|---|
| ≤ 40 | 43 |
| 41–80 | 8 |
| 81–120 | 5 |
| 121–160 | 3 |
| > 160 | 2 |

最長的五個：`tune_hyperparameters` 228 行（`training/nodes.py:520`）、`predict_and_write_test_predictions` 172 行（`training/nodes.py:1082`）、`finalize_model` 155 行（`training/nodes.py:750`）、`prepare_eval_data` 140 行（`evaluation/nodes_spark.py:74`）、`validate_predictions` 134 行（`inference/nodes_spark.py:246`）。

**這是事實不是規則**——本檔不訂函式長度門檻。記錄它是為了讓你知道常態（七成的 node 函式在 40 行內），寫出第 62 個函式時心裡有個尺。行數本來就只是「這個函式只做一件事」的粗略代理，60 行可以混五種職責，130 行也可能只是一段長而平的轉換。

## F9. 測試的三個層次

| 層次 | 驗什麼 | 範例 |
|---|---|---|
| 單元 | 單一 node 函式的輸入輸出 | `tests/test_pipelines/test_training/test_nodes.py` |
| 整合 | pipeline 組起來跑得動、node 相依正確 | `tests/test_pipelines/test_training/test_pipeline.py` |
| 一致性 | `core/consistency.py` 的 predicate 本身 | `tests/test_core/test_consistency.py` |

改一個 node 函式的行為 → 單元層。改 pipeline 組裝或 node 相依 → 加整合層。加或改不變量 → 一定要動一致性層。

（測試怎麼跑得快、哪些是已知 failing，見 `CLAUDE.md` 與 `docs/operations/known-pitfalls.md`；本檔只講分層。）

## F10. 中介產物命名

`conf/base/catalog.yaml` 目前 42 個條目，命名全部帶業務語意，零 `tmp`／`_v2`／`_final`／`_new` 這類殘留。

新增 catalog 條目時對齊既有命名感覺即可——**這是事實不是約束**，因為禁用字清單只抓得到最粗糙的一類命名問題，真正的「有業務語意」是判斷題，機械檢查給不了。

---

# 節二 · 約束（新增或修改 node、catalog 條目之前先讀）

檢查由 `tests/test_core/test_architecture_constraints.py` 執行（17 個測試，約 0.5 秒）。

**兩個 A 系列不是同一套編號。** 本檔的 A1–A7 是**結構約束**（node 與 catalog 該長什麼樣，AST 稽核）；
`core/consistency.py` 的 A1–A24 是**設定不變量 predicate**（config 值彼此矛不矛盾，執行期 raise）。
兩邊的 A5、A7 已經在撞車、意思完全不同。**本 repo 不重編號**——重編號會讓既有文件與 commit message 的引用
全部指錯，理由同 A16/A17/A18 退休不回填（見 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 第四節）。
引用時請連模組一起寫（「consistency 的 A5」／「本檔的 A5」）。S 系列（structure）是為了不再增加撞車面而另起的前綴。

**這些約束的管轄範圍**：A1、A2、A5、A6、A7 只管 `src/recsys_tfb/pipelines/` 底下的 node 函式與 `Node(...)` 定義；
A3、A4 管整個 `src/recsys_tfb/`；S1、S2 只管 `pipelines/dataset/`。
**CLI 層（`__main__.py`）、`core/`、`io/` 不在 A1／A2 管轄內**——那幾層本來就負責 I/O 與程序級資源。

**檢查看得到什麼、看不到什麼**（不要把「測試綠」讀成「一定沒問題」）：

- 登記清單比對的是 **Counter**，所以「多一個同名站點」會被抓到；但不釘行號，所以站點上方的一般編輯不會誤報。
- A5／A6 對**動態組出來的** `inputs`／`outputs`／`writes` 無法判定，59 個 `Node` 中有 4 個是這種。
  `test_static_coverage_floor` 把「59 個裡有 55 個可判定」釘住，這個盲區不會悄悄變大。
- A1 的寫檔掃描只看得到**直接呼叫**（`open`／`mkdir`／`log_artifacts`…）。經由專案 helper 的間接寫入它看不到——
  `tune_hyperparameters` 正是這種，靠人工登記在 R4。

### A1. 資料流產物一律經 catalog；node 不得自己讀寫它們

pipeline 各節點之間傳遞的資料（會被下游 node 消費的東西）一律由 catalog 條目宣告，node 只做輸入到輸出的轉換。

- **例外一（資料流產物）**：`Node(writes=[...])` 列出的名稱，Runner 交給 node 的是 catalog **dataset 物件本身**而非載入後的資料（`core/runner.py:121-128`）。拿到它的 node 可以自行管理**這個 dataset 的分區寫入生命週期**——包含 `.save()` 寫入，以及查詢哪些分區已存在（`existing_partition_values()`）。**不含**把它當一般資料來源整批讀取（那該用普通 input）。
- 交出去的是**完整的 dataset 物件，不是 write-only proxy**：續跑要能反問「已經有哪些分區」，包裝成只能寫的東西就答不出來。
- **寫入目標必須是已註冊的 catalog 條目**（Runner 在啟動時檢查），光是「某個 node 的 output」不算。理由：`writes` 不建立拓撲相依邊，而啟動驗證是順序盲的——放行「由某個 node 生產」會讓生產者還沒跑時 `get_dataset()` 回 `None`，node 靜默拿到 `None`。擋在啟動階段，這條路就不存在。
- **`inputs` 位置對應、`writes` 以 keyword 綁定**：node 函式的參數名必須**逐字等於** dataset 名。這不是風格選擇——本 repo 的慣例是「新的可選 input 加在最後」（見 `pipelines/training/pipeline.py:203-206` 的 `log_experiment` 註解），位置綁定下照著做會把 dataset 物件擠到可選參數的槽位，而尾端的 `=None` 正好把 arity 錯誤吃掉，**不報錯**。keyword 綁定讓同一個錯誤在 node 執行前就 raise。（寫入目標的參數仍須排在所有 input 參數之後，因為 input 是位置填進去的。）
- 這個例外**必須寫在 pipeline 定義的 `writes` 參數裡**，不得在函式體內自己取得 catalog。理由與 Kedro 的 `confirms` 一致：**side effect 要用宣告的，不能藏在函式體裡**。`writes` 是獨立參數而非 `inputs` 裡的前綴字串，就是為了讓這件事在讀 pipeline 定義時一眼可見（issue #186）。
- **例外二（診斷副產物）**：有 3 個 node 會自己寫診斷／稽核檔（JSON 報表、HPO 診斷、MLflow artifact）。這些**不是資料流產物**——沒有任何 node 消費它們，它們寫進 model version 目錄或 MLflow。這是既有慣例，見 R4 登記。
- 兩類例外的已核准清單都在節三。**要新增一筆必須先問使用者。**

**檢查**：四項——(a) `Node(...)` 的 `writes` 內容必須與 R1 登記相符；(b) `Node(...)` 的 `inputs` 中不得再出現 `"@…"` 字串（舊 sigil 已移除，殘留的話 Runner 會把它當成一個不存在的 dataset 名而在驗證階段報錯，這條檢查讓它在測試期就指出檔案與行號）；(c) node 模組不得出現 `DataCatalog` 或 `catalog.load()`／`catalog.save()`（AST 比對，不是文字比對——這些字眼在註解裡合法出現）；(d) node 模組裡有直接寫檔呼叫的函式必須與 R4 登記相符。

### A2. node 函式不得依賴可變全域狀態

`pipelines/` 底下不得出現 `global` 宣告。

- 這條的界線是**層**而不是檔案：`core/` 與 `utils/` 管理程序級資源（一個 JVM、一份 run context），pipeline node 不管。
- 框架層那 5 處**不是 A2 的豁免例外，是根本不在 A2 的掃描範圍內**。它們另外由 R2 登記盯著——那份登記的作用是「框架層的全域狀態不會悄悄變多」，不是「A2 允許 5 個」。

**檢查**：`src/recsys_tfb/pipelines/` 底下 `global` 宣告數必須為 0。

### A3. 不得用 `print()`

`src/recsys_tfb/` 底下一律用 `logging.getLogger(__name__)`。理由見 F2——結構化日誌是這個框架的觀測基礎，`print` 出來的東西不帶 `RunContext`、進不了 log 聚合、也不會出現在失敗追蹤裡。

**檢查**：`src/recsys_tfb/` 底下 `print(` 呼叫數必須為 0。

### A4. `src/` 不得 import `notebooks/`

生產程式碼與探索程式碼單向隔離：notebook 可以 import `src/`，反過來不行。

**誠實說明**：`notebooks/` 目前只有兩個 `.ipynb`、沒有任何 `.py`、也沒有 `__init__.py`，所以 `import notebooks` 現在**根本不可能成立**——這條是預防性守衛（成本近乎零），不是在擋一個現存風險。真正該注意的是別把探索性程式碼搬進 `src/`，那個機械檢查抓不到。

**檢查**：`src/recsys_tfb/` 底下不得出現 `import notebooks` 或 `from notebooks`。

### A5. 每個 node 至少要有一個 input、一個 output，或一個 writes

三者皆空的 node 在這個框架裡不可能有作用——它拿不到資料，也不會被任何東西消費。

`writes` 算數：只宣告寫入目標的 node 拿得到 dataset 物件，也確實會產生效果，所以它不屬於「不可能有作用」那一類。

`core/node.py` 不做這個檢查（見 F4），由稽核測試把關。

**檢查**：AST 掃描 `pipelines/` 底下**所有** `.py` 的 `Node(...)` 定義（`rglob`，不只 `pipeline.py`）。掃描範圍止於 `pipelines/`——該目錄外的 `Node(...)` 建構看不到（現況為零）。

### A6. 同一 node 的 input／writes 名不得與 output 名相同

Runner 先載入全部 inputs 再執行、再存 outputs（`core/runner.py:127-142`）。名稱相同代表你打算原地覆寫一個 dataset，而執行順序讓這件事的語意不明確。要覆寫就用不同的 catalog 條目名，或明確走 `writes`（見 A1）。

**檢查**：同上 AST 掃描；`inputs` 與 `writes` 合起來跟 `outputs` 比對。

### A7. 零輸出的 side-effect node 必須登記

因為切片會跳過它們（見 F5），新增一個零輸出 node 等於新增一個「接續執行時會靜默不跑」的東西。這需要是有意識的決定。

**檢查**：`pipelines/` 底下所有 `.py` 中 `outputs=None` 的 `Node` 定義，必須與節三登記清單相符。

### S1. dataset 的每個 node 必須**定義**在 `pipelines/dataset/nodes.py`

`pipelines/dataset/pipeline.py` 中每個 `Node(...)` 的第一參數，必須是 `pipelines/dataset/nodes.py` 裡以 `def` 定義的名稱。

**是「def 定義」而不是「從 nodes.py import」**：後者有 re-export 漏洞——`nodes.py` 加一行 `from .sampling import some_step`，
`pipeline.py` 照樣「來自 nodes.py」、檢查全綠，而函式定義在別的檔，正是 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 要消滅的形狀。

**S1 只擋位置，擋不住內容。** 一個 12 行的轉手 node 加一個裝著四個決策的 helper 完全滿足 S1。
內容那一半沒有機械檢查，靠下方的判定程序 ＋ code review（這是 ADR-0008 已知的最大殘留風險，不是疏漏）。

**檢查**：AST 取 `nodes.py` 的 `FunctionDef` 名稱集合（不含 import），比對 `pipeline.py` 中所有 `Node(...)` 的第一參數。

### S2. `pipelines/dataset/month_plans.py` 不得 import pyspark

含函式體內的延遲 import。

守的不是風格，是**檔案切分的承重前提**：`steps/scoping.py` 之所以不能併進 `month_plans.py`，唯一理由就是這條純度；
而該模組 436 行的測試（`tests/test_pipelines/test_dataset/test_month_plans.py`）不需要 SparkSession 也是靠它——這個 repo 的 Spark cold start 是 2–4 分鐘。
一句沒有機制強制的「必須」會漂移（ADR-0002:21 那段三天就過時的補釘是前例）。

**檢查**：兩個測試，缺一不可。

1. **直接掃描**——AST 掃該模組所有 `Import`／`ImportFrom`（用 `ast.walk`，所以**函式體內的延遲 import** 一樣掃得到），root package 不得為 `pyspark`。
2. **可達性**——沿 `pipelines/dataset/` 的 import 遞迴一跳以上，任何路徑都不得抵達 pyspark。缺了這條，`from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date` 會被第 1 條讀成「import 了 `recsys_tfb`」而放行，但該模組已經是 Spark-typed 了——而這正是本 repo 實際在用的 import 寫法。**模組路徑必須按 `.` 展開成子路徑**（`...dataset.steps.scoping` → `dataset/steps/scoping.py`）：只取第一段會找到不存在的 `dataset/steps.py`，遞迴回傳「查無」，於是靜默放行——失效方向剛好是它要守的那一個。這條由一個「解析器跨得進子套件」的測試釘住，見 ADR-0008 第四節 2026-08-07 那段。

**刻意不驗**「`pyspark` 不進 `sys.modules`」。那才是真正想要的性質，但它因為與本模組無關的理由不可能成立：`pipelines/__init__.py` → `core` → `io` → `models` → `mlflow`，終點是 `mlflow/types/schema.py` 自己那行 `import pyspark`。S2 買到的是**結構**邊界——month_plans 不碰 Spark 型別，所以它的測試不需要 SparkSession，而 2–4 分鐘的成本是 session 不是 import。

### 動 dataset 的 node 之前：先讀 ADR-0008 第二節

S1／S2 管得到位置與純度，管不到「這個 node 讀起來說不說得出它做了什麼 ML 決定」。
那條線由 [ADR-0008 第二節](../adr/0008-dataset-modules-split-by-role.md) 的**兩條判準**與**判定程序**定義：

1. node body ＝ 具名步驟的組合，每個步驟名就是一個 ML 決策；
2. 一個 helper 至多承載一個決策；
3. 底線前綴 ＝ 只有本模組呼叫——`nodes.py` 呼叫得到的一律無底線（同節「底線前綴的判準」，同樣沒有機械檢查）。判準的範圍是 `pipelines/dataset/` 內部；**已知的界外違例**是 `nodes.py:77-80` 從 `recsys_tfb.preprocessing` import 的兩個底線函式，那個模組被 dataset 與 inference 共用，動它會碰到 inference（登記在 [ADR-0008](../adr/0008-dataset-modules-split-by-role.md) 的「這條 ADR 沒有解決的事」）。看到它不必以為判準是裝飾。

判定程序（把 helper 的名字換成純機械的名字，看 node 是否仍講得完整個 ML 故事）也在那一節。
**新增或修改 dataset node 前先讀那一節**——判準只活在 ADR 裡等於對執行者不可見，而這份檔案才是路由表指定的必讀檔。

**新模組放 `steps/` 還是根層**：只有 `nodes.py` 呼叫 → `steps/`；有 `pipelines/dataset/` 以外的 src 側消費者 → 根層（現況只有 `month_plans.py`，消費者是 `__main__.py`）。

---

# 節三 · 例外登記

**清單內的既有案例合法。要新增任何一筆，必須先取得使用者同意——不得自行擴充。**

## R1. `Node(writes=[...])`（A1 的例外）── 1 筆

| 位置 | dataset | 理由 |
|---|---|---|
| `pipelines/training/pipeline.py:146`（`writes=` 那一行） | `training_eval_predictions` | 逐 partition 存檔：每次 `.save()` 恰好寫一個 partition 的列，讓 dynamic-partition overwrite 乾淨覆蓋單一分區，避免整表重寫。改成由 catalog 統一寫入就得先把所有 partition 物化在記憶體裡，正好抵銷這個設計要省的東西。消費端在 `pipelines/training/nodes.py:1082-1253` |

**這個 dataset 物件上被用到的方法（不只 `.save()`）**：

| 方法 | 用途 | 位置 |
|---|---|---|
| `.save(pdf)` | 寫入單一 partition | `pipelines/training/nodes.py:1230` |
| `.existing_partition_values()` | 查詢哪些 partition 已寫過，供增量計畫判斷跳過哪些月份 | 呼叫在 `pipelines/training/nodes.py:1051`（經 `_written_prediction_partitions`），方法定義在 `io/hive_table_dataset.py:277` |

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

## R3. 零輸出 side-effect node（A7 的例外）── 2 筆

| 位置 | node | 被切片跳過的後果 |
|---|---|---|
| `pipelines/dataset/pipeline.py:28` | `validate_data_consistency` | **Layer-2 資料一致性閘不會跑**。用 `--from-node` 接續 dataset pipeline 時，資料層不變量未經檢查 |
| `pipelines/training/pipeline.py:202` | `log_experiment` | MLflow 實驗記錄不會寫。不影響產物正確性，影響可追溯性 |

（兩列的行號都指 `Node(...)` 第一參數、也就是函式名那一行，與 F5 的引用一致。）

## R4. 自己寫診斷副產物的 node（A1 的例外二）── 3 筆

這些寫的**不是資料流產物**——沒有任何 node 消費它們，所以不經 catalog。它們落在 model version 目錄或 MLflow。

| 函式 | 寫什麼 | 檢查看得到嗎 |
|---|---|---|
| `persist_sample_weight_report`（`pipelines/training/nodes.py:78`，註冊於 `pipelines/training/pipeline.py:93`） | `sample_weight_report.json` 進 model version 目錄（`mkdir`＋`open(w)`＋`json.dump`，`nodes.py:93-96`） | ✅ 直接呼叫，掃得到 |
| `log_experiment`（`pipelines/training/nodes.py:1256`） | MLflow params／metrics／artifacts（`mlflow.log_artifacts`，`nodes.py:1339`） | ✅ 直接呼叫，掃得到 |
| `tune_hyperparameters`（`pipelines/training/nodes.py:520`） | HPO 搜尋診斷進 `diagnostics_dir/hpo/`，經 `recsys_tfb.diagnosis.hpo.write_hpo_diagnostics`（呼叫在 `nodes.py:739`） | ❌ **間接寫入，掃描看不到**——靠這份登記人工盯著 |

另有一個非 node 的內部 helper 也會直接動檔案系統：`_materialize_parquet_handle`（`pipelines/training/nodes.py:275`，`shutil.rmtree` 於 `:318`／`:337`），管理本機 parquet cache。它在稽核清單裡，但不是 `Node(...)` 註冊的函式。

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 程式碼現在長什麼樣（模組關係、god node） | `graphify-out/GRAPH_REPORT.md` |
| 設計取捨的白話背景（給人讀） | `docs/design-principles.md` |
| 不變量代號 A 系列／B 系列的意義 | `src/recsys_tfb/core/consistency.py` 模組 docstring |
| Kedro 官方立場的出處與原文 | `docs/notes/2026-08-03-kedro-official-design-rationale.md` |
| 本檔各條目怎麼推導出來的 | `docs/notes/2026-08-03-kedro-vs-handrolled-gap-table.md` |
