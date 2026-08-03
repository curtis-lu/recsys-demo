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

Kedro 把 observability 當成 hook 的一種**使用場景**，也就是可以不裝。本框架把它做進 Runner：每個 node 執行時必定記錄 `node_started`／`node_completed`／`node_failed` 結構化事件（`core/runner.py:60-181`），失敗時帶 `exc_info=True`。

所以：**新增 node 時不需要自己寫「開始了／完成了」的 log**，Runner 已經記了。你該記的是 node 內部的業務判斷（跳過了什麼、選了哪條分支、處理了幾列）。

另有兩層執行中繼資料：`core/logging.py` 的 `RunContext`（run_id／pipeline／env／dataset_version／model_version）掛在每筆結構化 log 上；`core/versioning.py:330-358` 的 `build_manifest_metadata` 把 version／pipeline／created_at／git_commit／parameters 落成 `manifest.json`。

## F3. 只有 sequential runner

`core/runner.py` 只有一個 `Runner.run()`，循序執行。沒有 `ParallelRunner`、沒有多行程。

連帶後果：Kedro 為多行程而設的一整組約束（dataset 與 node 必須可 pickle、不得用 lambda／巢狀函式／closure、不能並用多行程的 dataset 要標記屬性）在本 repo **不適用**。

**但這是「現在不適用」，不是「永遠不必管」。** 若未來要加平行執行，`tune_hyperparameters`（`pipelines/training/nodes.py:520`）內嵌的 Optuna 閉包會是第一個擋路的東西——它不可 pickle。

## F4. Node 極薄：沒有 namespace、沒有 tags

`core/node.py` 全長 19 行，`Node` 只有 `func`／`inputs`／`outputs`／`name` 四個屬性，**沒有任何驗證邏輯**（零 `raise`）。

連帶後果：Kedro 靠 namespace 與 tag 做的事（分組執行、模組化 pipeline、`.` 分隔的命名空間規則）在本 repo 都不存在。dataset 名稱可以自由使用 `.`，因為沒有東西保留它。

節二的 A5／A6 兩條約束就是為了補上 Node 沒做的驗證——它們由稽核測試在測試期把關，不是由 `Node.__init__` 在建構期擋。

## F5. 切片語意：單一起點、自動上游擴張、**跳過零輸出 node**

- `--from-node` 與 `--only-node` 各只吃**一個** node 名稱，且**互斥**（`__main__.py:116`）。沒有多條件過濾，也沒有「只跑缺漏輸出」這種功能。
- 切片會透過 `can_load` 詢問 catalog：缺少的輸入會**自動把上游生產者拉回來執行**（`core/pipeline.py` 的 `_slice_with_expansion`），並在 `SlicePlan.auto_included` 回報。
- **零輸出的 side-effect node 會被跳過**，並印出 `[plan] skipped side-effect nodes (outputs=None, not re-validated)`（`__main__.py:137-141`）。

最後一條與 Kedro 相反——Kedro 的「只跑缺漏輸出」明文規定無 output 的 node **永遠跑**，因為沒有輸出可檢查、無法判斷 side effect 是否發生過。

**這件事有現實後果**：`dataset` pipeline 的第一個節點 `validate_data_consistency`（`pipelines/dataset/pipeline.py:31`）就是零輸出 node，它是 Layer-2 資料一致性閘。用 `--from-node` 接續 dataset pipeline 時，**這道閘不會跑**。框架會印警告，但不會阻止。

## F6. `--env` 的覆蓋層語意會靜默退化

`ConfigLoader._load()`（`core/config.py:140-147`）讀 `conf/base` 再深度合併 `conf/<env>`。而 `_load_yaml_dir` 在目錄不存在時**靜默回傳空 dict**（`core/config.py:131-132`），`--env` 本身是自由字串、沒有任何驗證（`__main__.py:531`）。

所以現況是：

- `conf/` 底下只有 `base`／`spark-local`／`sql`，**沒有 `conf/local/`**。`--env local` 讀的是不存在的 `conf/local/`，實際效果等同「只用 base」。
- **打錯環境名不會有任何錯誤訊息**，只會拿到 base 設定。被交代「用 `--env <某環境>` 跑」時，不要把「跑完沒報錯」當成「讀到那個環境的設定」。
- `conf/spark-local/` **不是**環境覆蓋層。它只有 `spark-defaults.conf` 與 `spark-env.sh`，是給 Spark 自己讀的 `SPARK_CONF_DIR`。名字跟 Kedro 的 `conf/local`（使用者專屬、不進版控）很像但語意相反——**本 repo 的 `conf/spark-local` 是進版控的**。

> 這個靜默退化是已知缺陷，修法（在 `core/consistency.py` 加一條 A 系列 predicate 檢查 `conf/<env>` 存在）已另開 **issue #153**，不在本檔範圍。

## F7. 我們有、Kedro 沒有的抽象

| 模組 | 行數 | 解決什麼 |
|---|---|---|
| `core/consistency.py` | 1374 | 不變量 predicate 的**唯一真實來源**（A 系列 config-static／B 系列資料閘） |
| `core/versioning.py` | 415 | 三層 hash 版本 ID |
| `core/logging.py` | 303 | `RunContext` 與結構化日誌 |
| `core/schema.py` | 189 | 欄位角色集中定義 |
| `core/safe_eval.py` | 141 | HPO 宣告式搜尋空間的受限求值（stdlib `ast`，無額外套件） |

其中 `consistency.py` 值得單獨講：**本框架的正確性重心不在 node 契約，而在集中式 predicate**。量體對比很直白——`consistency.py` 1374 行，`node.py` 19 行。Kedro 把正確性押在「node 是純函式且輸入輸出宣告清楚」，我們押在「所有不變量集中成可測試的 predicate」。

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

七條全部可機械檢查，且**目前全綠**。檢查由 `tests/test_core/test_architecture_constraints.py` 執行。

### A1. node 函式不得自己做 I/O

I/O 由 catalog 條目宣告，node 只做輸入到輸出的轉換。

- **例外**：input 名稱加 `@` 前綴時，Runner 交給 node 的是 catalog dataset **handle** 而非載入後的資料（`core/runner.py:79-87`），node 可以自己呼叫 `.save()`。
- 這個例外**必須寫在 pipeline 定義的 `inputs` 裡**，不得在函式體內自己取得 catalog。理由與 Kedro 的 `confirms` 一致：**side effect 要用宣告的，不能藏在函式體裡**。
- 已核准的使用點見節三登記。**要新增一筆必須先問使用者。**

**檢查**：`pipelines/*/pipeline.py` 中 `"@…"` 字串的出現位置與數量，必須與節三登記清單完全相符。

### A2. node 函式不得依賴可變全域狀態

`pipelines/*/nodes*.py` 不得出現 `global` 宣告。

- **例外**：框架基礎設施層（SparkSession 生命週期管理、run context）的 5 處，見節三登記。
- 這條的界線是**層**而不是**檔案**：`core/` 與 `utils/` 管理程序級資源，pipeline node 不管。

**檢查**：`src/recsys_tfb/pipelines/` 底下 `global` 宣告數必須為 0。

### A3. 不得用 `print()`

`src/recsys_tfb/` 底下一律用 `logging.getLogger(__name__)`。理由見 F2——結構化日誌是這個框架的觀測基礎，`print` 出來的東西不帶 `RunContext`、進不了 log 聚合、也不會出現在失敗追蹤裡。

**檢查**：`src/recsys_tfb/` 底下 `print(` 呼叫數必須為 0。

### A4. `src/` 不得 import `notebooks/`

生產程式碼與探索程式碼單向隔離：notebook 可以 import `src/`，反過來不行。

**檢查**：`src/recsys_tfb/` 底下不得出現 `import notebooks` 或 `from notebooks`。

### A5. 每個 node 至少要有一個 input 或一個 output

兩者皆空的 node 在這個框架裡不可能有作用——它拿不到資料，也不會被任何東西消費。

`core/node.py` 不做這個檢查（見 F4），由稽核測試把關。

**檢查**：AST 掃描 `pipelines/*/pipeline.py` 的所有 `Node(...)` 定義。

### A6. 同一 node 的 input 名不得與 output 名相同

Runner 先載入全部 inputs 再執行、再存 outputs（`core/runner.py:82-99`）。名稱相同代表你打算原地覆寫一個 dataset，而執行順序讓這件事的語意不明確。要覆寫就用不同的 catalog 條目名，或明確走 `@` handle（見 A1）。

**檢查**：同上 AST 掃描；比對時 `@` 前綴會被去除後再比。

### A7. 零輸出的 side-effect node 必須登記

因為切片會跳過它們（見 F5），新增一個零輸出 node 等於新增一個「接續執行時會靜默不跑」的東西。這需要是有意識的決定。

**檢查**：`pipelines/*/pipeline.py` 中 `outputs=None` 的 `Node` 定義，必須與節三登記清單相符。

---

# 節三 · 例外登記

**清單內的既有案例合法。要新增任何一筆，必須先取得使用者同意——不得自行擴充。**

## R1. `@` handle（A1 的例外）── 1 筆

| 位置 | dataset | 理由 |
|---|---|---|
| `pipelines/training/pipeline.py:140` | `training_eval_predictions` | 逐 partition 存檔：每次 `.save()` 恰好寫一個 partition 的列，讓 dynamic-partition overwrite 乾淨覆蓋單一分區，避免整表重寫。改成由 catalog 統一寫入就得先把所有 partition 物化在記憶體裡，正好抵銷這個設計要省的東西。消費端在 `pipelines/training/nodes.py:1082-1253` |

> **待辦**：目前 `@` 是混在 `inputs` 清單裡靠 sigil 辨識，看起來像個輸入。比照 Kedro 的 `confirms`，更好的形狀是在 `Node` 上開一個獨立參數（例如 `writes=[...]`），讓「這個 node 會寫什麼」在定義上一眼可見。這是框架變更，已另開 **issue #154**。

## R2. 框架層可變全域狀態（A2 的例外）── 5 筆

| 位置 | 變數 | 用途 |
|---|---|---|
| `core/logging.py:139` | `_current_context` | run context 的程序級單例 |
| `utils/spark.py:49` | `_canonical_configs`, `_canonical_enable_hive`, `_last_app_id`, `_last_alive_ts` | SparkSession 生命週期 |
| `utils/spark.py:87` | `_canonical_configs`, `_canonical_enable_hive` | 同上 |
| `utils/spark.py:161` | `_last_alive_ts` | 同上 |
| `utils/spark.py:232` | `_last_app_id`, `_last_alive_ts` | 同上 |

這些管理的是程序級資源（一個 JVM、一份 run context），本質上就是單例。pipeline node 沒有這種需求。

## R3. 零輸出 side-effect node（A7 的例外）── 2 筆

| 位置 | node | 被切片跳過的後果 |
|---|---|---|
| `pipelines/dataset/pipeline.py:31` | `validate_data_consistency` | **Layer-2 資料一致性閘不會跑**。用 `--from-node` 接續 dataset pipeline 時，資料層不變量未經檢查 |
| `pipelines/training/pipeline.py:196` | `log_experiment` | MLflow 實驗記錄不會寫。不影響產物正確性，影響可追溯性 |

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 程式碼現在長什麼樣（模組關係、god node） | `graphify-out/GRAPH_REPORT.md` |
| 設計取捨的白話背景（給人讀） | `docs/design-principles.md` |
| 不變量代號 A 系列／B 系列的意義 | `src/recsys_tfb/core/consistency.py` 模組 docstring |
| Kedro 官方立場的出處與原文 | `docs/notes/2026-08-03-kedro-official-design-rationale.md` |
| 本檔各條目怎麼推導出來的 | `docs/notes/2026-08-03-kedro-vs-handrolled-gap-table.md` |
