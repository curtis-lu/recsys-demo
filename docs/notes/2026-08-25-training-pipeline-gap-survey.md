# training pipeline 對形狀判準的落差盤點（2026-08-25）

**這份是現況調查，不是決策。** 梯 4 那四條只寫「要問使用者」，不預先裁決。

基準：`main` @ `55be018`。判準＝[`docs/agents/pipeline-node-design.md`](../agents/pipeline-node-design.md)（13 條規則，本檔用它的編號）。流程＝[`docs/agents/pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)。

> **前提變更註記（本文正文一律不改，它是 2026-08-25 那天的快照）**：第 16 列
> 「從 `recsys_tfb.io.extract` import 三個底線私有名，沒登記在任何例外表（既有豁免只
> 涵蓋 `preprocessing` 的兩個名字…）」的**兩個前提今天都已失效**——`_pdf_to_X` 已於
> #199（2026-08-31）改名，`preprocessing` 那兩個名字已於 #254（2026-09-03）改名、
> 該筆豁免登記一併撤掉。〈已登記的例外〉現在是 2 筆，都與 `preprocessing` 無關。
> 這條註記是 #254 補的：那次的識別字 grep 掃不到這一列，因為它描述豁免、沒有寫出名字。

---

## 結論先行

1. **核心問題不是「node 太長」，是「node 裡沒有故事」。** 決策全部沉在 helper 裡：

   | pipeline | `# Decision` | `Pre-check` | `require_*` |
   |---|---|---|---|
   | dataset | 21 | 10 | 5 |
   | inference | 22 | 5 | 5 |
   | **training** | **0** | **0** | **0** |
   | evaluation | 0 | 0 | 0 |

   （實測指令：`grep -rc '# Decision' src/recsys_tfb/pipelines/<name>/`）

2. **最重的一條**是 `pipelines/training/nodes.py` 的 `_materialize_parquet_handle`：75 行裝 4 個決策，上面掛 4 個 node、其中 3 個只有 3–5 行——正是規則 3 明寫的失效形態。

3. **16 條落差分四梯**：梯 1–3（12 條）只動 `pipelines/training/` 內部；梯 4（4 條）會動到 `core/consistency.py`、catalog 或架構例外登記，**每條動手前都要問使用者**。

4. **calibration 預計拿掉（使用者 2026-08-25 告知），這會改變重構的順序。** 它在 `nodes.py` 佔 48 行、`pipeline.py` 佔 13 行——重整它等於整理即將被刪的程式碼。**建議先移除 calibration，再開始重構**，否則純搬移那張票的 byte-identity 證明會被後續的刪除動作作廢一次。受影響的落差條目已在下節逐一標明。

5. **兩處不是違例，不要順手修好**：`log_step` 的 14 個區塊全部合格（逐一追一層都找得到 action）；`select_features` 委派到 `models/feature_selection.py` 有明文理由（放進 `pipelines/` 會造成 `models/` → `pipelines/` 的反向依賴）。

6. **diagnosis 的介接口比預期乾淨**：使用者 2026-08-25 決定 **diagnosis 一律先留在 training**，切不切出去還沒有定論。盤點結果是真正的漏點只有兩個，而且兩個都是「拿掉一條跨界的邊」，不是「加一個新產物」。詳見末節。

---

## 一 · 16 條落差（另一條因 calibration 移除而不處理）

檔案路徑都相對於 `src/recsys_tfb/pipelines/training/`。

### 梯 1 — 只動 training 內部，不碰登記、不改測試斷言

| # | 位置 | 違反 | 現況 | 目標 |
|---|---|---|---|---|
| 1 | `nodes.py:1` | 規則 6、13 | 模組 docstring 寫 `"""Pure functions for the training pipeline."""`，但這個檔寫檔（:93）、mutate `parameters`（:233）、停 SparkSession（:550）、跑 MLflow（:1286）——**沒有一個是 pure function** | 比照 `inference/nodes.py` 開頭那段 |
| 2 | 全檔 | 規則 2、9 | `# Decision —` 零條，node body 是一串裸呼叫 | 每個具名步驟上方一段，說出決策與選錯的後果 |
| 3 | `nodes.py:357,362,369,1268` | 規則 13 | 四個 docstring 只重述簽章（`"Skip-if-exists local-parquet cache for X."` ×3、`"Log training results to MLflow."`） | 說「為什麼不是另一種做法」與「選錯會不會報錯」。**calibration 移除後 `:421`／`:913` 兩處一起消失，已從清單拿掉** |
| 4 | `nodes.py:302,324,404,1000,1125` | 規則 11 | 五個 `raise` 都沒標是**前置檢查**還是**後置條件**（兩者失敗時要找的人不同） | docstring 首行標明。其中 `:324` 是後置條件，其餘四個是前置 |
| 5 | 同上五處 | 規則 12 | 一個 `require_*` 守衛都沒有 | 改成 `require_*`／`warn_*` |
| 6 | `nodes.py:476,539-549,732-735,1273-1276` ＋ `pipeline.py` 八行 | 體例（註解一律英文） | **training 是唯一還有中文註解的 pipeline** | 翻英文。⚠ `:539-549` 是 SparkContext 死亡的現場記錄，**內容不得精簡** |
| 7 | `nodes.py:813-850` vs `:851-881` | 規則 5 | `finalize_model` 的 ranking／非 ranking 兩分支把 extract → concat → `lgb.Dataset` 幾乎逐字重複 | 決策各寫各的，concat 與 Dataset 建構的機制抽一份 |
| 8 | `nodes.py:603-666` | 規則 9 | 64 行的 Optuna `objective` 閉包內嵌在 `tune_hyperparameters` 裡，捕捉十個以上的外層變數 | 拉成具名 callable。**順帶消掉架構文件登記的「未來要平行執行時第一個擋路的東西」（不可 pickle）** |

### 梯 2 — 動 training ＋ 一個外部檔的 import 路徑

| # | 位置 | 違反 | 現況 | 目標 |
|---|---|---|---|---|
| 9 | `nodes.py:206-233` | 規則 8 | `inject_cache_source_tables` 是**非 node** 的公開函式卻住在 `nodes.py`，而 src 側外部消費者是 `__main__.py:53`／`:516`（pipeline 開跑前注進 parameters） | 搬到根層 `cache_sources.py`，與 `dataset/month_plans.py` 同型 |
| 10 | `search_space.py`（82 行）、`hpo_resume.py`（119 行） | 規則 8 | 在根層，但 src 側呼叫端**只有** `nodes.py`（`:570`、`:595`）。`core/consistency.py` 是自己重寫一份 schema 檢查，並未 import `search_space` | 搬進 `steps/`。規則 8 明說「測試不算」，所以測試 import 不是留在根層的理由 |

### 梯 3 — 建 `steps/`，會動到測試 import 但不動登記

| # | 位置 | 違反 | 現況 | 目標 |
|---|---|---|---|---|
| 11 | `nodes.py:956-1033` ＋ `:1036-1079` | 規則 3 | `_plan_predict_months` 78 行裝 4–5 個「這一次要不要重算」的決策 | 決策上浮，機制進 `steps/predict_months.py`。**這三者現況都不 import pyspark，可做成零 pyspark 純模組**，測試就不必開 SparkSession |
| 12 | `nodes.py:36-75`，呼叫端 `:78-101` | 規則 3 | `resolve_weight_diagnostics` 40 行，而呼叫它的 node 整段就一個呼叫 | 「哪些欄構成 weight key」「什麼算 unmatched」上浮，機制進 `steps/sample_weights.py` |

### 梯 4 — 動共用檔或例外登記，**每條動手前都要問使用者**

| # | 位置 | 違反 | 現況 | 為什麼要問 |
|---|---|---|---|---|
| 13 | `nodes.py:563,514,778` | 規則 11 | `hpo_objective` 與 `final_model_strategy` 是**純 config** 的合法值檢查卻留在 node body。**`final_model_strategy` 打錯要等整輪 HPO 跑完（`:772`）才炸**，一個 typo 賠掉整輪 | 要加 predicate 進 `core/consistency.py`（共用檔、編號要動）。判準本身是明文要求這樣做，不是新增例外 |
| 14 | `pipeline.py:92-96` | 規則 1 | `sample_weight_report` 是**死輸出**：catalog 無條目、零下游消費者。實質是零輸出 side-effect node 靠一個假 output 繞過登記 | 兩條路（拿掉 `outputs=` 進登記／給 catalog 條目）都動到例外登記或 catalog |
| 16 | `nodes.py:48,1117` | 規則 12 | 從 `recsys_tfb.io.extract` import 三個底線私有名，**沒登記在任何例外表**（既有豁免只涵蓋 `preprocessing` 的兩個名字，且只列 dataset 與 inference） | 改名會同時動到 inference 與兩個 diagnosis 檔；登記則要問使用者 |
| 17 | `nodes.py:275-349`，呼叫端 `:356,361,368,373`（calibration 移除前另有 `:420`） | 規則 3、5 | **最重的一條**：75 行 helper 裝 4 個決策（見下節），上面掛 4 個 node、其中 3 個只有 3–5 行 | 把 `shutil.rmtree` 搬進 `steps/` 會讓 `test_architecture_constraints.py` 的「寫檔函式集合恰好等於三筆」斷言**轉紅**。要嘛同步改登記（問使用者），要嘛把寫檔呼叫留在 `nodes.py`。**不得靠放寬那個 glob 解決** |

### 因 calibration 將移除而不處理（原落差 15）

**保留這條記錄，是為了讓後人知道它被看過而不是被漏掉。**

`pipeline.py:37`／`:118`：開 calibration 時 `finalize_model` 的輸出是 `trained_model`，而 **catalog 沒有這個條目** → `--from-node calibrate_model` 一定把 `finalize_model` 拉回重跑；在 `final_model_strategy: refit_on_full` 下那是一次完整 refit。`test_resume_contracts.py` 只釘了 `finalize_model`，所以這個成本現在沒有任何測試看得見。

**不修的理由**：整條 calibration 路徑要移除，補 catalog 條目或改接續契約都是投資在即將刪掉的程式碼上。calibration 若最後沒有移除，這條要放回梯 4。

### 判準沒涵蓋、但值得一提

- **20 處函式體內 import**（`nodes.py:46,48,88,437,480,552,569,570,595,737,784,785,787,814,852,914,1115,1117,1269,1378,1380`）。判準沒有規則管這個，但「import 那一行就說出這個步驟來自哪個 concern」在 training 整個失效。其中 `:785 import numpy as np` 重複 import 模組層已有的 `np`（`:11`）。
- **`compute_test_mAP_spark`（`:1355-1443`）完全沒有 `log_step`**，但裡面有三個真 Spark action。規則 10 只禁「包 lazy 區塊」、不要求「action 一定要被包」，所以**不是違例**；但這個 node 的時間去向不可見。
- **`log_step(f"partition_{snap_date}_{prod_name}")`（`:1198`）事件名隨資料變動**，log 聚合端會炸出 n_months × n_items 個不同的 step 名。

---

## 二 · node 清單與接續成本

`pipeline.py` 裡有 **21 個 `Node(...)` 建構**，其中 2 個（`cache_calibration_model_input`、`calibrate_model`）只在 calibration 開啟時加入——**calibration 移除後是 19 個，且 `create_pipeline` 的 `enable_calibration` 分支一併消失**。⚠ **沒有實跑 `--list-nodes` 對照**，這是逐個數出來的。

**產物落地狀況**：`best_params`／`best_iteration`／`hpo_best_model`／`model`／`evaluation_results`／`feature_statistics`／`feature_importance`／`shap_diagnostics`／`gain_ledger`／`quadrant_profiles`／`cases_manifest`／`training_eval_predictions` 有 catalog 條目；`preprocessor_view`／五個 `*_parquet_handle`（calibration 移除後四個）／兩個 `*_lgb_handle`／`shap_population`／`case_rows`／`predict_manifest`／`sample_weight_report` 是 memory-only。

**兩個規則 1／7 的疑慮**：

- `sample_weight_report`（`pipeline.py:92-96`）撈不出來看——見落差 14。
- `select_shap_population`（`:177-189`）一個 node 出兩樣東西、被兩個不同的下游各自消費，是規則 1 反方向「切開」的字面命中。**但兩個輸出共用同一次 Spark ranking，切開會重跑排序**——列為候選，不列為必修。

---

## 三 · `nodes.py` 1443 行的結構

**14 個 node 函式（894 行）＋ 10 個 helper（386 行）＋ 1 個 NamedTuple ＋ 6 個模組常數。**

四個最值得注意的 helper（「裝幾個決策」是語意判定，附信心）：

| helper | 行號 | 決策數 | 是哪幾個 |
|---|---|---|---|
| `_materialize_parquet_handle` | :275-349 | **4**（高信心） | ①什麼算 cache hit（只看 `_SUCCESS`、永不看新鮮度）②`force_refresh` 清不掉時硬失敗 ③半成品 cache 清掉重建 ④非 Spark 輸入 → TypeError |
| `_plan_predict_months` | :956-1033 | **4–5**（高信心） | ①config 是月份的權威、cache 只是資料源 ②「完整」＝item 集合相等而非「有 partition 就算」 ③`--rebuild-dates` 蓋過完整性 ④surplus partition 只 warn 但仍重做 ⑤設定的月份不在 cache → raise |
| `resolve_weight_diagnostics` | :36-75 | 2（中信心） | 純診斷、不改模型看到的資料，判定上偏機制；算它是因為呼叫端的 node 幾乎沒有別的內容 |
| `_written_prediction_partitions` | :1036-1079 | 2（中信心） | 兩個都是「往哪個方向錯」的決定 |

**`_resolve_cache_path`（:236-272）與 `_test_month_dir`（:108-117）是純機制，不是違例**——換成機械名不損失任何資訊。

### `steps/` 初步分檔建議

| 模組 | concern |
|---|---|
| `steps/local_cache.py` | driver-local parquet cache 的路徑與複製機制（含六個路徑常數；calibration 移除後 `_CACHE_PATH_LAYOUT` 少一筆） |
| `steps/predict_months.py` | 「這一次要 predict 哪些月」。**可做成零 pyspark 純模組** |
| `steps/search_space.py` | 宣告式 HPO 搜尋空間 → Optuna 取樣（從根層原樣搬入） |
| `steps/hpo_resume.py` | study 生命週期、checkpoint、search_id 身分 |
| `steps/hpo_scoring.py` | 一個 trial 的分數怎麼算 |
| `steps/refit.py` | train ＋ train_dev 併成一份 refit 用的 Dataset |
| `steps/sample_weights.py` | sample_weight 設定與實際訓練列的比對 |
| `steps/experiment_log.py` | 往 MLflow 記什麼 |
| **根層** `cache_sources.py` | pipeline 開跑前要算好、注進 parameters 的對外契約（**不是** `steps/`） |

---

## 四 · 7 個 node 的 `def` 在 `pipelines/training/` 之外

全部在 `src/recsys_tfb/diagnosis/model/` 底下：

| node | `def` 在哪 |
|---|---|
| `compute_feature_statistics` | `feature_stats.py:16` |
| `compute_feature_importance` | `importance.py:8` |
| `compute_quadrant_profiles` | `shap_cases.py:18` |
| `compute_quadrant_cases` | `shap_cases.py:120` |
| `compute_shap_diagnostics` | `shap_per_item.py:103` |
| `compute_gain_ledger` | `gain_ledger.py:342` |
| `select_shap_population` | `population_spark.py:12` |

這就是形狀判準〈已登記的例外〉裡「training 尚未依本檔重整」那一筆的實際清單（原文只說「部分 node」、沒有列舉）。

**邊界**：把它們搬進 `pipelines/training/nodes.py` 會撞上地雷圖——「SHAP／象限診斷搬到 evaluation」是使用者要另開 grill 的接縫問題。**這次重構只能處理「def 在哪」，不能處理「該屬於哪條 pipeline」。**

---

## 五 · diagnosis 的介接口現況

使用者 2026-08-25 決定：**diagnosis 一律先留在 training，切不切出去還沒有定論。** 以下是「未來要切得動的話，現在缺什麼」的盤點。

**介接口的可機械檢查定義**：7 個 diagnosis node 的每一個輸入，要嘛在 catalog 有條目，要嘛是 diagnosis 群自己產的。驗收＝`--from-node compute_feature_statistics` 單獨跑得起來。

**已經達標的**：`model`、`test_model_input`、`training_eval_predictions`、`preprocessor` 都有 catalog 條目。`shap_population`／`case_rows` 只在 diagnosis 群內流動，`predict_manifest` 是純排序依賴——三者都不擋切割。

**兩個漏點，都是「拿掉一條邊」而不是「加一個產物」**：

1. **`preprocessor_view`**（7 個裡有 5 個吃它）。它是 `apply_feature_selection(preprocessor, parameters)` 的純函式結果，兩個輸入都已落地。**inference 已經解過同一題**：`model_feature_columns()` 讓模型當「哪些特徵、什麼順序」的權威，preprocessor artifact 當「怎麼編碼」的權威——因為 `preprocessor.json` 是全集，說不出 `training.feature_selection.exclude` 有沒有動過。套過來之後 6 個 node 本來就有材料，只有 `compute_feature_statistics` 要多接一個 `model` 輸入。
   - **副作用**：`model_feature_columns` 現在住在 `pipelines/inference/steps/`，一旦有外部消費者就要換家（規則 8）。最自然的落腳處是 `models/`，與 `feature_selection.py` 作伴。
   - ⚠ **這不是修 bug，是未來準備。** 曾經以為它修掉一個靜默錯誤（config 漂移），查證後發現**版本擋得住**：`feature_selection` 住在 `training:` block，改它會 bump `model_version` → model 的 catalog 路徑跟著變 → 整條訓練鏈被拉回重跑。只剩「改 code 不改 config」那條，而那是全 repo 共通的已知風險。

2. **`train_parquet_handle` / `test_parquet_handle`**（2 個 node 吃）。`ParquetHandle` 就是一個 `path: str` 的 frozen dataclass，而路徑是 config 算得出來的（`_resolve_cache_path`），cache 又是 skip-if-exists。**所以 diagnosis 拿到 `parameters` 就自己算得出同一個路徑，根本不用別人傳。**
   - ⚠ **兩個要釘的細節**：`cache.root` 是相對路徑（`conf/base/parameters_training.yaml`），兩條 pipeline 從不同 CWD 啟動會指到不同地方；生產的 driver 是不是同一台**沒有證據**（`spark.master: yarn` 在 `conf/base/parameters.yaml` 是註解掉的，deployMode 只有 local 那份寫了 client）。若是 cluster mode，cache 每次 miss → 從 Hive 重灌，**慢但不會壞**。

**一條反向邊**：`log_experiment` 吃 5 個 diagnosis 產物。它的 10 個輸入**全部 catalog 落地**，所以技術上站哪邊都跑得動——真要切割時這是要決定的事。

---

## 六 · 地雷圖裡會擋到 training 重構的條目

出處全部是 [`docs/agents/deliberate-non-goals.md`](../agents/deliberate-non-goals.md)。**四條會直接擋住動線**：

1. **HPO 搜尋診斷必須留在 `tune_hyperparameters` 尾端，不得抽成 DAG node**——這樣對 `RESUME_CONTRACTS` 隱形，`--from-node finalize_model` 跳過 HPO 的行為不變。重構那段只能搬位置，不能升格成 node。
2. **`overall_map` 跨月合併與「SHAP／象限診斷搬到 evaluation」是同一個接縫問題，使用者決定先 grill 不先做。** 那 7 個 node 的歸屬不得在這次重構裡自行裁決。
3. **架構約束的例外登記加一筆必須先問使用者**，且「別為了讓自己的新程式碼合規而擴充登記」。
4. **架構稽核用檔名 glob 當「模組含不含 node」的代理，兩個方向都失準**（票 #163）。建了 `steps/` 之後稽核就掃不到那些檔——**不得順手放寬 glob**。

**動到相鄰區域會踩到的**：`release_spark_session` 那段中文註解是「HPO 後 SparkContext 被誰停掉」的現場記錄，搬動時整段要跟著走；`hpo_checkpointing`／`release_during_hpo`／`diagnostics` 刻意放頂層 config（放進 `training:` 會 bust `model_version`）；**calibration 預計移除**（使用者 2026-08-25 告知），不要重整那條路徑上的程式碼，也不要順手補它的洞；`sample_weight` 的多槽 `.bin` cache 延後中；`search_space` 不要擴成 (t, α) 參數化；已退場的診斷項目勿復活。

---

## 我不確定的部分

1. **node 數量沒有實跑 `--list-nodes` 對照**，是逐個 `Node(...)` 數出來的。近期有一個 commit 訊息提到「19 個 node 不是出廠實際值」，所以與預設 config 下的實際值可能有出入。
2. **「一個 helper 承載幾個決策」是語意判定。** 高信心的只有 `_materialize_parquet_handle` 與 `_plan_predict_months`，其餘標了中信心或不建議單獨開工。
3. **落差 17 的連鎖效應是從測試碼推的**，沒有實際搬檔跑一次看它是否轉紅。
4. **地雷圖裡「已退場的診斷項目」含 `quadrant`**，與 training 現行的 `compute_quadrant_profiles`／`compute_quadrant_cases` 撞名。判斷退場的是 `diagnosis/metric/` 那一族、training 這兩個是另一件事，但**沒有證據能證實**——動它們之前要問使用者。
5. **`docs/pipelines/training.md`（686 行）只讀了部分章節**，若其他章節另有與判準衝突的敘述，本盤點沒有涵蓋。
