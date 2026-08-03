# 2026-08-03 檔案退役前逐條比對：`kedro_design_philosophy.md`（檔 A）vs `docs/design-principles.md`（檔 B）

> 目的：判定檔 A（3a3f696，框架第一個 commit 寫下、此後從未更新、無任何反向連結）裡的每條原則現在是死是活，找出「檔 B 沒收錄、但程式碼現況確實在遵守」的搶救候選，再決定退役方案。
> 方法：**只採信程式碼證據**，不採信任一文件怎麼描述。逐條分類為 (a) 已收錄／(b) 活著未寫下／(c) 已推翻或從未實現／(d) 與檔B衝突／(e) 不是架構原則／(f) 無法判定。
> 檔案路徑：檔 A = `/Users/curtislu/projects/recsys_tfb/kedro_design_philosophy.md`；檔 B = `/Users/curtislu/projects/recsys_tfb/docs/design-principles.md`。行號皆指這兩份檔案讀取當下（2026-08-03）的內容。
> **⚠ 檔 A 的行號已位移**：同一次交付在檔 A 頂部加了 20 行的 deprecated banner，因此本文所有指向檔 A 的行號都要 **+20**。檔 B 的行號不受影響。

## 結論摘要

- **搶救清單（(b) 類）共 6 條**，詳見文末「搶救清單」一節，依重要性排序。
- **(d) 衝突：0 條**——逐條核對後，檔 A 與檔 B 在文字層級沒有互相打對台的宣稱；檔 A 的落差全部屬於「沒收錄」或「與程式碼現況不符」，不是「跟檔 B 講的相反」。
- **(f) 無法判定：3 條**（見下方各節），主要集中在「Rules for Refactoring」這類本質是行為指引、無法從現況程式碼驗證的段落，以及「## Guiding Principle」這種摘要式收尾語。
- **(c) 退役候選中最值得注意的一條**：`### 3. Build small, composable processing units` 與其呼應的反模式「giant training functions that do everything」——在最核心的 training pipeline 中有清楚反例，這條原則沒有被落實為強制規範，見下方詳細拆解。
- 三段 AI 協作指令（`## How Claude Should Think`／`## Preferred Collaboration Style`／`## Output Standard for Claude`）不歸架構原則管，且已被檔 B 與現行全域 `~/.claude/rules/` 制度取代（見文末 (e) 段）。

---

## 一、### 1–10 逐條比對

### ### 1. Separate transformation logic from I/O
**(a) 已收錄。** 對應檔 B §1「資料處理邏輯與 I/O 解耦」`docs/design-principles.md:43-56`：「node 使用資料集名稱...而不直接處理 Hive table 名稱、檔案路徑或儲存格式」「同一個 node 因此可以在不修改處理邏輯的情況下，更換：Hive database 或 table 名稱／partition 與版本 filter／...」。

**已知例外（判定時已算入）**：`core/runner.py:79-87` 的 `@` 前綴慣例讓 node 拿到 catalog dataset handle 自己做 I/O，全 repo 唯一使用點是 `pipelines/training/pipeline.py:140`（`"@training_eval_predictions"`），對應 `pipelines/training/nodes.py:1082-1253` 的 `predict_and_write_test_predictions`。此函式除了呼叫 `training_eval_predictions.save(out_pdf)`（`nodes.py:1230` 附近）之外，還做 partition 枚舉、增量計畫決策、特徵轉換與預測——**是這條原則唯一的、框架層級刻意支援的例外**，檔 B 完全沒有提到這個例外的存在。這點不影響「已收錄」的整體判定（B 的敘述在絕大多數 node 上成立），但檔 B 若要更新，這是一個值得補充的邊界案例。

### ### 2. Prefer pipeline-oriented design over script-oriented design
**(a) 已收錄。** 檔 B §1「以 pipeline 劃分責任」`docs/design-principles.md:8-22`：五個具明確邊界的 pipeline（source ETL/dataset/training/evaluation/inference），「pipeline 邊界同時也是責任邊界」。程式碼證據：`src/recsys_tfb/pipelines/{dataset,evaluation,inference,source_etl,training}/` 各自獨立成套，`core/node.py`／`core/pipeline.py` 提供 Node/Pipeline 抽象。

### ### 3. Build small, composable processing units
**混合證據，主判定 (c) 已被推翻或從未實現落實為強制規範（部分屬實，需拆解）。** 檔 B 沒有專門討論函式大小/單一職責的段落。程式碼證據顯示落實不一致：

- 遵守（小而單一職責、易獨測）：`pipelines/dataset/nodes_spark.py:30-41`（`select_train_keys`，12 行，純轉呼）、`pipelines/dataset/nodes_spark.py:162-182`（`_date_filter`，純函式）、`pipelines/training/nodes.py:356-368`（`cache_train_model_input` 等單行 wrapper）、`pipelines/dataset/nodes_shared.py:108-127`（`validate_date_splits`，20 行純驗證）。
- 違反（>80 行、明顯混雜多階段職責）：`pipelines/training/nodes.py:1082-1253`（`predict_and_write_test_predictions`，174 行，同時做 partition 枚舉＋增量計畫＋讀取＋特徵轉換＋預測＋Hive 寫入＋manifest 組裝）、`pipelines/training/nodes.py:750-904`（`finalize_model`，157 行，策略分派＋特徵抽取＋numpy 運算＋`lgb.Dataset` 構造＋呼叫 adapter）、`pipelines/training/nodes.py:520-749`（`tune_hyperparameters`，Optuna 搜尋迴圈與每 trial 建模評分邏輯全內嵌一個閉包，無法獨立測試）、`pipelines/evaluation/nodes_spark.py:74-215`（`prepare_eval_data`，142 行）、`pipelines/inference/nodes_spark.py:246-381`（`validate_predictions`，136 行）。

**判定理由**：違反案例集中且明確落在最核心的 training pipeline（訓練、預測、超參數搜尋這三個對正確性影響最大的環節），代表這條原則從未被落實為強制規範，不是零星意外。同一份反模式清單裡「giant training functions that do everything」也直接對應這個發現（見下方 Anti-Patterns 段）。因此**不建議搶救成一條規範性陳述**，但這個發現本身值得記錄在別處（例如未來重構 backlog），不是這次「文件退役」任務的範圍。

### ### 4. Make data flow explicit
**(b) 搶救候選（部分成立，拆解如下）。** 檔 B 沒有專門討論「避免 mutate 全域狀態／避免隱藏 side effect」的段落（B 的「node 只描述一段處理責任」`docs/design-principles.md:24-41` 討論的是 node 依賴宣告與拓撲排序，角度不同）。

- 「避免 node 層級可變全域狀態」：**確認活著**。全 repo 搜尋 `global` 關鍵字，pipeline node 函式本身（`pipelines/*/nodes*.py`）零命中。唯二命中在框架基礎設施層：`src/recsys_tfb/utils/spark.py:49,87,161,232`（`global _canonical_configs, _canonical_enable_hive, _last_app_id, _last_alive_ts`，管理 SparkSession 生命週期）與 `src/recsys_tfb/core/logging.py:139`（`global _current_context`，管理 run context）。這兩處都有明確用途且有註解說明，**不算違反「node 不應依賴可變全域狀態」的精神**——它們是框架層而非 pipeline node 層。
- 「避免隱藏 side effect」：`pipelines/training/nodes.py:1082-1253`（`predict_and_write_test_predictions`，同 ### 1 的例外）是唯一明確違反案例——它在 node 函式內部自己做 I/O，不是單純輸入轉輸出。
- 「讓每步產生/消費什麼容易理解」：與 ### 1／### 2 重疊，已由 B 的具名 catalog 依賴機制涵蓋（`docs/design-principles.md:24-41`）。

綜合判定為 **(b)**：以「pipeline node 不應 mutate 全域狀態」為代表主張，這件事在程式碼現況裡確實成立（僅有框架基礎設施層的、有註解說明的例外），但檔 B 完全沒有把這條寫下來過，值得搶救。

### ### 5. Keep configuration externalized
**(a) 已收錄，且程式碼驗證支持。** 檔 B §1「設定與 schema 也是公開合約」`docs/design-principles.md:58-69`。額外程式碼驗證：搜尋 `pipelines/*/nodes*.py` 內業務相關 magic number，僅發現一處低風險弱案例——`pipelines/training/nodes.py:253`：`root = Path(cache_cfg.get("root", "/tmp/recsys_cache"))`，fallback 值與實際設定 `conf/base/parameters_training.yaml:159`（`root: data/recsys_cache`）不同；但同檔 `pipelines/training/nodes.py:452` 走的是 `parameters["cache"]["root"]`（無 fallback，必填），正常路徑不會觸發這個 default。其餘候選（`random_seed=42`、`num_iterations=500`、`early_stopping_rounds=50`、`n_calibration_bins=10`，見 `pipelines/training/nodes.py:557-559`、`pipelines/evaluation/nodes_spark.py:582`）皆與對應 `conf/base/parameters*.yaml` 的顯式設定值一致，非隱藏預設。

### ### 6. Design for reproducibility
**(a) 已收錄，且是檔 B 篇幅最大的一節。** 檔 B §3「版本化設計」`docs/design-principles.md:137-196` 完整涵蓋：分層版本（`base_dataset_version`/`train_variant_id`/`calibration_variant_id`/`model_version`）、決定性抽樣（`docs/design-principles.md:180-184`）、manifest 記錄（`docs/design-principles.md:160-178`）。

### ### 7. Design for production, not only notebooks
**(b) 搶救候選。** 檔 B §2「生產限制反映在架構中」`docs/design-principles.md:126-135` 談的是無網路/無 UDF/CPU-only 這類**執行環境限制**如何反映在架構分工上，跟檔 A 這條「避免 notebook 風格程式邏輯混進生產模組」是不同角度，B 完全沒有觸及後者。

程式碼證據：`notebooks/` 目錄下只有 2 個檔案（`inspect_artifacts.ipynb`、`verify_metrics_spark.ipynb`）；`grep -rn "notebooks" src/` 唯一命中是 `pipelines/dataset/nodes_shared.py:139` 的一句註解（提到「driven outside the CLI (tests, notebooks)」，不是 import）；`src/recsys_tfb/` 下沒有任何模組 import 或依賴 notebooks 目錄的程式碼。結構化 logger 覆蓋 43 個檔案、`src/recsys_tfb/` 全域 `print(` 用量為 0，佐證沒有 notebook 風格的 ad hoc 輸出散落在生產模組裡。**這條原則現況確實遵守，且完全沒被寫下來過。**

### ### 8. Support partial reruns and modular execution
**(a) 已收錄。** 檔 B §6「可恢復執行」`docs/design-principles.md:286-343`，含 `--from-node`/`--only-node`/`--dry-run`/`--list-nodes` 機制說明（`docs/design-principles.md:298-314`）。程式碼佐證：`src/recsys_tfb/__main__.py:110` 起實作切片旗標。

### ### 9. Treat observability as a first-class concern
**(b) 搶救候選，且是搶救清單裡證據最扎實的一條。** 檔 B 沒有「observability」專屬章節；相關機制散落在 §3 版本化（manifest）與 §2 ModelAdapter 一句提及 MLflow logging（`docs/design-principles.md:113`），但檔 A 這條講的「logging is structured／monitoring can be inserted cleanly／execution boundaries are visible」這個**設計價值本身**從未被檔 B 明白陳述過。

程式碼證據——完整且多層：
1. 結構化 logger：43 個檔案使用 `logging.getLogger(__name__)`，`src/recsys_tfb/` 全域 `print(` 用量為 0。
2. 執行邊界可見：`core/runner.py:60-181` 對每個 node 記錄 `node_started`/`node_completed`/`node_failed` 結構化事件（`extra={"event":..., "duration_seconds":...}`），失敗時 `exc_info=True`。
3. MLflow 記錄 metadata：`pipelines/training/nodes.py:1283-1339`（`log_experiment`）呼叫 `mlflow.log_params`/`log_metric`/`log_artifacts`。
4. manifest 落地：`core/versioning.py:330-358`（`build_manifest_metadata`）記錄 `version`/`pipeline`/`created_at`/`git_commit`（`get_git_commit()`）/`parameters`；`core/versioning.py:212-219`（`write_manifest`）落成 `manifest.json`；`core/logging.py:20-33` 另有 `RunContext`（run_id/pipeline/env/dataset_version/model_version）供結構化 log 附掛。

### ### 10. Optimize for team readability
**(f) 無法判定（大部分子句是品味題，不強行下結論）。** 檔 A 這條包含「explicit naming」「low surprise」「clear interfaces」「comments that explain intent」等本質主觀的子句，無法用二元的程式碼證據判定整體是否「遵守」。可提供的客觀結構性事實：5 個 pipeline 子目錄（`dataset`/`evaluation`/`inference`/`source_etl`/`training`）都有 `__init__.py` + `pipeline.py`；4/5 都有某種 `nodes*.py`，唯獨 `source_etl` 因為是 SQL-driven 而非 Spark node function-driven，改用 `sql_runner.py`/`sql_renderer.py`/`checks.py`/`audit.py`/`models.py`（刻意的領域差異，非隨機不一致）。這只能佐證「folder 結構有一致 pattern」這一個子句，其餘子句（命名品味、註解品質、介面清晰度）沒有系統性稽核，誠實標成無法判定，不編造。

---

## 二、`## Rules for Writing Pipeline Logic` 逐條

### Prefer pure functions where possible
**(b) 搶救候選（連同已知例外一併記錄）。** 檔 B 完全沒有「pure function」這個框架，程式碼現況是「絕大多數 node 為純函式，但框架刻意提供一個具名的例外機制」：`core/runner.py:79-87` 的 `@` 前綴讓 node 拿 catalog handle 自己 I/O，唯一使用點 `pipelines/training/pipeline.py:140` → `pipelines/training/nodes.py:1082-1253`（`predict_and_write_test_predictions`）——這個函式同時做 I/O 與轉換，明確違反「node 應為純函式」的字面規則，但這是框架**刻意支援**的設計（分區級 Hive overwrite 避免整表重寫），不是意外違反。**這正是任務指示要求算入的例外**：搶救時應該把「預設純函式、`@` 前綴是唯一被允許打破純度的具名機制」一起寫下來，而不是照抄檔 A 的無條件「prefer pure functions」。

### Keep orchestration separate from computation
**(a) 已收錄（實質對應，措辭不同）。** 檔 B §1「node 只描述一段處理責任」`docs/design-principles.md:24-41` 講的正是這件事：`pipeline.py` 宣告 wiring（orchestration），`nodes.py` 放轉換邏輯（computation）。程式碼佐證：所有 5 個 pipeline 都遵循 `pipeline.py` + `nodes*.py` 兩檔分離的慣例（見 ### 10 的結構性證據）。`predict_and_write_test_predictions` 的 `@` 例外是對這條原則的局部混入，但不影響整體判定為已收錄。

### Name intermediate artifacts clearly
**(b) 搶救候選。** 檔 B 沒有專門的命名規則段落。程式碼證據：`conf/base/catalog.yaml` 68 個 dataset 名稱皆語意清楚（`feature_table`、`training_eval_predictions`、`ranked_predictions`、`gain_ledger` 等），`grep -in "tmp|_v[0-9]|_final|_new\b|_old\b" conf/base/catalog.yaml` 零命中，完全對應檔 A 的 good/bad 範例標準（好：`customer_features_30d` 風格；壞：`tmp1`/`result_final2`）。**這條命名品質是真實存在的、有具體檢驗證據的活規則，但沒有任何文件把它寫成規範。**

### Make stage boundaries obvious
**(a) 已收錄。** 對應檔 B 的 pipeline 責任表（`docs/design-principles.md:8-22`）與 Fail-fast 分層驗證表（`docs/design-principles.md:245-251`），兩處都清楚標示「原始資料進入／驗證／特徵建構／訓練／預測／發布」各階段邊界。

---

## 三、`## Rules for Machine Learning Pipelines` 逐條

### 保留 ingestion/cleaning/label/feature/split/train/eval/select/inference/publish 十階段的區隔
**(a) 已收錄（粒度較粗，仍算對應）。** 檔 B 的 5-pipeline 拆分（`docs/design-principles.md:8-22`）是這個原則的具體實作，只是粒度不同——B 在 pipeline 層級劃分（5 個），A 列到更細的子階段（10 個，例如 label generation、train/val/test split）。細部子階段確實存在於程式碼（`pipelines/dataset/` 底下處理 label 與切分），只是沒有被 B 用獨立小節逐一點名。判定為已收錄不強求逐字對應。

### Training and inference should be separated
**(a) 已收錄，且有明確程式碼佐證。** 檔 B §2「前處理採 fit／transform 分離」`docs/design-principles.md:86-97`：「之後再將相同 metadata 套用到 train、calibration、val、test 與 inference」「模型、test 預測與 inference 必須使用同一份 feature 順序與類別編碼」。程式碼證據：`pipelines/inference/nodes_spark.py:14` 直接 `from recsys_tfb.preprocessing._spark import apply_preprocessor`，`pipelines/inference/nodes_spark.py:129-135` 呼叫；training 側經 `pipelines/dataset/nodes_spark.py:280-299` 呼叫的 `apply_preprocessor_to_features`（實作於 `preprocessing/_spark.py:366-471`）。兩條路徑都呼叫**同一組** helper——`_encode_categoricals`（`preprocessing/_spark.py:85`，分別於 `447-450` 與 `564-565` 呼叫）與 `_cast_feature_floats_to_float32`（`preprocessing/_spark.py:47`，分別於 `529-530` 與 `573-574` 呼叫）——類別編碼與型別轉換只有一份實作，沒有重複邏輯。

### Parameters should be explicit
**(a) 已收錄。** 檔 B §1「設定與 schema 也是公開合約」`docs/design-principles.md:58-69` 與 §4「抽樣與權重由 profiling 輔助、業務目標決定」`docs/design-principles.md:208-219`。

### Outputs should be traceable
**(a) 已收錄，逐字對應。** 檔 B §3「manifest 記錄版本關聯」`docs/design-principles.md:160-178` 明確列出 manifest 記錄「本次版本與 pipeline／實際使用的參數／上游 dataset／variant 版本／建立時間與 git commit／產物清單」——與檔 A 這條「Outputs should be traceable to input data/parameter set/code version/run context」逐項對應。

---

## 四、`## Rules for Configuration` 逐條

**(a) 已收錄，且比檔 A 更明確化。** 檔 A 泛稱「avoid duplicated constants across files」，檔 B §5「一致性規則集中管理」`docs/design-principles.md:255-266` 把這個精神落實成更具體的規範：「同一條規則應只有一個 canonical predicate，再由 CLI、dataset 或測試重用，避免不同 pipeline 各自實作而產生訊息或語意漂移」，對應 `src/recsys_tfb/core/consistency.py` 作為唯一真實來源。「Configuration should help answer 環境/資料/參數/輸出」這組問題雖非逐字出現在 B，但「開發與正式環境的儲存位置」等環境變數概念見於 `docs/design-principles.md:47-52`。程式碼驗證（見 ### 5）未發現明顯 magic number 違規。

---

## 五、`## Rules for Testing` 逐條

**(b) 搶救候選。** 檔 B 完全沒有測試哲學／分層章節（專案 `CLAUDE.md` 的「測試」段落談的是**測試效能**——跑快不是少跑，跟檔 A 這條談的**測試分層策略**是不同主題，不能算已收錄）。程式碼證據——三層測試確實存在：
- 單元測試：`tests/test_pipelines/test_training/test_nodes.py:184`（`class TestComputeAP`，測單一函式）。
- 整合測試：`tests/test_pipelines/test_training/test_pipeline.py:167`（`class TestTrainingPipelineE2E`，用 `Runner` 串 dataset→training 兩條 pipeline 端到端跑）。
- schema/一致性專屬測試：`tests/test_core/test_consistency.py`、`test_consistency_compare.py`、`test_consistency_cli_wiring.py`，逐一測 `core/consistency.py` 的 predicate（例如 `TestConfigRoleConflicts`、`TestRankingObjectiveConflicts` 等 15+ 個 class）。

**這是一條有扎實三層證據、卻在任何現行文件裡都找不到書面陳述的活規則。**

---

## 六、`## Rules for Refactoring` 逐條

**(f) 無法判定（本質是行為指引，非現況宣稱）。** 這段講的是「Claude 重構時應該往哪個方向推」，不是對程式碼現況的宣稱，因此不存在可驗證的「現在是死是活」——沒有一個穩定的「目前正在進行的重構」樣本可供稽核。

但有一個重要的交叉發現值得記錄：檔 B 的「修改架構時的判斷順序」`docs/design-principles.md:345-357`（7 條檢查問題：這功能屬於哪個 pipeline／能否形成明確輸入輸出的 node／I/O 路徑欄名閾值是否該移到 catalog／產物是否需要跨次重用／設定是否該納入版本 hash／錯誤是否需要提早攔截／這是技術契約還是業務決策）**在結構與精神上直接對應**檔 A 的「## How Claude Should Think When Making Changes」（`kedro_design_philosophy.md:122-149`，6 步驟：pipeline stage／輸入輸出／該放哪個模組／能否拆小／是否硬編碼／10 倍成長後還能維護嗎）——B 的版本更成熟，額外納入了版本 hash、一致性檢查、人工決策邊界這些 A 寫成當時框架還不存在時無從得知的概念。**這強烈佐證檔 A 的「How Claude Should Think」段落已被檔 B 實質取代**，退役時不需要搶救（詳見文末 (e) 段）。

---

## 七、`## Anti-Patterns to Avoid` 交叉核對

大多數反模式已在上方各原則判定中處理過，此處只列交叉結果與**唯一一個新發現**：

| 反模式 | 現況 | 對應 |
|---|---|---|
| monolithic end-to-end scripts | 無殘留：`scripts/local_e2e.sh:21-23` 仍是依序呼叫 `python -m recsys_tfb dataset/training --env local`，走 pipeline 架構 | 對應 ### 2，(a) |
| notebook logic copied into production modules | 無殘留（見 ### 7 證據） | 對應 ### 7，(b) |
| business logic mixed with file/db access | 幾乎無殘留，唯一例外＝`@` 慣例（見 ### 1） | 對應 ### 1，(a) |
| hidden side effects | 幾乎無殘留，同上例外 | 對應 ### 4，(b) |
| hardcoded paths and table names | 無殘留：`grep "hive_metastore\|/data/\|\.parquet\"\|/Users/" pipelines/*/nodes*.py` 只命中 `pipelines/training/nodes.py:268`（`Path(f"{dataset_name}.parquet")`，`dataset_name` 是參數非寫死表名） | 對應 ### 5，(a) |
| **giant training functions that do everything** | **有殘留**：`pipelines/training/nodes.py:520-749`（`tune_hyperparameters`）、`pipelines/training/nodes.py:750-904`（`finalize_model`） | 對應 ### 3，**(c) 退役候選**——反模式確實存在於現況程式碼 |
| duplicated preprocessing logic across training/inference | 無殘留（見「Rules for ML Pipelines」段的證據） | 對應該條，(a) |
| weakly named intermediate outputs | 無殘留（見 catalog.yaml 命名證據） | 對應「Name intermediate artifacts clearly」，(b) |
| environment assumptions embedded in transformation code | 無殘留：環境限制被提升成架構分工決策，見 `docs/design-principles.md:126-135` | (a) |
| tightly coupled pipeline stages that cannot be rerun independently | 已否證：`src/recsys_tfb/__main__.py:110` 起實作 `--from-node`/`--only-node` | 對應 ### 8，(a) |

---

## 八、(d) 衝突清單

**0 條。** 逐條核對後，檔 A 與檔 B 在文字層級沒有互相矛盾的陳述。檔 A 相對於程式碼現況的落差，全部落在「B 沒收錄」或「與程式碼現況不符」兩類，沒有一條是「B 明確主張相反的事」。曾懷疑過的候選（B 允許獨立 node 依宣告順序執行 vs A 警告「不要依賴 cell 執行順序」）查證後不成立——B 講的是 DAG 中無資料相依的 node 之間、為了讓 guard node 優先執行而採用的**顯式**宣告順序慣例（`docs/design-principles.md:24-41`「獨立且沒有相依關係的 node 會依 pipeline.py 中的宣告順序執行」），跟 A 警告的 notebook cell 隱性執行順序耦合是不同性質，不構成衝突。

---

## 九、(e) 不是架構原則的段落

檔 A 的三段 AI 協作指令——`## How Claude Should Think When Making Changes`（`kedro_design_philosophy.md:120-149`）、`## Preferred Collaboration Style`（`kedro_design_philosophy.md:344-361`）、`## Output Standard for Claude`（`kedro_design_philosophy.md:365-376`）——性質是「Claude 應該怎麼跟這個 repo 協作」，不歸架構原則管，不逐條判死活。

值得記錄的一點：`## How Claude Should Think` 已被檔 B「修改架構時的判斷順序」（`docs/design-principles.md:345-357`）以更成熟的版本實質取代（見上方「Rules for Refactoring」段的交叉發現）；`## Preferred Collaboration Style` 與 `## Output Standard for Claude` 這兩段的精神（先想清楚驗收條件、先講架構再給程式碼、留意 trade-off）也已被現行全域 `~/.claude/rules/10-model-dispatch.md`、`20-judgment-rubrics.md` 等制度以遠更細緻的判準取代。三段都可以安心退役，不需要搶救任何內容。

檔 A 開頭的 `## Purpose`（`kedro_design_philosophy.md:1-16`，列出 modular/reproducible/testable/maintainable/environment-independent/production-easy-to-reason 六個目標）與結尾的 `## Guiding Principle`（`kedro_design_philosophy.md:380-384`，「Write this codebase as if it will be maintained by a team...」）都是摘要式收尾語，不是可獨立驗證的規則，這六個目標已分散被檔 B 全篇（尤其 §3 版本化、§6 可恢復執行）具體落實，不需要單獨判定。

---

## 搶救清單（(b) 類，依「不搶救就會失去什麼」排序）

1. **Observability 是一等關注點**——43 個檔案用結構化 logger、`runner.py` 對每個 node 記錄開始/結束/失敗事件、MLflow 記錄實驗 metadata、manifest 落地 git commit/參數/時間戳（`core/runner.py:60-181`、`core/versioning.py:330-358`、`pipelines/training/nodes.py:1283-1339`）。這是實作最完整、最貼近生產運維價值的一條，卻是**唯一一個從未在任何現行文件裡被明白陳述為「設計價值」的實踐**——不搶救的話，未來新增功能時沒有文件提醒「要不要加 log／要不要讓失敗可追溯」，容易在新模組裡漏掉。
2. **預設純函式、`@` 是唯一被允許打破純度的具名機制**——`predict_and_write_test_predictions`（`pipelines/training/nodes.py:1082-1253`）是全 repo 唯一一處 node 自己做 I/O 的案例，框架特地為它做了 `@` 前綴機制（`core/runner.py:79-87`）。這是一個真實存在、有明確取捨理由（分區級 Hive overwrite）的架構決策點，但目前只能從 runner.py 的程式碼註解看出來——不搶救的話，未來若有人想比照辦理（讓另一個 node 也自己做 I/O），沒有文件可查「這條路什麼時候該走、什麼時候不該走」。
3. **測試三層策略（unit／integration／schema-consistency）**——`tests/test_pipelines/test_training/test_nodes.py:184`、`test_pipeline.py:167`、`tests/test_core/test_consistency.py` 都是活的、扎實的分層測試，但檔 B 完全沒寫、專案 `CLAUDE.md` 也只談測試效能不談測試分層哲學——新貢獻者不知道「這個 repo 期待每種改動配哪種測試」，只能自己從既有測試檔案裡歸納。
4. **生產程式碼與 notebook 探索碼徹底隔離**——`notebooks/` 只有 2 個檔案，且零被 `src/` 引用（`pipelines/dataset/nodes_shared.py:139` 唯一命中僅是註解）。這條現況良好、風險相對低（目錄慣例本身已有天然隔離作用），但完全沒被寫成規範，新人可能誤把探索性程式碼直接放進 `src/`。
5. **中介產物命名要有業務語意**——`conf/base/catalog.yaml` 68 個名稱全部語意清楚、零 `tmp`/`_v2`/`_final` 殘留，是活生生的好範例，但沒有任何文件把它寫成「規則」，只能靠新貢獻者自己抓既有 catalog 條目的命名感覺模仿。
6. **pipeline node 不得依賴可變全域狀態**——node 層級零全域可變狀態污染（僅框架基礎設施層兩處有註解說明的例外：`utils/spark.py:49,87,161,232`、`core/logging.py:139`）。現況良好但檔 B 從未提及；優先度較低是因為這條偏底層實作細節，且既有的兩個例外本身已經有程式碼註解自我說明，不搶救造成的風險相對可控。

---

## 沒做到或不確定的事

- 「Rules for Refactoring」與檔 A 開頭 `## Purpose`／結尾 `## Guiding Principle` 這類摘要/行為指引性質的段落，沒有强行套入 (a)-(f) 分類，而是說明為什麼無法／不需要這樣判定（見上方對應段落），這是刻意的處理方式，不是漏做。
- `### 10. Optimize for team readability` 的多數子句（命名品味、註解品質、介面清晰度、"low surprise"）因為本質主觀，沒有做全面稽核，只驗證了「folder 結構一致性」這一個可觀察的子句，其餘標為 (f) 無法判定，如上文所述。
- 沒有對 repo 之外的任何來源（例如 git blame 找出檔 A 是否曾被其他 PR 引用過）做額外考古；「無反向連結」與「commit 3a3f696 之後從未修改」兩個事實是任務給定的已知前提，本次沒有重新驗證，直接採信。
