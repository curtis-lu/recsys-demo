# Staged Modeling PR-D（文件）實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development（成本調度照 `~/.claude/rules/30-delegation-templates.md` 追加護欄：互不相依檔案合批、審查設里程碑）。
>
> Spec：`docs/superpowers/specs/2026-07-23-staged-modeling-design.md` §10 PR-D 列——「training.md 章節、README、design-principles、相關手冊交叉引用；驗收＝fresh 讀者驗收（handbook 風格規範）」。
> Stacked base：`feat/staged-diagnostics`（PR #119）；本分支 `feat/staged-docs`；merge 順序 #117→#118→#119→本 PR。

**Goal:** 讓一個「只有 repo、沒讀過 spec 與開發對話」的資料科學家，能從文件理解、設定、執行、驗收 staged 兩階段建模模式。

**Architecture:** training.md 加一個整合章節（新 §10）為唯一深度來源；README／design-principles／inference.md 只做輕量提及＋連結回該章節，不複述（CLAUDE.md「只路由不複述」原則）。

**必讀規範（每個執行 agent 都要遵守）：**
- `docs/handbooks/handbook-writing-guide.md`——本計畫已把關鍵條款內化為下方「風格鐵則」，執行時照鐵則走即可，不確定時回頭查該檔。
- 全部繁體中文；識別字（config 鍵、node 名、檔名、JSON 鍵、log 訊息）逐字對齊程式碼，寫之前 grep 核實，不憑本計畫記憶。

## 風格鐵則（違反即 FAIL）

1. **不洩漏開發脈絡**：使用者文件中不得出現 PR 編號、spec 檔路徑、內部代號（D1–D15、A21 除外——A21 是 consistency legend 的正式代號，可用）、「本期／日後」這類開發時間軸措辭。寫「目前僅支援 binary」即可，不寫「本期固定 binary、預留擴充」。
2. **為什麼先於做什麼**：每個設定／機制先一句動機再講操作。通用原理（per-group 建模的取捨）與專案落地（本框架的 staged 模式）分開講。
3. **不下結論、不過度宣稱**：staged 是否比 shared 好是實驗問題，文件只講機制與取捨，品質判斷留給讀者的評估指標。
4. **對齊既有體例**：training.md 既有章節的表格式寫法（設定表含「版本影響」欄、node 表、症狀排查表）照抄結構。
5. **抽象框架定位**：銀行產品只是示例；例子沿用 conf 既有示例值（`prod_name`），並保持示例語氣。

## 與 spec 的已知偏差（文件必須照「實作」寫，不照 spec 示意寫）

| spec 說法 | 實作事實 | 文件寫法 |
|---|---|---|
| `stage1/groups_index.json` | index JSON 寫在根 `model.txt`（重用檔名讓 catalog `model` entry 與載入路徑不變）；`stage1_groups.json`（根目錄）是訓練 report | 照實作 |
| §8「每 worker 的 LightGBM num_threads 配額化」 | 未實作；只有 `max_workers` 跨群平行＋大群先進 pool＋群內 trial 序列 | 不得宣稱配額；如實寫 `num_threads` 沿用 `training.algorithm_params` 全域值，`max_workers>1` 時使用者自行注意超訂 |
| 手冊交叉引用（雙向） | 手冊 offline HTML export 無可重跑腳本，動手冊本體會讓 export 靜默漂移 | **單向**：staged 章節連到手冊；不改手冊檔（此偏差經 controller 2026-07-27 裁決） |
| config 形狀示意 `staged.stage1.hpo.search_space: {...}` | 實際是 ParamSpec list（`conf/base/parameters_training.yaml:132-154`） | 照 conf 實際內容 |

## 事實清單（已對 code 核實，含 檔案:行號；寫文件前仍要 grep 複核你實際引用的每個識別字）

### F1 設定面（`conf/base/parameters_training.yaml:132-154`）
`training.model_structure: shared|staged`（缺省 shared；shared 時 staged 區塊完全忽略、A21 不驗其內容）。`training.staged.stage1.{partition_keys, objective, hpo.{n_trials, metric, search_space}, params, gates.{max_groups, min_rows, min_positives, min_negatives}, max_workers}`；`training.staged.stage2.{mode, oof_folds, params}`。全部位於 `training:` 區塊 → 進 `model_version` hash（既有機制）。`stage1.hpo.search_space` 同 `training.search_space` 的 ParamSpec 格式；`stage1.params`／`stage2.params` 覆蓋 `algorithm_params` 的基底參數（stage2 的 objective 由 mode 決定）。**Stage-2 HPO 讀 flat `training.*` 鍵**：`n_trials`、`hpo_objective`、`search_space`（`staged_stage2.py:125-137`）——與 shared 模式同一組鍵。

### F2 一致性檢查（`core/consistency.py`，legend `:116-132`）
A21 error：model_structure 非法／staged 區塊空／`stage1.objective != binary`／`stage2.mode` 非 {none,binary,lambdarank}／`oof_folds` 非 int≥2／staged 時 calibration 必須關閉／`hpo.metric` 非 {auc,logloss}／`hpo.n_trials` 非負 int／partition_keys 空、含 label|score|rank|time|entity 角色欄、或不在允許集合（`schema.item` ∪ `dataset.carry_columns` ∪ 宣告 categorical）。A21 known quirk：partition key 若是宣告 categorical 欄，model_input 中已整數編碼 → 群鍵以編碼字串呈現（如 `"3"`），非原始類別值。WARN（log 不 raise，`:596-619`）：partition_keys ≠ `sample_group_keys`（群內抽樣比例可能不均勻）；stage2=none 且 partition key 含 item（跨模型分數可比性，實驗對照模式）。

### F3 訓練流程與節點（`pipelines/training/pipeline.py:243-406`）
`create_pipeline` 依 `model_structure` 分派；staged 兩形狀由 `stage2.mode` 決定。共通：`select_features` → `cache_train/train_dev/test_model_input`（`cache_val_model_input` 只在 stage2≠none）→ `persist_sample_weight_report` → `train_staged_model` →（stage2≠none 時 `train_stage2_model`）→ `predict_and_write_test_predictions` → `compute_test_mAP_spark` → 診斷 → `log_staged_experiment`。shared 專屬節點（`prepare_lgb_train_inputs`、`tune_hyperparameters`、`finalize_model`、`calibrate_model`、`log_experiment`）staged 不用。診斷分岔：stage2≠none 走既有全套（`compute_feature_importance`／`compute_gain_ledger`／`compute_shap_diagnostics`／`select_shap_population`／`compute_quadrant_profiles`／`compute_quadrant_cases`）＋`compute_stage1_overview`；stage2=none 走 `compute_stage1_overview`＋`compute_staged_group_diagnostics`；`compute_feature_statistics` 兩形狀都有。

### F4 Stage-1 訓練語意（`models/staged/train_stage1.py`、`pipelines/training/staged.py`）
單次 extract 後記憶體切群；每群 in-memory Optuna（無 resume、無 search_id），sampler 種子由 `random_seed`＋群鍵派生（每群固定）、群內 trial 序列（確定性）；trial 評分＝該群 **train_dev** 子集的 AUC/logloss（val 保留給 Stage-2 HPO、test 兩階段皆不碰）。`n_trials: 0`＝不搜、直接用 `params`。跨群平行 `max_workers`（ThreadPoolExecutor；LightGBM 訓練釋放 GIL），群序按 train 列數**大群先跑**（`staged.py:115`）。泛化契約＝temporal（val/test 是時間切分，entity 可與 train 重疊；entity 互斥只存在於 train vs train_dev），與 shared 相同。

### F5 資料閘（`models/staged/gates.py`）
`check_stage1_gates` 在 `train_staged_model` 內、訓練前觸發：`max_groups` 超限、每群 train 與 train_dev 的 `min_rows`／`min_positives`／`min_negatives`、只出現在 train_dev 的 orphan 群；collect-all 一次 raise `StagedGateError`（`"stage-1 data gates failed (N issue(s))"`）。stage2≠none 另有 `check_oof_gates`：每（群 × held-out fold）的 fit set 需 ≥1 正例與 ≥1 負例。門檻設寬即近似不擋。

### F6 Stage-2 語意（`models/staged/stage2.py`、`pipelines/training/staged_stage2.py`）
OOF：entity-hash（crc32）互斥 K 折（`oof_folds`，≥2）；訓練列的 Stage-1 分數由「不含該 entity 的折模型」產生，serving／val 用全量 refit 模型。Stage-2 訓練矩陣＝`[X | stage1_score | partition_gcode]`（兩欄**尾端追加**，保住 Stage-1 categorical index）；`partition_gcode`＝群鍵在 `sorted(group_keys)` 中的名次（sorted-rank，不落地 mapping），宣告為 categorical，`stage1_score` 不宣告。已知取捨（stacking 標準作法）：Stage-2 訓練吃 OOF 分數、val 與 serving 吃 full-refit 分數——regime 不同，但 val 與 serving 同 regime，選參評分與上線行為一致。Stage-2 HPO 比照 shared：persistent study、resume、`--fresh-hpo`、搜尋診斷（`hpo/`）；`search_id` 只涵蓋 Stage-2 搜尋。

### F7 產物布局與載入（`models/staged/adapter.py`、`partition.py`；catalog `catalog.yaml:191-199,258-264`）
```
data/models/<model_version>/
  model.txt            # staged 下＝bundle index JSON（index_version/bundle_id/partition_keys/groups/stage2）
  model_meta.json      # algorithm: "staged"、adapter_class
  stage1/<slug>.txt    # 每群一個 booster；stage1/.bundle_id
  stage2/model.txt     # stage2≠none 時；stage2/.bundle_id
  stage1_groups.json   # 訓練 report：partition_keys ＋ 每群 {best_params, score, metric, n_rows, n_pos, train_seconds}
```
slug＝群鍵值經 `[^A-Za-z0-9_.-]`→`_`、截 40 字元、後接 `_{crc32:08x}`。寫入原子性：`stage1.tmp-<bundle_id>`／`stage2.tmp-<bundle_id>`／`model.txt.tmp-<bundle_id>` 全寫完才 rename，index 最後寫（＝commit 記號）。載入時驗完整性（index 宣告群 vs 實際模型檔、`.bundle_id` 一致性），不符 fail-fast（`"staged bundle failed integrity check"`），擋混合 bundle。

### F8 checkpoint 與重跑（`staged.py:18-85`、`staged_stage2.py:86-101`）
群級 checkpoint 在 `data/models/_staged_wip/<model_version>/<slug>/`（`model.txt`＋`meta.json`＋`_SUCCESS`）；OOF 另有 `<slug>/oof/`（`scores.npy`＋meta，n_rows/n_folds/seed 全 match 才 restore）。重跑同 `model_version` 時已完成群直接載回（log `"restored from checkpoint"`）；群內 HPO 中斷＝該群整段重搜（粒度＝群完成）。**非 catalog dataset、不自動清理**，確認 bundle 發布後可手動刪。`--fresh-hpo` 只清 Stage-2 persistent study（`data/models/_hpo/<search_id>/`），不影響 Stage-1 與群 checkpoint。

### F9 診斷產物（`diagnosis/model/staged.py`、`pipelines/training/nodes.py:1050-1107`）
`diagnostics/stage1_overview.json`：`partition_keys`／`n_groups`／`total_rows`／`total_positives`／`total_train_seconds`／`groups[]`（`group, n_rows, n_pos, pos_rate, metric, score, train_seconds, best_params`）；stage2≠none 時另有 `stage2` 區塊（`mode, oof_folds, oof_rows, n_groups, best_params`）。stage2=none：每群四件落 `diagnostics/groups/<slug>/`（`feature_importance.json`、`gain_ledger.json`、`feature_statistics.json`、`shap_top_features.json`＋best-effort `shap_summary.png`）；`diagnostics/staged_groups_manifest.json` 每群 entry 含 `group/error/n_train_rows/n_shap_sampled/seconds`——單群失敗隔離（error 記錄、其他群照跑）；per-group SHAP 有 tree-count 預算閘（`sample_rows × n_trees > max_budget` 時自動降抽樣，WARN）。stage2≠none：既有診斷全套掛 **Stage-2 booster**（`stage1_score` 在 importance／SHAP 中直接可見＝「Stage-1 分數有多重要」視角）。MLflow：`log_staged_experiment` 單一 run——params（`model_structure/algorithm/partition_keys/n_groups/stage2_mode`＋stage2 best_params）、metrics（`overall_map`、`map_attr_<item>`、`n_queries`、`n_excluded_queries`）、`diagnostics/` 整目錄 artifacts；best-effort（`mlflow.strict` 控制）。

### F10 未見分群值分流（`adapter.py:112-126`、`nodes_spark.py:151-293`、catalog `:363-368`）
- evaluation 路徑（training test 預測、stage-2 val 組裝、診斷）：`on_missing="raise"` → `StagedMissingGroupError`——test 與 train 同一份 sample_pool build，缺群＝drift 或版本錯置的異常訊號。
- inference 路徑：`on_missing="skip"`＋兩層 WARN（adapter 層＋`predict_scores` 層「the candidate universe SHRANK…retrain to cover new groups」）；結構化缺群統計寫 `data/inference/<model_version>/missing_groups.json`（`model_structure/missing_groups/rows_skipped/rows_total`；shared 模型也寫、`missing_groups` 為空）。全部列都被 skip 時 raise。
- 前置 schema 檢查：partition_keys 欄位缺於 scoring 資料＝schema 問題，fail-fast（非跳過）。

### F11 抽樣與權重
staged 吃同一份 dataset 產物；分群鍵⊆identity（如 `prod_name`）時 dataset 零變更，entity 側欄位須入 `dataset.carry_columns`（觸發 dataset 重建，與 `sample_weight_keys` 同型契約）。各群平衡靠既有 `sample_group_keys`＋sampling overrides，無 staged 專屬抽樣。權重：Stage-1 各群繼承自己子集的列權重、Stage-2 繼承全量（`staged.py:50-55`、`staged_stage2.py:289,324`）；`persist_sample_weight_report` 兩形狀都跑。

### F12 stage2=none 分數可比性
最終排序在 query（time × entity）內跨 item 比分數。分群鍵只含 entity 側欄位→同 query 全落同一模型，無可比性問題；含 item→同 query 候選由不同模型評分，各群負樣本下採比例不同時機率估計系統性偏移、跨模型比較有偏——定位為實驗對照模式（A21 WARN），promote 前由人工確認（promote 本為人工步驟）。

---

## Task 1：training.md 新增 staged 章節＋既有段落接點

**Files:** Modify `docs/pipelines/training.md`

- [ ] **Step 1 插入新章節**：在 §9（限制與注意事項）之後、原 §10（相關文件）之前插入 `## 10. Staged modeling（兩階段建模模式）`；原 §10 改為 `## 11. 相關文件`。小節結構（10.1–10.8，每節內容以上方 F1–F12 為準）：
  - **10.1 定位與適用情境**：opt-in（`model_structure: staged`，預設 shared 行為零變更）；一句話流程；何時考慮——shared 診斷顯示某些 item 依賴明顯不同的特徵組合（`item_idiosyncrasy` 偏離高）且其離線指標偏弱時（呼應 §6.2 第 7 點）；通用取捨——per-group 模型消除群間負遷移、也一併放棄正遷移，值不值得取決於群間相似度，理論見手冊 [`../handbooks/gbdt/gbdt_learning_to_rank.md`](../handbooks/gbdt/gbdt_learning_to_rank.md)（per-item 模型段）；staged 好壞是實驗問題，由評估指標說話。
  - **10.2 設定方式**：完整 config 區塊（照 conf 實際鍵）＋逐鍵表（含版本影響欄：全部在 `training:` 下 → 進 `model_version`）；partition_keys 允許集合與 A21（F2，含 categorical 編碼 quirk 與 entity 側欄位須入 carry_columns）；Stage-2 HPO 用 flat `training.*` 鍵這一點要明寫（易誤會成 `staged.stage2` 下有 HPO 鍵）；gates 表。
  - **10.3 訓練流程**：staged node 表（仿 §5 體例，標注兩形狀差異）；split 衛生（trial 評分＝train_dev、val 留 Stage-2、test 不碰）；泛化契約＝temporal（F4）；OOF 與 Stage-2 特徵組裝（F6，含 stacking regime 取捨的誠實說明）；效率（max_workers、大群先跑、群內序列＝確定性；`num_threads` 沿用 `algorithm_params`，無 per-worker 配額）。
  - **10.4 stage2=none 的分數可比性**（F12）。
  - **10.5 產物布局與載入**（F7 樹狀圖＋atomic 寫入＋載入完整性驗證；明寫「staged 下根 `model.txt` 是 bundle index JSON，非 LightGBM 模型檔」）。
  - **10.6 版本化、checkpoint 與重跑**（F8；`search_id` 只涵蓋 Stage-2、Stage-1 無 resume；Stage-1 實搜參數的可稽核處＝`stage1_groups.json`／overview）。
  - **10.7 診斷產物**（F9；含三情境對照表：shared／staged+stage2／staged+none）。
  - **10.8 評估與推論的未見分群值**（F10）。
  - **10.9 常見錯誤與排查**：仿 §8 三欄表，staged 專屬 rows——A21 各類、`StagedGateError`、OOF gate、bundle integrity check 失敗、`StagedMissingGroupError`、inference 缺群 WARN／missing_groups.json、checkpoint 未生效（`model_version` 變了）。
- [ ] **Step 2 既有段落接點**（全部只加一句話＋連結，不改既有語意）：§1 總覽段（`:18` 附近「訓練一個跨 item 共用的模型」後）補「另有 opt-in 的 staged 兩階段模式（每群獨立模型＋可選第二階段），見 §10」；`:285` 與 `:486` 的「兩階段模型」提及後補「（框架已內建，見 §10）」式連結；§3.6 校準段補「staged 模式下 calibration 必須關閉（A21）」一句。
- [ ] **Step 3 自查**：全文 grep 風格鐵則違禁詞（`PR-`、`spec`、`本期`、`D1`–`D15`、簡體字）；逐一核對你寫進文件的每個識別字（`grep -rn` 對 src/ 與 conf/）；檢查內部 anchor（§10 引用、§11 改號後 `sampling-overrides-editor.md` 引用的 §3.5／§7 不受影響——確認你沒動到 §3–§7 編號）。
- [ ] **Step 4 Commit**：`docs(training): staged modeling 章節（設定/流程/產物/診斷/缺群分流）`

## Task 2：README＋design-principles＋inference.md 輕量接點

**Files:** Modify `README.md`、`docs/design-principles.md`、`docs/pipelines/inference.md`

- [ ] **Step 1 README**：
  - §2 training pipeline bullets（`:137-145`）加一條：「**可切換的兩階段建模（staged）**：`training.model_structure: staged` 時依設定欄位把訓練資料分群、每群獨立訓練與搜參，可選第二階段（binary／lambdarank）或直接以第一階段分數排序；預設 `shared` 行為不變。詳見 [`docs/pipelines/training.md`](docs/pipelines/training.md) §10」（措辭可修飾，資訊點不減）。
  - §5 FAQ 在 Q3 之後插入新 Q「shared 一個模型 vs staged 每群一個模型，怎麼選？」：先答預設 shared＋何時考慮 staged（診斷訊號）＋取捨一句（正負遷移）＋連結 training.md §10 與手冊；原 Q4–Q6 順推編號（先 grep README 確認無他處引用 Q 編號）。
  - 修 3 處失效手冊連結（`:414`、`:433`、`:463`）：`docs/handbooks/gbdt_*.md` → `docs/handbooks/gbdt/gbdt_*.md`（`:463` 一行含 4 個連結全修）；修完逐一 `ls` 驗證目標存在。
- [ ] **Step 2 design-principles.md**：
  - `:124`「日後擴充至 composite（兩階段）模型時…」——此事已實現，改寫為現在式：staged（兩階段）模型即經由此接縫掛上診斷（Stage-2 booster、staged 專屬輸入組裝），上層診斷邏輯不變；連結 training.md §10。
  - `:228` 診斷 bullet 的「per-item 或兩階段模型的優化方向」後補「（框架已內建 staged 模式，見 [`pipelines/training.md`](pipelines/training.md) §10）」。
  - §6 可恢復執行（`:284` 起）補一句：staged 模式另有群級訓練 checkpoint（`data/models/_staged_wip/`），重跑時已完成群直接載回；連結 training.md §10.6。
- [ ] **Step 3 inference.md**：§5.3（`:284-292`）之後補一小段「Staged 模型的分群路由」：載入的 staged 模型依列的分群鍵值路由至對應第一階段模型（有第二階段時再疊加）；scoring 資料缺分群鍵欄位＝schema 錯誤 fail-fast；未見過的分群值＝跳過＋WARN，缺群統計寫 `data/inference/<model_version>/missing_groups.json`，全部列被跳過則中止；連結 training.md §10.8。
- [ ] **Step 4 自查**：同 Task 1 Step 3（違禁詞、識別字、連結逐一驗證）。
- [ ] **Step 5 Commit**：`docs: staged 交叉引用（README bullet+FAQ+失效手冊連結修復/design-principles/inference 路由段）`

## Task 3：fresh 讀者驗收＋修訂

- [ ] **Step 1**：派 fresh-context reader agent（opus）扮演「熟 LightGBM／排序建模、第一次用本框架、沒讀過任何內部規劃」的資料科學家，通讀 training.md §10 全章＋Task 2 改動段落。檢查面向（handbook guide §11）：卡關處、主旨一致性／過度宣稱、抽象未落地、鷹架洩漏（開發脈絡代號）、識別字正確性（親自 grep 抽查 ≥8 個）、連結有效性、範疇失衡。要求列 ≥3 個具體問題（附 檔案:行號），找不到則逐項列出檢查過程。
- [ ] **Step 2**：依回報分「真缺陷（修）／可加強（斟酌）／誤讀（不改，但若是誤讀代表文字有歧義、考慮改寫）」處理，修訂後 commit。

## Task 4：收尾

- [ ] graphify rebuild 不需（純 docs）；`git diff feat/staged-diagnostics..HEAD --stat` 確認只動 4 個文件檔＋本計畫檔。
- [ ] 開 PR（base=`feat/staged-diagnostics`）：標題 `docs: staged modeling 使用者文件（PR-D）`；body 含偏差清單（手冊單向引用、num_threads 配額未實作故未記載、bundle index 落點照實作）。
- [ ] 更新 memory `project_two_stage_stacking.md`。

## Self-review 記錄（controller，2026-07-27）

- Spec §10 PR-D 四項覆蓋：training.md 章節（Task 1）、README（Task 2）、design-principles（Task 2）、手冊交叉引用（Task 1 的 10.1 單向連結；偏差已記錄）。fresh 讀者驗收（Task 3）＝spec 驗收欄。
- 佔位掃描：無 TBD／待補；F1–F12 全部有 檔案:行號。
- 識別字一致性：F7 的 `stage1_groups.json` 鍵序與實測 e2e 產物（mv a8f8bc76）核對過；F9 overview 鍵與 `diagnostics/stage1_overview.json` 實物核對過。
