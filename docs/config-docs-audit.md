# Config × Docs 盤點報告

> 對 `conf/base/*.yaml` 的設定與註解，盤點三件事：(Q1) 未在活文件說明、(Q2) 說明落點應搬移、(Q3) 註解寫錯或與 docs/README 不一致。
> 方法與目標文件架構見 [`superpowers/specs/2026-06-12-config-docs-audit-design.md`](superpowers/specs/2026-06-12-config-docs-audit-design.md)。

## 基準與方法

- **程式基準**：`origin/main` @ `52567ed`（含 PR#81 HPO 崩潰復原）。本報告所有「實際行為」均以此版 `src/` 為準。
- **仲裁原則**：對不對以**程式行為**為準，不以 docs。每筆 Q3 附 `file:line` code 證據。
- **活文件集合**：`README.md`、`docs/pipelines/*`、`docs/operations/*`、`docs/change-guide.md`、`docs/design-principles.md`、`docs/handbooks/*`、生成檔 `docs/metrics.html`（指標概念）。`docs/superpowers/specs|plans/*` 為歷史快照、不納入。
- **severity**：高＝會誤導使用者做錯動作 / 文件與行為直接矛盾；中＝落點不清或長註解漂移風險；低＝措辭/完整度小瑕疵。

---

## Q1 — yaml 有、活文件未說明

> 「未說明」＝該 key 在活文件找不到對應說明（yaml 自己的註解不算）。多數 yaml 註解品質良好，問題是文件層沒有對應落點。

| 設定 key | 檔案 | 活文件落點 | 缺口 | sev |
|---|---|---|---|---|
| `diagnostics.*`（`feature_stats.sample_rows`/`high_null_threshold`、`shap.sample_rows`/`top_k`/`n_examples`/`min_rows_per_item`/`max_budget`、`feature_importance.enabled`） | training | training.md 只列診斷**節點**，未提這些**設定旋鈕** | 整個 diagnostics 設定塊無文件 | 中 |
| `final_model_strategy`（`hpo_best`/`refit_on_full`） | training | 無 | training.md 關鍵設定未提 | 中 |
| `n_trials` / `num_iterations` / `early_stopping_rounds` | training | training.md 僅泛談 HPO | 三個 HPO 數值旋鈕未列 | 低 |
| `mlflow.*`（`experiment_name`/`tracking_uri`/`strict`） | training | 無（training.md 只說 `log_experiment` 節點記 MLflow） | `strict` 的 best-effort 行為只在 yaml 註解 | 中 |
| `cache.root` | training | 無 | dataset.md §規模談 spark 旋鈕，未提 cache.root | 低 |
| `evaluation.product_categories`（大類平行評估整塊） | evaluation | metrics.html 概念提及 1 次；evaluation.md 關鍵設定**未列** | 一整個平行評估功能在 pipeline 文件無落點 | 中 |
| `evaluation.baseline.lookback_months` | evaluation | evaluation.md 節點表提 baseline，但**設定**未列 | popularity baseline 視窗無文件 | 中 |
| `evaluation.report.*`（`sections`/`display`/`diagnostics`/`n_calibration_bins`） | evaluation | 無（metrics.html 不含這些 key） | 報表開關/顯示設定無文件 | 中 |
| `evaluation.snap_date`、`k_values` | evaluation | evaluation.md 泛稱「k 值」、指標細節指 metrics.html | 兩個 key 無精確說明 | 低 |
| `parameters.yaml` `logging` 塊 | global | 無 | level/console/file 設定無文件 | 低 |
| `parameters.yaml` `random_seed`、`project_name`、`hive.db` | global | design-principles §6 概念提 seed；`hive.db` 在 catalog 被參照 | 三個全域 key 無正式說明 | 低 |

**小結**：缺口集中在 **training 的 `diagnostics` / `final_model_strategy` / `mlflow`** 與 **evaluation 的 `product_categories` / `baseline` / `report`**——都是「pipeline 文件關鍵設定 section 該收、但漏收」的旋鈕。依目標架構，補進各 pipeline 文件「關鍵設定」即可（不需新文件）。

---

## Q2 — 說明落點應搬移（依目標架構）

> 量尺＝design spec 的「資訊型別→正典」表 + 放寬後的原則 #1（短 gloss / 安全警語允許就地保留）。每筆給「現況 → 應落點 → 依據」。

1. **`parameters_evaluation.yaml` `compare_sources` 的 schema 長註解（72–95 行，約 24 行）** — sev 中高
   - 現況：把 A11 的完整欄位 schema（kind/label/model_version/source/table/columns/prod_mapping/unmapped_policy）逐項複寫在 yaml。
   - 應落點：欄位 schema 正典＝`consistency.py` A11（`compare_source_well_formed_errors`）＋ evaluation.md 的 kind 對照表。yaml 收斂成「標 A11 + 一句 gloss + 指 evaluation.md」，**保留**下方註解掉的 example（97–114 行，編輯時有用）。
   - 依據：「key 作用/合法值」正典在 pipeline 文件；此處是最長的單點重複，漂移風險最高。

2. **`parameters_training.yaml` `sample_weight_keys` 註解（39–50 行，約 12 行）** — sev 中
   - 現況：合法欄集合、與 `sample_group_keys` 對稱、encode-aware 機制、unmatched_keys 全寫在 yaml。
   - 應落點：作用/合法值→training.md；跨 pipeline 對稱（↔`carry_columns`）→ change-guide 情境4（**已在那**）。yaml 留 gloss + A9a/A9b/A9c tag + 「打錯靜默 no-op」安全警語。
   - 依據：跨 pipeline 連動正典在 change-guide；保留安全警語符合放寬原則 #1。

3. **`parameters_training.yaml` `feature_selection` 註解（81–93 行，約 13 行）** — sev 中
   - 現況：select_features node / preprocessor_view / `.bin` 快取路徑 / inference 端 feature_name() 全寫 yaml。
   - 應落點：機制細節→training.md。yaml 留 gloss +「只 bump model_version、不動 base」安全警語 + A14 tag。
   - 依據：機制屬「為什麼/怎麼運作」，正典在 pipeline 文件/design-principles。

4. **`catalog.yaml` MemoryDataset footgun 註解（309–313 行）** — sev 低中
   - 現況：重述「未註冊會 fallback MemoryDataset 靜默壞掉」，並寫死 `core/catalog.py:71` 行號。
   - 應落點：此 footgun 正典＝design-principles §9（**已完整說明**）。yaml 收斂為一句 + 指 §9；**移除寫死行號**（易漂移，見 Q3-附錄）。

5. **`parameters.yaml` `schema.categorical_values` 註解** — sev 低
   - 現況：解釋「為什麼這些值無法從 feature_table 推得、要顯式宣告」+「改動 bust base_dataset_version」。
   - 應落點：「為什麼」抽象→design-principles §1；「改動 bust base」是昂貴 gotcha → **就地保留安全警語**（放寬原則 #1）。

6. **`parameters_training.yaml` model_version scope 頂註（1–5 行）＋ `diagnostics` 頂註（148–150 行）** — sev 低
   - 現況：兩處各自重述 model_version hash 規則；design-principles §3 也有規則表。
   - 應落點：規則正典＝`versioning.py` + design-principles §3。兩處 yaml 皆已指回 versioning.py，屬合格 gloss → **維持**，僅確認 §3 為被引用正典即可。不需搬，列此供你判斷是否要再精簡。

---

## Q3 — 註解寫錯 / 過時 / 與 docs/README 矛盾

> 仲裁＝程式行為。每筆附 code 證據。

1. **evaluation.md:81「（schema evolution 是待辦）」與 catalog.yaml 矛盾，且與 code 不符** — sev 高
   - docs 說：segment_sources 改變「需先 drop 該表再重跑（schema evolution 是待辦）」。
   - 真實：`enriched_eval_predictions` 是 `columns: "auto"`，`HiveTableDataset` 會自動 append-only 演化。
   - 證據：`src/recsys_tfb/io/hive_table_dataset.py:31`（docstring）＋`:280 _evolve_schema`＋`:338 ALTER TABLE … ADD COLUMNS`；catalog.yaml:262–264 註解亦如此說（catalog 正確、evaluation.md 過時）。
   - 影響：使用者照 evaluation.md 會做多餘的 drop table。**建議改 evaluation.md**。

2. **dataset.md:64「carry_columns 只帶進 train / calibration（這兩個 split 才做加權）」寫錯** — sev 中
   - 真實（加權）：`sample_weights` 只套 **train / train_dev**，calibration **不**加權。
   - 真實（carry）：carry 實際落 train / train_dev / calibration（val/test 不帶）。
   - 證據：weight → `pipelines/training/nodes.py` `with_weights=True` 僅 train + train_dev（約 658/662、696/700）；carry → `pipelines/dataset/nodes_spark.py:34 select_sample_keys`、`:40 select_calibration_keys` 皆走 `helpers_spark.py:53 select_keys`（回傳含 carry）。
   - 矛盾對象：`parameters_training.yaml:55`「val/**calibration**/evaluation 不加權」與 dataset.md 直接相反。
   - 影響：誤導對「哪些 split 加權」的理解。**建議改 dataset.md**（calibration 不加權；carry 達 train/train_dev/calibration）。

3. **parameters_dataset.yaml carry_columns 註解（33–35 行）物理上不完整** — sev 低
   - 寫「帶進 train/train_dev model_input parquet」，但 code 顯示 calibration_keys 也帶（見上 §2 證據）。
   - 性質：以「用途」（供 train/train_dev 加權）論述正確，但物理落點漏 calibration。**可補一句**或維持（用途導向）。

4. **source_etl.md:37「本機 dev-cluster」為過時術語** — sev 低（docs-only，非 yaml）
   - dev-cluster 已退役（PR#69）；本機測試＝`--env local` + `SPARK_CONF_DIR=conf/spark-local`。**建議改 source_etl.md** 措辭。

5. **parameters_training.yaml:28 LTR 範例「metric: ndcg # 必填 ranking metric」措辭略誤導** — sev 低
   - A7 實際：metric **可省略**，省略時 `group_utils.default_metric_for_objective` 預設 `ndcg`（`consistency.py:262` 註解亦明示 unset 允許）。下一句「省略則預設 ndcg」已自我修正，但「必填」二字與之打架。**可微調措辭**。

### 附錄：順手發現（非 yaml/docs，供參考）

- **A9a 的 code 錯誤訊息不完整**：`consistency.py:523–528` 的 raise 文字只列「identity ∪ {label} ∪ dataset.carry_columns」，漏了 predicate 實際也接受的 `categorical_columns`（`weight_key_columns_unavailable` 第 407–414 行有含）。**yaml 註解（42–43 行）反而比 code 自己的錯誤訊息完整**。屬 code message 小瑕疵，修不修由你。
- **catalog.yaml 寫死行號 `core/catalog.py:71`（313 行）易漂移**：建議改引用符號（如「`DataCatalog` 對未知名稱 fallback MemoryDataset」）而非行號（併入 Q2-4 一起處理）。

---

## 建議處理順序（供你逐項確認）

1. **Q3-1（evaluation.md schema evolution 過時）** — 高，先修。
2. **Q3-2（dataset.md carry/加權寫錯）** — 中，先修。
3. **Q1：training `diagnostics`/`final_model_strategy`/`mlflow` + evaluation `product_categories`/`baseline`/`report`** — 補進各 pipeline 文件「關鍵設定」。
4. **Q2-1、Q2-2、Q2-3（三段長 yaml 註解瘦身 + 指回正典）** — 中，逐段處理。
5. **Q3-3/4/5、Q2-4/5/6、附錄** — 低，順手。
