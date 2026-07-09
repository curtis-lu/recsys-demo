# Phase 5：結構層（gain_ledger）＋條件化 SHAP background＋triage 總表 — 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 診斷框架的最後一階段（框架診斷項目 8、9；判讀第 3 步定型）：(a) **Gain 帳本**——從 LightGBM booster 跨樹按 item 記帳（item-id 切點樹序分佈、item 隔離子樹內 context 切點數與累積 Gain、item-id vs context 全模型 Gain 占比），落 `data/models/${model_version}/diagnostics/gain_ledger.json`；(b) **條件化 SHAP**——`diagnostics.shap.background: global|per_item` 選項（per_item＝背景取自該 item 子母體、interventional），attribution 接縫開放 explainer 選項傳遞（只留參數空間）；(c) **triage 總表**——把三層診斷合成 per-item 判定 {健康｜水準-配置型｜水準-指標再平衡型｜餓死型｜特徵缺失型}＋建議槓桿＋起手值，落 `diagnosis/triage.json`＋report 新 section。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 5（158–175 行）。**開工第一件實作 task＝spike**（spec 明文前置：categorical threshold 解析＋子樹遍歷可行性）。

**Architecture:** 三塊互相獨立、最後由 triage 收攏：(1) 訓練側新檔 `diagnosis/model/gain_ledger.py`（純 pandas 核心 `_ledger_from_trees` ＋ thin wrapper `compute_gain_ledger`）＋ training pipeline 新 node（插 `compute_feature_importance` 之後）＋ catalog JSONDataset；(2) `diagnosis/model/attribution.py` 簽名開放 `background`/`feature_perturbation`、`shap_per_item.py` per-item 迴圈分歧；(3) 評估側新檔 `diagnosis/metric/triage.py`（純 dict 判定表）＋ `assemble_triage_summary` node（gain_ledger **跨側走 catalog JSON、不 import 訓練側**；缺席 best-effort 降級）＋ report section ＋ consistency **A20**。跨側讀取靠 io `JSONDataset` 新增 `optional: true` 載入語意（檔案缺席回 None 不炸）。

**Tech Stack:** pandas／numpy（driver-side）、LightGBM 4.6.0 `booster.trees_to_dataframe()`（repo 首次使用——所以要 spike）、shap TreeExplainer（interventional 模式）、pytest、本機 local Spark。

**Scope note（閘門需要什麼真跑）:** Phase 5 是**第一個需要 training 真跑的階段**，但預設路徑避開重訓：`diagnostics.*` 是頂層鍵、model_version 只雜湊 `training:` block（allowlist，`core/versioning.py:137-145`；`tests/test_core/test_versioning.py:377-385` 已釘 diagnostics 不變性）→ 對既有模型 **6059dcef** 用 `--only-node compute_gain_ledger` 切片補產 gain_ledger.json，**零重訓**。已知答案 2（triage 重演 Phase 2 fund_bond 抽樣注入）才需要一次完整 dataset＋training＋evaluation（產新 mv，跑完還原 config）。負控制＝評估側五份既有 JSON（metric_ci／reconciliation／quadrant_summary／offset_sweep／pair_ledger）在乾淨態必須與 Phase 4b 基準**位元一致**；**報表逐字回歸（State A）必須在注入重訓之前完成**（執行協議鐵則）。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd <該路徑> && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`（裸跑會抓到 main 的 src）。
3. **可能超過 2 分鐘的指令（training／evaluation 真跑）一律背景執行**。evaluation CLI 必帶 `--model-version <mv> --post-training`（本機無 inference 產物；無 `best` symlink，promote 是使用者保留的人工步驟）。
4. **生產不變量**：no Spark UDF、no new packages（shap 已在 tech stack 內）；`diagnosis/*` 只 import `core / evaluation(僅 numpy 原語 metrics.py) / io / utils(不含 utils.hashing) `＋pandas/numpy/lightgbm(僅 model 側)/shap(僅 model 側)/標準庫；**diagnosis/metric/ 不 import pyspark／plotly**；**diagnosis/metric/triage.py 不 import 任何 pipelines.training 或 diagnosis.model 模組**（跨側只吃 catalog JSON dict）。報表圖表在 report_builder 側建。
5. **HPO objective 不動；本計畫不改任何 `training:` block 底下的鍵**（除了閘門 Task 9c 的暫時注入，跑完必還原）。新 config 鍵只在頂層 `diagnostics.*` 與 `evaluation.diagnosis.triage`／`evaluation.report.sections.triage`。
6. 測試判準＝與 baseline 一致（known-pitfalls §5）；`SPARK_LOCAL_IP=127.0.0.1` 已釘進 conftest。
7. 欄名一律經 `get_schema(parameters)` 取（item 欄＝`get_schema(parameters)["item"]`），勿硬編 `prod_name`。
8. 中文輸出（docstring、note 文案、報表文字、commit message）一律**繁體中文**。

## 設計定案（所有 task 共用語意，不要各自發明）

- **model_version 不受影響（已查證）**：`_model_version_payload` 只取 `params.get("training")`（頂層 allowlist，`core/versioning.py:137-145`）；頂層 `diagnostics.*` 新鍵不 bump hash，`test_model_version_invariant_to_diagnostics`（`tests/test_core/test_versioning.py:377-385`）已釘。**任何 task 若發現自己需要動 `training:` 底下的鍵才做得下去 → 停下回報**。
- **gain_ledger node 簽名（spec 修訂，計畫階段拍板）**：spec 原文 in=[`model`,`parameters`]，但碼→item 名映射在 preprocessor 的 `category_mappings[item_col]`（`preprocessing/_spark.py:200-212`：col → 值的有序 list，**碼＝list 索引**；booster 只看得到整數碼）→ node 實作為 in=[`"model"`,`"preprocessor_view"`,`"parameters"`] out=`"gain_ledger"`。`preprocessor_view`＝`select_features(preprocessor, parameters)` 的輸出（`pipelines/training/pipeline.py:43-47`，便宜純轉換、`preprocessor` 有 catalog 落地→切片自動補跑成本近零）。此偏差由 Task 10 寫進 spec 帶日期執行時修訂。
- **Gain 帳本的記帳規則（核心語意，單元測試釘手算錨）**：對每棵樹從 root 遞迴走訪，攜帶 `reachable`（該節點可達的 item 值集合，root＝全 item）與 `conditioned`（路徑上是否已經過 ≥1 個 item-id 切點）：
  - **item-id 切點**（`split_feature == item 欄`，`decision_type == "=="`，threshold 形如 `"0||2"`＝類別碼集合，左子樹＝集合內）：碼經 `categories[code]` 映射成 item 值；左子 `reachable ∩ S`、右子 `reachable − S`，兩側 `conditioned=True`；`split_gain` 記入 `item_id` 帳（不記入任何單一 item 的 context 帳）。空集合側不遞迴。threshold 出現超出 `categories` 範圍的碼 → 忽略該碼＋notes 記一筆（不炸）。
  - **context 切點**（其他特徵）：若 `conditioned`，對 `reachable` 中**每個** item 記 `context_split_count += 1`、`context_gain += split_gain`；若 `len(reachable) == 1` 另記 `context_gain_isolated`。未 conditioned（root 段的全域切點）不記入任何 per-item 帳。兩子樹以同 `reachable`/`conditioned` 遞迴。
  - **per-item 占比**：`context_gain_share = context_gain / Σ_items context_gain`（分母 0 → None）。triage 的餓死判準用 **conditioned 版**（`context_gain_share`）——8 item 的淺樹上 isolated（|reachable|=1）可能普遍為 0，isolated 版只作輔助欄。
  - 手算錨（Task 3 測試釘死；fixture 見該 task）：單棵樹 root 為 item 切點 S={A,C}（gain 10）→ 左子 context 切點 f_age（gain 6）；右子再一個 item 切點 S={B}（gain 4）→ 右子的右（reachable={D}）context 切點 f_inc（gain 2）。期望：`item_id = {split_count: 2, gain_sum: 14.0}`；per_item context_gain：A=6.0、C=6.0、B=0.0、D=2.0（D 的 2.0 同時是 isolated）；context_split_count：A=1、C=1、B=0、D=1；isolating_split_count（j ∈ 父節點 reachable 的 item 切點數）：A=1、C=1、B=2、D=2；`total_gain=22.0`、item_id `gain_share=14/22`；per-item `context_gain_share`：A=6/14、C=6/14、B=0、D=2/14。
- **gain_ledger 輸出 dict（JSON-ready，鍵名即契約）**：`enabled / item_feature / n_trees / n_items / total_gain / item_id{split_count, gain_sum, gain_share, tree_index_summary{min,p25,p50,p75,max}} / context{split_count, gain_sum, gain_share}（全模型 conditioned context 帳，不分 item）/ per_item{item:{isolating_split_count, context_split_count, context_gain, context_gain_isolated, context_gain_share, first_tree_index, trees_touched}} / fallback(bool) / notes[]`。所有 dict 鍵排序輸出。停用 → `{"enabled": False}`。
- **粗帳本退路（spike 失敗才用；輸出契約先定）**：categorical threshold 解析或遍歷不可行時降級為 by-feature 帳：`item_id` 與 `context` 區塊照算（不需遍歷，groupby `split_feature` 即得）、`per_item` 缺席、`fallback: true`＋notes 說明。triage 對 `fallback: true` 的處理＝等同 gain_ledger 缺席（見 triage 定案）。**選用哪個變體由 controller 在 Task 2 spike 報告後拍板，寫進 Task 3 派工 prompt**。
- **條件化 SHAP background（spec (b)）**：
  - `attribution.py::feature_attributions` 簽名開放為 `feature_attributions(model, X, feature_names, *, background=None, feature_perturbation="tree_path_dependent")`——`background=None` 時**與現行為位元等價**（`shap.TreeExplainer(booster)` 不帶 data）；有 background 時 `shap.TreeExplainer(booster, data=background, feature_perturbation=feature_perturbation)`。這是唯一觸碰 explainer 的接縫（檔頭 docstring `:1-6` 的既有設計宣稱）。
  - `shap_per_item.py::compute_shap_diagnostics` 讀新鍵 `background = str(cfg.get("background", "global"))`。`global`＝現行單次全域 pass＋per-item 切片，**一行都不改行為**（回歸測試釘 dict 全等）。`per_item`＝per-item 迴圈（現 `:167-187`）內對每個 item：背景＝該 item 在既有分層抽樣 X 中的列（超過 `_BACKGROUND_CAP = 128` 用既有 seed 純隨機抽 128——起手常數，非 config 鍵，YAGNI）、`feature_perturbation="interventional"`，該 item 的 profile 與正例 profile 都改用此 explainer 的輸出；**全域 top_features／divergence 的全域向量仍來自全域 pass**（背景不同會讓 divergence 混入背景效應——JSON notes 記一筆、手冊已知限制註明）。無新 budget 閘（本機/公司 M=8/22、列數已被 `sample_rows` 管住；interventional 成本 O(rows×bg×trees) 在 notes 記估算、閘門 Task 9e 實測）。
  - **loss-SHAP 不做**（spec 明文 v2；只留 `feature_perturbation` 參數空間）。
- **triage 判定表（框架 Ch 4.1/4.2 寫成 code；優先序即契約）**：per item 從 `quadrant_summary.by_item[j]`（`level_status`/`disc_status`/`auc`/`auc_reason`/`y_rate`/`gap_vs_global`）、`reconciliation.by_item[j]`（`verdict`/`theory_min`/`theory_max`/`theory_approx`/`residual`）、`offset_sweep.per_item[j]`（`delta_star_centered`/`loo_contribution_holdout`）、`gain_ledger.per_item[j]`（`context_gain_share`）判定：
  1. `disc_low = (disc_status == "差" and auc_reason is None)`；`level_off = (level_status != "正常")`；`config_signal = (reconciliation verdict == "可解釋" and (theory_max − theory_min > 0 or theory_approx))`——**band 退化為 [0,0] 的「可解釋」＝「本來就沒偏」，不算配置訊號**。
  2. 優先序：`level_off and config_signal` → **水準-配置型**（閉式解最便宜，先修；框架槓桿 1）→ `disc_low` → 結構層裁決（gain_ledger 有 per_item：`context_gain_share < _STARVE_RATIO(=0.25) × max_share` → **餓死型**；否則 → **特徵缺失型**＋note「待條件化 SHAP 佐證」；gain_ledger 缺席/fallback → **「餓死型或特徵缺失型（無結構層證據）」**降級判定）→ `level_off`（未被配置解釋）→ **水準-指標再平衡型** → 其餘 → **健康**。
  3. 次要訊號不吞：判水準型但 disc 也低 → notes「條件判別力軸同時偏低，修完水準重量再判（框架 Ch 4 第 5 步）」；判健康但 `|delta_star_centered| ≥ 0.3` 且 `loo_contribution_holdout > 0` → notes 記 δ* 觀測。`auc_reason` 非 null → notes「AUC 樣本不足」且 disc_low 視為 False。
  4. **起手值欄（每列必帶 `caveat: "起手值，須經快迴路驗證，非定案"`）**：配置型 → `{type: "logq_offset", value: reconciliation.theory.by_item[j].mean（缺則 (theory_min+theory_max)/2）, band: [theory_min, theory_max], unit: "log-odds"}`；再平衡型 → `{type: "delta_star_centered", value: per_item[j].delta_star_centered, unit: "logit（centered——跨執行只有相對差可比，gauge 讀法；加到分數上時與 raw 差一共同平移、排序等價）"}`（**計畫階段拍板：用 centered**）；餓死型 → `{type: "item_weight", value: round(min(_WEIGHT_CAP=8.0, sqrt(max_rate / y_rate_j)), 2), unit: "sample_weight 相對倍率（w∝1/√P 加上限，手冊3 Ch8）"}`（`y_rate` 取自 quadrant_summary；`y_rate_j ≤ 0` → value None＋note）；健康／降級判定 → starter None。
  5. `_STARVE_RATIO = 0.25`、`_WEIGHT_CAP = 8.0` 是**起手門檻常數**（code 內註記非定案；閘門記錄實際值；v2 若要 config 化再議）。
- **triage 輸出 dict（鍵名即契約）**：`enabled / gain_ledger_present(bool) / thresholds{starve_ratio, weight_cap} / verdicts{item:{verdict, lever, starter{type,value,unit,caveat,band?}, evidence{auc, disc_status, level_status, gap_vs_global, recon_verdict, theory_min, theory_max, residual, delta_star_centered, loo_contribution_holdout, context_gain_share, y_rate}, notes[]}} / summary{verdict: count} / notes[]`。上游任一 dict 缺席或 `enabled: False` stub → 對應 evidence 欄 None＋頂層 notes 記「上游 X 缺席」；quadrant_summary 缺席 → `verdicts` 空＋note（判定至少需要兩軸）。所有鍵排序輸出。
- **跨側讀取＝io 層 optional 載入（計畫階段拍板）**：evaluation pipeline 的 triage node 宣告 input `"gain_ledger"`（與 training 側**同一個** catalog 條目，路徑 `data/models/${model_version}/diagnostics/gain_ledger.json`——evaluation 已有 `${model_version}` runtime 替換）。Runner 載入 input 發生在 node 之外，node 接不到 FileNotFoundError → 在 `io` 的 `JSONDataset` 加建構參數 `optional: bool = False`：`load()` 時檔案不存在且 optional → 回 `None`（log 一行）而不 raise；`save()`／`exists()` 語意不變。catalog 的 `gain_ledger` 條目標 `optional: true`。這讓「訓練側未跑該 node」時 triage 拿到 None → best-effort 降級（spec 明文），且不需任何跨側 import。
- **consistency A20（一個 predicate 涵蓋 Phase 5 三鍵）**：`diagnostics.shap.background ∈ {global, per_item}`（spec 明文）＋`diagnostics.gain_ledger.enabled` 為 bool＋`evaluation.diagnosis.triage.enabled` 為 bool（後兩者沿 A19 的 enabled-bool 慣例，additive）。`validate_config_consistency` 拿到的是全量 deep-merge dict（`config.py:169-175`），頂層 `diagnostics.*` 讀得到（已查證）。落點：docstring legend `consistency.py:99` 之後、predicate 接 `pair_ledger_param_errors`（`:690-702`）之後、aggregator 註冊 `:809` 之後。
- **報表**：`build_triage_section(triage, parameters)`——無圖、一張主表（列＝item；欄＝判定／建議槓桿／起手值／關鍵證據（AUC、gap_vs_global、δ*centered、context_gain_share）／備註），判定欄用全形括注不用色碼（HTML 簡單優先）；`summary` 一行文字（各判定 item 數）；glossary 補 3 條（triage、餓死型、起手值）。`assemble_report` 尾參 `triage=None`；`generate_report` node inputs 尾端加 `"evaluation_triage"`。
- **既有測試會被本計畫「合法」改到的（預先授權，僅 additive）**：(a) `tests/test_pipelines/test_evaluation/test_pipeline.py` 節點結構——default/post_training **10→11**、compare **13→14**、node 名單加 `assemble_triage_summary`（`compute_pair_ledger` 之後、`generate_report` 之前）、outputs 加 `evaluation_triage`、`generate_report` inputs 尾端加 `"evaluation_triage"`；(b) training pipeline／`tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS`——新增 `compute_gain_ledger` node（輸出有 catalog 落地、非昂貴）需把它加進受影響接續點的允許集合（含 calibration-enabled 變體），改動附一句理由；(c) report sections／diagnosis config 鍵的 exact-set 斷言（`tests/test_evaluation/test_parameters_evaluation_yaml.py`、`test_report_builder.py`）additive 更新；(d) `tests/test_core/test_consistency.py` 加 A20 測試。**任何非 additive 的既有測試改動 → 停下回報**。
- **效能觀測義務**：Task 9 記錄 gain_ledger node 秒數（trees_to_dataframe ~800 樹預估秒級）、triage node（純 dict，毫秒級）、per_item SHAP smoke 的 wall time，按公司規模（22 item）外推寫進手冊。
- **文件是一等交付物（spec §3 固定結構）**：Task 10 內建——判讀手冊 `docs/pipelines/evaluation-diagnosis.md` 新增 **§12 結構層（gain_ledger 判讀＋條件化 SHAP background）**、**§13 triage 總表**（含數感節＋真跑示例走讀）、既有已知限制節改號為 **§14** 並新增條目；`docs/pipelines/training.md` 診斷節加路由行（判讀在手冊，training.md 不複述）；spec 帶日期修訂（node inputs 加 preprocessor_view、A20 涵蓋三鍵、起手值 centered 定案）。寫法鐵則（禁開發詞彙、真跑產物印進文件、讀者 agent 驗洩漏）不可省。

## 執行模式（controller 注意）

沿 Phase 4b＋提速協議（HANDOFF 執行協議 9 條）：Task 1、2 後半拍板、9 controller 直跑；**Task 2 派 sonnet implementer A**（spike 腳本＋事實回報，不改 src）；**Task 3 派 sonnet implementer B**（gain_ledger 模組＋單元測試，prompt 內嵌 spike 結論＋controller 拍板的變體）；**Task 4 派 sonnet implementer C**（training node／catalog／config／A20／RESUME_CONTRACTS）；**Task 5 派 sonnet implementer D**（SHAP background＋attribution，與 B/C 無檔案重疊但仍串行）；**Task 6 派 sonnet implementer E**（io optional＋triage 模組＋單元測試）；**Task 7 派 sonnet implementer F**（evaluation node／catalog／pipeline／報表／config）；**Task 8 合併 reviewer（sonnet）背景執行**、與 Task 9 真跑並行，prompt 附 controller 綠燈證據＋明令只讀 diff、只跑新增/變更測試檔；**Task 10 文件 writer 的素材包由 controller 從 Task 9 產物先備好**；**Task 11 opus 總審背景執行**。所有 implementer prompt **直接內嵌該 task 全文＋執行者必讀＋設計定案**，計畫檔路徑只作查證。controller 在 Task 7 完成後跑一次 graphify rebuild（CLAUDE.md §graphify）。

---

### Task 1：pre-flight ＋ baseline（controller 直跑）

**Files:** 無程式碼變更；產出 `/tmp/phase5_test_baseline.txt`、`/tmp/phase5_json_before/`、`/tmp/phase5_manifest_before.json`。

- [ ] **Step 1: pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd && readlink .venv && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: worktree root、Python 3.10.9、isolation OK、working tree 乾淨（@4367896 之後）。

- [ ] **Step 2: 相關測試 baseline（背景）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/ tests/test_core/test_consistency.py tests/test_core/test_versioning.py \
  tests/test_evaluation/test_report_builder.py tests/test_evaluation/test_parameters_evaluation_yaml.py \
  tests/test_pipelines/ tests/test_io/ \
  -q 2>&1 | tail -5 | tee /tmp/phase5_test_baseline.txt
```
Expected: 全 pass（記下確切數字；main 既知互擾清單見 known-pitfalls §5，若有 fail 先歸因）。

- [ ] **Step 3: 產物快照（負控制基準）＋ manifest 快照**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
mkdir -p /tmp/phase5_json_before && \
cp data/evaluation/6059dcef/20260131/diagnosis/*.json /tmp/phase5_json_before/ && \
cp data/models/6059dcef/manifest.json /tmp/phase5_manifest_before.json && \
ls -la /tmp/phase5_json_before/
```
Expected: 五份 JSON（metric_ci／reconciliation／quadrant_summary／offset_sweep／pair_ledger）＝Phase 4b 乾淨基準。

---

### Task 2：Spike——trees_to_dataframe 解析可行性（implementer A，sonnet；不改 src）

**Files:**
- Create: `/private/tmp/claude-501/-Users-curtislu-projects-recsys-tfb/061ce671-c9d6-4cf9-820b-8957aa20e461/scratchpad/spike_gain_ledger.py`（或 implementer 自己 session 的 scratchpad）
- 產出：spike 報告（回報正文，含下列每一問的答案＋輸出原文片段）

**動機：** `booster.trees_to_dataframe()` repo 全域零使用（已查證）；spec 明文「進實作前必驗」：categorical threshold 的表示與 parent-child 遍歷可行性。答案決定 Task 3 用完整帳本還是粗帳本。

- [ ] **Step 1: 寫 spike 腳本並執行**

```python
"""Spike：對真實模型 6059dcef 驗證 gain_ledger 的技術前提。"""
import json
import lightgbm as lgb
import yaml

BOOSTER_PATH = "data/models/6059dcef/model.txt"

b = lgb.Booster(model_file=BOOSTER_PATH)
print("Q1 feature_name:", b.feature_name())
print("Q1 num_trees:", b.num_trees())

df = b.trees_to_dataframe()
print("Q2 columns:", df.columns.tolist())
print("Q2 shape:", df.shape)

# item 欄名從 conf 讀（schema.item）
with open("conf/base/parameters.yaml") as f:
    params = yaml.safe_load(f)
item_col = (params.get("schema", {}) or {}).get("item", "prod_name")
print("Q3 item_col:", item_col, "| in features:", item_col in b.feature_name())

it = df[df["split_feature"] == item_col]
print("Q4 item splits:", len(it),
      "| decision_type set:", sorted(it["decision_type"].dropna().unique().tolist()) if len(it) else [])
print("Q4 threshold samples:", it["threshold"].head(8).tolist())

# Q5：parent-child 遍歷——挑第一棵含 item 切點的樹，手走一次
if len(it):
    t0 = int(it["tree_index"].iloc[0])
    tree = df[df["tree_index"] == t0].set_index("node_index")
    root = tree[tree["parent_index"].isna()].index[0]
    print("Q5 tree", t0, "root:", root)
    def walk(node, depth=0):
        row = tree.loc[node]
        feat = row["split_feature"]
        print("  " * depth + f"{node} feat={feat} thr={row['threshold']} gain={row['split_gain']}")
        if isinstance(feat, str):  # 非 leaf
            walk(row["left_child"], depth + 1)
            walk(row["right_child"], depth + 1)
    walk(root)

# Q6：碼→item 名映射確認（category_mappings 碼＝索引）
cat_vals = ((params.get("schema", {}) or {}).get("categorical_values", {}) or {}).get(item_col)
print("Q6 schema.categorical_values[item]:", cat_vals)

# Q7：已知答案預演——粗略 conditioned gain 比較（不求精確，只看方向）
# 對每個 item 切點，把 gain 掛到 item_id 帳；context 帳的方向感用「item 切點以下的
# 子孫 gain」近似（BFS 收集子孫）——正式版在 Task 3 做集合遍歷，這裡只要方向。
```

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python <scratchpad>/spike_gain_ledger.py
```

- [ ] **Step 2: 回報七問**（Q1 booster 特徵名含不含 item 欄；Q2 欄位清單；Q3 item 欄名；Q4 item 切點數與 threshold 格式——是否 `"0||2"` 類別碼集合、decision_type 是否 `==`；Q5 遍歷可行性——left_child/right_child 標籤能否閉環走完一棵樹；Q6 碼→名映射對得上嗎（list 索引＝碼）；Q7 fund_mix vs 熱門 item 的方向感）。**查不到／格式不符預期就如實回報，不要硬湊**——粗帳本退路就是為此而設。

- [ ] **Step 3（controller）: 拍板變體**——Q4/Q5/Q6 全通過 → Task 3 用完整帳本；任一失敗 → 粗帳本（照設計定案的退路契約），並把 spike 輸出原文塞進 Task 3 派工 prompt。

回報格式（必遵守）：結論先行（≤5 行）；七問逐條附輸出原文；「沒做到或不確定的事」獨立一段。

---

### Task 3：`gain_ledger.py` 模組＋單元測試（implementer B，sonnet）

**Files:**
- Create: `src/recsys_tfb/diagnosis/model/gain_ledger.py`
- Test: `tests/test_diagnosis/test_model/test_gain_ledger.py`

**前置：** controller 已把 spike 結論（threshold 格式原文、欄位清單）與拍板變體嵌進 prompt。以下代碼以**完整帳本**為準；粗帳本＝去掉 `_walk` 與 `per_item`，其餘同。

- [ ] **Step 1: 寫失敗測試**

```python
"""gain_ledger 單元測試。

手算錨（設計定案）：單棵樹
  S0: item 切點 S={A,C}（碼 0||2）gain 10 → 左 S1、右 S2
  S1: context f_age gain 6 → 兩 leaf
  S2: item 切點 S={B}（碼 1）gain 4 → 左 leaf、右 S3
  S3: context f_inc gain 2 → 兩 leaf（reachable={D}，isolated）
期望：item_id {split_count 2, gain_sum 14}；context_gain A=6 C=6 B=0 D=2；
isolating_split_count A=1 C=1 B=2 D=2；total_gain 22。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.model.gain_ledger import _ledger_from_trees, compute_gain_ledger

ITEM = "prod_code"
CATS = ["A", "B", "C", "D"]  # 碼＝索引


def _node(tree, idx, left, right, feat, gain, thr, parent):
    return {
        "tree_index": tree, "node_index": idx, "left_child": left,
        "right_child": right, "parent_index": parent, "split_feature": feat,
        "split_gain": gain, "threshold": thr, "decision_type": "==" if feat == ITEM else "<=",
    }


def _trees_df():
    rows = [
        _node(0, "0-S0", "0-S1", "0-S2", ITEM, 10.0, "0||2", None),
        _node(0, "0-S1", "0-L0", "0-L1", "f_age", 6.0, 1.5, "0-S0"),
        _node(0, "0-L0", None, None, None, np.nan, np.nan, "0-S1"),
        _node(0, "0-L1", None, None, None, np.nan, np.nan, "0-S1"),
        _node(0, "0-S2", "0-L2", "0-S3", ITEM, 4.0, "1", "0-S0"),
        _node(0, "0-L2", None, None, None, np.nan, np.nan, "0-S2"),
        _node(0, "0-S3", "0-L3", "0-L4", "f_inc", 2.0, 0.5, "0-S2"),
        _node(0, "0-L3", None, None, None, np.nan, np.nan, "0-S3"),
        _node(0, "0-L4", None, None, None, np.nan, np.nan, "0-S3"),
    ]
    return pd.DataFrame(rows)


class TestKnownAnswer:
    def test_item_id_account(self):
        out = _ledger_from_trees(_trees_df(), ITEM, CATS)
        assert out["item_id"]["split_count"] == 2
        assert out["item_id"]["gain_sum"] == pytest.approx(14.0)
        assert out["total_gain"] == pytest.approx(22.0)
        assert out["item_id"]["gain_share"] == pytest.approx(14.0 / 22.0)
        assert out["item_id"]["tree_index_summary"]["min"] == 0

    def test_per_item_context_accounts(self):
        p = _ledger_from_trees(_trees_df(), ITEM, CATS)["per_item"]
        assert p["A"]["context_gain"] == pytest.approx(6.0)
        assert p["C"]["context_gain"] == pytest.approx(6.0)
        assert p["B"]["context_gain"] == pytest.approx(0.0)
        assert p["D"]["context_gain"] == pytest.approx(2.0)
        assert p["D"]["context_gain_isolated"] == pytest.approx(2.0)
        assert p["A"]["context_gain_isolated"] == pytest.approx(0.0)
        assert p["A"]["context_split_count"] == 1 and p["B"]["context_split_count"] == 0

    def test_isolating_split_counts_and_shares(self):
        p = _ledger_from_trees(_trees_df(), ITEM, CATS)["per_item"]
        assert [p[i]["isolating_split_count"] for i in "ABCD"] == [1, 2, 1, 2]
        assert p["A"]["context_gain_share"] == pytest.approx(6.0 / 14.0)
        assert p["B"]["context_gain_share"] == pytest.approx(0.0)

    def test_global_context_split_not_attributed(self):
        # root 是 context 切點（未 conditioned）→ 不進任何 per-item 帳
        df = _trees_df()
        extra = pd.DataFrame([
            _node(1, "1-S0", "1-L0", "1-L1", "f_age", 9.0, 2.5, None),
            _node(1, "1-L0", None, None, None, np.nan, np.nan, "1-S0"),
            _node(1, "1-L1", None, None, None, np.nan, np.nan, "1-S0"),
        ])
        out = _ledger_from_trees(pd.concat([df, extra], ignore_index=True), ITEM, CATS)
        assert out["per_item"]["A"]["context_gain"] == pytest.approx(6.0)  # 不變
        assert out["context"]["gain_sum"] == pytest.approx(8.0)            # conditioned 帳不含 9.0
        assert out["total_gain"] == pytest.approx(31.0)

    def test_unknown_code_ignored_with_note(self):
        df = _trees_df()
        df.loc[df["node_index"] == "0-S0", "threshold"] = "0||2||9"  # 碼 9 超界
        out = _ledger_from_trees(df, ITEM, CATS)
        assert out["per_item"]["A"]["context_gain"] == pytest.approx(6.0)
        assert any("9" in n for n in out["notes"])

    def test_empty_reachable_side_is_skipped(self):
        # S2 的 S={B}，右側 reachable={D}；若左側集合為空不遞迴、不炸
        df = _trees_df()
        df.loc[df["node_index"] == "0-S0", "threshold"] = "0||1||2||3"  # 右側空
        out = _ledger_from_trees(df, ITEM, CATS)
        assert out["item_id"]["split_count"] == 2  # 帳照記


class TestWrapper:
    def test_disabled_returns_stub(self):
        params = {"diagnostics": {"gain_ledger": {"enabled": False}}}
        assert compute_gain_ledger(None, None, params) == {"enabled": False}

    def test_real_booster_contract(self, tiny_model):
        # tiny_model：模組級 fixture——lgb.train 在 ~40 列玩具資料上訓 3 棵樹、
        # categorical_feature=[ITEM]（照 tests/test_diagnosis/test_model/
        # test_attribution.py 的 tiny-booster 慣例；ITEM 欄用 0..3 整數碼）。
        model, preprocessor, params = tiny_model  # preprocessor 含 category_mappings[ITEM]=CATS
        out = compute_gain_ledger(model, preprocessor, params)
        assert out["enabled"] is True and out["fallback"] is False
        assert out["n_trees"] == model.booster.num_trees()
        assert set(out["per_item"]) == set(CATS)
        for key in ("item_id", "context", "total_gain", "notes"):
            assert key in out

    def test_missing_category_mapping_degrades_to_fallback(self, tiny_model):
        model, _preprocessor, params = tiny_model
        out = compute_gain_ledger(model, {"category_mappings": {}}, params)
        assert out["fallback"] is True and out["per_item"] is None
        assert any("category_mappings" in n for n in out["notes"])
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_model/test_gain_ledger.py -q
```
Expected: FAIL（ModuleNotFoundError）。

- [ ] **Step 3: 實作模組**

```python
"""Gain 帳本（框架診斷項目 8；spec §3 Phase 5）.

從 LightGBM booster 的 ``trees_to_dataframe()`` 跨樹按 item 記帳：item-id
切點的樹序分佈、item 隔離後子樹內 context 切點數與累積 Gain、item-id vs
context 的全模型 Gain 占比。冷門 item 隔出後幾乎沒有後續切點＝「先驗有修、
個人化沒學」的結構鐵證（手冊3 Ch4）。

分兩層：``_ledger_from_trees``（純 pandas/dict，單元測試釘手算錨）與
``compute_gain_ledger``（thin wrapper：booster → trees_df → 碼映射）。
categorical threshold 在 trees_to_dataframe 是 ``"0||2"`` 形式的類別碼集合；
碼→item 值走 preprocessor 的 ``category_mappings[item_col]``（碼＝list 索引，
``preprocessing/_spark.py`` 的編碼慣例）。
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _parse_cat_codes(threshold) -> list[int]:
    return [int(tok) for tok in str(threshold).split("||")]


def _percentiles(values: list[int]) -> dict:
    if not values:
        return {"min": None, "p25": None, "p50": None, "p75": None, "max": None}
    arr = np.asarray(sorted(values), dtype=np.float64)
    return {
        "min": int(arr.min()),
        "p25": float(np.percentile(arr, 25)),
        "p50": float(np.percentile(arr, 50)),
        "p75": float(np.percentile(arr, 75)),
        "max": int(arr.max()),
    }


def _ledger_from_trees(trees: pd.DataFrame, item_feature: str,
                       categories: list) -> dict:
    """純核心：對 trees_to_dataframe 輸出做集合遍歷記帳（設計定案的規則）。"""
    all_items = [str(c) for c in categories]
    notes: list[str] = []
    acct = {
        i: {"isolating_split_count": 0, "context_split_count": 0,
            "context_gain": 0.0, "context_gain_isolated": 0.0,
            "first_tree_index": None, "trees_touched": set()}
        for i in all_items
    }
    item_id = {"split_count": 0, "gain_sum": 0.0, "tree_indices": []}
    context_cond = {"split_count": 0, "gain_sum": 0.0}
    total_gain = float(pd.to_numeric(trees["split_gain"], errors="coerce")
                       .fillna(0.0).clip(lower=0.0).sum())
    unknown_codes: set[int] = set()

    for tree_idx, tdf in trees.groupby("tree_index", sort=True):
        nodes = tdf.set_index("node_index")
        roots = nodes[nodes["parent_index"].isna()]
        if len(roots) != 1:
            notes.append(f"tree {tree_idx}: root 數 {len(roots)} ≠ 1，跳過該樹")
            continue
        stack = [(roots.index[0], frozenset(all_items), False)]
        while stack:
            node_id, reachable, conditioned = stack.pop()
            row = nodes.loc[node_id]
            feat = row["split_feature"]
            if not isinstance(feat, str):      # leaf
                continue
            gain = float(row["split_gain"]) if pd.notna(row["split_gain"]) else 0.0
            if feat == item_feature:
                codes = _parse_cat_codes(row["threshold"])
                in_set = set()
                for c in codes:
                    if 0 <= c < len(all_items):
                        in_set.add(all_items[c])
                    else:
                        unknown_codes.add(c)
                item_id["split_count"] += 1
                item_id["gain_sum"] += gain
                item_id["tree_indices"].append(int(tree_idx))
                for i in reachable:
                    a = acct[i]
                    a["isolating_split_count"] += 1
                    a["trees_touched"].add(int(tree_idx))
                    if a["first_tree_index"] is None:
                        a["first_tree_index"] = int(tree_idx)
                left_r = frozenset(reachable & in_set)
                right_r = frozenset(reachable - in_set)
                if left_r:
                    stack.append((row["left_child"], left_r, True))
                if right_r:
                    stack.append((row["right_child"], right_r, True))
            else:
                if conditioned:
                    context_cond["split_count"] += 1
                    context_cond["gain_sum"] += gain
                    for i in reachable:
                        a = acct[i]
                        a["context_split_count"] += 1
                        a["context_gain"] += gain
                        a["trees_touched"].add(int(tree_idx))
                        if len(reachable) == 1:
                            a["context_gain_isolated"] += gain
                stack.append((row["left_child"], reachable, conditioned))
                stack.append((row["right_child"], reachable, conditioned))

    if unknown_codes:
        notes.append(
            f"item 切點 threshold 含超出映射範圍的類別碼（忽略）：{sorted(unknown_codes)}")

    denom = sum(a["context_gain"] for a in acct.values())
    per_item = {}
    for i in sorted(acct):
        a = acct[i]
        per_item[i] = {
            "isolating_split_count": a["isolating_split_count"],
            "context_split_count": a["context_split_count"],
            "context_gain": a["context_gain"],
            "context_gain_isolated": a["context_gain_isolated"],
            "context_gain_share": (a["context_gain"] / denom) if denom > 0 else None,
            "first_tree_index": a["first_tree_index"],
            "trees_touched": len(a["trees_touched"]),
        }
    return {
        "enabled": True,
        "item_feature": item_feature,
        "n_trees": int(trees["tree_index"].nunique()),
        "n_items": len(all_items),
        "total_gain": total_gain,
        "item_id": {
            "split_count": item_id["split_count"],
            "gain_sum": item_id["gain_sum"],
            "gain_share": (item_id["gain_sum"] / total_gain) if total_gain > 0 else None,
            "tree_index_summary": _percentiles(item_id["tree_indices"]),
        },
        "context": {
            "split_count": context_cond["split_count"],
            "gain_sum": context_cond["gain_sum"],
            "gain_share": (context_cond["gain_sum"] / total_gain) if total_gain > 0 else None,
        },
        "per_item": per_item,
        "fallback": False,
        "notes": notes,
    }


def compute_gain_ledger(model, preprocessor: Optional[dict],
                        parameters: dict) -> dict:
    """Thin wrapper：booster → trees_df → 碼映射 → ``_ledger_from_trees``。"""
    cfg = (parameters.get("diagnostics", {}) or {}).get("gain_ledger", {}) or {}
    if not cfg.get("enabled", True):
        return {"enabled": False}
    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.diagnosis.model.attribution import _resolve_booster

    item_col = get_schema(parameters)["item"]
    booster = _resolve_booster(model)
    trees = booster.trees_to_dataframe()
    categories = ((preprocessor or {}).get("category_mappings", {}) or {}).get(item_col)
    if not categories:
        # 映射缺席 → 粗帳本降級（per_item 不可得）
        by_feat = (trees.dropna(subset=["split_feature"])
                   .groupby("split_feature")["split_gain"].agg(["count", "sum"]))
        total = float(by_feat["sum"].sum())
        item_row = by_feat.loc[item_col] if item_col in by_feat.index else None
        return {
            "enabled": True, "item_feature": item_col,
            "n_trees": int(trees["tree_index"].nunique()), "n_items": None,
            "total_gain": total,
            "item_id": {
                "split_count": int(item_row["count"]) if item_row is not None else 0,
                "gain_sum": float(item_row["sum"]) if item_row is not None else 0.0,
                "gain_share": (float(item_row["sum"]) / total)
                              if (item_row is not None and total > 0) else None,
                "tree_index_summary": _percentiles(
                    trees.loc[trees["split_feature"] == item_col, "tree_index"]
                    .astype(int).tolist()),
            },
            "context": None, "per_item": None, "fallback": True,
            "notes": [f"preprocessor 缺 category_mappings[{item_col}]，降級為粗帳本"],
        }
    return _ledger_from_trees(trees, item_col, list(categories))
```

（粗帳本變體＝spike 失敗時：`_ledger_from_trees` 換成上面 wrapper 內的 by-feature 帳、`fallback: True`；測試對應縮減——controller 在派工 prompt 指明。）

- [ ] **Step 4: 跑測試 GREEN**（同 Step 2 指令）Expected: 全 pass。
- [ ] **Step 5: 故意弄壞一行**（例如 `context_gain += gain` 改 `+= 0.0`）確認對應測試轉紅後改回，回報弄壞了哪行。
- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/model/gain_ledger.py tests/test_diagnosis/test_model/test_gain_ledger.py && \
git commit -m "feat(diagnosis): gain_ledger 結構層帳本（集合遍歷＋手算錨單元測試）"
```

---

### Task 4：training node＋catalog＋config＋consistency A20＋RESUME_CONTRACTS（implementer C，sonnet）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`（`compute_feature_importance` node 之後，約 :157）
- Modify: `conf/base/catalog.yaml`（`cases_manifest` 條目 :232-234 之後）
- Modify: `conf/base/parameters_training.yaml`（diagnostics 區塊 :142-167）
- Modify: `src/recsys_tfb/core/consistency.py`（legend :99 後、predicate :702 後、aggregator :809 後）
- Modify（additive）: `tests/test_pipelines/test_resume_contracts.py`、training pipeline 結構測試（若有 exact-set 斷言）、`tests/test_evaluation/test_parameters_evaluation_yaml.py`（config 鍵回歸）、`tests/test_core/test_consistency.py`
- 注意：**io `JSONDataset` 的 `optional` 參數屬 Task 6**；本 task 的 catalog `gain_ledger` 條目**先不帶** `optional:` 鍵（Task 7 再補），避免依賴倒置。

- [ ] **Step 1: 失敗測試——A20**（`tests/test_core/test_consistency.py` 追加，照 A19 測試版型）

```python
class TestA20Phase5Diagnostics:
    def test_background_domain(self):
        params = _base_params()
        params.setdefault("diagnostics", {})["shap"] = {"background": "per_query"}
        errors = validate_config_consistency(params, raise_on_error=False)
        assert any("A20" in e for e in errors)

    def test_background_valid_values_pass(self):
        for v in ("global", "per_item"):
            params = _base_params()
            params.setdefault("diagnostics", {})["shap"] = {"background": v}
            assert not [e for e in validate_config_consistency(params, raise_on_error=False)
                        if "A20" in e]

    def test_gain_ledger_and_triage_enabled_must_be_bool(self):
        params = _base_params()
        params.setdefault("diagnostics", {})["gain_ledger"] = {"enabled": "yes"}
        params.setdefault("evaluation", {}).setdefault("diagnosis", {})["triage"] = {"enabled": 1}
        errors = [e for e in validate_config_consistency(params, raise_on_error=False) if "A20" in e]
        assert len(errors) == 2
```
（`_base_params`／`raise_on_error` 介面以該測試檔既有寫法為準——先讀檔對齊，不要憑本計畫猜。）

- [ ] **Step 2: RED** → **Step 3: 實作 A20 predicate**

```python
def phase5_diagnostics_param_errors(parameters: dict) -> list[str]:
    """A20 — Phase 5 參數域：shap.background 域＋兩個 enabled bool。"""
    errors: list[str] = []
    diag = parameters.get("diagnostics", {}) or {}
    bg = (diag.get("shap", {}) or {}).get("background", "global")
    if bg not in ("global", "per_item"):
        errors.append(
            f"A20: diagnostics.shap.background 必須是 global|per_item，得到 {bg!r}")
    gl_en = (diag.get("gain_ledger", {}) or {}).get("enabled", True)
    if not isinstance(gl_en, bool):
        errors.append("A20: diagnostics.gain_ledger.enabled 必須是 bool")
    tri_en = (((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
              .get("triage", {}) or {}).get("enabled", True)
    if not isinstance(tri_en, bool):
        errors.append("A20: evaluation.diagnosis.triage.enabled 必須是 bool")
    return errors
```
註冊：aggregator 尾端 `errors.extend(phase5_diagnostics_param_errors(parameters))`；legend 加一行 `* A20 — Phase 5 診斷參數域（diagnostics.shap.background ∈ {global, per_item}；gain_ledger/triage enabled 為 bool）`。

- [ ] **Step 4: pipeline node ＋ catalog ＋ config**

`pipelines/training/pipeline.py`（import `compute_gain_ledger` 照 `compute_feature_importance` 的 import 慣例；node 插 :153-157 那顆之後）：

```python
        Node(
            compute_gain_ledger,
            inputs=["model", "preprocessor_view", "parameters"],
            outputs="gain_ledger",
        ),
```

`conf/base/catalog.yaml`（`cases_manifest` 之後）：

```yaml
gain_ledger:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/gain_ledger.json
```

`conf/base/parameters_training.yaml`（`feature_importance:` block :147-148 之後、`shap:` 之前）：

```yaml
  gain_ledger:
    enabled: true   # 結構層 Gain 帳本（診斷項目 8）；判讀見 docs/pipelines/evaluation-diagnosis.md §12
```

同檔 `shap:` block 內（與 `sample_rows` 同層）：

```yaml
    background: global   # global | per_item（per_item＝背景取該 item 子母體，interventional；成本較高）
```

- [ ] **Step 5: RESUME_CONTRACTS 與結構測試 additive 更新**——跑 `tests/test_pipelines/`，凡因新 node 而紅的 exact-set 斷言 additive 補上（含 calibration-enabled 變體）；每處改動附一句理由註解。**非 additive 的紅 → 停下回報**。
- [ ] **Step 6: 全綠驗證**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py tests/test_core/test_versioning.py \
  tests/test_pipelines/ tests/test_evaluation/test_parameters_evaluation_yaml.py -q 2>&1 | tail -5
```
- [ ] **Step 7: 故意弄壞 A20 predicate 一行**（域檢查改永真）確認測試轉紅後改回。
- [ ] **Step 8: Commit**（`feat(training): gain_ledger node＋catalog＋config＋consistency A20`）

---

### Task 5：條件化 SHAP background＋attribution 接縫（implementer D，sonnet）

**Files:**
- Modify: `src/recsys_tfb/diagnosis/model/attribution.py`（`feature_attributions` :20-28）
- Modify: `src/recsys_tfb/diagnosis/model/shap_per_item.py`（cfg 讀取 :104-112、per-item 迴圈 :167-187、正例 profile `_positive_profiles` :76）
- Test: `tests/test_diagnosis/test_model/test_attribution.py`、`tests/test_diagnosis/test_model/` 既有 shap 測試檔（額外案例 additive）

**動機：** 診斷項目 9（條件化 SHAP）——per_item 背景才能回答「這個 item 內部哪個 context 特徵沒把正負分開」。只開參數空間、預設 `global` 行為不變（loss-SHAP 是 v2，明文不做）。

- [ ] **Step 1: 先 Read 兩檔全文**（本計畫行號來自 2026-07-08 的接縫盤點，以檔案現況為準）。
- [ ] **Step 2: 失敗測試**——(a) `feature_attributions` 帶 `background=` 的 kwargs 存在且 `background=None` 走原路徑（tiny booster 上兩次呼叫輸出 `np.array_equal`）；(b) `compute_shap_diagnostics` 在 `background: "global"` 下輸出 dict 與未設鍵時**全等**（回歸鎖）；(c) `background: "per_item"` 下輸出契約鍵齊全、每 item profile 存在、JSON notes 含 per_item 背景說明；(d) 非法值由 A20 擋（consistency 測試已在 Task 4，這裡不重複）。
- [ ] **Step 3: 實作**——attribution 簽名照設計定案：

```python
def feature_attributions(model, X, feature_names, *, background=None,
                         feature_perturbation="tree_path_dependent") -> np.ndarray:
    booster = _resolve_booster(model)
    if background is None:
        explainer = shap.TreeExplainer(booster)
    else:
        explainer = shap.TreeExplainer(
            booster, data=background, feature_perturbation=feature_perturbation)
    ...
```
（bias 欄剝除等既有後處理照舊。）`shap_per_item.py`：讀 `background = str(cfg.get("background", "global"))`；`per_item` 時在 per-item 迴圈內——背景＝該 item 列（`_BACKGROUND_CAP = 128`，超過用既有 seed `np.random.RandomState(seed)` 抽）、`feature_perturbation="interventional"`、該 item 的 profile 與正例 profile 改用此輸出；全域 top_features／divergence 全域向量仍用全域 pass；notes append `"shap background=per_item（interventional，背景=各 item 子母體，cap 128）；divergence 的全域向量仍為 global 背景——占比混入背景效應，判讀見手冊 §12"`。
- [ ] **Step 4: GREEN**（跑 `tests/test_diagnosis/test_model/ -q`）。
- [ ] **Step 5: 故意弄壞 per_item 分支一行**（背景改成全體列）確認 (c) 類測試轉紅後改回。
- [ ] **Step 6: Commit**（`feat(diagnosis): shap 條件化 background（global|per_item）＋attribution explainer 選項接縫`）

---

### Task 6：io optional 載入＋`triage.py` 模組＋單元測試（implementer E，sonnet）

**Files:**
- Modify: `src/recsys_tfb/io/` 的 JSONDataset 定義檔（先 grep `class JSONDataset` 定位）
- Create: `src/recsys_tfb/diagnosis/metric/triage.py`
- Test: `tests/test_io/`（JSONDataset optional 案例，additive）、`tests/test_diagnosis/test_metric/test_triage.py`

- [ ] **Step 1: 失敗測試——io optional**

```python
def test_json_dataset_optional_load_returns_none_when_missing(tmp_path):
    ds = JSONDataset(filepath=str(tmp_path / "absent.json"), optional=True)
    assert ds.load() is None

def test_json_dataset_default_still_raises_when_missing(tmp_path):
    ds = JSONDataset(filepath=str(tmp_path / "absent.json"))
    with pytest.raises(Exception):
        ds.load()

def test_json_dataset_optional_loads_normally_when_present(tmp_path):
    p = tmp_path / "x.json"; p.write_text('{"a": 1}')
    assert JSONDataset(filepath=str(p), optional=True).load() == {"a": 1}
```

- [ ] **Step 2: RED → 實作**——`__init__` 加 `optional: bool = False`；`load()` 開頭 `if self._optional and not <既有存在檢查>: logger.info(...); return None`。`save()`/`exists()` 不動。（catalog yaml 的額外鍵是否會原樣傳進 ctor——以 `io/catalog` 的 dataset 建構程式碼為準，先讀再改；若 kwargs 不透傳，把 `optional` 加進允許集合。）
- [ ] **Step 3: 失敗測試——triage 判定表**（`test_triage.py`；合成 dict fixture 覆蓋六種輸出）

```python
"""triage 單元測試——六種判定各一手算 fixture＋降級案例。"""
import pytest

from recsys_tfb.diagnosis.metric.triage import triage

def _q_item(auc, disc, level, y_rate=0.1, gap=0.0, auc_reason=None):
    return {"auc": auc, "auc_reason": auc_reason, "disc_status": disc,
            "level_status": level, "gap_vs_global": gap, "y_rate": y_rate}

def _params():
    return {"evaluation": {"diagnosis": {"triage": {"enabled": True}}}}

def _recon(verdict="可解釋", tmin=0.0, tmax=0.0, approx=False, residual=0.0):
    return {"verdict": verdict, "theory_min": tmin, "theory_max": tmax,
            "theory_approx": approx, "residual": residual}

QUADRANT = {"enabled": True, "by_item": {
    "cfg":  _q_item(0.7, "好", "偏低", gap=-0.6),           # 配置型
    "reb":  _q_item(0.7, "好", "偏高", gap=0.5),            # 再平衡型
    "stv":  _q_item(0.5, "差", "正常", y_rate=0.02),        # 餓死型
    "feat": _q_item(0.5, "差", "正常", y_rate=0.05),        # 特徵缺失型
    "ok":   _q_item(0.8, "好", "正常", y_rate=0.4),         # 健康
}}
RECON = {"enabled": True, "by_item": {
    "cfg": _recon(tmin=0.1, tmax=0.7, approx=True),
    "reb": _recon(),          # band 退化 [0,0] → 非配置訊號
    "stv": _recon(), "feat": _recon(), "ok": _recon(),
}, "theory": {"by_item": {"cfg": {"min": 0.1, "max": 0.7, "mean": 0.4}}}}
SWEEP = {"enabled": True, "per_item": {k: {"delta_star_centered": v, "loo_contribution_holdout": 0.0}
         for k, v in {"cfg": -0.5, "reb": 0.45, "stv": 0.3, "feat": 0.1, "ok": 0.0}.items()}}
GL = {"enabled": True, "fallback": False, "per_item": {
    "cfg": {"context_gain_share": 0.3}, "reb": {"context_gain_share": 0.3},
    "stv": {"context_gain_share": 0.02},   # < 0.25 × max(0.3)
    "feat": {"context_gain_share": 0.35}, "ok": {"context_gain_share": 0.3}}}

class TestVerdicts:
    def test_five_verdicts(self):
        out = triage(QUADRANT, RECON, SWEEP, GL, _params())
        v = {k: out["verdicts"][k]["verdict"] for k in out["verdicts"]}
        assert v == {"cfg": "水準-配置型", "reb": "水準-指標再平衡型",
                     "stv": "餓死型", "feat": "特徵缺失型", "ok": "健康"}

    def test_starters(self):
        out = triage(QUADRANT, RECON, SWEEP, GL, _params())["verdicts"]
        assert out["cfg"]["starter"]["value"] == pytest.approx(0.4)      # theory mean
        assert out["reb"]["starter"]["value"] == pytest.approx(0.45)     # δ* centered
        w = out["stv"]["starter"]["value"]                                # min(8, √(0.4/0.02))=4.47
        assert w == pytest.approx(4.47, abs=0.01)
        assert out["ok"]["starter"] is None
        assert all(o["starter"] is None or "起手值" in o["starter"]["caveat"]
                   for o in out.values())

    def test_config_beats_starved(self):
        # 同 item 又 level_off+config_signal 又 disc_low → 配置型優先＋次要 note
        q = {"enabled": True, "by_item": {"x": _q_item(0.5, "差", "偏低")}}
        r = {"enabled": True, "by_item": {"x": _recon(tmin=0.1, tmax=0.7)},
             "theory": {"by_item": {}}}
        out = triage(q, r, SWEEP, GL, _params())["verdicts"]["x"]
        assert out["verdict"] == "水準-配置型"
        assert any("條件判別力" in n for n in out["notes"])

class TestDegrade:
    def test_no_gain_ledger(self):
        out = triage(QUADRANT, RECON, SWEEP, None, _params())
        assert out["gain_ledger_present"] is False
        assert out["verdicts"]["stv"]["verdict"] == "餓死型或特徵缺失型（無結構層證據）"

    def test_fallback_ledger_treated_as_absent(self):
        gl = {"enabled": True, "fallback": True, "per_item": None}
        out = triage(QUADRANT, RECON, SWEEP, gl, _params())
        assert out["gain_ledger_present"] is False

    def test_quadrant_stub_yields_empty_verdicts(self):
        out = triage({"enabled": False}, RECON, SWEEP, GL, _params())
        assert out["verdicts"] == {} and any("quadrant" in n for n in out["notes"])

    def test_sweep_stub_starter_none_with_note(self):
        out = triage(QUADRANT, RECON, {"enabled": False}, GL, _params())
        assert out["verdicts"]["reb"]["starter"] is None

    def test_auc_reason_blocks_disc(self):
        q = {"enabled": True, "by_item": {"x": _q_item(0.5, "差", "正常", auc_reason="樣本不足")}}
        out = triage(q, RECON, SWEEP, GL, _params())["verdicts"]["x"]
        assert out["verdict"] == "健康" and any("AUC" in n for n in out["notes"])
```

- [ ] **Step 4: RED → 實作 `triage.py`**——照設計定案的優先序與輸出契約逐條寫（模組常數 `_STARVE_RATIO = 0.25`、`_WEIGHT_CAP = 8.0`、`STARTER_CAVEAT = "起手值，須經快迴路驗證，非定案"`、`_LEVERS` 映射表照框架 Ch 5 總表六行文案）。**不 import pyspark／plotly／pipelines／diagnosis.model**。
- [ ] **Step 5: GREEN**（`tests/test_io/ tests/test_diagnosis/test_metric/test_triage.py -q`）。
- [ ] **Step 6: 故意弄壞優先序一行**（配置型與餓死型對調）確認 `test_config_beats_starved` 轉紅後改回。
- [ ] **Step 7: Commit**（`feat(diagnosis): triage 判定表模組＋io JSONDataset optional 載入`）

---

### Task 7：evaluation node＋catalog＋pipeline＋報表 section＋config（implementer F，sonnet）

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（`compute_pair_ledger` :402-439 之後加 node 函式；`generate_report` :451 尾參）
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`（node :114-118 之後；`generate_report` inputs :119-126）
- Modify: `conf/base/catalog.yaml`（`evaluation_pair_ledger` :253-255 之後；`gain_ledger` 條目補 `optional: true`）
- Modify: `conf/base/parameters_evaluation.yaml`（diagnosis 區塊 `pair_ledger` :146-147 之後；`report.sections` :64 之後）
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（`build_triage_section`＋`assemble_report` :950-978 尾參＋glossary）
- Test（additive）: `tests/test_pipelines/test_evaluation/test_pipeline.py`、`tests/test_evaluation/test_report_builder.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 失敗測試**——(a) pipeline 結構：default/post_training 11 node、compare 14 node、`assemble_triage_summary` 位於 `compute_pair_ledger` 之後、`generate_report` inputs 尾端 `"evaluation_triage"`；(b) node 函式：`enabled: false` → `{"enabled": False}` stub；gain_ledger=None 傳遞後 `gain_ledger_present is False`；(c) 報表：triage dict → section 有主表、`triage=None` → 無 section、sections 開關 false → 無 section。
- [ ] **Step 2: RED → 實作**

`nodes_spark.py`（照 `compute_pair_ledger` 版型）：

```python
def assemble_triage_summary(quadrant: Optional[dict], reconciliation: Optional[dict],
                            offset_sweep: Optional[dict], gain_ledger: Optional[dict],
                            parameters: dict) -> dict:
    """Triage 總表 node：純 dict 合成，gain_ledger 缺席 best-effort 降級。"""
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    if not (diag.get("triage", {}) or {}).get("enabled", True):
        return {"enabled": False}
    from recsys_tfb.diagnosis.metric.triage import triage
    return triage(quadrant, reconciliation, offset_sweep, gain_ledger, parameters)
```

`pipeline.py`（`compute_pair_ledger` node 之後）：

```python
        Node(
            assemble_triage_summary,
            inputs=["evaluation_quadrant", "evaluation_reconciliation",
                    "evaluation_offset_sweep", "gain_ledger", "parameters"],
            outputs="evaluation_triage",
        ),
```
`generate_report` inputs 尾端加 `"evaluation_triage"`；函式尾參 `triage: Optional[dict] = None` 傳進 `assemble_report`。

`catalog.yaml`：

```yaml
evaluation_triage:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/triage.json
```
並在既有 `gain_ledger` 條目補 `optional: true`（跨側讀取：evaluation 載入時檔案缺席回 None）。

`parameters_evaluation.yaml`：`pair_ledger` block 之後加

```yaml
  triage:
    enabled: true   # 三層診斷合成總表；判讀見 docs/pipelines/evaluation-diagnosis.md §13
```
`report.sections` 加 `triage: true`。

`report_builder.py`：`build_triage_section(triage, parameters)` 照 `build_pair_ledger_section` 版型——主表欄位（item／判定／建議槓桿／起手值（type=value unit，含 caveat 註腳一次）／AUC／gap_vs_global／δ*centered／context_gain_share／備註）＋summary 一行；glossary 加 3 條（triage 總表、餓死型、起手值）。`assemble_report` 簽名尾加 `triage=None`、組裝順序在 pair_ledger section 之後。

- [ ] **Step 3: GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ tests/test_evaluation/test_report_builder.py \
   tests/test_evaluation/test_parameters_evaluation_yaml.py -q 2>&1 | tail -5
```
- [ ] **Step 4: 故意弄壞 node stub 分支**確認測試轉紅後改回。
- [ ] **Step 5: Commit**（`feat(evaluation): assemble_triage_summary node＋triage 報表 section＋catalog/config 接線`）
- [ ] **Step 6（controller）: graphify rebuild**（CLAUDE.md §graphify 指令）。

---

### Task 8：合併審查（sonnet reviewer，**背景執行**，與 Task 9 並行）

待審物＝Task 3–7 全部 commit 的 `git diff`（controller 提供 SHA 範圍）。prompt 照 `~/.claude/rules/30-delegation-templates.md` 模板 5＋追加護欄：附 controller 既有綠燈證據（各 task 測試輸出），明令**只讀 diff＋只跑新增/變更的測試檔**，列至少 3 個具體問題（檔案:行號＋失敗情境），verdict PASS/PASS-with-nits/FAIL。重點面向：邊界（triage 不 import 訓練側／diagnosis 不 import pyspark）、A20 全量 dict 假設、optional 載入對既有 JSONDataset 使用者零影響、遍歷演算法 stack 語意（reachable 傳遞）、報表 XSS/格式。

---

### Task 9：真跑閘門（controller 直跑；Task 8 審查背景同時進行）

**閘門結構**：State A（乾淨態）→ 已知答案 1（gain_ledger 方向性）→ per_item SHAP smoke → 已知答案 2（注入重演，唯一重訓）→ State C（還原位元一致）。**順序不可換**：State A 的報表逐字回歸必須在注入重訓之前（執行協議鐵則）。

- [ ] **Step 1: pre-flight**（同 Task 1 Step 1，另 `grep -n "background:\|gain_ledger:\|triage:" conf/base/parameters_training.yaml conf/base/parameters_evaluation.yaml` 確認讀到 worktree 新值）。
- [ ] **Step 2: 切片補產 gain_ledger（6059dcef，零重訓）**——先 `--dry-run` 看 `[plan]`：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training \
  --env local --only-node compute_gain_ledger --dry-run
```
Expected：auto-included 只有便宜 node（`select_features` 等）、**無 `[retrain]` 警告、`finalize_model` 不在必跑集合**、model_version 解析為 6059dcef。任一不符 → 停，回頭查 config 是否誤動 `training:`。確認後去掉 `--dry-run` 真跑（背景）。產物：`data/models/6059dcef/diagnostics/gain_ledger.json`。注意 manifest 會被覆寫（skip-if-present 的 stub 不覆蓋、post-run metadata 多 `only_node` 留痕）——跑完 `diff <(jq -S . /tmp/phase5_manifest_before.json) <(jq -S . data/models/6059dcef/manifest.json)`，差異僅限 metadata 留痕欄即接受、記錄原文。
- [ ] **Step 3: 已知答案 1（方向性）**——讀 gain_ledger.json：`fund_mix`（n_pos=23 最冷）的 `context_gain_share` 應顯著低於熱門 item（對照 `exchange_usd`/`ccard_bill`）；同時記錄 `item_id.gain_share`、per-item 全表、node 秒數。**如實記錄實際值**；方向不符→停下分析（先查記帳規則再懷疑模型）。
- [ ] **Step 4: State A——evaluation 乾淨態（報表逐字回歸）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation \
  --env local --model-version 6059dcef --post-training   # 背景執行
```
驗收：(a) **五份既有 JSON 與 `/tmp/phase5_json_before/` 逐檔 `cmp` 位元一致**（負控制；任何差異→停）；(b) 新 `diagnosis/triage.json` 產出，`gain_ledger_present: true`；(c) report.html 新增 triage section、其餘 section 與 Phase 4b 基準逐字一致（`diff` 舊報表快照，僅 triage 段新增）；(d) **交叉驗證錨**：`fund_stock`/`fund_bond` 判定應落餓死型（Phase 4b 結論：AUC≈0.5＋substitution 淨傷害）、`exchange_usd`/`ccard_ins` 應判健康——實際判定全表如實記錄（含 `_STARVE_RATIO` 門檻下的實際 share 值）；錨不符→停下分析判定表，不改門檻硬湊。State A 產物快照到 `/tmp/phase5_state_A/`。
- [ ] **Step 5: per_item SHAP smoke（可還原性驗證）**——先備份 `cp data/models/6059dcef/diagnostics/shap_diagnostics.json /tmp/phase5_shap_before.json`；`parameters_training.yaml` 暫設 `background: per_item` → `--only-node compute_shap_diagnostics` 切片（背景；記 wall time）→ 檢視輸出結構與 notes → 還原 `background: global` → 重跑同切片 → `cmp` 與 `/tmp/phase5_shap_before.json` **位元一致**（SHAP 決定性）；不一致→如實記錄差異並分析（浮點/版本因素），以「還原後 dict 數值 allclose」為底線。
- [ ] **Step 6: 已知答案 2——Phase 2 注入重演（唯一重訓；背景執行全程）**——(a) 從 Phase 2 計畫檔（`ls docs/superpowers/plans/ | grep phase2` 定位）抄出 fund_bond 抽樣注入 yaml（三 segment 0.5）原文照設；(b) 依序真跑 `dataset` → `training` → `evaluation --model-version <新 mv> --post-training`（新 mv 從 training 輸出取；Phase 2 當時為 8883dd58，這次因 Phase 5 config 無關鍵不同可能相同——如實記錄）；(c) 驗收：新 mv 的 triage 判 `fund_bond` 為**「水準-配置型」而非餓死型**、starter 帶 logQ offset 值（對照 reconciliation theory band）＋notes 帶次要訊號；(d) 還原注入 config（`git checkout -- conf/` 或逐鍵還原），`git status` 乾淨。
- [ ] **Step 7: State C——還原位元一致**——重跑 Step 4 同指令 → `data/evaluation/6059dcef/20260131/diagnosis/` **六份 JSON**（五份既有＋triage.json）與 `/tmp/phase5_state_A/` 逐檔 `cmp` 位元一致。
- [ ] **Step 8: 效能與素材包**——彙整：gain_ledger node 秒數、triage node 秒數、per_item SHAP wall time、公司規模（22 item、200k query）外推；State A 判定全表、fund_mix vs 熱門 share 對照、注入前後 fund_bond 判定對照——寫進 `/tmp/phase5_doc_materials.md` 供 Task 10。

---

### Task 10：文件（writer＋讀者 agent；素材包由 controller 從 Task 9 備好）

**Files:**
- Modify: `docs/pipelines/evaluation-diagnosis.md`（新 §12 結構層、§13 triage、已知限制節改號 §14＋新增條目、名詞速查）
- Modify: `docs/pipelines/training.md`（診斷節加路由行）
- Modify: `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`（帶日期執行時修訂）

- [ ] **Step 1: 手冊 §12 結構層**——gain_ledger 是什麼（先通用原理：GBDT 切點預算與 Gain；再套本框架）、輸出鍵逐一判讀、**數感節**（真跑值：fund_mix vs 熱門的 share 對照、item_id.gain_share）、conditioned vs isolated 兩欄的差別與陷阱（8 item 淺樹 isolated 普遍 0）、條件化 SHAP background 判讀（per_item 時 divergence 混入背景效應的已知限制）、成本註記（interventional O(rows×bg×trees)）。
- [ ] **Step 2: 手冊 §13 triage**——判定表優先序原文（含「配置型優先於餓死型」的理由：閉式解便宜先修）、五種判定＋槓桿＋起手值欄各自的讀法、**起手值 caveat 的意義**（centered δ* 的 gauge 說明；w∝1/√P 上限 8 的出處手冊3 Ch8）、真跑走讀（State A 判定全表＋注入重演前後 fund_bond 對照）、`_STARVE_RATIO`/`_WEIGHT_CAP` 是起手門檻非定案、gain_ledger 缺席的降級行為。§14 已知限制新增：triage 單標籤但病灶可複合（notes 補次要訊號）、starve 門檻的任意性、per_item SHAP 背景效應。
- [ ] **Step 3: training.md 路由行**＋spec 三處帶日期修訂（gain_ledger node inputs 含 preprocessor_view 及理由；A20 涵蓋三鍵；triage 起手值採 centered 定案）。
- [ ] **Step 4: 讀者 agent 驗收**——fresh sonnet 通讀 §12/§13：禁開發詞彙、只有 repo 的人能照著判讀、數字與 JSON 對得上；問題逐條修。
- [ ] **Step 5: Commit**（`docs: 判讀手冊 §12 結構層＋§13 triage＋spec Phase 5 執行時修訂`）

---

### Task 11：opus 總審＋nit 修復（**背景執行**）

待審物＝Phase 5 全部 commit（Task 3–10）＋Task 9 閘門證據原文。模板 5＋護欄；重點：記帳規則對框架 Ch 3 項目 8 的忠實度、triage 優先序的判讀正確性（對照 Ch 4.1）、已知答案證據鏈完整性、手冊與 code 識別字逐字對齊。verdict READY / READY-with-nits / NOT-READY；nits 由 controller 修復後 commit。

### Task 12：使用者閘門

彙報：閘門五步證據（State A 位元一致、已知答案 1/2 實際值、State C 位元一致）、triage 判定全表、效能數字、opus verdict、**沒做的事**（loss-SHAP 只留參數空間、starve 門檻未 config 化、粗帳本是否啟用、per_item SHAP 未進正式產物流程——只 smoke）。等使用者指示後進收尾（finishing-a-development-branch → PR，涵蓋 Phase 0–5）。

---

## Self-review（writing-plans skill 要求，寫完計畫後 controller 自查）

- [x] Spec 覆蓋：spec §3 Phase 5 三塊（gain_ledger＋node＋catalog；shap background＋attribution 接縫；triage＋node＋report＋降級）、config 三鍵、A20、驗收 1–3 全部有對應 task；spike 排第一個實作 task（Task 2）。
- [x] Spec 偏差已顯式化：node inputs 加 `preprocessor_view`（Task 10 寫回 spec 修訂）；A20 擴為三鍵（additive）；跨側讀取用 io optional 而非 node 內 try/except（機制寫進設計定案）。
- [x] 無占位符：每個 code step 有完整代碼或「先 Read 再改」的精確錨點＋契約；測試有手算錨精確值。
- [x] 型別/鍵名一致性：`context_gain_share`／`delta_star_centered`／`y_rate`／`verdict`／`starter` 在 Task 3/6/7/9/10 之間逐字一致；triage 讀的上游鍵名對照真實 JSON（2026-07-08 實測 dump）核過。
- [x] 已知答案雙錨：方向性（fund_mix share 低）＋重演（fund_bond 配置型）＋Phase 4b 交叉錨（fund_stock/fund_bond 餓死型、exchange_usd/ccard_ins 健康）。
- [x] 協議合規：報表逐字回歸在唯一重訓（Task 9 Step 6）之前（Step 4）；注入 config 必還原＋State C 位元驗證；>2 分鐘指令全背景；驗證不自驗（Task 8/10/11 fresh agent）。
