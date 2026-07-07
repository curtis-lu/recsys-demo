# 診斷框架開發交接檔（/compact 前固化；最後更新：2026-07-07 Phase 2 閘門通過後）

> 給續作 session：讀完本檔＋下列文件，即可直接開工，不需要舊對話。

## 現在進行到哪

- **Phase 0（診斷域歸位）、Phase 1（指標基座）、Phase 2（對帳層）全部完成且使用者閘門通過**（2026-07-07）。branch `feat/diag-framework`（worktree `.worktrees/diag-framework`）@ `cf9404f`。
- **下一步＝寫 Phase 3（行為層象限）的實作計畫**，然後 subagent-driven 執行。

## 唯一真實來源（先讀這些）

1. **Spec**：`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`——**Phase 3 要做什麼全在 §3 Phase 3**（框架診斷項目 3、5、10）。注意 spec 有兩處執行時修訂已入文：§3 固定結構含「文件」一等交付物；Phase 2 段的 verdict 相對全局修訂。
2. **計畫範本**：`docs/superpowers/plans/2026-07-07-diag-phase2-reconciliation.md`（最新一份；含「設計定案」節與追加的 Task 8 文件交付模式）。Phase 0/1 計畫同目錄。
3. **判讀手冊**：`docs/pipelines/evaluation-diagnosis.md`——Phase 3 新增報表段落時**必須**擴充此檔（見下方執行協議 6）。
4. 方法論背景（需要時再讀）：`docs/ranking-diagnosis-framework.md`。

## Phase 0–2 之後的 code 現狀

- `src/recsys_tfb/diagnosis/`：`model/`＝訓練側診斷（Phase 0 平移）；`metric/`＝`sample.py`（兩趟診斷抽樣 `draw_diagnosis_sample`）、`uncertainty.py`（cluster bootstrap `bootstrap_per_item_ci`）、`reconciliation.py`（`theoretical_offsets`／`calibration_gap_by_item`／`reconcile`，verdict＝gap−global_reference 比理論帶）。依賴白名單：`pipelines/* → diagnosis → core / evaluation(僅 numpy 原語 metrics.py) / io / utils`。
- `evaluation/metrics.py`：`positive_row_contributions`、`macro_from_per_item`、參數化 `compute_macro_per_item_map`（weight_alpha/min_positives/shrinkage_k，預設等價現行）。`metrics_spark.py`：`aggregate_per_item` 出 `n_pos`、`macro_average` 接參數（預設路徑保留原 sum/len 逐位元）、`_compute_core` 出 `observation_items`。
- 評估 pipeline **7 個 node**（新增 `compute_metric_ci`、`compute_reconciliation`）；catalog：`evaluation_metric_ci`、`evaluation_reconciliation` → `data/evaluation/${mv}/${snap}/diagnosis/*.json`。
- consistency：**A15**（metric/diagnosis 參數域）＋ **A16**（reconciliation；含 enabled bool 檢查）；下一個代號 **A17**。
- config：`evaluation.metric`、`evaluation.diagnosis.{sample,ci,reconciliation}`；`report.sections.reconciliation`。
- 測試新家：`tests/test_diagnosis/test_metric/`（sample/uncertainty/reconciliation）；相關測試全綠 @ cf9404f。

## 本機環境狀態

- local Spark 已 setup。`data/models/` 有 **6059dcef**（正式示例模型，Phase 2 閘門還原時重訓過——與最初版位元級不同屬預期）與 **8883dd58**（fund_bond 注入實驗產物，留作對照可不理）。`data/models/` 另有測試殘留目錄（e2e_test_mv、mvx…）。
- **evaluation CLI 必帶 `--model-version 6059dcef`**（無 `best` symlink；promote 是使用者保留的人工步驟）。
- **取 model_version 禁用 `ls -t data/models`**（目錄 mtime 陷阱，見 known-pitfalls.md §6 末條）——從 training log 的 `Wrote manifest` 行取。
- `/tmp` 的 phase1/phase2 基準檔重開機會消失；Phase 3 不依賴它們。

## 已確立的執行協議（使用者定案＋實證教訓，勿走回頭路）

1. **一階段一份計畫**；每階段結束＝本機真跑產物＋已知答案注入閘門，**使用者檢視通過才進下一階段**。
2. **subagent token 成本控制**：機械步驟 controller 直跑；sonnet implementer（prompt 給「計畫檔路徑＋要點摘錄」即可，計畫檔本身是零佔位符規格）；合併 reviewer 批次審；opus 只做階段總審。
3. 行為不變類判準＝與 baseline 一致；報表逐字回歸比對必須在任何重訓**之前**做（重訓位元重現性未鎖）。
4. shaprx 已擱置；HPO objective 不動。
5. 既有測試的 exact-set／結構斷言若因 additive 鍵或新節點必須更新，屬合法改動——在計畫「設計定案」節預授權或執行時裁決後記錄。
6. **文件是一等交付物（spec §3 固定結構）**：新報表段落必同步擴充 `docs/pipelines/evaluation-diagnosis.md`。寫法鐵則（Phase 2 四輪返工的教訓，詳見 memory feedback_analysis_docs_handbook_style）：(a) 手冊禁用開發詞彙（本機/Phase N/spec/驗收/真跑），交付前 grep；(b) 貫穿範例契約——**把示例產物直接印進文件**，各節走讀看得見的表，嚴禁敘述讀者看不見的報表；(c) 對無直覺尺度建「數感」節（錨點表＋門檻合理性的夾擠論證）；(d) 報表描述只留短判讀順序＋指向手冊；(e) 交付前派讀者 agent，驗證清單含「列出所有指涉你看不到的東西的詞」。
7. **質性反饋（讀不懂類）不得用字面替換修復＋自驗**——判準已入 `~/.claude/rules/20-judgment-rubrics.md` §2 反例：修法可用 sed 表達＝假修復；必須從段落目的重寫＋fresh 讀者驗收。

## Phase 3 開工提醒

1. 先讀 spec §3 Phase 3 全文再拆計畫。既知前置（spec 審查記錄）：within-item AUC 需 **midrank tie 處理**＋「全同分 item 必須恰得 0.5」的測試 fixture；象限判準＝加害者只看水準偏高（手冊定案）；兩軸＝水準（Phase 2 的 gap_vs_global 可重用）×條件判別力。
2. 抽樣底座（`diagnosis/metric/sample.py`）與 reconciliation 的 per-item 產物都是現成積木，先查可重用性再寫新件。
3. 計畫必含「文件」task（判讀手冊擴充＋示例表走讀＋讀者通讀），照 Phase 2 計畫 Task 8 的追加模式直接內建。
4. 新 consistency 代號從 **A17** 起。
