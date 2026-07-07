# 診斷框架開發交接檔（/compact 前固化；最後更新：2026-07-08 Phase 3 閘門通過後）

> 給續作 session：讀完本檔＋下列文件，即可直接開工，不需要舊對話。

## 現在進行到哪

- **Phase 0（診斷域歸位）、Phase 1（指標基座）、Phase 2（對帳層）、Phase 3（行為層象限）全部完成且使用者閘門通過**（Phase 3 於 2026-07-08 通過）。branch `feat/diag-framework`（worktree `.worktrees/diag-framework`）@ `8cc4651`。
- **下一步＝寫 Phase 4（指標層分流：offset_sweep＋pair_ledger）的實作計畫**，然後 subagent-driven 執行。

## 唯一真實來源（先讀這些）

1. **Spec**：`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`——**Phase 4 要做什麼全在 §3 Phase 4**（框架診斷項目 6、7；約 131–151 行）。spec 有**四處**帶日期的執行時修訂已入文（固定結構含文件、Phase 2 verdict 相對全局、Phase 3 散布圖 plotly 非 matplotlib、A17 域排除 0.5）——都是合法修訂，不是矛盾。
2. **計畫範本**：`docs/superpowers/plans/2026-07-07-diag-phase3-quadrant.md`（最新一份；「設計定案」節＋內建文件 task＋閘門三狀態模式）。Phase 0/1/2 計畫同目錄。
3. **判讀手冊**：`docs/pipelines/evaluation-diagnosis.md`——Phase 4 新增報表段落時**必須**擴充此檔（見執行協議 6；現況：§8 象限層已入，§9 metric_ci、§10 已知限制）。
4. 方法論背景（需要時再讀）：`docs/ranking-diagnosis-framework.md`。

## Phase 0–3 之後的 code 現狀

- `src/recsys_tfb/diagnosis/metric/`：`sample.py`（兩趟診斷抽樣 `draw_diagnosis_sample(eval_predictions, parameters) -> (pandas DF, meta)`）、`uncertainty.py`（`bootstrap_per_item_ci`）、`reconciliation.py`（`theoretical_offsets`／`calibration_gap_by_item`／`reconcile`，verdict＝gap_vs_global 比理論帶）、**Phase 3 新增**：`discrimination.py`（`within_item_auc`，midrank rank-sum）、`occupancy_spark.py`（`top_slot_share`／`suppression_counts`，rank 欄已防禦 cast）、`cross_purchase.py`（`cross_purchase_matrix -> (prob_df, n_buyers)`）、`quadrant.py`（`build_quadrant_summary`，六格標籤＋best-effort 降級）。`diagnosis/model/`＝訓練側診斷。依賴白名單：`pipelines/* → diagnosis → core / evaluation(僅 numpy 原語 metrics.py) / io / utils`；plotly 只准在 evaluation 報表側。
- `evaluation/metrics.py`：`positive_row_contributions`、`macro_from_per_item`、參數化 `compute_macro_per_item_map`（weight_alpha/min_positives/shrinkage_k）——**這組 numpy 原語就是 Phase 4 offset_sweep 的目標函數積木**。
- 評估 pipeline **8 個 node**（`compute_metric_ci`、`compute_reconciliation`、`compute_quadrant`）；catalog：`evaluation_metric_ci`／`evaluation_reconciliation`／`evaluation_quadrant` → `data/evaluation/${mv}/${snap}/diagnosis/*.json`。注意 node 實際順序由拓撲排序決定（`persist_eval_predictions` 會插在 `compute_quadrant` 前），結構測試已依實測順序寫。
- 報表：`report_builder.py` 的 `build_reconciliation_section`／`build_quadrant_section`（含 `_quadrant_scatter`，plotly `go.Figure` 內嵌）；`assemble_report(metrics, parameters, baseline_metrics, diagnostics_frames, metric_ci, reconciliation, quadrant)`。
- consistency：**A15**（metric/diagnosis）＋**A16**（reconciliation）＋**A17**（quadrant；auc_threshold ∈ (0.5,1) 嚴格排除 0.5、gap_band>0、top_k int≥1、enabled bool）；**下一個代號 A18**。
- config：`evaluation.metric`、`evaluation.diagnosis.{sample,ci,reconciliation,quadrant}`；`report.sections.{reconciliation,quadrant}`。
- 測試：`tests/test_diagnosis/test_metric/`（7 檔）＋pipeline/report/consistency/yaml 測試；相關全套 **297 passed 零 fail** @ a4e3256。

## 本機環境狀態

- local Spark 已 setup。**SPARK_LOCAL_IP=127.0.0.1 已釘進 `tests/conftest.py` 與 `conf/spark-local/spark-env.sh`**（macOS 換網路後 hostname 解析過期 IP 會讓 Spark bind 秒炸——症狀識別見 known-pitfalls.md §7，勿在下游 debug）。
- `data/models/` 有 **6059dcef**（正式示例模型）與 **8883dd58**（fund_bond 注入實驗產物，可不理）＋測試殘留目錄。**evaluation CLI 必帶 `--model-version 6059dcef`**（無 `best` symlink；promote 是使用者保留的人工步驟）。
- **取 model_version 禁用 `ls -t data/models`**（known-pitfalls.md §6 末條）——從 training log 的 `Wrote manifest` 行取。
- 現行 `data/evaluation/6059dcef/20260131/diagnosis/` 有三份 JSON（metric_ci／reconciliation／quadrant_summary），是 Phase 3 還原後的乾淨狀態，可直接當 Phase 4 的改動前基準。`/tmp/phase3_*` 檔重開機會消失，勿依賴。
- Phase 3 實測基準數字（寫死在 quadrant 計畫「設計定案」，Phase 4 注入設計可參考）：`ccard_ins` gap_vs_global ≈ +0.329；7 個中性 item 落 −0.186～+0.071；fund_* 三個 AUC 0.46–0.52。

## 已確立的執行協議（使用者定案＋實證教訓，勿走回頭路）

1. **一階段一份計畫**；每階段結束＝本機真跑產物＋已知答案注入閘門，**使用者檢視通過才進下一階段**。
2. **subagent token 成本控制**：機械步驟 controller 直跑；sonnet implementer；合併 reviewer 批次審；opus 只做階段總審。
3. 行為不變類判準＝與 baseline 一致；報表逐字回歸比對必須在任何重訓**之前**做（重訓位元重現性未鎖；Phase 4 若如 spec 所寫只動 evaluation 側就不需重訓）。
4. shaprx 已擱置；HPO objective 不動。
5. 既有測試的 exact-set／結構斷言若因 additive 鍵或新節點必須更新，屬合法改動——在計畫「設計定案」節預授權或執行時裁決後記錄。
6. **文件是一等交付物（spec §3 固定結構）**：新報表段落必同步擴充 `docs/pipelines/evaluation-diagnosis.md`。寫法鐵則（Phase 2 四輪返工的教訓，詳見 memory feedback_analysis_docs_handbook_style）：(a) 手冊禁用開發詞彙（本機/Phase N/spec/驗收/真跑），交付前 grep；(b) 貫穿範例契約——**把示例產物直接印進文件**，各節走讀看得見的表，嚴禁敘述讀者看不見的報表；(c) 對無直覺尺度建「數感」節（錨點表＋門檻合理性論證）；(d) 報表描述只留短判讀順序＋指向手冊；(e) 交付前派讀者 agent，驗證清單含「列出所有指涉你看不到的東西的詞」。
7. **質性反饋（讀不懂類）不得用字面替換修復＋自驗**——判準已入 `~/.claude/rules/20-judgment-rubrics.md` §2 反例：修法可用 sed 表達＝假修復；必須從段落目的重寫＋fresh 讀者驗收。
8. **對使用者的訊息中，檔案引用一律絕對路徑（含 `.worktrees/diag-framework`）**（使用者多次糾正後定案）：相對路徑在 worktree 工作流下點不開／開到 main 舊檔；轉述 subagent 回報時把 `檔案:行號` 補上絕對前綴再交付。詳見 memory feedback_clickable_absolute_paths。
9. **提速協議（2026-07-08 Phase 3 覆盤定案；品質手段——TDD、突變檢查、雙審、讀者 agent、真跑閘門——一項不減）**：
   - (a) **派工內嵌全文**：implementer prompt 直接貼該 task 全文＋執行者必讀＋設計定案，計畫檔路徑只作查證用——Phase 3 七個 agent 各自重讀 1,480 行計畫，估浪費 10+ 分鐘與數萬 tokens。
   - (b) **同構小模組合批**：互不相依、setup 相同的小模組合成一次派工，agent 內部逐模組 TDD＋各自 commit。
   - (c) **審查分工釘死、禁止重工**：合併 reviewer 只讀 diff＋只跑新增測試檔（prompt 附 controller 的綠燈證據並明令不重跑全套）；opus 總審管閘門證據核驗與跨檔一致性。Phase 3 的 sonnet 合併審自行重驗全套花了 65 分鐘，大半與既有證據重複。
   - (d) **審查一律背景執行、與真跑閘門/文件任務並行**：Phase 3 靠這招把 65 分鐘審查幾乎完全遮進閘門與文件的時段——是制度不是巧合，照做。
   - (e) **文件任務的素材包由 controller 先備好**（示例表 markdown、判讀素材數字、既有節指向），writer 不自己挖數字。

## Phase 4 開工提醒

1. **先讀 spec §3 Phase 4 全文再拆計畫**（框架診斷項目 6、7）。交付物：`diagnosis/metric/offset_sweep.py`（`sweep`：座標下降、δ 向 0 收縮 shrink_lambda、holdout_fraction 折外報告——防「收復缺口只是擬合驗證雜訊」）；`diagnosis/metric/pair_ledger.py`（`pair_ledger` 壓制者×受害者矩陣＋`substitution_ablation`＋**傷害×segment 分組** by_segment 區塊）；config `evaluation.diagnosis.{offset_sweep,pair_ledger,debug_inject_offsets}`；**A18**；report section `offset_sweep`（waterfall 圖——**照 spec 修訂走 plotly**，樣式參考手冊 fig6，非 matplotlib）。
2. **全部跑在診斷抽樣上（driver-side numpy）**：輸入用 Phase 1 的 `draw_diagnosis_sample`（現成）＋`evaluation/metrics.py` 參數化 numpy 家族當 sweep 目標函數（白名單允許 diagnosis import 它）。不是 Spark 聚合——這與 Phase 2/3 的模式不同，計畫的效能考量（22 item × 抽樣上限 200k query 的座標下降輪數）要先估。
3. **閘門零重訓**：`debug_inject_offsets` 對某 item 設 +1.0 → sweep 的 δ*_j ≈ −1.0（容差配 CI 讀）、折外 mAP(δ*) > mAP(0)、pair_ledger 該 item 壓制暴增；清掉 → δ* 全體接近 0（收縮生效）。注入只動 evaluation config，沿 Phase 3 的門檻注入模式。
4. **邊界注意（spec 明文）**：segment 欄位在 `prepare_eval_data` 就已併進 eval_predictions——pair_ledger 的 by_segment 只消費欄位、**不 import** `evaluation/segments.py`。
5. **計畫階段先問使用者要不要拆兩個閘門**（offset_sweep 與 pair_ledger 各一）——Phase 3 覆盤的結論之一：範圍厚度要在計畫時決定，不是事後覺得久。
6. 計畫必含「文件」task（判讀手冊擴充；sweep 的 δ*、折內/折外、waterfall 都是無直覺概念，數感節與示例表走讀比照 §8 規格）。
7. 新 consistency 代號從 **A18** 起。
