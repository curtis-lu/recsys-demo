# 診斷框架開發交接檔（/compact 前固化；最後更新：2026-07-08 Phase 4a 閘門通過後）

> 給續作 session：讀完本檔＋下列文件，即可直接開工，不需要舊對話。

## 現在進行到哪

- **Phase 0（診斷域歸位）、Phase 1（指標基座）、Phase 2（對帳層）、Phase 3（行為層象限）、Phase 4a（分流層 offset sweep）全部完成且使用者閘門通過**（Phase 4a 於 2026-07-08 通過）。branch `feat/diag-framework`（worktree `.worktrees/diag-framework`）@ `f44d4f8` 之後。
- **Phase 4 已由使用者定案拆兩個閘門**（2026-07-08）：4a=offset_sweep（已完成）、4b=pair_ledger。
- **下一步＝寫 Phase 4b（壓制帳本：pair_ledger＋substitution_ablation＋傷害×segment）的實作計畫**，然後 subagent-driven 執行。

## 唯一真實來源（先讀這些）

1. **Spec**：`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`——**Phase 4b 要做什麼在 §3 Phase 4 的 pair_ledger 段**（約 138–141 行；診斷項目 7）。spec 有**五處**帶日期執行時修訂已入文（固定結構含文件、Phase 2 verdict 相對全局、Phase 3 散布圖 plotly、A17 域排除 0.5、**Phase 4 gauge/centered 讀法＋合成資料指標再平衡缺口實證**）——都是合法修訂，不是矛盾。
2. **計畫範本**：`docs/superpowers/plans/2026-07-08-diag-phase4a-offset-sweep.md`（最新一份；「設計定案」節＋內建文件 task＋閘門三狀態＋提速協議內建）。
3. **判讀手冊**：`docs/pipelines/evaluation-diagnosis.md`——現況：§10 分流層（10.1–10.7）、§11 已知限制（檔尾）。Phase 4b 新增報表段落**必須**擴充此檔。
4. 方法論背景（需要時再讀）：`docs/ranking-diagnosis-framework.md`（pair_ledger＝Ch 3 診斷項目 7、λ 會計）。

## Phase 0–4a 之後的 code 現狀

- `src/recsys_tfb/diagnosis/metric/`：`sample.py`（`draw_diagnosis_sample`；**keep_cols 目前不含 segment 欄**——4b 前置要補）、`uncertainty.py`、`reconciliation.py`、`discrimination.py`、`occupancy_spark.py`、`cross_purchase.py`、`quadrant.py`、**Phase 4a 新增 `offset_sweep.py`**（`sweep(sample_pdf, parameters) -> dict`：logit 平移、座標下降＋λ·g²/M 收縮＋平手偏 0、query 層折切 seed 重用 sample.seed、折外 LOO 拆解、`delta_star`＋**`delta_star_centered`**（gauge 去均值）、`debug_inject_offsets` 注入 scope 僅此模組、groupby `dropna=False` 防 null 鍵繞折）。依賴白名單不變：diagnosis 不 import plotly/pyspark（offset_sweep 純 numpy/pandas）。
- 評估 pipeline **9 個 node**（`compute_offset_sweep` in=[eval_predictions, parameters] out=`evaluation_offset_sweep`；拓樸序落在 `compute_reconciliation` 後、`persist_eval_predictions` 前）；catalog：`evaluation_offset_sweep` → `diagnosis/offset_sweep.json`。
- 報表：`report_builder.py` 的 `build_offset_sweep_section`＋`_offset_sweep_waterfall`（plotly `go.Waterfall`，藍 #1565c0 收復/橘 #e65100 負向；全零 δ* 不畫圖）；`assemble_report(..., offset_sweep=None)`。
- consistency：A15–A17＋**A18**（offset_sweep 參數域＋debug_inject_offsets 有限實數；grid 須含 0）；**下一個代號 A19**。
- config：`evaluation.metric`（含 k——診斷家族截斷，null=full mAP，**不影響報表主指標**）、`evaluation.diagnosis.{sample,ci,reconciliation,quadrant,offset_sweep,debug_inject_offsets}`；`report.sections.{reconciliation,quadrant,offset_sweep}`。
- 測試：相關四目錄（test_diagnosis/test_metric、test_consistency、test_report_builder、test_pipelines/test_evaluation）**259 passed 零 fail** @ f44d4f8。

## 本機環境狀態

- **本機跑 evaluation 必帶 `--post-training`**（known-pitfalls §6 首條，2026-07-08）：default 模式讀 `ml_recsys.ranked_predictions`（inference 產物）本機沒有，`prepare_eval_data` 秒炸 Table not found。
- SPARK_LOCAL_IP=127.0.0.1 已釘 conftest＋spark-env.sh（known-pitfalls §7）；模型 **6059dcef**、`--model-version` 必帶、`ls -t` 禁用（§6）。
- 現行 `data/evaluation/6059dcef/20260131/diagnosis/` 有**四份 JSON**（metric_ci／reconciliation／quadrant_summary／offset_sweep），是 Phase 4a 還原後的乾淨基準（位元復原已驗，含 k=1/k=3 實驗後的復原）。`/tmp/phase4a_*`、`/tmp/k*_*.json` 重開機會消失，勿依賴。
- Phase 4a 實測基準數字（乾淨態）：δ*（centered）fund_bond +0.66、fund_stock +0.51、exchange_usd −0.54、其餘≈0；折內收復 +0.0515、折外 +0.0352；節點 5.6s（654 query×8 item）。注入 ccard_ins +1.0 時 centered 位移 −1.05（原始 −1.40——**注入判讀一律用 centered 位移**，spec 修訂有寫）。
- `evaluation.metric.k` 實驗結論（2026-07-08，產物已還原）：k=1/k=3 都讓冷門 fund 系 AP 精確歸零（CI 飽和 [0,0]＝從未進榜，不是精確的零）、診斷解析度喪失；預設留 `k: null`。

## 已確立的執行協議（使用者定案＋實證教訓，勿走回頭路）

1. **一階段一份計畫**；每階段結束＝本機真跑產物＋已知答案注入閘門，**使用者檢視通過才進下一階段**。
2. **subagent token 成本控制**：機械步驟 controller 直跑；sonnet implementer；合併 reviewer 批次審；opus 只做階段總審。
3. 行為不變類判準＝與 baseline 一致；報表逐字回歸比對必須在任何重訓**之前**做（Phase 4b 只動 evaluation 側就不需重訓）。
4. shaprx 已擱置；HPO objective 不動。
5. 既有測試的 exact-set／結構斷言若因 additive 鍵或新節點必須更新，屬合法改動——計畫「設計定案」節預授權或執行時裁決後記錄。
6. **文件是一等交付物（spec §3 固定結構）**：新報表段落必同步擴充 `docs/pipelines/evaluation-diagnosis.md`。寫法鐵則（詳見 memory feedback_analysis_docs_handbook_style）：(a) 手冊禁用開發詞彙，交付前 grep；(b) 示例產物直接印進文件走讀；(c) 無直覺尺度建「數感」節；(d) 報表描述只留短判讀順序＋指向手冊；(e) 交付前派 fresh 讀者 agent（驗證清單含「列出所有指涉你看不到的東西的詞」）。
7. **質性反饋（讀不懂類）不得用字面替換修復＋自驗**——判準見 `~/.claude/rules/20-judgment-rubrics.md` §2 反例。
8. **對使用者的訊息中，檔案引用一律絕對路徑（含 `.worktrees/diag-framework`）**；轉述 subagent 回報時補上絕對前綴。
9. **提速協議（Phase 3 覆盤定案；品質手段一項不減）**：(a) 派工內嵌 task 全文＋執行者必讀＋設計定案，計畫檔路徑只作查證；(b) 同構小模組合批一次派工（Phase 4a 的 Task 3–5 合批實證有效）；(c) 審查分工釘死——合併 reviewer 只讀 diff＋只跑新增/變更測試檔（附 controller 綠燈證據、明令不重跑全套）、opus 管閘門證據核驗與跨檔一致性；(d) 審查一律背景執行、與真跑閘門/文件任務並行；(e) 文件素材包由 controller 先備好。

## Phase 4b 開工提醒

1. **先讀 spec §3 Phase 4 的 pair_ledger 段全文再拆計畫**（約 138–141 行）。交付物：`diagnosis/metric/pair_ledger.py`——`pair_ledger(sample_pdf, parameters) -> dict`（對每個正例列，列舉同 query 排其上方的 item、記 |ΔAP| 指標敏感度 → 壓制者×受害者矩陣）＋`substitution_ablation(sample_pdf, parameters) -> dict`（逐 item 換成 base-rate logit 常數、重算指標 O(M) 次 → 淨貢獻/淨傷害）＋**傷害×segment 分組 by_segment 區塊**（併入 pair_ledger.json）；config `evaluation.diagnosis.pair_ledger: {enabled: true}`；catalog `diagnosis/pair_ledger.json`；報表 section（spec 只明定 offset_sweep 有 waterfall——pair_ledger 的報表呈現在計畫階段定，矩陣熱圖沿 plotly 慣例）；**A19**；手冊擴充（已知限制節維持檔尾、需再改號）。
2. **開工第一件事＝修 opus N1（折別穩定性）**：現行 fit/holdout 折別依 `ngroup()` 位置碼＝依 `toPandas()` 列序（`sample.py:115` 無 orderBy），公司叢集不同平行度重跑可能翻折別。修法方向＝對 query key 做 hash 折別（列序無關）。**修了 δ*/LOO 數字會變**→手冊 §10.6 示例表＋乾淨基準快照要一起重生（一次真跑即可），閘門判準（centered 位移）不受影響。
3. **前置：`draw_diagnosis_sample` keep_cols 補 segment 欄**（`sample.py:55-61` 目前只有 query cols＋item/label/score/score_uncalibrated；spec §風險表已預告「5 個核心欄＋配置的 segment_columns」）。segment 欄由 `prepare_eval_data` 的 `join_segment_sources` 併進 eval_predictions（config 鍵 `evaluation.segment_columns`，本機合成資料只有 `cust_segment_typ`）；**by_segment 只消費欄位、不得 import `evaluation/segments.py`**（spec 明文邊界）。
4. **閘門重用 Phase 4a 注入模式**：`debug_inject_offsets: {ccard_ins: 1.0}` → pair_ledger 應顯示 ccard_ins 的壓制次數暴增（spec 驗收 1 後半）；還原 → 位元復原。注意 debug_inject_offsets 目前 scope 只在 offset_sweep 模組內——**pair_ledger 要不要也吃注入，在計畫階段決定**（要吃就把注入邏輯抽成共用 helper，不要複製貼上）。
5. **效能**：pair_ledger 的 |ΔAP| 會計與 substitution_ablation 都跑診斷抽樣（driver-side numpy）；substitution_ablation 是 O(M) 次全量指標評估（8 次本機、22 次公司）＝比 offset_sweep 便宜得多；pair 枚舉是 per 正例列 × 其上方 items（本機 ~5k 列可忽略，公司 4.4M 列 × 平均深度——計畫要先估記憶體/時間）。
6. 計畫必含「文件」task（壓制矩陣、|ΔAP|、substitution ablation、by_segment 都是無直覺概念，數感節＋示例表走讀比照 §10 規格；素材包由 controller 從閘門真跑備好）。
7. 新 consistency 代號從 **A19** 起。閘門真跑指令：`python -m recsys_tfb evaluation --env local --model-version 6059dcef --post-training`（背景執行）。
8. 遺留取捨（已記錄、非 4b 義務）：N2=debug_inject_offsets 無硬閘門只告警（leaf 節點可接受）；N3=空抽樣時 offset_sweep 報表段渲染退化 section（與姊妹段一致）。
