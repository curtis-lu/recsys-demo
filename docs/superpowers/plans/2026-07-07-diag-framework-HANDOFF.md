# 診斷框架開發交接檔（最後更新：2026-07-08 Phase 5 完成後）

> 給續作 session：讀完本檔＋下列文件，即可直接開工，不需要舊對話。

## 現在進行到哪

- **Phase 0–5 全部完成**（0 歸位、1 指標基座、2 對帳層、3 象限層、4a 分流 offset_sweep、4b 壓制帳本 pair_ledger、5 結構層 gain_ledger＋條件化 SHAP background＋triage 總表）。branch `feat/diag-framework`（worktree `.worktrees/diag-framework`）@ `913144b`。
- **Phase 5 閘門已通過、文件已交付**；剩：讀者驗收（sonnet）＋opus 總審的 nit 回收（若有）→ 使用者閘門 → **收尾：superpowers:finishing-a-development-branch → 開 PR（feat/diag-framework 涵蓋 Phase 0–5 全部）**。這是整個診斷框架的最後動作。
- **Phase 5 交付 commit 串**：587c0a8（gain_ledger 模組）→ 9335af0（training 接線＋A20）→ ab25a4a（SHAP background）→ 0fbe1dc（triage 模組＋io optional）→ bb5bd61（evaluation 接線＋報表）→ f3673bc（審查修復 3 項）→ 913144b（手冊 §12/§13/§14＋spec 修訂）。計畫檔＝`docs/superpowers/plans/2026-07-08-diag-phase5-structure-triage.md`。
- **PR 描述要列的非義務遺留**：(1) per_item SHAP interventional 在 shap 0.42.1×LightGBM 類別切分下不可行→降級 global（版本解鎖後自動生效，見手冊 §12.5）；(2) triage 起手門檻 starve_ratio/weight_cap 未 config 化；(3) substitution 反向、B′ 產物、`_HASH_BUCKETS` 底線 import 等 Phase 4b 遺留；(4) gain_ledger `item_id` 帳與遍歷解耦（病態樹下理論落差，已文件化為設計選擇）；(5) main 既有 fail `test_inference/.../test_pipeline_inputs`（PR#85 未同步，非本 branch）。

## 唯一真實來源（先讀這些）

1. **Spec**：`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`——**Phase 5 在 §3 Phase 5 段（約 157–175 行）**。spec 有**七處**帶日期執行時修訂已入文（固定結構含文件、Phase 2 verdict 相對全局、Phase 3 plotly、A17 域排除 0.5、Phase 4a gauge/centered、**Phase 4b 注入閘門主判準改 pair_ledger（λ 懲罰 vs 實際損傷實證）**、**Phase 5 consistency 代號 A19→A20 讓號**）——都是合法修訂。
2. **計畫範本**：`docs/superpowers/plans/2026-07-08-diag-phase4b-pair-ledger.md`（最新一份；「設計定案」節＋執行者必讀＋三狀態閘門＋提速協議內建）。
3. **判讀手冊**：`docs/pipelines/evaluation-diagnosis.md`——現況：§10 分流層、§11 壓制帳本、**§12 結構層（gain_ledger＋條件化 SHAP background）**、**§13 triage 總表**、§14 已知限制（檔尾）、名詞速查 27 條。gain_ledger 判讀落在此手冊（§12），training.md 只加路由行不複述。
4. 方法論背景：`docs/ranking-diagnosis-framework.md`（gain_ledger＝Ch 3 診斷項目 8、條件化 SHAP＝項目 9、triage＝Ch 5 槓桿映射）。

## Phase 0–4b 之後的 code 現狀

- `src/recsys_tfb/diagnosis/metric/`：sample.py（keep_cols 已含 `evaluation.segment_columns`）、uncertainty、reconciliation、discrimination、occupancy_spark、cross_purchase、quadrant、offset_sweep（**hash 折別**：`_fold_split` CRC32 分桶、列序無關、datetime64 NaT 已正規化）、**pair_ledger**（傘函數 `pair_ledger()` 內含 `substitution_ablation()`；|ΔAP| λ 會計＋by_segment；全樣本不切折）、**`_common.py`**（家族共用：diag_cfg/metric_params/to_logit/parse_injection/apply_injection；numpy-leaf 三模組刻意 pyspark-free）。
- `src/recsys_tfb/diagnosis/model/`：Phase 0 平移的訓練側診斷（shap_per_item.py、attribution.py 等）——**Phase 5 的 gain_ledger 加在這裡**。
- 評估 pipeline **10 個 node**（`compute_pair_ledger` 在 `compute_offset_sweep` 後）；catalog：`evaluation_pair_ledger` → `diagnosis/pair_ledger.json`。報表：`build_pair_ledger_section`（go.Heatmap＋三表）＋glossary 三條。
- consistency：A15–A18＋**A19**（pair_ledger.enabled bool）；**下一個代號 A20**（spec Phase 5 已改號）。
- `debug_inject_offsets` scope＝分流層家族兩節點（offset_sweep＋pair_ledger），config 註解已同步。
- 測試：相關四目錄 **285 passed 零 fail** @ 611e0f3（Phase 4b +26）。

## 本機環境狀態

- 本機 evaluation 必帶 `--post-training`（known-pitfalls §6）；模型 **6059dcef**、`--model-version` 必帶；SPARK_LOCAL_IP 已釘（§7）。
- `data/evaluation/6059dcef/20260131/diagnosis/` 現有**五份 JSON**（metric_ci／reconciliation／quadrant_summary／offset_sweep／pair_ledger）＝Phase 4b 還原後乾淨基準（位元復原已驗，含 NaT 修復後重驗）。`/tmp/phase4b_*` 重開機會消失；**B′（λ=0）實驗產物未留存**（機制算術可從留存 JSON 重算，opus 已核）。
- Phase 4b 乾淨態基準數字：pair_ledger 654 query/1006 正例列/1273 pairs、map_current 0.5410、top 壓制者 exchange_usd 41.5%／ccard_ins 34.2%、substitution 淨傷害只有 fund_stock +0.0199/fund_bond +0.0038；offset_sweep（hash 折別後）fit 317/hold 337、折外收復 +0.0295、centered：fund_bond +0.494/fund_stock +0.344/ccard_ins −0.456/exchange_usd −0.306。
- **λ vs 損傷教訓（Phase 4b 核心發現）**：注入 ccard_ins +1.0 對 macro mAP 實際損傷僅 +0.0034 < λ 懲罰 0.0125 → sweep 不反制是正確行為；已知答案主判準＝pair_ledger 暴增（dap ×1.98）。Phase 4a 的 −1.05 帶折別運氣。sweep 類已知答案驗收都要附「損傷 vs 懲罰」數字對照。
- 效能：compute_pair_ledger 本機 0.83s；公司外推 pair 枚舉 4–5 分鐘＋substitution ~1 分鐘。

## 已確立的執行協議（不變，勿走回頭路）

同前版 9 條：一階段一計畫＋使用者閘門；subagent token 成本控制（sonnet implementer／合併 reviewer 批次／opus 只總審）；行為不變類判準＝與 baseline 一致、**報表逐字回歸比對必須在任何重訓之前做**；shaprx 擱置、HPO objective 不動；exact-set 斷言 additive 更新屬合法（預授權或記錄）；文件一等交付物（禁開發詞彙＋真跑示例印進文件＋數感節＋fresh 讀者 agent）；質性反饋不得字面替換自驗；檔案引用絕對路徑；提速協議五款（內嵌全文／合批／審查附證據禁重跑全套／審查背景並行／素材包 controller 先備）。

## Phase 5 開工提醒

1. **先讀 spec §3 Phase 5 段全文再拆計畫**（約 157–175 行）。交付物三塊：(a) `diagnosis/model/gain_ledger.py`＝`compute_gain_ledger(model, parameters) -> dict`（經 `LightGBMAdapter.booster` 取 `trees_to_dataframe()`，跨樹按 item 記帳：item 切點樹序分佈、item 隔離子樹內 context 切點數與 Gain、item-id vs context Gain 占比）＋training pipeline 新 node＋catalog `data/models/${model_version}/diagnostics/gain_ledger.json`；(b) 條件化 SHAP：`shap_per_item.py` 增 `diagnostics.shap.background: global|per_item`（預設 global 行為不變）＋attribution 接縫開放 explainer 選項傳遞（只留參數空間）；(c) `diagnosis/metric/triage.py`＋evaluation 新 node `assemble_triage_summary`（in 含 gain_ledger **走 catalog JSON 跨側、不 import**）→ 判定 {健康|水準-配置型|水準-指標再平衡型|餓死型|特徵缺失型}＋建議槓桿＋起手值欄（配置型附 logQ offset、再平衡型附 δ*、餓死型附 w∝1/√P_j）→ report section＋`diagnosis/triage.json`；gain_ledger 缺席時 best-effort 降級。config 三鍵＋**A20**。
2. **Spike 前置（spec 明文，進實作前必驗）**：LightGBM 類別切分在 `trees_to_dataframe` 的 threshold（category set）解析＋item 隔離子樹的 parent-child 遍歷可行性。退路＝降級粗帳本（切點計數＋Gain 占比，仍可判餓死型）。計畫第一個實作 task 就排 spike。
3. **閘門首次需要 training 真跑**（Phase 1–4b 都只跑 evaluation）。計畫階段先查清楚：(a) `diagnostics.*` config 是否參與 model_version hash——會 bump 的話 6059dcef 基準銜接怎麼處理（歷史先例：cases_manifest 的 config top-level 不影響 model_version，見 memory project_training_diagnostics）；(b) 能否用 pipeline 切片 `--only-node compute_gain_ledger` 對既有模型補產 gain_ledger（讀 `docs/operations/pipeline-slicing.md`＋RESUME_CONTRACTS；注意 sliced 把 model 生產節點拉回會觸發 [retrain] WARN advisory）；(c) 協議鐵則「報表逐字回歸在重訓前做」——若真要重訓，先跑一次 evaluation 報表基準。
4. **已知答案（spec 驗收 3）**：方向性＝最冷合成 item 的 per-item 個人化 Gain 顯著低於熱門（fund_mix n_pos=23 最冷）；triage 重演＝Phase 2 的 fund_bond 抽樣注入組合（三 segment 0.5，會重訓出新 mv——Phase 2 當時是 8883dd58）應被判「水準-配置型」而非餓死型。Phase 4b 的診斷結論可當交叉驗證錨：fund_stock/fund_bond 應落餓死型輪廓（AUC≈0.5＋substitution 淨傷害）、exchange_usd/ccard_ins 應判健康。
5. **triage 讀 offset_sweep 的 δ* 起手值用 centered 還是 raw**：計畫階段定案（gauge 教訓——跨執行比較只有 centered 有意義；但「加到分數上」的起手值語意是 raw…兩者差共同平移，對排序等價，建議 centered＋文件說明）。
6. 文件 task：triage 是總表（無直覺概念：判定類型、槓桿、起手值），手冊擴充比照 §11 規格；gain_ledger 判讀落點（本手冊 vs training.md）計畫階段定。
7. 閘門指令：training＝`python -m recsys_tfb training --env local`（背景；Spark cold start 2–4 分）→ evaluation 同 4b。真跑前 pre-flight 照抄 CLAUDE.md §Worktree。
8. Phase 5 之後的收尾清單：superpowers:finishing-a-development-branch → 開 PR（整條 feat/diag-framework，Phase 0–5）；HANDOFF/memory 收尾；遺留非義務項（N2 注入無硬閘門、substitution 反向、B′ 產物留存、`_HASH_BUCKETS` 底線 import nit）在 PR 描述如實列出。
