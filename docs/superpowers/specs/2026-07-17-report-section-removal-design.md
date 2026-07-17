# evaluation report.html 三塊移除 — 設計

> 2026-07-17。範圍：`report.html` 與 `report_comparison.html` 的內容裁減，
> 以及只為產出這些內容而存在的程式碼。**本輪只動程式碼與 config，文件另開一輪。**

## 動機

`report.html` 有三塊內容要移除：對帳 Reconciliation 段落、Score Distribution
(Boxplot) 圖、所有 NDCG 相關呈現。對帳的移除方向與
`docs/superpowers/plans/2026-07-17-diag-framework-REDESIGN-HANDOFF.md` §2.5
一致——`gap`／`theory_min/max`／`residual`／`verdict` 這一整組量都是為了回答
「絕對水準對不對」而生，而那個問題在純排序（macro per-item mAP）的推導鏈上
不存在。

## 使用者已定案的決策（不要重新辯論）

| # | 決策 | 決策者選項 |
|---|---|---|
| 1 | 對帳全刪，象限的水準軸**一起退場** | 而非「縱軸接 offset sweep 的 δ*_j」或「留『無法評估』」 |
| 2 | 只刪 `Score Distribution (Boxplot)`，**保留** `Score Distribution by Label` | 而非三張分布圖全刪 |
| 3 | NDCG **兩份報表都不顯示**，但 `metrics_spark` 的計算保留 | 而非連計算一起刪 |
| 4 | `quadrant` 的程式識別字**不改名**，只改 report 的顯示字 | 見下方「為什麼不改名」 |
| 5 | 程式碼先行，文件另開一輪 | 不碰 `docs/ranking-diagnosis-framework.md` |

### 為什麼不改名

縱軸拿掉後「象限」這個名字會暫時不精確（只剩一軸）。仍不改名，因為交接檔
§7.4 把純排序的縱軸替代品（`δ*_j`，offset sweep 已算）列為未定案——若之後
補回來，2×2 象限與這個名字就再度正確。現在改名要動 catalog key、資料落地路
徑與文件，之後可能要改回去。代價是名字暫時不精確，由 report 的顯示字（section
標題與描述）承擔說明責任。

### 為什麼 A16 留編號洞

`consistency.py` 的不變量代號被既有文件與 plan 檔以編號引用。刪掉 A16 後
**不重編** A17/A18/A19，否則那些引用會靜默指向錯誤的不變量。legend 中 A16
整條移除，編號序列留洞。

## 模組邊界（本次不打破）

- `diagnosis/metric/*` 只吃 dict、不 import pipeline 內部——維持。
- `core/consistency.py` 是不變量的唯一真實來源；刪 config 鍵必須同步刪
  predicate，不得留下驗不到東西的死 predicate，也不得讓 Layer-1 對已不存在
  的鍵 raise。
- `core/group_utils.py`（`default_metric_for_objective` 等）是 **training 側**，
  與 evaluation 零耦合（已驗：無任何 `evaluation/` 檔案 import 它）。本次
  一行不碰。

## 改動設計

### A. Reconciliation（全刪）

| 對象 | 位置 | 動作 |
|---|---|---|
| 實作模組 | `src/recsys_tfb/diagnosis/metric/reconciliation.py` | 整檔刪（458 行） |
| 測試 | `tests/test_diagnosis/test_metric/test_reconciliation.py` | 整檔刪 |
| 節點 | `pipelines/evaluation/nodes_spark.py:351-376` `compute_reconciliation` | 整段刪 |
| 接線 | `pipelines/evaluation/pipeline.py:32`（import）、`:108-112`（Node） | 刪 |
| 下游斷線 | `pipeline.py:116,131,139` 三個 inputs 清單 | 移除 `evaluation_reconciliation` |
| catalog | `conf/base/catalog.yaml:246-248` `evaluation_reconciliation` | 刪；`reconciliation.json` 不再產出 |
| config | `conf/base/parameters_evaluation.yaml:61`（`report.sections.reconciliation`）、`:108-116`（`diagnosis.reconciliation` 區塊含註解） | 刪 |
| 不變量 | `core/consistency.py:87-89`（A16 legend）、`:570-596`（`reconciliation_param_errors`）、`:851`（接線） | 刪 |
| report | `evaluation/report_builder.py:415-473` `build_reconciliation_section`、`:1037`（`assemble_report` 參數）、`:1051`（candidates） | 刪 |
| 測試連帶 | `test_core/test_consistency.py:663-690,703-707`、`test_evaluation/test_report_builder.py:535-605`、`test_parameters_evaluation_yaml.py:52-62`、`test_pipelines/test_evaluation/test_nodes_spark.py:577-613` | 刪 |

`score_col` 這個 config 鍵隨模組一起消失（唯一讀取者是 `reconciliation.py:293`，
唯一驗證者是 `consistency.py:577-582`）。**欄名** `score_uncalibrated` 是全域
資產（`pipelines/training/nodes.py:898-905` 產出、`diagnosis/metric/sample.py:63`
等多處消費），一根寒毛不碰。

### B. 象限水準軸（決策 1 的連帶）

`diagnosis/metric/quadrant.py`：

- 刪 `_level_status`（:34-41）、`reconciliation` 參數（:54）、`gap_band`
  （:63）、`recon_ok`/`recon_items`（:71-72,77-78）、`gvg`（:85）。
- `_QUADRANT_LABELS`（:24-31）從 2×2 六格塌成判別力單軸：好 → `健康`、
  差 → `冷門受害者（判別力差）`。
- `by_item` 輸出移除 `gap_vs_global`、`level_status`、`is_aggressor`。
- `thresholds` 移除 `gap_band`；`sources` 移除 `reconciliation`。
- module docstring（:1-8）重寫——現況明寫「兩軸」，會變成謊。

連帶：

- `nodes_spark.py:408` 的 `is_aggressor` 計數 log 移除。
- `conf/base/parameters_evaluation.yaml:118-127` 的 `quadrant.gap_band` 與註解刪。
- `consistency.py:91`（A17 legend 的 `gap_band` 那句）、`:612-615`
  （`quadrant_param_errors` 驗 `gap_band` 那段）刪。**A17 本身保留**
  （`auc_threshold`/`top_k_occupancy` 還在）。

report 側（`evaluation/report_builder.py`）：

- `_quadrant_scatter`（:476-507）**整個函式刪**。它的縱軸就是 `gap_vs_global`；
  只剩橫軸的一維散布沒有資訊量。
- `build_quadrant_section`（:510-）：`cols`（:518-520）移除 `gap_vs_global`；
  `fig` 相關全刪；`desc`（:536-544）重寫——現況的判讀順序 (1)(2) 講的是散布圖
  與水準帶，(2) 還指路「回對帳表查」，全部失效。
- section 標題「象限 Quadrant（水準 × 條件判別力）」改成只講判別力。

**已知能力損失（設計上接受）**：「加害者」判定消失，因為它的判準就是水準偏高
（`quadrant.py:104`）。壓制的**證據**仍在 per-item 表（`suppression_count`、
`top_share`），消失的是那個**結論**。

### C. Triage（決策 1 的連帶）

`diagnosis/metric/triage.py`：

- `_V_CONFIG`（水準-配置型）與 `_V_REBALANCE`（水準-指標再平衡型）失去觸發器
  ——兩者都由 `level_off`（`:194`）閘住，而 `level_status` 只有 quadrant 產出。
  兩個 verdict 與其 `_LEVERS`（:30-31）條目刪。
- 隨之刪：`_config_signal`（:38-44）、`_config_starter`（:67-82）、
  `_rebalance_starter`（:85-103）、`reconciliation` 參數（:125）、
  `recon_by_item` 與其 note（:148-153）、level 分支（:191-199, 215-216）、
  `:220-225` 的 `verdict in (_V_CONFIG, _V_REBALANCE)` 分支。
- `evidence` 移除 `level_status`、`gap_vs_global`、`recon_verdict`、
  `theory_min`、`theory_max`、`residual` 六欄。
- 剩下的判定鏈：`disc_low` → （`餓死型` │ `特徵缺失型` │ `無結構層證據`），
  否則 `健康`。
- `:226-233` 的「健康但 δ* 漂移」note **保留**——它直接讀 `sweep_entry`，不經過
  `_rebalance_starter`。故 `offset_sweep` 參數保留。
- module docstring（:1-12）與 `parameters_evaluation.yaml:150-153` 的 triage
  註解都明列「四個診斷 dict」含 reconciliation，改字。

**已知能力損失（設計上接受）**：槓桿 1（配置修正）與槓桿 2（再平衡 per-item
offset）不再有任何觸發器。offset_sweep 照跑、δ* 照算、report 的 sweep 段落照
在，但 triage 不再據此建議槓桿 2。

### D. Score Distribution (Boxplot)

| 對象 | 位置 | 動作 |
|---|---|---|
| 繪圖 | `evaluation/distributions.py:63-82` `plot_score_boxplot` | 刪 |
| 聚合 | `evaluation/diagnostics_spark.py:87-97` `score_box_stats` | 刪 |
| 呼叫 | `pipelines/evaluation/nodes_spark.py:540-543` | 刪（含 import） |
| 測試 | `test_evaluation/test_distributions.py:50-76` `TestPlotScoreBoxplot`、`test_diagnostics_spark.py:56-77` `TestScoreBoxStats` | 刪 |

**保留**（`Score Distribution by Label` 仍在用）：`distributions.py:47-60`
`_add_box`、`diagnostics_spark.py:21` `_PCTS`、`:62-69` `_fences`、`:72-84`
`_box_stats`、`:100-113` `score_box_stats_by_label`、`distributions.py:85-109`
`plot_score_boxplot_by_label`。histogram 與三張 rank heatmap 完全不動。

無 config 開關、無 catalog 條目、無落地產物需要處理——boxplot 只是
`generate_report` 內 in-memory append 的一張圖。

### E. NDCG（兩份報表都不顯示，計算保留）

`metrics_spark.py` **完全不動**——`ndcg@k`／`ndcg_attr@k`／`ndcg_contrib@k`／
`dcg_term` 照算。

顯性移除（`evaluation/report_builder.py`）：

- `:127` family tuple 移除 `"ndcg"`；`:149` 描述改字。
- `:341-342`（`ndcg_tbl_plain`）、`:348-351`（`ndcg_fig`）、`:356-359`
  （`ndcg_tbl`）、`:384-385`（`tables`/`table_titles`）、`:400,406-407`（描述）、
  `:409`（`figures`）。
- `:964-965` baseline 的 `ndcg_attr` tuple；`:979` 描述改字。
- `:990`（`ndcg@k`）、`:995-997`（`ndcg_attr@k`）兩條 glossary 刪。

`evaluation/comparison/report.py`：`:144`、`:204` 兩個 tuple 移除 `ndcg_attr`
項；`:155`、`:215` 描述改字。glossary 由 `:33` 共用 `build_glossary_section`，
上面刪掉兩條即同時生效於兩份報表。

隱性移除（**這是本塊的核心**）：以下**四處**是 key-agnostic 表格，把 metrics dict
的 key 直接攤成欄／列，原始碼裡沒有 `ndcg` 字樣，但 `ndcg@k` 會自動渲染出來：

| 位置 | 函式 | 機制 |
|---|---|---|
| `report_builder.py:870` | `build_segment_section` | `pd.DataFrame(rows).T`，`rows` ＝ per_segment dict |
| `report_builder.py:940` | `build_baseline_section` | `sorted(set(a) \| set(b) \| set(delta))` |
| `comparison/report.py:104` | `_build_overall_section` | 同上 |
| `comparison/report.py:192` | `_build_category_section`（**大類 overall**） | 同上 |

> 第四處是 controller 自行核對時抓到的，探索 agent 的清單只有前三處。教訓：
> 「agent 說掃過了」不算證據，key-agnostic 洩漏點必須自己用
> `grep -n "sorted(set(\|pd.DataFrame(rows).T"` 掃一遍收斂。

**已核對為安全、不需動的攤平點**（同一次掃描的結果，列此以免下次重查）：
`report_builder.py:77,79`（card/meta metadata）、`:95,97,98`（dataset_overview）、
`:841`（大類組成 mapping）攤的都不是 metrics dict；`:132`
（`build_primary_map_section`）與 `:190`／`:269-278`
（`_per_item_metric_table`／`_per_item_metric_compare_table`）是顯性 family
tuple 或 metric_key 參數化，由上面的顯性移除涵蓋。`report_builder.py` 自己的
`build_category_section`（`:809-853`）**顯性寫死** `map@{k}` 與 recall，不漏
ndcg——與 `comparison/report.py` 的大類段落不同，別搞混。

作法：`report_builder.py` 新增模組級常數與 helper——

```python
_HIDDEN_METRIC_PREFIXES = ("ndcg",)  # 仍由 metrics_spark 計算，但刻意不呈現

def _visible_metric_keys(keys): ...  # 濾掉以上述 prefix 起頭的 metric key
```

`comparison/report.py` import 同一個 helper。用具名常數而非散落的字串比對，是
為了讓「算了但故意不顯示」在程式碼裡講得出來，且日後要放回來是改一行。

**誠實標註**：在「保留計算」的決策下，evaluation 側的 ndcg 會變成算了完全沒有
消費者——`save_metrics_json`（`evaluation/report.py:212`）唯一呼叫者是測試，
production 從不落地 `metrics.json`；`diagnosis/`、`scripts/`、`compare.py` 的
delta 計算（key-agnostic）都不讀 ndcg。這是使用者知情下的選擇（保留未來可能的
用途），不是疏漏。

## 測試策略

TDD：每塊先改測試到 RED，再改實作到 GREEN。**不跑全量**
（`tests/test_evaluation` 約 33 分鐘）。

**Baseline 已建（2026-07-17，`feat/report-slim` @ b0c431e，未改任何檔）**：下列
13 個檔 `329 passed, 0 failed, 19.21s`。這一輪的驗證範圍沒有踩到
`docs/operations/known-pitfalls.md` §5 的既知 fail，所以改動後**任何一條紅的都
是本次造成的**，不得歸因於既有問題。

相關測試檔（本輪的驗證範圍）：

```
tests/test_evaluation/{test_report_builder,test_distributions,test_diagnostics_spark,
                       test_comparison_report,test_compare,test_parameters_evaluation_yaml}.py
tests/test_diagnosis/test_metric/{test_reconciliation,test_quadrant,test_triage}.py
tests/test_pipelines/test_evaluation/{test_pipeline,test_nodes_spark,test_generate_report}.py
tests/test_core/test_consistency.py
```

已知會炸、必須同步改的斷言（靜態閱讀盤點，非推測）：

- `test_pipelines/test_evaluation/test_pipeline.py:12,62`（節點數 12→11）、
  `:88`（15→14）、`:25,75`（outputs set）、`:37`（node names）、`:43-53`
  （`test_compute_quadrant_inputs_wired_in_order`——該測試的存在理由是
  「兩個 dict 交換會 type-check 通過但靜默餵錯軸」，刪一個輸入後要重寫而非刪除）。
- `test_evaluation/test_report_builder.py:73`（family set 嚴格相等）、
  `:197-198`、`:214-219`、`:242-248`、`:373-385`（測試名寫死 "three"，刪後變
  two，要改名）、`:413-440`。
- `test_evaluation/test_metrics_spark.py` 的 ndcg 斷言（`:200-232,247,305,309,
  358,380,385`）**應全數保持綠**——計算不動。若轉紅代表誤刪了計算，是紅旗。

**mutation check**（`~/.claude/rules/20-judgment-rubrics.md` §2）：驗「新路徑
真的被執行過」時，mutation 要下在因果鏈上唯一不可省的那一步。本次三個檢查點：

1. 把 `_visible_metric_keys` 改成 identity（不濾）→ per-segment／baseline
   overall 的 ndcg 斷言應轉紅。**不要**下在 `_HIDDEN_METRIC_PREFIXES` 的字串上
   ——那只證明常數被讀到，證不了過濾真的接在三處表格上。
2. 把 `quadrant.py` 的 `_QUADRANT_LABELS` 查表換成固定回傳 → 象限標籤斷言轉紅。
3. 把 `triage.py` 的 `disc_low` 分支條件反轉 → verdict 斷言轉紅。

## 不做（YAGNI／範圍外）

- **不碰** `docs/ranking-diagnosis-framework.md`——它是使用者正在重新設計的對象，
  且 main 上有 +85/−7 未 commit 的改寫，動它會撞車。
- **不碰** `docs/pipelines/evaluation-diagnosis.md`（95KB 判讀手冊，§1/§3/§5/§7
  是對帳章節）與其餘文件——文件另開一輪（使用者決策 5）。**這代表本 PR 合併後
  到文件輪之間，判讀手冊會教人看一個已不存在的段落**，是知情的中間態。
- **不做** 象限縱軸接 `δ*_j`（交接檔 §7.4，未定案）。
- **不碰** training/HPO 側的 ndcg：`core/group_utils.py`、A7
  （`ranking_objective_conflicts`）、`parameters_training.yaml:27-28,39-41`、
  `models/lightgbm_adapter.py`。`rank_xendcg` 是 objective 名稱含 "ndcg" 的
  grep 假陽性。
- **不重編** A17/A18/A19 的不變量代號。
- **不改名** `quadrant` 的程式識別字。
- **不刪** `docs/superpowers/plans/` 底下提到 reconciliation 的歷史計畫書——
  它們是已完成工作的記錄，不是活的規格。
