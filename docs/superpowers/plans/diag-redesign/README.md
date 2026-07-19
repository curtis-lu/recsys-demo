# Evaluation 診斷重構：計畫索引

把 evaluation pipeline 的診斷層換成五項模組化診斷。**系統忠實呈現資料並說明每個數字的邊界，不下結論、不給處方**——判斷是讀者的工作。

## 讀的順序

1. **`00-shared-context.md`** — 開工前必讀。五項診斷的邏輯架構、檔案結構、持久化邊界、共同統計限制、診斷契約。六份計畫都依賴它，都不複述它。
2. 然後照編號執行下面六份。

## 進度（最後更新 2026-07-20）

| Plan | 狀態 |
|---|---|
| **0 地基** | ✅ **已 merge（PR #109）**。清場＋抽樣加權＋`recsys_tfb/report/` 五個檔全部在 main |
| **1 config_shift** | 🔨 **進行中**，branch `feat/diag-config-shift`（從 merged main 開出）。7 個 task 皆未開始 |
| 2–5 | 未開始 |

**續作接手方式**：讀 `00-shared-context.md` ＋ `02-plan-1-config-shift.md`，從 Task 2.1 開始。worktree 是 `/Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign`，**本機 Spark 環境已建好**（dataset／training 跑過，model_version `6059dcef`），不必重跑 `local_spark_setup --reset`。

**Plan 0 已落地、後續計畫可直接用的地基**：
- `src/recsys_tfb/report/`：`types`（`ReportSection`／`ScopeNote`／`Page`）、`fmt`（六個語意化格式器）、`scales`、`figures`（含 `MAX_FIGURE_POINTS`）、`pages`（多頁 HTML ＋共用 plotly.js，單頁實測 9.3KB）
- `diagnosis/metric/sample.py`：回傳的 `sample_pdf` 帶 `stratum`／`inclusion_weight`；`meta` 帶 `strata`／`sampling_description`
- `diagnosis/metric/uncertainty.py::paired_bootstrap_delta`：分層配對 cluster bootstrap（**不要再寫第二份 bootstrap**）
- `evaluation/metrics.py`：mAP 原語支援 optional `weights`（不傳時位元等價）＋`align_positive_row_weights`

**公司環境實況（本機測不到，已據此設定）**：有正例的 query 約 22 萬、driver 128GB → `max_queries` 設 250,000 → `ratio == 1.0` → 診斷是**普查**、權重全 1。

## 六份計畫

| # | 檔案 | 一句話 | 交付後你看什麼 |
|---|---|---|---|
| 0 | `01-plan-0-foundation.md` | 清場 ＋ 抽樣加權 ＋ 呈現層。**不含任何新診斷** | 公司環境的 `sample_ratio` 到底是多少 |
| 1 | `02-plan-1-config-shift.md` | 契約 ＋ 第一項診斷 ＋ 離線重繪工具 | **樣板形狀**（後三份照抄它） |
| 2 | `03-plan-2-item-ability-capacity.md` | 第二、三項診斷 | AUC 對照散點、gain 三分 |
| 3 | `04-plan-3-suppression.md` | 第四項診斷 ＋ 交叉購買 | 壓制矩陣與共買圖並排對照 |
| 4 | `05-plan-4-score-shift.md` | 第五項診斷（最貴的一項） | 執行時間、Δ 與 CI |
| 5 | `06-plan-5-wrapup.md` | 改名 ＋ ScopeNote 驗收 ＋ 文件 | 框架文件講不講得通 |

**必須依序執行。** Plan 0 的抽樣加權是五項診斷共同的地基；Plan 1 立下的樣板，Plan 2–4 照抄。

## 五項診斷在回答什麼

順序是歸因優先權，**不是硬閘門**——五項全跑、全呈現。

| # | 診斷 | 回答什麼 | 排除什麼 |
|---|---|---|---|
| 1 | `config_shift` | 抽樣比例與 sample weight 有沒有引入 per-item 的 log-odds 偏移 | 若偏移為 0，排序問題就不是訓練設定造成的 |
| 2 | `item_ability` | 模型能不能在同一個 query 內分辨誰會買哪個 item | 把客戶活躍度誤判成 item 推薦能力 |
| 3 | `model_capacity` | gain／split 花在 item 身分還是 context 特徵 | 「學到互動訊號」與「只記住 item prior」 |
| 4 | `suppression` | 哪些 label=0 排在 label=1 之前、造成多少 AP 缺口 | 「模型排錯」與「商品本來就競爭」 |
| 5 | `score_shift` | 不重訓、只加 per-item 常數位移，holdout mAP 能不能提升 | 問題偏 item 水準，還是偏辨識力／特徵表達 |

## 三條鐵則（每份計畫都重貼一次）

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別。顏色只編碼資料的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，`blind_to` 為空即契約違反，有測試擋。

## 參考素材（在本 branch，非產品程式碼）

`scripts/*_diagnosis.py` 六份 ＋ `tests/scripts/test_*_diagnosis.py` 兩份，是與 codex 討論後的試作實作。各計畫的移植步驟會逐一引用它們的 `檔案:行號`。**功能全部進 `src/` 之後要不要刪，由使用者決定**（見 Plan 5 的全案驗收）。
