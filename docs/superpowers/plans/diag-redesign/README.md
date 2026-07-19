# Evaluation 診斷重構：計畫索引

把 evaluation pipeline 的診斷層換成五項模組化診斷。**系統忠實呈現資料並說明每個數字的邊界，不下結論、不給處方**——判斷是讀者的工作。

## 讀的順序

1. **`00-shared-context.md`** — 開工前必讀。五項診斷的邏輯架構、檔案結構、持久化邊界、共同統計限制、診斷契約。六份計畫都依賴它，都不複述它。
2. 然後照編號執行下面六份。

## 進度（最後更新 2026-07-20）

| Plan | 狀態 |
|---|---|
| **0 地基** | ✅ **已 merge（PR #109）**。清場＋抽樣加權＋`recsys_tfb/report/` 五個檔全部在 main |
| **1 config_shift** | 🔨 **進行中**，branch `feat/diag-config-shift`（從 merged main 開出）。2.1 契約 ✅／2.2 計算層 ✅（審查中）／2.3 起未開始 |
| 2–5 | 未開始 |

**續作接手方式**：讀 `00-shared-context.md` ＋ `02-plan-1-config-shift.md`，從上表標示的下一個 task 開始。worktree 是 `/Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign`，**本機 Spark 環境已建好**（dataset／training 跑過，model_version `6059dcef`），不必重跑 `local_spark_setup --reset`。

**執行過程中對計畫原稿的修正（照計畫檔逐字實作會撞到，先看這裡）**：

- **Task 2.2 的 `build_offset_frame` 簽章**：計畫的測試寫成單參數 `build_offset_frame(PARAMS)`，但同一個 task 的移植表要求「原樣移植」——原函式是 `(pdf, parameters, schema)` 且回傳 `(offset_df, meta)` tuple。單參數版本推導不出未出現在 `sample_ratio_overrides` 裡的 item，該測試無解。**以移植表為準**。
- **Task 2.2 的 `test_group_internal_spread_not_global` 原稿是假綠**：`_sample()` 只有 `mass` 一個客群，單一 context 下「群內 spread」恆等於「全域 spread」，Step 5 的 mutation 改成全域之後測試照樣過。已改成兩個客群（`mass`／`affluent`）且群間 offset 不同。
- **`paired_bootstrap_delta` 與各診斷的 Δ 反號**：它回的是 `mAP(F) − mAP(F − shift)`，而 `config_shift` 的 `delta = corrected − baseline`。呼叫端要取負並對調上下界。**Plan 2–5 照抄樣板時會踩到同一個坑**，且符號寫反不會有任何數值測試轉紅（大小完全正確、只有正負相反），必須用「CI 與 Δ 同號」這種結構性斷言釘住。
- **診斷子套件的檔名用 `_compute.py`／`_render.py`（前綴底線）**，不是計畫原稿寫的 `compute.py`／`render.py`。理由：`from .compute import compute` 會把子模組名重綁成函式（`pkg.compute is mod` → `False`），而 `contract.check_module` 走 `getattr` 剛好拿到函式、**抓不到這個遮蔽**。與 repo 既有的 `_common.py`／`_spark.py` 命名也一致。
- **「同一客群內 offset 同加常數 → Δ 不變」不是無條件成立的不變量**：前提是「每個 query 完整落在單一 context group 內」，只有 context 欄是 **entity 級**屬性時才成立。context ＝ `sample_group_keys ∪ sample_weight_keys − {item, label}`，而 `parameters_training.yaml:54-55` 明文允許它取自 item 級屬性（產品層級／類別）。實測反例：`offset_spread` 報 `{hi: 0.0, lo: 0.0}` 而 `delta = 0.1875`。**Task 2.3 的 `SCOPE.blind_to` 原稿把它寫成無條件成立，要改成有前提的敘述。**

## 已裁決延後的重構（不是遺漏，時機到了再做）

兩關審查提出、我判斷**現在做是從單一實例猜共用抽象**，延到有第二個實例時再收：

| 項目 | 何時做 | 為什麼不是現在 |
|---|---|---|
| 把 query key／`ht_weights`／CI frame 組裝那 ~40 行樣板抽到 `_common.py` | **Plan 2 開工時**（有第二個實例可對照） | 一個實例看不出哪些是共通、哪些是 `config_shift` 特有 |
| `_common` 加「CI 方向自帶名字」的包裝，讓不知道反號這回事的人也不會寫錯 | Plan 2，與上一項一起 | 同上；目前靠 config_shift 自己的測試守著，風險是重寫的人不是照抄的人 |
| `contract.check_module` 補簽名檢查（`compute(diagnosis_sample, parameters)`）與三態 key-set 一致性檢查 | **Task 2.3**（`render` 存在之後） | 只有 `compute` 時釘不了完整契約；目前簽名形狀是默默立的，後四項寫錯簽名契約測試照樣綠 |
| `field_notes` 與實際輸出鍵的一致性檢查（新增欄位忘了補說明不會有測試轉紅） | Task 2.3，與上一列一起 | 同屬 contract 強化 |
| `q_agg` 的 `weight=("w","max")` 假設 `inclusion_weight` 在 query 內為常數 | **Plan 2 開工時檢查** | 對 `draw_diagnosis_sample` 的產出成立（權重由 stratum 決定、stratum 是 query 級屬性），但無測試釘住；上游若讓同一 query 的列帶不同權重，`max` 會靜默選一個 |

**Task 2.2 已知的弱斷言（不擋交付，但別當成有守住）**：`test_null_context_group_survives_and_is_visible` 對 note 的斷言是 `"prod_tier" in n`，靠當下文案措辭撐著——目前只有結構 note 含欄名所以有鑑別力（mutation 1b 驗證過），但改文案可能讓它退化成假綠。

**一個跨版本比對的注意事項**：`query_offset_spread` 的分位數用 inverse-CDF（不插值），與 `np.percentile` 的線性插值**算出來的數字不同**，即使權重全為 1 也不同。理由是插值會產生一個資料裡不存在的 spread 值，而每個值都該對應「某個 query 實際出現的偏移範圍」。目前尚無任何一次公司環境 real-run，所以沒有舊 JSON 需要對照；日後若要跨版本比 `p50`／`p90`，要知道這件事（`mean`／`max` 不受影響）。

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
