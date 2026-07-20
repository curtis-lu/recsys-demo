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

## Task 2.8 使用者回饋（2026-07-20，公司環境實跑後）

### 回饋 1：診斷的開關散在三個地方，分不清誰管誰

使用者原話：「診斷項目的開關有點混亂，evaluation pipeline 的 `pipeline.py` 中可以定義要跑的 node、`parameters_evaluation.yaml` 中的 `report` 段落、然後又有 `diagnosis` 段落」。

**實際的開關面（盤點，不是印象）**：

| # | 在哪 | 管什麼 | 舊診斷 | registry 診斷 |
|---|---|---|---|---|
| 1 | `pipeline.py` 的 Node 清單 | 這個 node 存不存在 | 手寫 | **也是手寫**（每項一個） |
| 2 | `diagnosis.<name>.enabled` | 算不算（不算就寫 stub） | ✓ | ✓ |
| 3 | `report.sections.<name>` | 在主報表渲不渲染 | ✓ | **無**（走自己的頁） |
| 4 | `contract.DIAGNOSES` | 進不進 registry（決定 catalog 鍵、頁面、`generate_report` 輸入） | 不適用 | ✓（**在 Python 裡，不是 config**） |
| 5 | `_sample_consumer_flags` ＋ `_registry_diagnosis_enabled` | 共用抽樣抽不抽 | 衍生 | 衍生 |
| 6 | CLI `--only-node` / `--from-node` | 這次跑哪些 | 正交 | 正交 |

**哪些是過渡期、哪些是真問題**：

- **過渡期（Plan 5 收尾自然消失）**：#3 只服務舊診斷。五項 registry 診斷全部上線、舊的移除後，`report.sections` 會只剩非診斷的區塊，#3 就不再是「診斷開關」。
- **真問題，且已經咬過一次**：#1 每項診斷手寫一個 Node，而 `generate_report` 用**位置**收 7 個具名參數 ＋ varargs。2026-07-20 公司環境實例：`node.inputs` 少了兩個元素 → 位置 6 的 `evaluation_config_shift` 綁進 `offset_sweep` 參數 → `build_offset_sweep_section` 拿到 `per_item` 是 list 而非 dict → `TypeError`。**運氣好才爆；型別相容的話會靜默把 A 診斷的數字印在 B 診斷的標題下。**
- **真問題**：#4 在 Python 裡而 #2 在 YAML 裡，「關掉一項診斷」有兩個位置、語意不同（不進 registry ＝ 完全不存在；`enabled: false` ＝ 存在但寫 stub）。使用者要關一項時該動哪個，目前沒有任何地方寫。

**目標狀態（Plan 2 開工前定案，因為 Plan 2–5 會照抄現在的形狀）**：

1. **每項診斷對使用者只有一個開關**：`evaluation.diagnosis.<name>.enabled`。`contract.DIAGNOSES` 降級成「這個診斷在程式碼裡存在」的宣告，不是使用者面的開關。
2. **Node 由 `DIAGNOSES` 導出**，不再每項手寫——同時消滅 #1 的手寫 Node 與 `generate_report` 的位置綁定脆弱性。
3. **`generate_report` 不再用 N 個位置參數收診斷結果**，改成收單一 dict（或由 runner 具名綁定）。這是上面那個 TypeError 的根治法，不是 workaround。

### 回饋 2：`generate_report` 應該只組裝，不該再做 Spark 計算

使用者原話：「我想把 `generate_report` 這個 node 的負擔降低，我希望跑這個 node 單純就是在組裝之前 node 已經跑過的結果，不要再有多餘的 spark 計算。」

**現況（`nodes_spark.py:542-570`）**：`generate_report` 吃 `eval_predictions: SparkDataFrame`，`.select(...).cache()` 之後跑五個 Spark 聚合——`score_histogram_counts`／`score_box_stats_by_label`／`rank_count_matrix`／`positive_rank_count_matrix`／`positive_rate_matrix`。每個都是一次全掃。

**為什麼這是問題，不只是效能**：

1. **它是最後一個 node，卻是最貴的之一。** 失敗成本最大化：前面全部跑完才死在這裡（2026-07-20 公司環境的 TypeError 正是如此）。
2. **主報表無法離線重繪。** 診斷頁已經做到「JSON 落地 → 純函式 render → 2 秒重繪」，主報表沒有——因為它的圖表要靠 Spark 現算。使用者要調主報表版面就得重跑整條 pipeline。
3. **它違反這次重構的核心邊界。** `compute` 純計算落地 JSON、`render` 純呈現讀 JSON——五項新診斷都遵守，而報表層自己沒有。

**修法（與診斷層同一個模式）**：把那五個聚合抽成獨立 node，輸出落地成 JSON（都是小東西：bin counts、quartiles、rank 矩陣），`generate_report` 改讀那份 JSON、簽章不再有 `SparkDataFrame`。

**連帶效果**：
- `generate_report` 變成純函式 → 可以納入 `scripts/render_diagnosis.py` 的離線重繪範圍，2 秒迴圈從診斷頁擴大到整份報表。
- 失敗點往上游移到真正做事的地方。
- 與回饋 1 的第 3 條（`generate_report` 不再用 N 個位置參數收診斷結果）是同一次改動的兩面，一起做比分兩次做便宜。

### 回饋 3：舊 `offset_sweep` section 的判讀語氣

見下方「過渡期的兩種語氣並存」。使用者可用 `report.sections.offset_sweep: false` 立即關掉，理由獨立成立（該段文字違反鐵則 1），不必等 Plan 4。

## 已裁決延後的重構（不是遺漏，時機到了再做）

兩關審查提出、我判斷**現在做是從單一實例猜共用抽象**，延到有第二個實例時再收：

| 項目 | 何時做 | 為什麼不是現在 |
|---|---|---|
| 把 query key／`ht_weights`／CI frame 組裝那 ~40 行樣板抽到 `_common.py` | **Plan 2 開工時**（有第二個實例可對照） | 一個實例看不出哪些是共通、哪些是 `config_shift` 特有 |
| `_common` 加「CI 方向自帶名字」的包裝，讓不知道反號這回事的人也不會寫錯 | Plan 2，與上一項一起 | 同上；目前靠 config_shift 自己的測試守著，風險是重寫的人不是照抄的人 |
| `contract.check_module` 補簽名檢查（`compute(diagnosis_sample, parameters)`）與三態 key-set 一致性檢查 | **Task 2.3**（`render` 存在之後） | 只有 `compute` 時釘不了完整契約；目前簽名形狀是默默立的，後四項寫錯簽名契約測試照樣綠 |
| `field_notes` 與實際輸出鍵的一致性檢查（新增欄位忘了補說明不會有測試轉紅） | Task 2.3，與上一列一起 | 同屬 contract 強化 |
| `q_agg` 的 `weight=("w","max")` 假設 `inclusion_weight` 在 query 內為常數 | **Plan 2 開工時檢查** | 對 `draw_diagnosis_sample` 的產出成立（權重由 stratum 決定、stratum 是 query 級屬性），但無測試釘住；上游若讓同一 query 的列帶不同權重，`max` 會靜默選一個 |
| 讓診斷提供 result-dependent 的 `blind_to`（例如偵測到 lambdarank 時動態多一條） | 有第二個實例需要時 | 目前 `notes` 已承載這類資訊、`render` 也顯示了；為此加一個每項診斷各一份的 hook，會讓同一段邏輯被抄五次（Task 2.3 的 `scope_for()` 即因此撤回） |
| `display` config 機制（YAML 覆寫表格欄位順序／欄名） | 使用者在公司環境看過真實產出、明確說出要調哪幾樣之後 | 中介層要的性質（JSON 持久化、呈現隨時可改、不必重跑公司環境）**離線重繪 2 秒就已經拿到了**。再疊一層 config：每個旋鈕都是程式碼＋測試＋文件，而且只調得動事先想到的那幾樣；直接改 `_render.py` 再重繪則什麼都能調、成本一樣。理由完整寫在 `scripts/render_diagnosis.py` 的 module docstring，**那是刻意不做、不是漏做** |
| `render_diagnosis.py` 的 `--params` 讀不到時退回空 dict | 某項診斷的 `render` 真的開始讀 `parameters` 時 | 今天無害（`config_shift.render` 完全沒用到 `parameters`，只有簽章要求），但屆時會變成「安靜地用空 config 畫圖」。**現在沒有守衛**，那時要補 |

**過渡期的兩種語氣並存（Plan 2–5 收尾前會一直存在）**：主報表裡舊的 `build_offset_sweep_section`（`report_builder.py:501-502`）寫著「大＝缺口主要在水準（配置／再平衡可修）、小＝缺口在條件判別力（必須動訓練）」——明確的處方，與三條鐵則相反。它服務的是 Plan 4 將由 `score_shift` 取代的舊診斷，且**今天就已在 main 出貨**，所以留著不會讓現況變差，改了也是改一段即將刪除的程式碼。**但 Task 2.8 檢視樣板時要把它排除在外**，否則會看到新舊兩種語氣打架而誤判樣板本身有問題。Plan 4 移除該診斷時一併清掉，Plan 5 全案驗收時複查。

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
