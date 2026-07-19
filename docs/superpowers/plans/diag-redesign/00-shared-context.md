# Evaluation 診斷重構 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 evaluation pipeline 的診斷層換成五項模組化診斷，每項忠實呈現資料並自帶「這個數字量什麼、看不見什麼」的說明，讓讀者自己判斷；系統不下結論、不給處方。

**Architecture:** 新增中性呈現層 `recsys_tfb/report/`（型別、語意化格式器、色階、plotly 圖表建構器、多頁 HTML 寫出）。每項診斷是 `diagnosis/metric/<name>/` 子套件，自帶 `compute.py` 與 `render.py`，對外只暴露 `compute()`／`render()`／`SCOPE` 三個符號。`report_builder` 退化成收集器，不再認識任何單一診斷。判定型模組（`triage`、`quadrant`）整批退場。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

---

## 0. 這份計畫的立場（開工前必讀）

這次重構的驗收標準跟一般功能不同，**寫錯方向比寫錯程式更貴**。三條鐵則：

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。診斷輸出的是數字、分布、對照點、範圍說明。判斷留給讀者。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別（舊 `quadrant.auc_threshold` 就是被這條判死的）。顏色只編碼資料本身的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，寫出它量的是什麼、算在哪批列上、**不能推論什麼**。`blind_to` 為空即契約違反，有測試擋。

**為什麼**：使用者的原話是「我沒有要把人類的思考與判斷外包給你，我要你做的是忠實呈現數據，但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。既有的 `triage.py` 正是被否決的那種東西——它已經實作了「per-item 判定＋槓桿建議」，所以它必須死，不是因為寫得不好。

---

## 1. 五項診斷與它們的閱讀順序

順序是**歸因優先權**，不是硬閘門——五項全跑、全呈現。上游有發現時，下游數字照常給，但讀者知道要先看上游。

| # | 名稱 | 回答什麼 | 排除什麼 | 資料來源 |
|---|---|---|---|---|
| 1 | `config_shift` | 抽樣比例與 sample weight 是否在理論上對每個 item 引入不同的 log-odds 偏移？扣掉之後 macro mAP 變多少？ | 若偏移為 0，後面看到的排序問題就不是訓練設定造成的 | 診斷抽樣 ＋ `dataset.sample_ratio*`／`training.sample_weight*` |
| 2 | `item_ability` | 模型能不能在**同一個 query 內**分辨「這個人會買哪個 item」？ | 把客戶活躍度誤判成 item 推薦能力 | 診斷抽樣 |
| 3 | `model_capacity` | 模型的 gain／split 花在 item 身分本身，還是花在 context 特徵上？ | 「模型學到互動訊號」與「只記住 item prior」 | `gain_ledger.json`（訓練側產物）＋ 第 2 項輸出 |
| 4 | `suppression` | 哪些 label=0 的 item 常排在 label=1 之前、造成多少 AP 缺口？這些組合在資料上本來就常一起買嗎？ | 「模型排錯」與「這兩個商品本來就競爭」 | 診斷抽樣 |
| 5 | `score_shift` | 不重訓，只給每個 item 一個固定位移，holdout mAP 能不能提升？ | 問題偏 item-level 水準，還是偏辨識力／特徵表達 | 診斷抽樣（切 tune/holdout） |

**五項共用同一份 `diagnosis_sample`。** 這是整套設計最重要的一致性保證：若 AUC 來自全量、壓制帳本來自抽樣、Δ 來自另一份抽樣，三個數字描述的不是同一批列，並排解讀會出現細微的錯。

### 運算預算：搜尋用子樣本，量測用全樣本

`max_queries` 已設在母體之上（250,000 vs 公司環境約 22 萬個有正例的 query，2026-07-19 實況），所以共用樣本是**普查**——`ratio == 1.0`、權重全 1、`sampling_description` 會寫「未抽樣」。

**不要用縮小共用樣本來控制運算成本**：那會讓便宜的診斷也一起失去母體性，換來的省錢在它們身上根本不需要。成本控制在各診斷自己的旋鈕上：

| 診斷 | 成本量級 | 預算旋鈕 |
|---|---|---|
| `config_shift` | (N_items+2) 次 mAP，閉式不搜尋 | 無需 |
| `item_ability` | sort-once 後主成本是線性掃 | `ci.n_boot`（已有） |
| `model_capacity` | 零（只讀 `gain_ledger.json`） | 無需 |
| `suppression` | 成對枚舉，向量化後仍隨誤序 pair 數成長 | `top_examples`；必要時加列數上限 |
| `score_shift` | **最貴：約 300 次全量 mAP** | `n_trials` ＋ `search_max_queries` |

**`score_shift` 的原則**：位移向量在 `search_max_queries` 的子樣本上搜出來，效果在 **holdout 全樣本**上量測。這不只是省錢——搜尋與量測分離本來就是避免過擬合的正確做法，省錢是附帶的。

**每項診斷必須把自己的實際執行秒數寫進輸出**，讓預算能依實測調整而不是靠猜。

> ⚠ 母體若成長超過 `max_queries`，次抽樣會自動恢復，分層權重接手做 Horvitz–Thompson 修正、`sampling_description` 也會改口——**不會靜默偏差**。但普查的解讀更乾淨，發現超過時優先調高 `max_queries`，而不是靠加權。

**抽樣母體已經是「有正例的 query」**（`src/recsys_tfb/diagnosis/metric/sample.py:70-72`），理由是 macro per-item mAP 只在這些 query 上累積。這件事必須寫進每項診斷的 `ScopeNote.population`。

---

## 2. 檔案結構

### 2.1 新增：`src/recsys_tfb/report/`（中性呈現層）

| 檔案 | 職責 |
|---|---|
| `report/__init__.py` | re-export `ReportSection`／`ScopeNote`／`Page` |
| `report/types.py` | `ReportSection`（自 `evaluation/report.py:59` 搬來）、`ScopeNote`、`Page` |
| `report/fmt.py` | 語意化格式器：`fmt_logodds`／`fmt_ratio`／`fmt_ap`／`fmt_auc`／`fmt_count`／`fmt_delta` |
| `report/scales.py` | `sequential_scale()`（單向大小）、`diverging_scale(center)`（有號量）；不提供任何「好／壞」配色 |
| `report/figures.py` | plotly 建構器：`heatmap`／`scatter`／`bar`／`bubble_grid`，統一主題、統一 hover |
| `report/pages.py` | 多頁 HTML 寫出：共用一份 `plotly.min.js`、寫 `index.html` |

**邊界**：`diagnosis/` 可以 import `recsys_tfb.report`（純型別與呈現原語，無 Spark、無 pipeline）。`diagnosis/` 依然**不得** import `pipelines/*`，也不得 import `evaluation/report_builder.py`。

### 2.2 新增：五個診斷子套件

```
src/recsys_tfb/diagnosis/metric/config_shift/{__init__,compute,render}.py
src/recsys_tfb/diagnosis/metric/item_ability/{__init__,compute,render}.py
src/recsys_tfb/diagnosis/metric/model_capacity/{__init__,compute,render}.py
src/recsys_tfb/diagnosis/metric/suppression/{__init__,compute,render}.py
src/recsys_tfb/diagnosis/metric/score_shift/{__init__,compute,render}.py
```

拆 `compute`／`render` 的先例是既有的 `diagnosis/hpo/`（`collect`／`summary`／`render`／`write` 四分）。

### 2.3 保留

| 檔案 | 為什麼留 |
|---|---|
| `diagnosis/metric/sample.py` | 共用抽樣底座，五項全都用 |
| `diagnosis/metric/_common.py` | `to_logit`／`metric_params`，五項全都用 |
| `diagnosis/metric/uncertainty.py` | cluster bootstrap 的**唯一**實作（腳本裡重寫了 3 份，這次收斂回來） |
| `diagnosis/metric/contract.py`（新增） | 診斷契約與契約測試的依據 |

### 2.4 刪除

| 檔案 | 行數 | 替代者 |
|---|---|---|
| `diagnosis/metric/quadrant.py` | 113 | 無（門檻切割丟資訊，散點圖嚴格優於象限） |
| `diagnosis/metric/triage.py` | 193 | 無（判定層，與本次立場矛盾） |
| `diagnosis/metric/offset_sweep.py` | 248 | `score_shift/` |
| `diagnosis/metric/pair_ledger.py` | 247 | `suppression/` |
| `diagnosis/metric/discrimination.py` | 74 | `item_ability/`（改 numpy、改用 `score_uncalibrated`、加 CI） |
| `diagnosis/metric/cross_purchase.py` | 54 | 併入 `suppression/`（改成泡泡格圖，見 §3.4） |
| `diagnosis/metric/occupancy_spark.py` | 84 | 併入 `score_shift/`（曝光份額 guardrail） |
| `docs/pipelines/evaluation-diagnosis.md` | 754 | 內容三分：判讀→報表、方法論→框架文件、操作→quickstart |

> **目前孤兒狀態，不是遺漏**：`quadrant.py`（表中第一列）已刪，導致
> `discrimination.py`／`cross_purchase.py`／`occupancy_spark.py` 這三個
> **目前**沒有任何 production 呼叫者——但它們不是被清場漏掉的殘骸，是還沒
> 輪到刪除的「等取代者落地」狀態：`discrimination.py` 等 Plan 2
> （`03-plan-2-item-ability-capacity.md`）把 `item_ability/` 生出來才刪；
> `cross_purchase.py` 等 Plan 3（`04-plan-3-suppression.md`）併入
> `suppression/` 才刪；`occupancy_spark.py` 等 Plan 4
> （`05-plan-4-score-shift.md`）併入 `score_shift/` 才刪。下一個人若只看
> import graph、看到零呼叫者，**不要直接當成死碼清掉**——先看這份表的
> 「替代者」欄有沒有排進計畫。

### 2.5 改名（純改名，功能不動）

repo 裡有第二套也叫「診斷」但與 `diagnosis/` 無關的東西（分數直方圖／箱型圖／名次熱圖／校準曲線）。兩套同名、報表上又相鄰，是可讀性的實際負擔。

| 現在 | 改成 |
|---|---|
| `evaluation/diagnostics_spark.py` | `evaluation/overview_spark.py` |
| `report_builder.build_diagnostics_section` | `report_builder.build_overview_section` |
| config `evaluation.report.sections.diagnostics` | `evaluation.report.sections.overview` |
| config `evaluation.report.diagnostics.*` | `evaluation.report.overview.*` |
| `tests/test_evaluation/test_diagnostics_spark.py` | `tests/test_evaluation/test_overview_spark.py` |

### 2.6 產物版面（拆多份 HTML）

```
data/evaluation/${model_version}/${snap_date}/
  report.html                     ← 指標報表；診斷區塊只放一段連結清單
  diagnosis/
    index.html                    ← 診斷首頁：五項的邏輯架構＋閱讀順序＋連結
    01-config-shift.html
    02-item-ability.html
    03-model-capacity.html
    04-suppression.html
    05-score-shift.html
    plotly.min.js                 ← 六份 HTML 共用一份（~3.5MB）
    config_shift.json             ← 完整明細
    item_ability.json
    model_capacity.json
    suppression.json
    score_shift.json
```

**檔名數字前綴就是閱讀順序。** `index.html` 承載「清楚好懂的邏輯架構」——它說明五項各自回答什麼、排除什麼、為什麼是這個順序。

**硬規則（契約測試強制）**：進 HTML 的資料一律先聚合到可視大小（每張圖 ≤ `MAX_FIGURE_POINTS` 點）；完整明細只落 JSON。`generate_html_report` 目前把整份 plotly.js 內嵌（`evaluation/report.py:85,118`），拆六份就是 6×3.5MB，所以 §Phase 1 必須先做出外部共用 js 的能力。

### 2.7 持久化邊界：什麼該固定、什麼會常變

使用者的 review 迴圈是「進公司環境跑 → 看產出 → 給回饋 → 改 → 再跑」。**如果改一個欄位順序要重跑一次 Spark，這個迴圈會死掉。** 所以邊界不畫在檔案格式，畫在「要不要重算」：

| 層 | 內容 | 變動頻率 | 改它的成本 |
|---|---|---|---|
| **計算** | `compute()` 算出的所有數字 | 低 | **高**——要重跑公司環境 |
| **持久化** | `<name>.json` | — | 這是兩層之間的契約 |
| **呈現** | 欄位順序／欄名／格式／排序／圖 | **高** | **低**——從 JSON 重繪，秒級 |

三條由此推出的規則：

1. **JSON 必須是超集。** `compute()` 要慷慨地多算——只要可能想看的數字就算進去，不因為「報表現在沒用到」而省略。
   > **JSON 有的欄位，加進表格是改 config；JSON 沒有的欄位，要重跑一次公司環境。** 這兩件事成本差三個數量級。

2. **`render()` 必須是 JSON 的純函式。** 不碰抽樣、不碰 Spark、不碰模型。用測試釘死：`render` 的參數裡不得出現 sample／SparkDataFrame。

3. **高頻變動項進 config，範圍嚴格限定在表格。** 圖表不進來——那會變成宣告式規格語言，總有 20% 的圖擠不進規格，最後變成規格＋例外。

```yaml
evaluation:
  diagnosis:
    item_ability:
      enabled: true
      display:
        per_item_table:
          columns: [item, ap, query_centered_auc, raw_within_item_auc,
                    auc_gap_raw_minus_centered, n_pos]   # 順序即呈現順序
          labels:  {item: 商品, query_centered_auc: "query 內 AUC"}
          formats: {ap: ap, query_centered_auc: auc, n_pos: count}  # 指向 fmt.py 的語意格式器
          sort_by: ap
          sort_desc: false
```

`display` 區塊**不進 consistency 閘門的嚴格驗證**（它是呈現偏好，錯了頂多表格難看，不影響任何數字的正確性）；但 `columns` 指到 JSON 沒有的鍵時要 fail-loud 並列出 JSON 實際有哪些鍵，否則使用者會對著空白欄位除錯。

**配套工具**（Phase 7）：`scripts/render_diagnosis.py --input-dir <JSON 目錄> --output-dir <輸出目錄>`——把公司環境的五份 JSON 拷回本機，秒級重繪。第一次真跑之後的所有 review 迭代都不必再進公司環境。

---

## 3. 各診斷的設計要點

### 3.1 `config_shift`

移植自 `scripts/config_sorting_shift_diagnosis.py`。

- **公式**：`offset(a,j) = ln( r_pos·w_pos / (r_neg·w_neg) )`，`a` = 客戶端屬性（`sample_group_keys` 扣掉 item 欄與 label 欄），`r` 來自 `dataset.sample_ratio_overrides`（查無用 `dataset.sample_ratio`），`w` 來自 `training.sample_weights`（查無用 1.0）。
- **必須用 `score_uncalibrated` 轉 logit，不能用 `score`。** offset 活在模型自己輸出的 log-odds 空間，校準層是後貼的；且 repo 的校準是 `IsotonicRegression`（階梯函數），會把相異分數壓成同值造成 tie，減掉不同 offset 之後名次由 offset 大小主導。用 `score` 算出來的 Δ 是錯的**而且不會報錯**。
- **群內 spread**：`spread(a) = max_j offset(a,j) − min_j offset(a,j)`。必須是群內不是全域——一個 query 只屬於一個群，群內均勻的 offset 對名次零影響。
- **Δ = mAP(F − offset) − mAP(F)**，CI 用**配對 bootstrap**（同一組重抽樣本上同時算兩個 mAP 再取差；分開算 CI 再相減是錯的且會寬到測不到）。
- **per-item Δ_j**：只扣 item j 的 offset。必須在報表明說 `Σ Δ_j ≠ Δ`（名次耦合），這是替換實驗不是分解。

### 3.2 `item_ability`

移植自 `scripts/item_ability_diagnosis.py`。

- 對每個 item 算 **raw within-item AUC**（在 `logit(score_uncalibrated)` 上）與 **query-centered AUC**（在「減掉 query 內平均 logit」的相對分數上），並輸出兩者的差。這個差就是「客戶活躍度」與「item 辨識力」的分離量——**這是對照點，不是判定**。
- **效能修正（必做）**：bootstrap 不需要重複排序。rank-sum AUC 只要每個 item 的列**先排序一次**，之後 200 次重抽是沿著已排好的順序做加權累加（O(n) 線性掃）。腳本現在每次 `weighted_auc` 呼叫都重排（`scripts/item_ability_diagnosis.py:313-359`），把 `N_items × 402` 次排序降成 `N_items` 次排序 + `N_items × 400` 次線性掃。**先做這個，不夠快再談 Spark。**
- **必寫進 `ScopeNote.blind_to`**：within-item AUC **不是指標原生的量**——item j 的正例列與負例列分屬不同 query，而 macro mAP 從頭到尾沒做過跨 query 的分數比較。它是 proxy，不是指標的分解。母體限定在有正例的 query，所以這個數字**不能跟任何外部引用的 AUC 比較**。

### 3.3 `model_capacity`

移植自 `scripts/model_capacity_diagnosis.py`。唯一不碰評測資料的一項（讀 `gain_ledger.json`）。

- 把模型全部 split gain 分成 Item Prior（在 item 欄分裂）／Post-Item Context（item 分裂之後的非 item 分裂）／Pre-Item 未分配三塊。
- 讀第 2 項的輸出畫 capacity vs ability 散點（腳本已有此依賴，`scripts/model_capacity_diagnosis.py:707`）。在 pipeline 裡改成明確的 node input，不是讀檔案。
- `gain_ledger` 是跨 pipeline 產物、catalog `optional: true`。缺席時降級：本項輸出 `{"enabled": true, "available": false, "reason": ...}`，報表顯示「訓練側未產出 gain_ledger」而非空白。

### 3.4 `suppression`（含交叉購買）

移植自 `scripts/suppression_ledger_diagnosis.py`，併入 `cross_purchase.py` 的能力。

- 每個 query 內按 `logit(score_uncalibrated)` 排序；對每個 label=1 列枚舉排在它上方的 label=0 列，把該列的 AP 缺口依 severity 比例分攤給壓制者。
- **效能修正（必做）**：最內層目前是純 Python 逐 pair 迴圈（`scripts/suppression_ledger_diagnosis.py:519`），未向量化。改成 numpy 向量化。
- **交叉購買泡泡格圖**（取代舊 `cross_purchase_matrix` 的純機率呈現）：
  - 顏色 = **lift** = `P(k|j) / P(k)`，發散色階以 1.0 為中心。**不能只用 `P(k|j)`**——熱門 item k 對任何 j 的條件機率都高，矩陣會退化成「熱門那幾行全亮」，讀不出關聯。
  - 泡泡大小 = **共買客戶數 `n(j∧k)`**，讓小樣本格子天然變小（結構性的，不是設門檻把它藏起來）。
  - hover = `n(j∧k)`／`n(j)`／`n(k)`／`P(k|j)`／`lift` 全給。
  - **座標軸順序與壓制矩陣完全一致**，兩張圖並排即可自行對照「模型的壓制」與「真實的共買」。

### 3.5 `score_shift`

移植自 `scripts/per_item_score_shift_optuna_diagnosis.py`（**用 Optuna 版，不用座標下降版**——多了 L2 正則與曝光 guardrail，成本又與 item 數脫鉤：固定 trial 數 vs 座標版的 36×N_items）。

- 用 entity hash 切 tune／holdout，避免同一 entity 跨兩邊洩漏。
- objective = `mAP(位移後)` − `shift_l2 × ‖δ‖²` − `exposure_penalty × Σ(top-k 曝光份額 − 正例份額)²`。曝光項防止搜尋把曝光病態集中到少數 item。
- 位移學在 tune、量測在 holdout，Δ 附配對 bootstrap CI。
- **必寫進 `ScopeNote`**：這是反事實測試，`Δ > 0` 只代表「per-item 常數位移這個手段在 holdout 上有增益」，**不代表**模型該被這樣改，也不代表問題的成因就是 item-level 水準。

---

### 3.6 五項共同的統計限制（全部必須寫進 `ScopeNote.blind_to`）

診斷工具不只是工程問題——它產出的數字會被拿去做判斷，所以估計量的合理性比程式碼漂亮重要。以下是五項共同繼承、且**無法靠寫程式消除**的限制：

| 限制 | 為什麼存在 | 處理方式 |
|---|---|---|
| **抽樣是分層的** | `draw_diagnosis_sample` 有 take-all 層（納入機率 1.0）與 hash-ratio 層（納入機率 `ratio`），兩層機率不同 | Phase 0.5 帶出 `inclusion_weight` 做 Horvitz–Thompson 加權；`ratio == 1.0` 時退化成無加權 |
| **多重比較** | 約 22 個 item 各有一組 CI，挑最極端的那個看，偽陽性率被低估 | 報表同時顯示「有幾個 item 的 CI 不跨 0」與「在真實效應全為 0 時期望有幾個」，讓讀者自己折扣 |
| **搜尋選擇偏誤** | `score_shift` 的 holdout CI 只涵蓋重抽變異，不涵蓋「換一組 tune/holdout 切分 Δ 會差多少」——那才是主要不確定性 | 寫進 `blind_to`；若成本允許，跑 3 組不同 seed 的切分並列 Δ |
| **`lift` 小 n 不穩** | 稀有組合的 `lift` 變異極大 | 泡泡大小已編碼 n；hover additionally 給 `n_joint` 原始值 |
| **Gain 份額不是變異數分解** | 「item prior 佔 60%」沒有「解釋了 60% 的什麼」這種統計意義 | 寫進 `blind_to`：它是分裂增益的份額，不是可解釋變異 |
| **within-item AUC 非指標原生** | 正例列與負例列分屬不同 query，macro mAP 沒做過這種比較 | 寫進 `blind_to`（已在 §3.2） |

## 4. 診斷契約

```python
# src/recsys_tfb/diagnosis/metric/contract.py
MAX_FIGURE_POINTS = 2000

@dataclass(frozen=True)
class ScopeNote:
    measures: str                 # 這個數字量的是什麼
    population: str               # 算在哪批列上
    blind_to: tuple[str, ...]     # 不能推論什麼（不得為空）
    reference_points: tuple[str, ...] = ()   # 報表放了哪些對照點、怎麼算的
```

registry 就是一個字串 tuple，**順序即閱讀順序**：

```python
DIAGNOSES = ("config_shift", "item_ability", "model_capacity",
             "suppression", "score_shift")
```

> 這裡刻意不用 dataclass 包 `(name, order)`：`order` 完全可由 tuple 順序推導，多一個型別只是多一個要學的概念。同理不做 `slug_for()` 函式——`f"{i+1:02d}-{name.replace('_','-')}"` 在唯一的呼叫點行內寫掉。

每個診斷子套件的 `__init__.py` 必須暴露：

- `NAME: str` — 產物檔名主幹（例：`"config_shift"`）
- `TITLE: str` — 報表標題（繁體中文）
- `SCOPE: ScopeNote`
- `compute(...) -> dict` — 純計算，回傳 JSON-safe dict。**輸入參數各診斷不同**（四項吃 `(diagnosis_sample, parameters)`，`model_capacity` 吃 `(gain_ledger, item_ability_result, parameters)`），由對應的薄 node 決定傳什麼；契約只要求「`compute` 存在且回傳 dict」，不統一簽章。
- `render(result, parameters) -> ReportSection | None` — 停用或不可用時回 `None`。**這個簽章是統一的**，因為報表收集器要對所有診斷一視同仁。

---

## 5. 六份計畫與執行順序

整個重構切成六份**可獨立執行、可獨立在公司環境檢視**的計畫。每份結束時 evaluation pipeline 必須可跑、相關測試全綠、且有東西可以看。

| # | 計畫檔 | 內容 | 公司環境看什麼 |
|---|---|---|---|
| 0 | `01-plan-0-foundation.md` | 清場（刪 `quadrant`／`triage`）＋抽樣加權＋`recsys_tfb/report/` 呈現層 | **`sample_ratio` 實際值**、`report.html` 變小、既有指標不變 |
| 1 | `02-plan-1-config-shift.md` | 診斷契約＋`config_shift`＋`scripts/render_diagnosis.py` | **第一個診斷頁面的形狀**（樣板，後三份照抄）＋離線重繪迴圈 |
| 2 | `03-plan-2-item-ability-capacity.md` | `item_ability`＋`model_capacity`（刪 `discrimination.py`） | raw vs centered AUC 散點、gain 三分、capacity vs ability |
| 3 | `04-plan-3-suppression.md` | `suppression`＋交叉購買（刪 `pair_ledger.py`／`cross_purchase.py`） | 壓制矩陣與共買圖並排讀不讀得出東西 |
| 4 | `05-plan-4-score-shift.md` | `score_shift` Optuna（刪 `offset_sweep.py`／`occupancy_spark.py`） | 執行時間、holdout Δ 與 CI、曝光 guardrail |
| 5 | `06-plan-5-wrapup.md` | `overview` 改名＋ScopeNote 驗收＋文件 | 重寫後的框架文件講不講得通 |

**必須依序執行**：Plan 1 立下的樣板，Plan 2–4 照抄；Plan 0 的抽樣加權是全部五項的地基。

**兩個關鍵檢視點**：

- **Plan 0 之後**：`sample_ratio` 若為 1.0，代表公司環境根本沒觸發次抽樣，後面所有數字都不必擔心抽樣偏差。這個事實只有真跑才知道（本機合成資料只有 654 個 query，`ratio` 恆為 1.0，永遠測不出來）。
- **Plan 1 之後**：樣板形狀定案。**這是整個重構最重要的檢視點**——後面三份照抄，形狀錯了就是錯三次。

**Plan 0 的抽樣加權為什麼要排在所有診斷之前**：五項診斷全部建在 `draw_diagnosis_sample` 上。抽樣的估計量若有偏差，五個模組會各自繼承它，事後補要改五處。地基先修。

**離線重繪為什麼排在 Plan 1 而不是收尾**：使用者的 review 迴圈是「進公司環境跑 → 看產出 → 給回饋」。若每次調欄位順序都要重跑一次公司環境，迴圈會死掉。工具越早到位，後面四次 review 越便宜。

---

## 6. 環境前置（每次進 worktree 動 python 前照抄執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # 應為 Python 3.10.9
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```

**測試一律用絕對 venv python ＋ `PYTHONPATH=src`**，裸跑會抓到 main 的 src 而靜默測錯 code：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <path> -v
```

**先建立 baseline**（main 上有既知 failing／互擾測試，清單見 `docs/operations/known-pitfalls.md` §5）：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  -q 2>&1 | tail -20 > /tmp/baseline.txt
```

---

