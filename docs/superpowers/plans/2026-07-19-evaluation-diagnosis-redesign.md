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

每個診斷子套件的 `__init__.py` 必須暴露：

- `NAME: str` — 產物檔名主幹（例：`"config_shift"`）
- `ORDER: int` — 閱讀順序（1–5），決定 HTML 檔名前綴
- `TITLE: str` — 報表標題（繁體中文）
- `SCOPE: ScopeNote`
- `compute(...) -> dict` — 純計算，回傳 JSON-safe dict。**輸入參數各診斷不同**（四項吃 `(diagnosis_sample, parameters)`，`model_capacity` 吃 `(gain_ledger, item_ability_result, parameters)`），由對應的薄 node 決定傳什麼；契約只要求「`compute` 存在且回傳 dict」，不統一簽章。
- `render(result, parameters) -> ReportSection | None` — 停用或不可用時回 `None`。**這個簽章是統一的**，因為報表收集器要對所有診斷一視同仁。

---

## 5. 階段總覽

| Phase | 內容 | 完成後的狀態 |
|---|---|---|
| 0 | 清場：刪 `quadrant`、`triage` 及其接線 | evaluation pipeline 全綠，診斷少兩項 |
| 1 | `recsys_tfb/report/` 呈現層 ＋ 外部共用 plotly.js | 行為不變，報表輸出位元等價（除 js 外置） |
| 2 | 契約 ＋ `config_shift`（樣板診斷，端到端） | 第一項新診斷可跑、可看、有頁面 |
| 3 | `item_ability`（刪 `discrimination.py`） | 第二項 |
| 4 | `model_capacity` | 第三項 |
| 5 | `suppression`（刪 `pair_ledger.py`、`cross_purchase.py`） | 第四項 |
| 6 | `score_shift`（刪 `offset_sweep.py`、`occupancy_spark.py`） | 五項到齊 |
| 7 | `overview` 改名 | 兩套「診斷」不再撞名 |
| 8 | 文件：刪 `evaluation-diagnosis.md`、重寫框架文件、寫 quickstart | 交付 |

每個 Phase 結束時 evaluation pipeline 必須可跑、相關測試全綠。

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

## Phase 0：清場

### Task 0.1: 刪除 `triage` 模組與測試

**Files:**
- Delete: `src/recsys_tfb/diagnosis/metric/triage.py`
- Delete: `tests/test_diagnosis/test_metric/test_triage.py`

- [ ] **Step 1: 確認呼叫點清單**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
grep -rn "triage" --include="*.py" --include="*.yaml" src tests conf
```
Expected: 命中 `nodes_spark.py:451`、`pipeline.py`、`report_builder.py:666`、`consistency.py:684` 一帶、`catalog.yaml`、`parameters_evaluation.yaml:142`、測試檔。把輸出貼進本步驟的紀錄，後續步驟逐一清掉。

- [ ] **Step 2: 刪模組與測試**

```bash
git rm src/recsys_tfb/diagnosis/metric/triage.py tests/test_diagnosis/test_metric/test_triage.py
```

- [ ] **Step 3: 拔掉 report section**

Modify `src/recsys_tfb/evaluation/report_builder.py`：刪掉 `build_triage_section`（`:659-714`）與 `_fmt_triage_starter`（`:645-657`）兩個函式、`:666` 的 `from recsys_tfb.diagnosis.metric.triage import STARTER_CAVEAT`、`assemble_report` 簽章的 `triage: dict | None = None` 參數、以及 `candidates` 清單裡的 `build_triage_section(triage, parameters)` 那一行。

- [ ] **Step 4: 拔掉 node 與 pipeline 接線**

Modify `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`：刪 `assemble_triage_summary`（`:451-460`）。刪 `generate_report`（`:463-538`）簽章中的 `triage` 參數與往下傳遞。
Modify `src/recsys_tfb/pipelines/evaluation/pipeline.py`：刪 `assemble_triage_summary` 的 Node 宣告，並把 `generate_report` 的 inputs 清單中的 `"evaluation_triage"` 移除。

- [ ] **Step 5: 拔掉 catalog 與 config**

Modify `conf/base/catalog.yaml`：刪 `evaluation_triage` 條目（`:258-260`）。
Modify `conf/base/parameters_evaluation.yaml`：刪 `evaluation.diagnosis.triage`（`:142` 一帶，含上方註解區塊）與 `evaluation.report.sections.triage`（`:64`）。

- [ ] **Step 6: 拆 A20 predicate（只拔 triage 那半，保留 shap 那半）**

Modify `src/recsys_tfb/core/consistency.py`：`structure_triage_param_errors`（`:684-712`）目前同時驗 `diagnostics.shap.background` 與 `evaluation.diagnosis.triage.enabled`。**只刪 triage 那段驗證**，函式改名為 `shap_background_param_errors`，A20 的 legend 說明同步改寫。呼叫點（`validate_config_consistency` 內的 `errors.extend(structure_triage_param_errors(parameters))`）同步改名。

> 為什麼不整條刪：`diagnostics.shap.background` 是 training 側 SHAP 的鍵，跟本次重構無關，刪掉會讓那個不變量失去守衛。

- [ ] **Step 7: 跑測試**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  tests/test_diagnosis/test_metric -q 2>&1 | tail -20
```
Expected: 全綠（與 `/tmp/baseline.txt` 相同的既有 fail 之外無新增 fail）。若 `test_consistency.py` 有 A20 的測試名稱含 `structure_triage`，同步改名。

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(diagnosis): triage 判定層退場（節點/報表/config/A20 半條）"
```

### Task 0.2: 刪除 `quadrant` 模組與測試

**Files:**
- Delete: `src/recsys_tfb/diagnosis/metric/quadrant.py`
- Delete: `tests/test_diagnosis/test_metric/test_quadrant.py`

- [ ] **Step 1: 刪模組與測試**

```bash
git rm src/recsys_tfb/diagnosis/metric/quadrant.py tests/test_diagnosis/test_metric/test_quadrant.py
```

- [ ] **Step 2: 拔掉 report section**

Modify `src/recsys_tfb/evaluation/report_builder.py`：刪 `build_quadrant_section`（`:422-467`）、`assemble_report` 的 `quadrant` 參數、`candidates` 裡對應那行。

- [ ] **Step 3: 拔掉 node 與 pipeline 接線**

Modify `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`：刪 `compute_quadrant`（`:349-375`）。
Modify `src/recsys_tfb/pipelines/evaluation/pipeline.py`：刪對應 Node 宣告；`generate_report` inputs 移除 `"evaluation_quadrant"`。

- [ ] **Step 4: 拔掉 catalog、config、predicate**

Modify `conf/base/catalog.yaml`：刪 `evaluation_quadrant`（`:246-248`）。
Modify `conf/base/parameters_evaluation.yaml`：刪 `evaluation.diagnosis.quadrant`（`:112-114` 含註解）與 `evaluation.report.sections.quadrant`（`:61`）。
Modify `src/recsys_tfb/core/consistency.py`：刪 `quadrant_param_errors`（`:568-593`）與其呼叫點。在 module docstring 的 Invariant legend 把 **A17 標為已退役**，比照 A16 的既有寫法（`consistency.py:96-97` 明文「不重編號，避免舊文件引用錯位」）——**不要重編號**。

- [ ] **Step 5: 跑測試**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  tests/test_diagnosis/test_metric -q 2>&1 | tail -20
```
Expected: 全綠。`test_consistency.py` 裡 A17 的測試要一併刪除。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(diagnosis): quadrant 象限層退場（門檻切割丟資訊，A17 退役不重編號）"
```

### Task 0.3: 驗證 evaluation pipeline 仍可端到端跑

- [ ] **Step 1: 跑 pre-flight**

照 §6 的指令塊執行，四行全過再繼續。

- [ ] **Step 2: 實跑 evaluation**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local
```
Expected: 成功結束，產出 `data/evaluation/<mv>/<snap>/report.html`，且該檔**不含** quadrant／triage 區塊。

> 這步可能 >2 分鐘，用 `run_in_background` 執行，不要 foreground 阻塞。

- [ ] **Step 3: read-back 確認**

Run:
```bash
grep -c "象限\|triage\|Triage" data/evaluation/*/*/report.html
```
Expected: `0`。

- [ ] **Step 4: Commit（若有殘留修正）**

```bash
git add -A && git commit -m "test(eval): Phase 0 清場後 real-run 驗證" || echo "無殘留改動"
```

---

## Phase 1：`recsys_tfb/report/` 共用呈現層

**這個 Phase 行為不變。** 只搬移與新增能力，報表內容除了 plotly.js 外置之外應完全相同。

### Task 1.1: 建立 `report/types.py` 並搬移 `ReportSection`

**Files:**
- Create: `src/recsys_tfb/report/__init__.py`
- Create: `src/recsys_tfb/report/types.py`
- Modify: `src/recsys_tfb/evaluation/report.py:59`（改成 re-export）
- Test: `tests/test_report/test_types.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_report/test_types.py
import pytest

from recsys_tfb.report import ReportSection, ScopeNote


def test_scope_note_requires_non_empty_blind_to():
    with pytest.raises(ValueError, match="blind_to"):
        ScopeNote(measures="x", population="y", blind_to=())


def test_scope_note_accepts_populated_blind_to():
    note = ScopeNote(
        measures="query 內的相對排序能力",
        population="有正例的 query",
        blind_to=("客戶之間誰更活躍",),
    )
    assert note.blind_to == ("客戶之間誰更活躍",)


def test_report_section_still_importable_from_old_location():
    from recsys_tfb.evaluation.report import ReportSection as Old
    assert Old is ReportSection
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_types.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.report'`

- [ ] **Step 3: 實作**

```python
# src/recsys_tfb/report/types.py
"""報表呈現層的中性型別。無 Spark、無 pipeline 依賴。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReportSection:
    """報表的一個區塊。"""
    title: str
    body_html: str = ""
    figures: list = field(default_factory=list)
    tables: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ScopeNote:
    """一項診斷的範圍說明——跟數字一起進報表，不放在分離的手冊裡。

    ``blind_to`` 不得為空：一個數字如果說不出它看不見什麼，讀者就會過度
    解讀。這是契約，不是建議。
    """
    measures: str
    population: str
    blind_to: tuple[str, ...]
    reference_points: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.blind_to:
            raise ValueError(
                "ScopeNote.blind_to 不得為空——每項診斷必須寫出它不能推論什麼"
            )


@dataclass(frozen=True)
class Page:
    """一份獨立 HTML 頁面。"""
    slug: str          # 檔名主幹，例 "01-config-shift"
    title: str
    scope: ScopeNote | None
    sections: tuple[ReportSection, ...]
```

> **`ReportSection` 的欄位必須與現況逐字一致。** 動手前先 `sed -n '55,70p' src/recsys_tfb/evaluation/report.py` 讀出實際定義照抄，不要用上面的示意當真實來源。
>
> **若實際欄名與上面示意不同**（本計畫後續所有測試都用 `title`／`body_html`／`figures`／`tables` 這四個名字），**以 repo 現況為準，並把後續測試裡的欄名一次改齊**——不要為了配合計畫去改既有 `ReportSection` 的欄名，那會波及全部 13 個既有 `build_*_section`。發現不一致時在本步驟記錄實際欄名，後面每個 Task 照著用。

```python
# src/recsys_tfb/report/__init__.py
from recsys_tfb.report.types import Page, ReportSection, ScopeNote

__all__ = ["Page", "ReportSection", "ScopeNote"]
```

Modify `src/recsys_tfb/evaluation/report.py`：刪掉原本的 `ReportSection` 定義，改成 `from recsys_tfb.report.types import ReportSection  # noqa: F401`（保留舊 import 路徑相容）。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_types.py -v`
Expected: PASS（3 passed）

- [ ] **Step 5: 跑既有報表測試確認沒破**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py tests/test_pipelines/test_evaluation/test_generate_report.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(report): 中性呈現層 types（ReportSection 搬移＋ScopeNote 契約）"
```

### Task 1.2: 語意化格式器 `report/fmt.py`

**Files:**
- Create: `src/recsys_tfb/report/fmt.py`
- Test: `tests/test_report/test_fmt.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_report/test_fmt.py
import math

from recsys_tfb.report.fmt import (
    fmt_ap, fmt_auc, fmt_count, fmt_delta, fmt_logodds, fmt_ratio,
)


def test_logodds_three_decimals_signed():
    assert fmt_logodds(6.90775) == "+6.908"
    assert fmt_logodds(-0.5) == "-0.500"
    assert fmt_logodds(0.0) == "0.000"


def test_auc_three_decimals_unsigned():
    assert fmt_auc(0.5471) == "0.547"


def test_ap_four_decimals():
    assert fmt_ap(0.123456) == "0.1235"


def test_delta_always_signed_four_decimals():
    assert fmt_delta(0.04) == "+0.0400"
    assert fmt_delta(-0.0008) == "-0.0008"


def test_ratio_two_decimals_with_x():
    assert fmt_ratio(1.5) == "1.50x"


def test_count_thousands_separator():
    assert fmt_count(4400000) == "4,400,000"


def test_nan_and_none_render_blank_everywhere():
    for f in (fmt_ap, fmt_auc, fmt_delta, fmt_logodds, fmt_ratio, fmt_count):
        assert f(None) == ""
        assert f(float("nan")) == ""
        assert f(math.inf) == ""
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_fmt.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.report.fmt'`

- [ ] **Step 3: 實作**

```python
# src/recsys_tfb/report/fmt.py
"""按「量的語意」決定顯示格式，不按呼叫點決定。

模組宣告的是「這一欄是 log-odds 量」，不是「這一欄要 3 位小數」。同一種量
在所有報表裡長得一樣，改全域顯示慣例只要動這一個檔案。

反例（本次重構要消滅的）：6 個腳本各有一份 ``fmt_num``，其中一份用
``math.isfinite`` 其餘用 ``np.isfinite``——各自設定的結果是漂移。
"""
from __future__ import annotations

import math


def _finite(x) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def fmt_logodds(x) -> str:
    """log-odds 量（offset、位移 δ）。帶正負號，3 位小數。"""
    v = _finite(x)
    if v is None:
        return ""
    return f"{v:+.3f}" if v != 0 else "0.000"


def fmt_auc(x) -> str:
    """AUC／份額等 [0,1] 量。3 位小數，不帶正負號。"""
    v = _finite(x)
    return "" if v is None else f"{v:.3f}"


def fmt_ap(x) -> str:
    """AP／mAP。4 位小數——mAP 的有意義差異常在第 3–4 位。"""
    v = _finite(x)
    return "" if v is None else f"{v:.4f}"


def fmt_delta(x) -> str:
    """指標差（Δ）。永遠帶正負號，4 位小數對齊 fmt_ap。"""
    v = _finite(x)
    return "" if v is None else f"{v:+.4f}"


def fmt_ratio(x) -> str:
    """倍率（lift、max/min ratio）。2 位小數＋x。"""
    v = _finite(x)
    return "" if v is None else f"{v:.2f}x"


def fmt_count(x) -> str:
    """計數。千分位，不帶小數。"""
    v = _finite(x)
    return "" if v is None else f"{int(round(v)):,}"
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_fmt.py -v`
Expected: PASS（7 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(report): 語意化格式器（按量的語意決定位數，消除 6 份 fmt_num 漂移）"
```

### Task 1.3: 色階 `report/scales.py`

**Files:**
- Create: `src/recsys_tfb/report/scales.py`
- Test: `tests/test_report/test_scales.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_report/test_scales.py
import pytest

from recsys_tfb.report.scales import diverging_scale, sequential_scale


def test_sequential_scale_is_single_hue_progression():
    scale = sequential_scale()
    assert len(scale) >= 3
    assert scale[0][0] == 0.0 and scale[-1][0] == 1.0


def test_diverging_scale_has_neutral_midpoint():
    scale = diverging_scale()
    mid = [c for pos, c in scale if pos == 0.5]
    assert len(mid) == 1, "發散色階必須有中點（0 或指定 center 處為中性色）"


def test_no_red_green_semantics_exposed():
    """色階模組不得提供帶價值判斷的 API。

    顏色編碼資料的大小或正負，不編碼好壞。這條是結構性防護——若未來有人
    想加 good/bad 配色，這個測試會擋住。
    """
    import recsys_tfb.report.scales as m
    banned = {"good_bad_scale", "pass_fail_scale", "severity_scale",
              "traffic_light", "red_green_scale"}
    assert banned.isdisjoint(set(dir(m)))


def test_center_shifts_normalised_midpoint():
    scale = diverging_scale(center=1.0, lo=0.0, hi=3.0)
    positions = [pos for pos, _ in scale]
    assert pytest.approx(1.0 / 3.0, abs=1e-9) in positions
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_scales.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.report.scales'`

- [ ] **Step 3: 實作**

```python
# src/recsys_tfb/report/scales.py
"""色階：只編碼資料的大小或正負，不編碼好壞。

單向量（計數、份額）用 sequential；有號量（Δ、lift−1、AUC 差）用
diverging，中點是中性色。**本模組刻意不提供任何 good/bad 配色**——
「這個數字是好是壞」是讀者的判斷，不是報表的。
"""
from __future__ import annotations

_SEQUENTIAL = [
    (0.0, "#f7fbff"), (0.25, "#c6dbef"), (0.5, "#6baed6"),
    (0.75, "#2171b5"), (1.0, "#08306b"),
]
_DIVERGING_LOW = ["#762a83", "#af8dc3", "#e7d4e8"]
_DIVERGING_MID = "#f7f7f7"
_DIVERGING_HIGH = ["#d9f0d3", "#7fbf7b", "#1b7837"]


def sequential_scale() -> list[tuple[float, str]]:
    """單向大小。0 = 最小、1 = 最大。"""
    return list(_SEQUENTIAL)


def diverging_scale(
    center: float = 0.0, lo: float | None = None, hi: float | None = None,
) -> list[tuple[float, str]]:
    """有號量。``center`` 在正規化後的位置為中性色。

    ``lo``/``hi`` 給定時按實際資料範圍把 center 正規化到 [0,1]；未給定時
    假設資料已對稱於 center，中點固定 0.5。
    """
    if lo is None or hi is None:
        mid = 0.5
    else:
        if hi <= lo:
            raise ValueError(f"diverging_scale: hi({hi}) 必須大於 lo({lo})")
        mid = (center - lo) / (hi - lo)
        mid = min(max(mid, 0.0), 1.0)
    out: list[tuple[float, str]] = []
    for i, c in enumerate(_DIVERGING_LOW):
        out.append((mid * i / len(_DIVERGING_LOW), c))
    out.append((mid, _DIVERGING_MID))
    n = len(_DIVERGING_HIGH)
    for i, c in enumerate(_DIVERGING_HIGH, start=1):
        out.append((mid + (1.0 - mid) * i / n, c))
    return out
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_scales.py -v`
Expected: PASS（4 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(report): 色階（sequential/diverging，結構上不提供好壞配色）"
```

### Task 1.4: 圖表建構器 `report/figures.py`

**Files:**
- Create: `src/recsys_tfb/report/figures.py`
- Test: `tests/test_report/test_figures.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_report/test_figures.py
import pytest

from recsys_tfb.report.figures import (
    MAX_FIGURE_POINTS, bubble_grid, heatmap, scatter, assert_within_budget,
)


def test_heatmap_returns_plotly_figure_with_given_axis_order():
    fig = heatmap(
        z=[[1.0, 2.0], [3.0, 4.0]],
        x=["b", "a"], y=["q", "p"],
        title="t", colorbar_title="c",
    )
    assert list(fig.data[0].x) == ["b", "a"], "軸順序必須照傳入的，不得自行排序"
    assert list(fig.data[0].y) == ["q", "p"]


def test_bubble_grid_encodes_size_and_colour_separately():
    fig = bubble_grid(
        x=["a", "b"], y=["p", "p"],
        size=[10, 200], colour=[0.5, 2.0],
        hover_text=["h1", "h2"], title="t", colorbar_title="lift",
    )
    marker = fig.data[0].marker
    assert list(marker.size) != list(marker.color), "大小與顏色編碼不同的量"
    assert list(fig.data[0].hovertext) == ["h1", "h2"]


def test_assert_within_budget_rejects_oversized_payload():
    with pytest.raises(ValueError, match="MAX_FIGURE_POINTS"):
        assert_within_budget(MAX_FIGURE_POINTS + 1, name="too_big")


def test_assert_within_budget_accepts_exact_limit():
    assert_within_budget(MAX_FIGURE_POINTS, name="ok")  # 不 raise 即通過


def test_scatter_carries_hover_labels():
    fig = scatter(x=[1, 2], y=[3, 4], labels=["i1", "i2"], title="t",
                  x_title="x", y_title="y")
    assert list(fig.data[0].text) == ["i1", "i2"]
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_figures.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.report.figures'`

- [ ] **Step 3: 實作**

建立 `src/recsys_tfb/report/figures.py`，內容包含：

- `MAX_FIGURE_POINTS = 2000` 常數與 `assert_within_budget(n, name)`——超過就 `raise ValueError`，訊息含 `MAX_FIGURE_POINTS` 字樣與 `name`。這是 §2.6 那條硬規則的執行點。
- `heatmap(z, x, y, title, colorbar_title, *, colorscale=None, center=None)` — `center` 給定時用 `diverging_scale(center, lo=min(z), hi=max(z))`，否則 `sequential_scale()`。**不得對 x/y 重新排序**（座標軸順序由呼叫端決定，因為 §3.4 要求壓制矩陣與交叉購買圖軸序一致）。
- `bubble_grid(x, y, size, colour, hover_text, title, colorbar_title, *, center=1.0)` — `go.Scatter(mode="markers")`，`marker.size` 由 `size` 經 `sizeref` 正規化，`marker.color` = `colour`，色階用 `diverging_scale(center=center, lo=min(colour), hi=max(colour))`。
- `scatter(x, y, labels, title, x_title, y_title)` — `text=labels`、`hovertemplate` 顯示標籤與兩軸值。
- `bar(x, y, title, x_title, y_title, *, colour=None, center=None)`。
- 共用主題：統一 `layout.template="plotly_white"`、統一字型大小、`margin` 一致。

每個建構器的第一行都呼叫 `assert_within_budget(len(x), name=<函式名>)`。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_figures.py -v`
Expected: PASS（5 passed）

- [ ] **Step 5: mutation check（證明預算檢查真的在因果鏈上）**

把 `heatmap` 第一行的 `assert_within_budget(...)` **呼叫**註解掉（不是改常數），跑 `test_assert_within_budget_rejects_oversized_payload` 之外的整個檔案 —— 再補一個「傳入 2001 個 x 給 heatmap 應 raise」的測試，確認它轉紅。改回後全綠。

> 為什麼 mutation 要下在**呼叫**而不是常數：改常數只會讓門檻位移，測試照樣紅，證明不了「這個檢查有被接上」。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(report): plotly 圖表建構器＋圖表資料量預算（軸序不自動重排）"
```

### Task 1.5: 多頁輸出 `report/pages.py`（共用 plotly.js）

**Files:**
- Create: `src/recsys_tfb/report/pages.py`
- Test: `tests/test_report/test_pages.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_report/test_pages.py
from recsys_tfb.report import Page, ReportSection, ScopeNote
from recsys_tfb.report.pages import write_pages

_SCOPE = ScopeNote(
    measures="測試量", population="有正例的 query",
    blind_to=("測不到的東西",), reference_points=("隨機 = 0.5",),
)


def _page(slug, title):
    return Page(slug=slug, title=title, scope=_SCOPE,
                sections=(ReportSection(title="s", body_html="<p>x</p>"),))


def test_writes_one_html_per_page_plus_index_and_shared_js(tmp_path):
    written = write_pages(
        [_page("01-a", "甲"), _page("02-b", "乙")],
        out_dir=tmp_path, index_title="診斷", index_intro="<p>導言</p>",
    )
    names = sorted(p.name for p in written)
    assert names == ["01-a.html", "02-b.html", "index.html", "plotly.min.js"]


def test_plotly_js_is_external_not_inlined(tmp_path):
    write_pages([_page("01-a", "甲")], out_dir=tmp_path,
                index_title="診斷", index_intro="")
    html = (tmp_path / "01-a.html").read_text(encoding="utf-8")
    assert 'src="plotly.min.js"' in html
    assert len(html) < 200_000, "頁面不得內嵌 plotly.js（會變成每份 3.5MB）"


def test_index_links_every_page_in_order(tmp_path):
    write_pages([_page("02-b", "乙"), _page("01-a", "甲")], out_dir=tmp_path,
                index_title="診斷", index_intro="")
    index = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert index.index("01-a.html") < index.index("02-b.html"), \
        "index 必須依 slug 排序，slug 數字前綴就是閱讀順序"


def test_scope_note_rendered_on_page(tmp_path):
    write_pages([_page("01-a", "甲")], out_dir=tmp_path,
                index_title="診斷", index_intro="")
    html = (tmp_path / "01-a.html").read_text(encoding="utf-8")
    assert "測不到的東西" in html, "blind_to 必須出現在頁面上"
    assert "有正例的 query" in html
    assert "隨機 = 0.5" in html
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_pages.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.report.pages'`

- [ ] **Step 3: 實作**

建立 `src/recsys_tfb/report/pages.py`：

- `write_pages(pages, out_dir, index_title, index_intro) -> list[Path]`
- 用 `plotly.offline.get_plotlyjs()` 取得 js 字串，**只寫一次**到 `out_dir/plotly.min.js`。
- 每頁 HTML 用 `<script src="plotly.min.js"></script>` 引用，圖用 `fig.to_html(full_html=False, include_plotlyjs=False)`。
- 每頁在標題下方渲染 `ScopeNote`：`measures`（這個數字量的是什麼）、`population`（算在哪批列上）、`blind_to`（**看不見什麼**，條列）、`reference_points`（對照點怎麼算的）。這一段的樣式要明顯區隔於數字區，讓人不會略過。
- `index.html` 依 `slug` 排序列出所有頁面標題與連結，`index_intro` 放在最上方（承載五項的邏輯架構）。
- CSS 走同一份內嵌樣板（一份，不是六份）。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_report/test_pages.py -v`
Expected: PASS（4 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(report): 多頁輸出＋共用 plotly.min.js＋ScopeNote 上頁"
```

---

## Phase 2：診斷契約 ＋ `config_shift`（樣板診斷）

**這個 Phase 立下的樣板，後面四項照抄。** 做完之後停下來檢查形狀對不對，再往下複製。

### Task 2.1: 診斷契約 `contract.py` 與契約測試

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/contract.py`
- Test: `tests/test_diagnosis/test_metric/test_contract.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_contract.py
import importlib

import pytest

from recsys_tfb.diagnosis.metric.contract import DIAGNOSES, check_module

# 隨 Phase 逐步補齊；Phase 2 只有一項
EXPECTED_ORDER = ["config_shift"]


def test_registry_lists_expected_diagnoses():
    assert [d.name for d in DIAGNOSES] == EXPECTED_ORDER


def test_every_registered_diagnosis_satisfies_contract():
    for spec in DIAGNOSES:
        mod = importlib.import_module(
            f"recsys_tfb.diagnosis.metric.{spec.name}"
        )
        check_module(mod)  # 缺任何必要符號就 raise


def test_orders_are_unique_and_contiguous_from_one():
    orders = sorted(d.order for d in DIAGNOSES)
    assert orders == list(range(1, len(orders) + 1))


def test_check_module_rejects_missing_scope():
    class Fake:
        NAME = "fake"
        ORDER = 1
        TITLE = "假的"
        def compute(self, *a, **k): ...
        def render(self, *a, **k): ...
    with pytest.raises(AttributeError, match="SCOPE"):
        check_module(Fake())
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_contract.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.contract'`

- [ ] **Step 3: 實作**

```python
# src/recsys_tfb/diagnosis/metric/contract.py
"""診斷契約：每項診斷必須長成同一個形狀。

契約存在的理由是「報表層不需要認識任何單一診斷」。新增第六項診斷＝新增
一個子套件 ＋ 在 DIAGNOSES 補一行，report_builder 零改動。
"""
from __future__ import annotations

from dataclasses import dataclass

from recsys_tfb.report import ScopeNote  # noqa: F401  （契約用到的型別）
from recsys_tfb.report.figures import MAX_FIGURE_POINTS  # noqa: F401

# MAX_FIGURE_POINTS 的唯一定義在 report/figures.py（Task 1.4）——那裡是實際
# 執行檢查的地方。這裡只 re-export 給診斷模組用，**不得另外賦值**，否則兩個
# 常數會各自漂移，而檢查只認 figures.py 那個。

_REQUIRED = ("NAME", "ORDER", "TITLE", "SCOPE", "compute", "render")


@dataclass(frozen=True)
class DiagnosisSpec:
    name: str
    order: int


DIAGNOSES: tuple[DiagnosisSpec, ...] = (
    DiagnosisSpec("config_shift", 1),
)


def check_module(mod) -> None:
    """缺任何必要符號就 raise AttributeError（訊息含缺的符號名）。"""
    for sym in _REQUIRED:
        if not hasattr(mod, sym):
            raise AttributeError(
                f"診斷模組 {getattr(mod, 'NAME', mod)!r} 缺少必要符號 {sym}"
            )


def slug_for(spec: DiagnosisSpec) -> str:
    """HTML 檔名主幹：數字前綴即閱讀順序。"""
    return f"{spec.order:02d}-{spec.name.replace('_', '-')}"
```

- [ ] **Step 4: 跑測試確認通過（第 2 條會因 `config_shift` 尚未存在而失敗）**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_contract.py -v`
Expected: `test_registry_lists_expected_diagnoses`、`test_orders_are_unique_and_contiguous_from_one`、`test_check_module_rejects_missing_scope` PASS；`test_every_registered_diagnosis_satisfies_contract` FAIL（`ModuleNotFoundError: recsys_tfb.diagnosis.metric.config_shift`）。**這是預期的 RED**，Task 2.2 會補上。若失敗訊息與此不同，停下回報。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): 診斷契約（必要符號＋registry＋slug 規則）"
```

### Task 2.2: `config_shift` 計算層

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/config_shift/__init__.py`
- Create: `src/recsys_tfb/diagnosis/metric/config_shift/compute.py`
- Test: `tests/test_diagnosis/test_metric/test_config_shift.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_config_shift.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.config_shift.compute import (
    build_offset_frame, compute,
)

PARAMS = {
    "schema": {"time": "snap_date", "entity": ["cust_id"],
               "item": "prod_name", "label": "label", "score": "score"},
    "dataset": {
        "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": 20},
                                 "config_shift": {"enabled": True}}},
}


def _sample():
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": "mass",
                "label": int(item == "ccard_ins" and c % 2 == 0),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_offset_matches_hand_computed_log_ratio():
    frame = build_offset_frame(PARAMS)
    row = frame[(frame["cust_segment_typ"] == "mass")
                & (frame["prod_name"] == "ccard_ins")].iloc[0]
    # r_pos = 1.0（無 override）, r_neg = 0.5 → ln(1.0/0.5) = ln 2
    assert row["offset"] == pytest.approx(np.log(2.0), abs=1e-12)


def test_item_without_override_gets_zero_offset():
    frame = build_offset_frame(PARAMS)
    row = frame[frame["prod_name"] == "fund_bond"].iloc[0]
    assert row["offset"] == pytest.approx(0.0, abs=1e-12)


def test_group_internal_spread_not_global():
    """群內均勻的 offset 對名次零影響——spread 必須是群內算的。"""
    params = {**PARAMS, "dataset": {**PARAMS["dataset"],
              "sample_ratio_overrides": {"mass|ccard_ins|0": 0.001,
                                         "mass|fund_bond|0": 0.001}}}
    out = compute(( _sample(), {"n_queries": 40}), params)
    assert out["offset_spread"]["mass"] == pytest.approx(0.0, abs=1e-12)


def test_delta_is_invariant_to_adding_a_constant_per_segment():
    """對某客群整組 offset 加常數，Δ 必須完全不變（query 內同減常數）。"""
    base = compute((_sample(), {"n_queries": 40}), PARAMS)
    shifted_params = {**PARAMS, "dataset": {**PARAMS["dataset"],
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5 * np.exp(-1.0),
                                   "mass|fund_bond|0": np.exp(-1.0)}}}
    shifted = compute((_sample(), {"n_queries": 40}), shifted_params)
    assert shifted["delta"] == pytest.approx(base["delta"], abs=1e-9)


def test_uses_uncalibrated_score_and_fails_loud_without_it():
    sample = _sample().drop(columns=["score_uncalibrated"])
    with pytest.raises(ValueError, match="score_uncalibrated"):
        compute((sample, {"n_queries": 40}), PARAMS)


def test_per_item_deltas_do_not_sum_to_total_delta():
    """替換實驗不是分解——這條契約必須在數字上成立，也要寫進報表。"""
    out = compute((_sample(), {"n_queries": 40}), PARAMS)
    total = out["delta"]
    per_item_sum = sum(r["delta_j"] for r in out["per_item"])
    assert out["per_item_sum_note"], "必須帶上 Σ Δ_j ≠ Δ 的說明字串"
    assert isinstance(total, float) and isinstance(per_item_sum, float)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_config_shift.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.config_shift'`

- [ ] **Step 3: 實作**

從 `scripts/config_sorting_shift_diagnosis.py` 移植，逐項對照：

| 移植來源 | 目的地 | 改動 |
|---|---|---|
| `_offset_for_values`（`:104-149`） | `compute.py` 同名私有函式 | 原樣移植 |
| `build_offset_frame`（`:281-341`） | `compute.py` 同名公開函式 | 原樣移植 |
| `run_diagnosis`（`:462-478`） | `compute.py::compute` | 簽章改成 `compute(diagnosis_sample, parameters)`，`diagnosis_sample` 是 `(sample_pdf, sample_meta)` tuple |
| `_bootstrap_macro_values`（`:370-409`） | **刪除，改呼叫** `recsys_tfb.diagnosis.metric.uncertainty` | 這是腳本裡重寫的第 1 份 cluster bootstrap；本次收斂回既有實作。若 `uncertainty.py` 的簽章不支援「配對 bootstrap」（同一組重抽同時算兩個 mAP），**在 `uncertainty.py` 加一個 `paired_bootstrap_delta()` 函式**，不要在診斷模組裡再寫一份 |
| `load_parameters`／`load_catalog`／`load_enriched_eval_predictions`（`:56-249`） | **不移植** | pipeline 已提供 parameters 與 `diagnosis_sample` |
| `render_html`／`table_html`／`fmt_num`／CSS（`:482-611`） | **不移植** | Task 2.3 用 `report/` 重寫 |

必做的行為修正：
- 讀不到 `score_uncalibrated` 欄時 `raise ValueError`，訊息含欄名——**不得靜默退回 `score`**（理由見 §3.1）。
- `offset_spread` 依客群分別計算（群內 `max − min`），不是全域。
- 輸出加 `per_item_sum_note` 字串：「Σ Δ_j ≠ Δ：名次耦合，這是逐項替換實驗不是分解」。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_config_shift.py -v`
Expected: PASS（6 passed）

- [ ] **Step 5: mutation check**

把 `compute()` 裡「群內 spread」改成「全域 spread」（一行），跑 `test_group_internal_spread_not_global`。
Expected: FAIL。改回後全綠。若改成全域之後測試仍綠，代表測試沒走到那條路徑，**先補測試再繼續，不要宣稱完成**。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): config_shift 計算層（群內 spread、配對 bootstrap、score_uncalibrated fail-loud）"
```

### Task 2.3: `config_shift` 呈現層與 `SCOPE`

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/config_shift/render.py`
- Modify: `src/recsys_tfb/diagnosis/metric/config_shift/__init__.py`
- Test: `tests/test_diagnosis/test_metric/test_config_shift_render.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_config_shift_render.py
import pytest

from recsys_tfb.diagnosis.metric import config_shift
from recsys_tfb.report import ReportSection

RESULT = {
    "enabled": True,
    "offset_spread": {"mass": 0.693, "affluent": 0.105},
    "offset_matrix": {"mass": {"ccard_ins": 0.693, "fund_bond": 0.0}},
    "baseline_map": 0.4210, "corrected_map": 0.4202,
    "delta": -0.0008, "delta_ci_low": -0.0030, "delta_ci_high": 0.0013,
    "per_item": [{"item": "ccard_ins", "delta_j": 0.0449, "n_pos": 120}],
    "per_item_sum_note": "Σ Δ_j ≠ Δ：名次耦合，逐項替換實驗不是分解",
    "sample": {"n_queries": 654, "n_items": 8},
}


def test_render_returns_section():
    section = config_shift.render(RESULT, {})
    assert isinstance(section, ReportSection)


def test_render_returns_none_when_disabled():
    assert config_shift.render({"enabled": False}, {}) is None


def test_scope_declares_what_it_cannot_tell():
    assert config_shift.SCOPE.blind_to
    assert "有正例" in config_shift.SCOPE.population


def test_no_verdict_vocabulary_in_output():
    """報表不得出現判定字眼——這是本次重構的核心約束。"""
    section = config_shift.render(RESULT, {})
    blob = section.body_html + "".join(str(t) for t in section.tables.values())
    banned = ["建議", "應該", "異常", "不足", "有問題", "健康", "通過", "失敗",
              "verdict", "severity", "recommend"]
    hit = [w for w in banned if w in blob]
    assert not hit, f"出現判定字眼：{hit}"


def test_sum_note_is_shown():
    section = config_shift.render(RESULT, {})
    assert "Σ Δ_j ≠ Δ" in section.body_html


def test_module_satisfies_contract():
    from recsys_tfb.diagnosis.metric.contract import check_module
    check_module(config_shift)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_config_shift_render.py -v`
Expected: FAIL — `AttributeError: module 'recsys_tfb.diagnosis.metric.config_shift' has no attribute 'render'`

- [ ] **Step 3: 實作**

`render.py` 產出的 section 內容（全部用 `report/` 的原語）：

1. **offset 矩陣熱圖**：`heatmap(z=客群×item 的 offset, center=0.0)`——有號量，發散色階。註明「顯示值已扣掉群內中位數（純美觀，不影響任何結論）」。
2. **群內 spread 條圖**：`bar(x=客群, y=spread)`，`fmt_logodds`。
3. **Δ 與 CI**：一行文字，`fmt_delta(delta)` ＋ `[fmt_delta(lo), fmt_delta(hi)]`。**不加任何判讀句**——不寫「顯著」「不顯著」，只給數字與區間，讀者自己看區間有沒有跨 0。
4. **per-item Δ_j 條圖**：`bar(x=item, y=delta_j, center=0.0)`，發散色階；下方緊接 `per_item_sum_note`。
5. **樣本規模**：`n_queries`／`n_items`／`n_positive_rows`，用 `fmt_count`。

`__init__.py`：

```python
# src/recsys_tfb/diagnosis/metric/config_shift/__init__.py
from recsys_tfb.diagnosis.metric.config_shift.compute import compute
from recsys_tfb.diagnosis.metric.config_shift.render import render
from recsys_tfb.report import ScopeNote

NAME = "config_shift"
ORDER = 1
TITLE = "配置引入的排序偏移"

SCOPE = ScopeNote(
    measures=(
        "抽樣比例與 sample weight 在理論上對每個 (客群, item) 引入的 "
        "log-odds 偏移，以及把它扣掉之後 macro per-item mAP 的變化量 Δ。"
    ),
    population="診斷抽樣：只含有正例的 query（macro mAP 只在這些 query 上累積）。",
    blind_to=(
        "偏移是否真的被模型吸收——這裡算的是理論值，不是從模型參數量出來的。",
        "Σ Δ_j ≠ Δ：逐 item 的 Δ_j 是替換實驗，名次互相耦合，不可相加。",
        "Δ 只反映『扣掉理論 offset』這一種操作的效果，不代表配置的全部影響。",
        "同一客群內所有 item 的 offset 同加一個常數時 Δ 完全不變——"
        "所以 Δ 量不到偏移的絕對水準，只量得到 item 之間的差。",
    ),
    reference_points=(
        "群內 spread = 0 代表該客群內 offset 均勻，對 query 內名次零影響（可直接推導，不需估計）。",
        "Δ 的 95% CI 來自配對 bootstrap：同一組重抽的 entity 上同時算 mAP(F) 與 mAP(F−offset) 再取差。",
    ),
)

__all__ = ["NAME", "ORDER", "TITLE", "SCOPE", "compute", "render"]
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_config_shift_render.py tests/test_diagnosis/test_metric/test_contract.py -v`
Expected: 全部 PASS（含 Task 2.1 那條原本 RED 的契約測試轉綠）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): config_shift 呈現層＋ScopeNote（禁判定字眼測試護欄）"
```

### Task 2.4: 接上 pipeline（node／catalog／config／predicate）

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `conf/base/catalog.yaml`
- Modify: `conf/base/parameters_evaluation.yaml`
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_pipelines/test_evaluation/test_pipeline.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_pipelines/test_evaluation/test_pipeline.py
def test_config_shift_node_wired_after_diagnosis_sample():
    from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
    pipe = create_pipeline({})
    names = [n.name for n in pipe.nodes]
    assert "diagnose_config_shift" in names
    assert names.index("draw_diagnosis_sample_node") < \
        names.index("diagnose_config_shift")


def test_config_shift_outputs_catalog_key():
    from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
    pipe = create_pipeline({})
    node = next(n for n in pipe.nodes if n.name == "diagnose_config_shift")
    assert node.outputs == ["evaluation_config_shift"]
    assert node.inputs == ["diagnosis_sample", "parameters"]
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_pipeline.py -k config_shift -v`
Expected: FAIL — `StopIteration` 或 assert 失敗（節點不存在）

- [ ] **Step 3: 實作**

在 `nodes_spark.py` 新增薄 node，形狀照抄既有 `compute_offset_sweep`（`:378-412`）：

```python
def diagnose_config_shift(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """薄 node：領域邏輯全在 diagnosis.metric.config_shift。停用時寫 stub。"""
    cfg = (((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {}).get("config_shift", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("config_shift disabled — writing stub")
        return {"enabled": False}
    if diagnosis_sample is None:
        raise ValueError(
            "diagnose_config_shift: diagnosis_sample is None while "
            "evaluation.diagnosis.config_shift.enabled is true — "
            "draw_diagnosis_sample_node gate out of sync with the consumer flag"
        )
    from recsys_tfb.diagnosis.metric import config_shift

    out = config_shift.compute(diagnosis_sample, parameters)
    logger.info(
        "config_shift computed: %d segments, delta=%s CI=[%s, %s]",
        len(out.get("offset_spread", {})), out.get("delta"),
        out.get("delta_ci_low"), out.get("delta_ci_high"),
    )
    return out
```

`pipeline.py`：在 `draw_diagnosis_sample_node` 之後、`generate_report` 之前插入該 Node。

`catalog.yaml`：
```yaml
evaluation_config_shift:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/config_shift.json
```

`parameters_evaluation.yaml`：在 `evaluation.diagnosis` 底下新增
```yaml
    # 診斷 1／5：配置引入的排序偏移。純 config 算術＋2 次 mAP＋每 item 一次
    # 替換實驗。用 score_uncalibrated（offset 活在模型輸出的 log-odds 空間，
    # 校準層是後貼的）。
    config_shift:
      enabled: true
```

`consistency.py`：把 **A18**（原 `offset_sweep_param_errors`，`:596-665`）暫時保留不動（Phase 6 才改），新增一條 predicate 驗 `evaluation.diagnosis.config_shift.enabled` 必須是 bool。沿用 A15 的 `diagnosis_metric_param_errors`（`:509-565`）加一段即可，**不新增代號**——它本來就是「診斷抽樣與 CI 的參數家族」那條。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_pipeline.py tests/test_core/test_consistency.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(eval): config_shift 接上 pipeline（node/catalog/config/A15 predicate）"
```

### Task 2.5: 報表收集器改造 ＋ 診斷頁面產出

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（`generate_report`）
- Modify: `conf/base/catalog.yaml`
- Test: `tests/test_pipelines/test_evaluation/test_generate_report.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_pipelines/test_evaluation/test_generate_report.py
def test_report_builder_has_no_per_diagnosis_builders():
    """報表層不得再認識任何單一診斷——這是解耦的驗收條件。"""
    import inspect

    from recsys_tfb.evaluation import report_builder

    names = [n for n, _ in inspect.getmembers(report_builder, inspect.isfunction)]
    forbidden = [n for n in names
                 if n.startswith("build_") and any(
                     d in n for d in ("quadrant", "offset_sweep", "pair_ledger",
                                      "triage", "config_shift", "item_ability",
                                      "suppression", "score_shift",
                                      "model_capacity"))]
    assert not forbidden, f"report_builder 仍認識個別診斷：{forbidden}"


def test_diagnosis_pages_written(tmp_path):
    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    results = {"config_shift": {
        "enabled": True, "offset_spread": {"mass": 0.693},
        "offset_matrix": {"mass": {"ccard_ins": 0.693}},
        "baseline_map": 0.42, "corrected_map": 0.42,
        "delta": -0.0008, "delta_ci_low": -0.003, "delta_ci_high": 0.0013,
        "per_item": [{"item": "ccard_ins", "delta_j": 0.04, "n_pos": 10}],
        "per_item_sum_note": "Σ Δ_j ≠ Δ",
        "sample": {"n_queries": 654, "n_items": 8},
    }}
    written = assemble_diagnosis_pages(results, {}, out_dir=tmp_path)
    names = sorted(p.name for p in written)
    assert "01-config-shift.html" in names
    assert "index.html" in names
    assert "plotly.min.js" in names
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_generate_report.py -k "diagnosis_pages or per_diagnosis" -v`
Expected: FAIL — `ImportError: cannot import name 'assemble_diagnosis_pages'`

- [ ] **Step 3: 實作**

在 `report_builder.py` 新增：

```python
def assemble_diagnosis_pages(results: dict, parameters: dict, out_dir) -> list:
    """把每項診斷的結果組成獨立頁面。本函式不認識任何單一診斷。"""
    import importlib

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES, slug_for
    from recsys_tfb.report import Page
    from recsys_tfb.report.pages import write_pages

    pages = []
    for spec in DIAGNOSES:
        result = results.get(spec.name)
        if result is None:
            continue
        mod = importlib.import_module(
            f"recsys_tfb.diagnosis.metric.{spec.name}"
        )
        section = mod.render(result, parameters)
        if section is None:
            continue
        pages.append(Page(slug=slug_for(spec), title=mod.TITLE,
                          scope=mod.SCOPE, sections=(section,)))
    return write_pages(pages, out_dir=out_dir,
                       index_title="排序診斷",
                       index_intro=_diagnosis_index_intro())
```

`_diagnosis_index_intro()` 回傳 §1 那張表的 HTML——**這段就是使用者要的「清楚好懂的邏輯架構」**，說明五項各回答什麼、排除什麼、為什麼是這個順序。它必須明說：這是閱讀順序與歸因優先權，不是硬閘門，五項都會跑。

`assemble_report` 的診斷區塊改成 `build_diagnosis_links_section(parameters)`——只放一段連結清單指向 `diagnosis/index.html`，不放任何診斷數字。

`generate_report` node：新增輸出 `evaluation_diagnosis_pages`，在 catalog 定義為指向 `data/evaluation/${model_version}/${snap_date}/diagnosis/` 的目錄型 dataset（若 repo 無目錄型 dataset，改成 node 內直接寫檔並回傳寫出的路徑清單，比照 `diagnosis/hpo/write.py` 的既有做法）。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_generate_report.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: real-run 端到端驗證**

Run（背景執行）：
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local
```
接著驗證產物：
```bash
ls -la data/evaluation/*/*/diagnosis/
du -h data/evaluation/*/*/diagnosis/*.html
```
Expected: `01-config-shift.html`、`index.html`、`plotly.min.js`、`config_shift.json` 都在；每份 HTML **小於 200KB**（js 外置），`plotly.min.js` 約 3.5MB 只有一份。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(eval): 報表收集器解耦＋診斷多頁輸出（report_builder 不再認識個別診斷）"
```

### Task 2.6: 樣板檢查點（**停下來給人看**）

- [ ] **Step 1: 產出樣板供審視**

用瀏覽器或 `open data/evaluation/*/*/diagnosis/index.html` 打開，檢查：
- `index.html` 的邏輯架構說明看得懂嗎？
- `01-config-shift.html` 的 ScopeNote 區塊夠明顯嗎？會被略過嗎？
- 圖表的顏色是否只編碼資料、沒有暗示好壞？
- 有沒有任何一句話在替讀者下結論？

- [ ] **Step 2: 交付給使用者確認樣板形狀**

**這是計畫中唯一一個強制的人工檢查點。** 後面四項診斷會照抄這個樣板，形狀錯了就是錯四次。取得確認後再進 Phase 3。

---

## Phase 3：`item_ability`

### Task 3.1: 計算層（含 sort-once bootstrap 最佳化）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/item_ability/{__init__,compute,render}.py`
- Delete: `src/recsys_tfb/diagnosis/metric/discrimination.py`
- Delete: `tests/test_diagnosis/test_metric/test_discrimination.py`
- Test: `tests/test_diagnosis/test_metric/test_item_ability.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_item_ability.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.item_ability.compute import (
    compute, weighted_auc_presorted,
)


def test_weighted_auc_matches_hand_computed_value():
    # 分數 [3,1,2]，label [1,0,1] → 正例 rank 和 = 3 + 2 = 5
    # AUC = (5 - 2*3/2) / (2*1) = (5-3)/2 = 1.0
    order = np.argsort([3.0, 1.0, 2.0])
    labels = np.array([1, 0, 1])[order]
    weights = np.ones(3)
    assert weighted_auc_presorted(labels, weights) == pytest.approx(1.0)


def test_weighted_auc_handles_ties_with_midrank():
    order = np.argsort([1.0, 1.0])
    labels = np.array([1, 0])[order]
    assert weighted_auc_presorted(labels, np.ones(2)) == pytest.approx(0.5)


def test_bootstrap_does_not_resort(monkeypatch):
    """效能契約：200 次重抽只能排序一次。

    腳本原版每次 weighted_auc 呼叫都重排（N_items × 402 次排序）。改成
    先排一次、重抽只換權重做線性掃之後，argsort 呼叫次數必須與 n_boot 無關。
    """
    import recsys_tfb.diagnosis.metric.item_ability.compute as m

    calls = {"n": 0}
    real = np.argsort

    def counting(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(m.np, "argsort", counting)
    sample = _sample()
    compute((sample, {"n_queries": 40}), _params(n_boot=5))
    few = calls["n"]
    calls["n"] = 0
    compute((sample, {"n_queries": 40}), _params(n_boot=200))
    many = calls["n"]
    assert many == few, f"argsort 次數隨 n_boot 增長：{few} → {many}"


def test_reports_both_raw_and_query_centered_auc():
    out = compute((_sample(), {"n_queries": 40}), _params())
    item = out["per_item"][0]
    assert "raw_within_item_auc" in item
    assert "query_centered_auc" in item
    assert "auc_gap_raw_minus_centered" in item


def test_requires_uncalibrated_score():
    sample = _sample().drop(columns=["score_uncalibrated"])
    with pytest.raises(ValueError, match="score_uncalibrated"):
        compute((sample, {"n_queries": 40}), _params())


def _params(n_boot=20):
    return {
        "schema": {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name", "label": "label", "score": "score"},
        "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": n_boot},
                                     "item_ability": {"enabled": True}}},
    }


def _sample():
    rng = np.random.default_rng(1)
    rows = []
    for c in range(40):
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item,
                "label": int(rng.random() < 0.3),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_item_ability.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.item_ability'`

- [ ] **Step 3: 實作**

從 `scripts/item_ability_diagnosis.py` 移植：

| 來源 | 目的地 | 改動 |
|---|---|---|
| `query_center_scores`（`:362-365`） | `compute.py` | 原樣 |
| `per_item_ap`（`:388-414`） | `compute.py` | 原樣（注意：與 `suppression_ledger_diagnosis.py:313-339` **逐位元組相同**，Phase 5 要抽到 `_common.py` 共用，本 Phase 先放這裡） |
| `rank_percentiles`（`:368-385`） | `compute.py` | 原樣 |
| `weighted_auc`（`:313-359`） | `compute.py::weighted_auc_presorted` | **改簽章**：接收「已排序好的 label 陣列與權重」，內部不再 `argsort`。呼叫端每個 item 先排一次序，bootstrap 迴圈重複使用該排序 |
| `_bootstrap_item_auc`（`:417-430`） | `compute.py` | 改成沿用上面的 presorted 排序；cluster 重抽骨架改呼叫 `uncertainty.py` 的共用函式 |
| `analyze_items`（`:604-618`） | `compute.py::compute` | 簽章改成 `compute(diagnosis_sample, parameters)` |
| load／HTML／CSS 相關 | **不移植** | pipeline 提供輸入，`report/` 負責呈現 |

同時 `git rm src/recsys_tfb/diagnosis/metric/discrimination.py tests/test_diagnosis/test_metric/test_discrimination.py`——它是同一統計量的 Spark 版，且用的是**校準後**的 `score` 欄，與本套設計的 `score_uncalibrated` 不一致。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_item_ability.py -v`
Expected: PASS（5 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): item_ability 計算層（sort-once bootstrap，discrimination.py 退場）"
```

### Task 3.2: 呈現層、`SCOPE`、pipeline 接線

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/item_ability/render.py`
- Modify: `src/recsys_tfb/diagnosis/metric/contract.py`（`DIAGNOSES` 加一行）
- Modify: `nodes_spark.py`／`pipeline.py`／`catalog.yaml`／`parameters_evaluation.yaml`
- Test: `tests/test_diagnosis/test_metric/test_item_ability_render.py`

- [ ] **Step 1: 寫失敗測試**

比照 `test_config_shift_render.py` 的六條（section 型別、停用回 None、SCOPE 有 blind_to、禁判定字眼、契約檢查），另加：

```python
def test_scope_states_auc_is_not_metric_native():
    """這條是誠實條款：AUC 不是 macro mAP 的分解，必須寫在 blind_to。"""
    from recsys_tfb.diagnosis.metric import item_ability
    joined = " ".join(item_ability.SCOPE.blind_to)
    assert "不同 query" in joined
    assert "proxy" in joined or "代理" in joined


def test_scope_warns_auc_not_comparable_externally():
    from recsys_tfb.diagnosis.metric import item_ability
    joined = " ".join(item_ability.SCOPE.blind_to) + \
        item_ability.SCOPE.population
    assert "有正例" in joined
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_item_ability_render.py -v`
Expected: FAIL — 模組缺 `render`／`SCOPE`

- [ ] **Step 3: 實作**

呈現內容：
1. **raw vs centered AUC 散點**：`scatter(x=raw, y=centered, labels=item)`，加 y=x 對角參考線。**這張圖是本項的核心**——偏離對角線的距離就是「客戶活躍度」被誤計入的量。
2. **per-item AUC 條圖含 CI 誤差線**，`fmt_auc`。
3. **AUC 差條圖**：`bar(y=auc_gap_raw_minus_centered, center=0.0)`，發散色階。
4. **正例名次百分位分布**：最低 AP 的前 N 個 item（`top_n` 預設 30）的名次分布條圖。
5. **對照點文字**：隨機打散 = 0.500；「只用 item 全域購買率排序」的 baseline **實跑數值**（不是假設值）。

`SCOPE.blind_to` 必含（逐字寫進程式碼）：
- 「item j 的正例列與負例列分屬**不同 query**，而 macro mAP 從頭到尾沒做過跨 query 的分數比較——這個 AUC 是 proxy，不是指標的分解。」
- 「母體限定在有正例的 query，所以這個數字**不能跟任何外部引用的 AUC 比較**，它會系統性地低於全母體 AUC。」
- 「AUC 高不代表 mAP 高：兩者對名次的加權方式不同。」

`contract.py` 的 `DIAGNOSES` 加 `DiagnosisSpec("item_ability", 2)`，`test_contract.py` 的 `EXPECTED_ORDER` 同步加。

pipeline 接線比照 Task 2.4（node `diagnose_item_ability`、catalog `evaluation_item_ability` → `.../diagnosis/item_ability.json`、config `evaluation.diagnosis.item_ability.enabled` ＋ `top_n`）。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric tests/test_pipelines/test_evaluation -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): item_ability 呈現層＋接線（raw vs centered AUC 對照）"
```

---

## Phase 4：`model_capacity`

### Task 4.1: 計算層

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/model_capacity/{__init__,compute,render}.py`
- Test: `tests/test_diagnosis/test_metric/test_model_capacity.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_model_capacity.py
import pytest

from recsys_tfb.diagnosis.metric.model_capacity.compute import compute

LEDGER = {
    "total_gain": 100.0,
    "item_id_gain": 60.0,
    "post_item_context_gain": 30.0,
    "per_item": {"ccard_ins": {"context_gain": 20.0},
                 "fund_bond": {"context_gain": 10.0}},
}
PARAMS = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": True}}}}


def test_gain_shares_sum_to_one():
    out = compute(LEDGER, None, PARAMS)
    s = (out["summary"]["item_id_gain_share"]
         + out["summary"]["context_gain_share"]
         + out["summary"]["unaccounted_gain_share"])
    assert s == pytest.approx(1.0, abs=1e-9)


def test_unaccounted_is_residual_not_assumed_zero():
    out = compute(LEDGER, None, PARAMS)
    assert out["summary"]["unaccounted_gain_share"] == pytest.approx(0.10)


def test_degrades_when_gain_ledger_absent():
    out = compute(None, None, PARAMS)
    assert out["enabled"] is True and out["available"] is False
    assert "gain_ledger" in out["reason"]


def test_joins_item_ability_when_model_version_matches():
    ability = {"per_item": [{"item": "ccard_ins", "query_centered_auc": 0.62}]}
    out = compute(LEDGER, ability, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["query_centered_auc"] == pytest.approx(0.62)


def test_missing_ability_leaves_auc_null_without_raising():
    out = compute(LEDGER, None, PARAMS)
    assert all(r.get("query_centered_auc") is None for r in out["per_item"])
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 實作**

從 `scripts/model_capacity_diagnosis.py` 移植 `summarize`（`:280-436`）為 `compute(gain_ledger, item_ability, parameters)`。

**關鍵改動**：腳本從檔案路徑讀 `item_ability.json` 並比對 `model_version`（`:97-109`、`:707`）；在 pipeline 裡改成**明確的 node input**，不再讀檔、不再需要版本比對——DAG 保證兩者同一次執行。

`parse_lightgbm_total_split_count`（`:48-65`，手動文字解析 model.txt）**不移植**：split 數應該從 `gain_ledger.json` 取，若 ledger 沒有這個欄位，在 `diagnosis/model/gain_ledger.py` 補上，不要在評估側重新解析模型檔。

> 為什麼：評估側解析訓練產出的 model.txt 是跨層讀內部格式，違反 `diagnosis/__init__.py:1-12` 宣告的依賴方向。

`gain_ledger` 缺席時回 `{"enabled": True, "available": False, "reason": "訓練側未產出 gain_ledger.json（catalog optional）"}`。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity.py -v`
Expected: PASS（5 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): model_capacity 計算層（gain 三分＋item_ability 併入，不再讀 model.txt）"
```

### Task 4.2: 呈現層與接線

- [ ] **Step 1: 寫失敗測試**

比照前例六條，另加：

```python
def test_capacity_vs_ability_scatter_present_when_ability_given():
    from recsys_tfb.diagnosis.metric import model_capacity
    section = model_capacity.render(RESULT_WITH_ABILITY, {})
    assert len(section.figures) >= 2, "必須含 gain 分配條圖與 capacity vs ability 散點"


def test_unavailable_result_renders_reason_not_blank():
    from recsys_tfb.diagnosis.metric import model_capacity
    section = model_capacity.render(
        {"enabled": True, "available": False, "reason": "訓練側未產出 gain_ledger.json"}, {})
    assert section is not None
    assert "gain_ledger" in section.body_html
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity_render.py -v`
Expected: FAIL — 缺 `render`

- [ ] **Step 3: 實作**

呈現內容：
1. **Gain 三分堆疊條圖**：Item Prior／Post-Item Context／未分配。
2. **per-item context gain 分配條圖**（排序後）。
3. **capacity vs ability 散點**：x = 該 item 分到的 context gain 份額、y = 該 item 的 query-centered AUC，`labels=item`。`item_ability` 缺席時略過此圖並在文字說明原因。

`SCOPE.blind_to` 必含：
- 「Gain 是**訓練期**的分裂增益，不是評測期的貢獻——gain 高不代表在這份評估資料上排得好。」
- 「未分配（Pre-Item）那塊是 item 分裂**之前**的分裂，無法歸給任何單一 item；它不是誤差。」
- 「這一項不碰評測資料，所以它跟其他四項的樣本規模無關，也不受診斷抽樣影響。」

`contract.py` 加 `DiagnosisSpec("model_capacity", 3)`。node input 是 `["gain_ledger", "evaluation_item_ability", "parameters"]`——注意 `gain_ledger` 是跨 pipeline 的 optional 產物。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric tests/test_pipelines/test_evaluation -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): model_capacity 呈現層＋接線（capacity vs ability 散點）"
```

---

## Phase 5：`suppression`（含交叉購買）

### Task 5.1: 把共用的 `per_item_ap` 抽進 `_common.py`

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/_common.py`
- Modify: `src/recsys_tfb/diagnosis/metric/item_ability/compute.py`
- Test: `tests/test_diagnosis/test_metric/test_common.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_diagnosis/test_metric/test_common.py
def test_per_item_ap_available_from_common():
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    assert callable(per_item_ap)


def test_item_ability_uses_shared_per_item_ap():
    import recsys_tfb.diagnosis.metric._common as common
    import recsys_tfb.diagnosis.metric.item_ability.compute as ia
    assert ia.per_item_ap is common.per_item_ap
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_common.py -k per_item_ap -v`
Expected: FAIL — `ImportError: cannot import name 'per_item_ap'`

- [ ] **Step 3: 實作**

把 `item_ability/compute.py` 的 `per_item_ap` 搬進 `_common.py`，兩處改為 import。

> 這個函式在兩個腳本裡是**逐位元組相同**的（`scripts/item_ability_diagnosis.py:388-414` 與 `scripts/suppression_ledger_diagnosis.py:313-339`，已用 `diff` 驗證零差異）。Phase 5 開始前抽出來，`suppression` 才不會又複製第三份。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric -q 2>&1 | tail -10`
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(diagnosis): per_item_ap 抽進 _common（消除逐位元組重複）"
```

### Task 5.2: `suppression` 計算層（向量化）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/suppression/{__init__,compute,render}.py`
- Delete: `src/recsys_tfb/diagnosis/metric/pair_ledger.py`、`cross_purchase.py`
- Delete: 對應測試
- Test: `tests/test_diagnosis/test_metric/test_suppression.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_suppression.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.suppression.compute import (
    compute, cross_purchase_stats,
)


def test_counts_negatives_ranked_above_each_positive():
    # 一個 query：分數 A=0.9(label0), B=0.5(label1) → B 被 A 壓制一次
    sample = pd.DataFrame([
        {"snap_date": "d", "cust_id": "c1", "prod_name": "A", "label": 0,
         "score_uncalibrated": 0.9, "score": 0.5},
        {"snap_date": "d", "cust_id": "c1", "prod_name": "B", "label": 1,
         "score_uncalibrated": 0.5, "score": 0.5},
    ])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_suppressed_positive_rows"] == 1
    assert out["mean_negatives_above_positive"] == pytest.approx(1.0)


def test_pair_ledger_attributes_gap_to_the_suppressor():
    sample = _two_query_sample()
    out = compute((sample, {"n_queries": 2}), _params())
    pair = next(p for p in out["pair_ledger"]
                if p["positive_item"] == "B" and p["suppressor_item"] == "A")
    assert pair["allocated_gap"] > 0


def test_cross_purchase_uses_lift_not_bare_conditional():
    """熱門 item 對任何 j 的 P(k|j) 都高——只給條件機率會退化成『熱門那行全亮』。"""
    stats = cross_purchase_stats(_cross_sample(), item_col="prod_name",
                                 entity_cols=["cust_id"])
    row = next(r for r in stats if r["item_j"] == "B" and r["item_k"] == "A")
    assert "lift" in row and "n_joint" in row and "p_k_given_j" in row


def test_cross_purchase_lift_is_one_for_independent_items():
    stats = cross_purchase_stats(_independent_sample(), item_col="prod_name",
                                 entity_cols=["cust_id"])
    row = next(r for r in stats if r["item_j"] == "X" and r["item_k"] == "Y")
    assert row["lift"] == pytest.approx(1.0, abs=0.15)


def test_axis_order_shared_between_matrices():
    """壓制矩陣與交叉購買圖必須同軸序，否則兩張圖不能對照著看。"""
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    assert out["axis_order"] == sorted(out["axis_order"])
    assert set(out["axis_order"]) >= {"A", "B"}


def test_allocation_is_vectorised():
    """效能契約：內層分攤必須向量化（腳本原版 :519 是純 Python 逐 pair 迴圈）。

    用「有沒有用到 numpy 的散射累加原語」判定，而不是「原始碼裡有沒有 for」
    ——list comprehension 也含 'for' 字樣，用字串比對會誤判。
    """
    import inspect

    import recsys_tfb.diagnosis.metric.suppression.compute as m

    src = inspect.getsource(m._allocate_gap)
    assert "np.add.at" in src or "np.bincount" in src, \
        "分攤要用 np.add.at / np.bincount 散射累加，不得逐 pair 累加"
```

> `_two_query_sample`／`_cross_sample`／`_independent_sample`／`_params` 依 Task 3.1 的 `_sample`／`_params` 同樣形狀自行構造，欄位必須含 `snap_date`／`cust_id`／`prod_name`／`label`／`score_uncalibrated`／`score`。

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 實作**

從 `scripts/suppression_ledger_diagnosis.py` 移植 `analyze_suppression`（`:727-757`）與其上游（`:464-574`）。

**必做的效能修正**：`:519` 的 `for a, raw_d, gap_d in zip(above, raw_severity, allocated_gap)` 是純 Python 逐 pair 迴圈。改成向量化：把 `(positive_row, suppressor)` 的分攤結果用 `np.add.at` 累加到以 item 索引的陣列，不逐筆迴圈。抽成 `_allocate_gap()` 私有函式，讓上面那條測試可以檢查它。

`cross_purchase_stats()` 是新函式（取代 `cross_purchase.py:cross_purchase_matrix`），對每組 `(j, k)` 輸出：
- `n_joint` = 同時買 j 與 k 的 entity 數
- `n_j`、`n_k`
- `p_k_given_j` = `n_joint / n_j`
- `lift` = `p_k_given_j / (n_k / n_entities)`

輸出加 `axis_order`（item 名稱排序後的清單），壓制矩陣與交叉購買資料都用同一組順序。

刪除：`git rm src/recsys_tfb/diagnosis/metric/pair_ledger.py src/recsys_tfb/diagnosis/metric/cross_purchase.py tests/test_diagnosis/test_metric/test_pair_ledger.py tests/test_diagnosis/test_metric/test_cross_purchase.py`，並清掉 `nodes_spark.py:415-448` 的 `compute_pair_ledger`、pipeline Node、catalog `evaluation_pair_ledger`、config `evaluation.diagnosis.pair_ledger`（`:135`）、`report_builder.build_pair_ledger_section`（`:589-643`）與 `_pair_ledger_heatmap`（`:563-587`）。

`consistency.py`：**A19**（`pair_ledger_param_errors`，`:668-679`）改寫為驗 `evaluation.diagnosis.suppression.{enabled, top_examples}`，函式改名 `suppression_param_errors`，legend 同步改寫。**沿用 A19 代號**（同一個概念槽：成對壓制帳本），不新增代號。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression.py -v`
Expected: PASS（6 passed）

- [ ] **Step 5: mutation check**

把 `_allocate_gap` 裡分攤比例的分母改成常數 1.0，跑 `test_pair_ledger_attributes_gap_to_the_suppressor`。
Expected: FAIL。改回後全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): suppression 計算層（向量化分攤＋lift 交叉購買，pair_ledger/cross_purchase 退場）"
```

### Task 5.3: 呈現層（壓制矩陣 ＋ 交叉購買泡泡格圖）

- [ ] **Step 1: 寫失敗測試**

比照前例六條，另加：

```python
def test_cross_purchase_uses_bubble_grid_with_size_and_colour():
    from recsys_tfb.diagnosis.metric import suppression
    section = suppression.render(RESULT, {})
    bubble = [f for f in section.figures
              if f.layout.title.text and "共買" in f.layout.title.text][0]
    marker = bubble.data[0].marker
    assert marker.size is not None, "泡泡大小必須編碼共買客戶數"
    assert marker.color is not None, "顏色必須編碼 lift"


def test_two_matrices_share_axis_order():
    from recsys_tfb.diagnosis.metric import suppression
    section = suppression.render(RESULT, {})
    supp = [f for f in section.figures if f.data[0].type == "heatmap"][0]
    bubble = [f for f in section.figures if f.data[0].type == "scatter"][0]
    assert list(supp.data[0].x) == sorted(set(bubble.data[0].x)), \
        "壓制矩陣與共買圖必須同軸序才能對照"
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression_render.py -v`
Expected: FAIL — 缺 `render`

- [ ] **Step 3: 實作**

呈現內容：
1. **壓制矩陣熱圖**：列 = 受害 item，欄 = 壓制者 item，值 = 分攤到的 AP 缺口份額。`sequential_scale()`（單向大小）。
2. **交叉購買泡泡格圖**：同軸序。顏色 = `lift`（`diverging_scale(center=1.0)`），大小 = `n_joint`，hover 給 `n_joint`／`n_j`／`n_k`／`p_k_given_j`／`lift`。
3. **兩張圖並排**，中間一句話說明怎麼對照著看——**只描述兩張圖各是什麼，不說「若 X 則代表 Y」**。
4. **具體案例表**：top-K 個實際被壓制的列（`top_examples` 預設 50），含 query、正例 item、壓制者、兩者的 logit 差。
5. **per-suppressor 彙總條圖**。

`SCOPE.blind_to` 必含：
- 「AP 缺口的分攤比例是**會計慣例**（依 severity 比例分攤），不是因果——它不代表『拿掉這個壓制者就會賺回這麼多』。」
- 「共買統計算的是**同一批 entity 的實際標籤共現**，與模型無關；它不解釋模型為什麼這樣排。」
- 「lift = 1 代表在這份樣本上兩個 item 的購買近似獨立，不代表商業上無關。」

`contract.py` 加 `DiagnosisSpec("suppression", 4)`。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric tests/test_pipelines/test_evaluation -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): suppression 呈現層（壓制矩陣＋共買泡泡格圖同軸序對照）"
```

---

## Phase 6：`score_shift`

### Task 6.1: 計算層（Optuna）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/score_shift/{__init__,compute,render}.py`
- Delete: `src/recsys_tfb/diagnosis/metric/offset_sweep.py`、`occupancy_spark.py` 及其測試
- Test: `tests/test_diagnosis/test_metric/test_score_shift.py`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_score_shift.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.score_shift.compute import (
    compute, split_by_entity, topk_share_by_item,
)


def test_split_by_entity_has_no_leakage():
    sample = _sample(n_cust=100)
    tune, holdout = split_by_entity(sample, ["cust_id"], fraction=0.5, seed=42)
    assert set(tune["cust_id"]) & set(holdout["cust_id"]) == set()


def test_split_is_deterministic_across_calls():
    sample = _sample(n_cust=100)
    a, _ = split_by_entity(sample, ["cust_id"], fraction=0.5, seed=42)
    b, _ = split_by_entity(sample, ["cust_id"], fraction=0.5, seed=42)
    assert sorted(a["cust_id"]) == sorted(b["cust_id"])


def test_topk_share_sums_to_one_over_items():
    shares = topk_share_by_item(_sample(n_cust=50), k=1,
                                item_col="prod_name",
                                query_cols=["snap_date", "cust_id"],
                                score_col="score_uncalibrated")
    assert sum(shares.values()) == pytest.approx(1.0, abs=1e-9)


def test_shifts_learned_on_tune_measured_on_holdout():
    out = compute((_sample(n_cust=200), {"n_queries": 200}), _params(n_trials=5))
    assert out["search"]["n_trials_completed"] == 5
    assert "holdout" in out and "baseline_map" in out["holdout"]
    assert set(out["shifts"]) <= set(_sample(n_cust=10)["prod_name"])


def test_items_below_min_positives_get_no_shift():
    out = compute((_sample(n_cust=200), {"n_queries": 200}),
                  _params(n_trials=3, min_pos_for_shift=10**9))
    assert out["shifts"] == {}, "正例數不足的 item 不得被賦予位移"


def test_delta_ci_is_paired_not_independent():
    out = compute((_sample(n_cust=200), {"n_queries": 200}), _params(n_trials=5))
    assert out["holdout"]["ci_method"] == "paired_cluster_bootstrap"
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_score_shift.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 實作**

從 `scripts/per_item_score_shift_optuna_diagnosis.py` 移植（**不是**座標下降版）：

| 來源 | 目的地 |
|---|---|
| `split_by_entity`（`:214-252`） | `compute.py` |
| `topk_share_by_item`（`:333-367`） | `compute.py` |
| `exposure_share_penalty`（`:370-378`） | `compute.py` |
| `objective`（`:480-507`） | `compute.py` 內部閉包 |
| Optuna study 建立（`:472-477`） | `compute.py` |
| `_bootstrap_macro_values`（`:614-653`） | **刪除，改呼叫 `uncertainty.py`** |
| load／HTML／CSS | **不移植** |

`holdout` 區塊必須含 `ci_method: "paired_cluster_bootstrap"`——配對是必要的，兩個 mAP 高度相關，分開算 CI 再相減會寬到測不到。

刪除 `offset_sweep.py`、`occupancy_spark.py` 及其測試，並清掉 `nodes_spark.py:378-412` 的 `compute_offset_sweep`、pipeline Node、catalog `evaluation_offset_sweep`、config `evaluation.diagnosis.offset_sweep`（`:123-127`）、`report_builder.build_offset_sweep_section`（`:513-561`）與 `_offset_sweep_waterfall`（`:469-511`）。

`consistency.py`：**A18**（`offset_sweep_param_errors`，`:596-665`）改寫為 `score_shift_param_errors`，驗 `evaluation.diagnosis.score_shift.{enabled, n_trials, n_startup_trials, shift_step, max_abs_shift, min_pos_for_shift, tune_fraction, shift_l2, exposure_k, exposure_penalty}`。`debug_inject_offsets` 的驗證留在 A18（它仍然是分流層的測試旁路）。**沿用 A18 代號**（同一概念槽：每 item 位移搜尋）。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_score_shift.py -v`
Expected: PASS（6 passed）

- [ ] **Step 5: mutation check**

把 `split_by_entity` 改成按列而非按 entity 切分（一行），跑 `test_split_by_entity_has_no_leakage`。
Expected: FAIL。改回後全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): score_shift 計算層（Optuna＋曝光 guardrail，offset_sweep/occupancy 退場）"
```

### Task 6.2: 呈現層與接線

- [ ] **Step 1: 寫失敗測試**

比照前例六條，另加：

```python
def test_exposure_guardrail_shown():
    from recsys_tfb.diagnosis.metric import score_shift
    section = score_shift.render(RESULT, {})
    assert "曝光" in section.body_html


def test_counterfactual_framing_in_scope():
    """必須說清楚這是反事實測試，不是處方。"""
    from recsys_tfb.diagnosis.metric import score_shift
    joined = " ".join(score_shift.SCOPE.blind_to)
    assert "不代表" in joined
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_score_shift_render.py -v`
Expected: FAIL — 缺 `render`

- [ ] **Step 3: 實作**

呈現內容：
1. **holdout Δ 與 CI**：`fmt_delta`，只給數字與區間。
2. **per-item 學到的位移條圖**：`bar(y=shift, center=0.0)`，發散色階，`fmt_logodds`。
3. **per-item holdout AP 前後對比**：`scatter(x=AP_before, y=AP_after, labels=item)` ＋ y=x 對角線。
4. **曝光 guardrail 對比**：每 item 的 top-k 曝光份額 vs 正例標籤份額，前後各一組。
5. **搜尋過程**：trial 收斂曲線、前 10 名 trial 明細表。

`SCOPE.blind_to` 必含：
- 「Δ > 0 只代表『per-item 常數位移』這個手段在 holdout 上有增益，**不代表**模型該被這樣改，也**不代表**問題的成因就是 item-level 水準。」
- 「位移是在 tune 折上搜出來的，holdout 只量了一次——它有過擬合的餘地，CI 只涵蓋重抽變異，不涵蓋搜尋過程本身的選擇偏誤。」
- 「曝光 guardrail 只約束 top-k 份額，不約束其他名次上的分布變化。」

`contract.py` 加 `DiagnosisSpec("score_shift", 5)`；`test_contract.py` 的 `EXPECTED_ORDER` 補齊五項。

- [ ] **Step 4: 跑全套測試**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_report tests/test_pipelines/test_evaluation \
  tests/test_core/test_consistency.py -q 2>&1 | tail -20
```
Expected: 全綠。

- [ ] **Step 5: real-run 五項到齊驗證**

Run（背景執行）：
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local
ls data/evaluation/*/*/diagnosis/
du -sh data/evaluation/*/*/diagnosis/
```
Expected: 五份 HTML ＋ `index.html` ＋ 一份 `plotly.min.js` ＋ 五份 JSON；每份 HTML < 500KB。

- [ ] **Step 6: 驗證切片重跑可行**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --list-nodes
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --from-node diagnose_score_shift
```
Expected: `--list-nodes` 列出五個 `diagnose_*` node；`--from-node` 能只重跑該診斷與下游報表。這驗證了「重跑靠 `--from-node`」這條設計前提真的成立。

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): score_shift 呈現層＋接線（五項到齊，切片重跑驗證）"
```

---

## Phase 7：`overview` 改名（純機械，行為不動）

### Task 7.1: 改名第二套「診斷」

**Files:**
- Rename: `src/recsys_tfb/evaluation/diagnostics_spark.py` → `overview_spark.py`
- Rename: `tests/test_evaluation/test_diagnostics_spark.py` → `test_overview_spark.py`
- Modify: `report_builder.py`、`nodes_spark.py`、`parameters_evaluation.yaml`

- [ ] **Step 1: 記錄改名前的報表輸出當 baseline**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
md5 data/evaluation/*/*/report.html > /tmp/report_before.md5 || \
  md5sum data/evaluation/*/*/report.html > /tmp/report_before.md5
```

- [ ] **Step 2: 執行改名**

```bash
git mv src/recsys_tfb/evaluation/diagnostics_spark.py \
       src/recsys_tfb/evaluation/overview_spark.py
git mv tests/test_evaluation/test_diagnostics_spark.py \
       tests/test_evaluation/test_overview_spark.py
```

改名對照（逐一執行，每項改完跑一次相關測試）：

| 現在 | 改成 |
|---|---|
| `report_builder.build_diagnostics_section`（`:794-807`） | `build_overview_section` |
| `generate_report` 的 `diagnostics_frames` 參數 | `overview_frames` |
| config `evaluation.report.sections.diagnostics`（`:59`） | `evaluation.report.sections.overview` |
| config `evaluation.report.diagnostics.include_distributions`（`:70`） | `evaluation.report.overview.include_distributions` |
| config `evaluation.report.diagnostics.include_calibration`（`:71`） | `evaluation.report.overview.include_calibration` |
| config `evaluation.report.diagnostics.n_calibration_bins`（`:72`） | `evaluation.report.overview.n_calibration_bins` |

在 `overview_spark.py` 的 module docstring 開頭加一段：

```
本模組是「分布概覽」——描述性的資料檢視（分數直方圖、箱型圖、名次熱圖、
校準曲線）。它與 ``recsys_tfb.diagnosis`` 套件**沒有關係**：那邊是因果歸因
（回答「為什麼」），這邊是描述（回答「長什麼樣」）。歷史上兩者都叫
「診斷」、報表上又相鄰，讀者分不出在看哪一套，2026-07-19 改名分開。
```

- [ ] **Step 3: 跑測試**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation tests/test_pipelines/test_evaluation \
  tests/test_core/test_consistency.py -q 2>&1 | tail -10
```
Expected: 全綠。若 `consistency.py` 有 predicate 驗那三個 config 鍵，鍵名同步改，否則 Layer-1 會對已不存在的鍵 raise。

- [ ] **Step 4: real-run 確認報表內容不變**

Run 一次 evaluation，比對 `report.html` 除了區塊標題文字之外內容相同。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(eval): diagnostics→overview 改名（兩套同名『診斷』分開，行為不動）"
```

---

## Phase 8：文件

### Task 8.1: 刪除失效的判讀手冊

**Files:**
- Delete: `docs/pipelines/evaluation-diagnosis.md`（754 行）

- [ ] **Step 1: 確認引用點**

Run:
```bash
grep -rn "evaluation-diagnosis.md" --include="*.md" --include="*.py" --include="*.yaml" . \
  | grep -v "docs/superpowers/plans/"
```
Expected: 列出所有引用。`docs/superpowers/plans/` 底下的是歷史紀錄，**不改**。

- [ ] **Step 2: 刪除並修引用**

```bash
git rm docs/pipelines/evaluation-diagnosis.md
```
把上一步找到的非歷史引用改指向新的 quickstart 或框架文件。`CLAUDE.md` 路由表若有引用，同步改。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: 刪除 evaluation-diagnosis.md（判讀內容已隨數字進報表）"
```

### Task 8.2: 重寫 `ranking-diagnosis-framework.md`

**Files:**
- Rewrite: `docs/ranking-diagnosis-framework.md`

- [ ] **Step 1: 讀寫作規範**

必讀 `docs/handbooks/handbook-writing-guide.md`，並使用 `writing-technical-handbooks` skill（在 `~/.claude/skills/`，手動觸發）。風格要求：白話＋英文括注、禁直譯腔、貫穿數字範例、不洩漏開發脈絡（不寫「我們原本以為…後來發現」這種敘事）。全繁體中文。

- [ ] **Step 2: 寫作**

新框架文件只放**方法論**，判讀說明已經在報表裡，**不得複述**。目標長度 **150–250 行**（舊的 477 行）。章節：

1. **指標是什麼**：macro per-item mAP 的定義，以及它只由 query 內名次決定這個性質。
2. **為什麼是這五項、為什麼是這個順序**：§1 那張表的展開版。每項寫清楚「它回答什麼」「它排除什麼」「它看不見什麼」。
3. **五項共用同一份抽樣**：為什麼這件事重要（不同母體的數字並排解讀會錯）。
4. **這套診斷不做什麼**：不下結論、不設門檻、不給處方。說明為什麼——判斷是讀者的工作，系統只負責讓資料清楚。
5. **誠實條款**：within-item AUC 不是指標原生的量；per-item Δ_j 不可加；Gain 是訓練期的量不是評測期的貢獻；score_shift 的 Δ 有搜尋選擇偏誤。

- [ ] **Step 3: 派 fresh reader 驗收**

派一個沒有本次對話脈絡的 subagent 通讀，要求它回答：「照這份文件，你能不能講出五項各回答什麼、為什麼是這個順序？哪一段你讀不懂？」——**不給它任何本次對話的結論**。至少 3 個具體問題，找不到就列出檢查過的面向。

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: 重寫排序診斷框架（方法論 150-250 行，判讀已進報表）"
```

### Task 8.3: 寫 quickstart

**Files:**
- Create: `docs/pipelines/evaluation-diagnosis-quickstart.md`

- [ ] **Step 1: 寫作**

只放**操作**，目標 **60–100 行**：
- 怎麼跑（`python -m recsys_tfb evaluation --env local`）
- 五個 `enabled` 開關在哪（逐字列出 config 鍵路徑，開檔核對不憑記憶）
- 產物在哪（§2.6 那張版面圖）
- 怎麼只重跑某一項（`--from-node diagnose_<name>`，附實測過的指令）
- 成本量級表（§Phase 6 Step 5 real-run 的實測秒數，**不是估計值**）

- [ ] **Step 2: 逐字核對 config 鍵**

Run:
```bash
grep -n "enabled" conf/base/parameters_evaluation.yaml
```
把輸出與文件中寫的鍵路徑逐字比對。**不得憑記憶寫鍵名。**

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: evaluation 診斷 quickstart（操作與成本實測）"
```

### Task 8.4: 更新 CLAUDE.md 路由表與 graphify

- [ ] **Step 1: 更新路由表**

Modify `CLAUDE.md`：路由表中指向 `evaluation-diagnosis.md` 的列改指向新的兩份文件。

- [ ] **Step 2: 重建 graphify**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: CLAUDE.md 路由表對齊新診斷文件＋graphify rebuild"
```

---

## 9. 全案驗收

- [ ] **全套測試綠**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_report tests/test_evaluation \
  tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  -q 2>&1 | tail -20
```
與 §6 建立的 `/tmp/baseline.txt` 比對，**不得有新增 fail**。

- [ ] **端到端 real-run 產物齊全**

五份診斷 HTML ＋ index ＋ 一份共用 js ＋ 五份 JSON，每份 HTML < 500KB。

- [ ] **禁判定字眼全域掃描**

```bash
grep -rn "建議\|應該\|異常\|不足\|verdict\|severity\|recommend" \
  src/recsys_tfb/diagnosis/metric/*/render.py
```
Expected: 零命中（`ScopeNote` 的 `blind_to` 裡「不代表」這類否定句不算判定，但不得出現對讀者的指示句）。

- [ ] **邊界宣稱仍成立**

```bash
grep -rn "from recsys_tfb.pipelines\|import recsys_tfb.pipelines" src/recsys_tfb/diagnosis/
grep -rn "from recsys_tfb.evaluation.report_builder" src/recsys_tfb/diagnosis/
```
Expected: 兩者皆零命中。`diagnosis/` 只依賴 `core`／`evaluation.metrics`／`io`／`utils`／`report`。

- [ ] **報表層不認識個別診斷**

```bash
grep -n "config_shift\|item_ability\|model_capacity\|suppression\|score_shift" \
  src/recsys_tfb/evaluation/report_builder.py
```
Expected: 零命中（`assemble_diagnosis_pages` 是透過 `DIAGNOSES` registry 動態載入的）。

- [ ] **清掉參考腳本**

`scripts/*_diagnosis.py` 六份與 `tests/scripts/test_*_diagnosis.py` 兩份是本次的參考實作，功能已進 `src/`。刪除或保留由使用者決定——**這一步要問，不要自己刪**。

- [ ] **fresh-context 驗收**

派一個沒有本次對話脈絡的 subagent 審查 `git diff main..feat/diag-redesign`，只給它 §0 的三條鐵則與 §9 的驗收條件，**不給任何作者結論**。要求至少 3 個具體問題（附檔案:行號與失敗情境），找不到就逐項列出檢查過的面向。

---

## 10. 這份計畫刻意不做的事

- **不做 severity／verdict／建議動作。** 見 §0。
- **不做 Spark 端的 AUC／壓制帳本。** 五項共用一份抽樣是一致性保證；效能先靠 sort-once 與向量化解決，不夠快再談，且要先有實測數字。
- **不做座標下降版的 score_shift。** Optuna 版有 L2 與曝光 guardrail，成本又與 item 數脫鉤。
- **不做校準相關的任何東西。** 本專案目標是排序不是校準。舊的 reconciliation 層已於 `48364d5` 整層刪除，不要復活它。
- **不改 `diagnosis/model/`（訓練側 SHAP）與 `diagnosis/hpo/`。** 它們與 `metric/` 零交叉 import，本次不動。注意 `hpo/paths.py:5` 依賴 `model/paths.py` 的 `diagnostics_dir`，動 `model/` 會波及 `hpo/`——本次兩者都不動。
- **不改 `docs/superpowers/plans/` 底下的歷史計畫檔。** 它們是紀錄，不是現況文件。
