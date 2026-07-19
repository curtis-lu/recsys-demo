# Plan 0：清場與地基（診斷重構 1/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 刪掉判定型的 triage/quadrant，把診斷抽樣改成設計無偏的加權估計，並建立中性呈現層 `recsys_tfb/report/`。本 Plan 不新增任何診斷。

**Architecture:** 三步：(1) 拔掉 quadrant/triage 及其 node/報表/config/predicate；(2) 讓 `draw_diagnosis_sample` 帶出 `inclusion_weight`＋`stratum`，mAP 原語支援 optional weights，bootstrap 改層內配對重抽；(3) 新增 `recsys_tfb/report/`（型別、語意化格式器、色階、plotly 建構器、多頁輸出）。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** 無。這是第一份，從 main 開分支。

**共用脈絡（開工前必讀，本檔不複述）：** `docs/superpowers/plans/diag-redesign/00-shared-context.md`
——五項診斷的邏輯架構與閱讀順序、檔案結構、持久化邊界（§2.7）、共同統計限制（§3.6）、診斷契約（§4）。

---

## 三條鐵則（每份計畫都重貼，不得省）

這次重構的驗收標準跟一般功能不同，**寫錯方向比寫錯程式更貴**。三條鐵則：

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。診斷輸出的是數字、分布、對照點、範圍說明。判斷留給讀者。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別（舊 `quadrant.auc_threshold` 就是被這條判死的）。顏色只編碼資料本身的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，寫出它量的是什麼、算在哪批列上、**不能推論什麼**。`blind_to` 為空即契約違反，有測試擋。

**為什麼**：使用者的原話是「我沒有要把人類的思考與判斷外包給你，我要你做的是忠實呈現數據，但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。既有的 `triage.py` 正是被否決的那種東西——它已經實作了「per-item 判定＋槓桿建議」，所以它必須死，不是因為寫得不好。

---


---

## 環境前置

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

### 本機 real-run（第一次要先建環境）

worktree 的 `data/` 樹是空的（鐵則 R3：每個 worktree 用自己的真 `data/`，不 symlink 到 main）。第一次要建完整鏈：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
V=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
PYTHONPATH=src $V scripts/local_spark_setup.py --reset
PYTHONPATH=src $V -m recsys_tfb dataset  --env local
PYTHONPATH=src $V -m recsys_tfb training --env local      # 印出 model_version，記下來
PYTHONPATH=src $V -m recsys_tfb evaluation --env local --post-training --model-version <mv>
```

**兩個旗標都是必要的，少一個就跑不動**（2026-07-19 實測）：

- `--post-training`：預設模式讀 inference 產出的 `ranked_predictions`，而本機 inference 撞既有 issue #63（`scripts/local_e2e.sh:6-9` 明寫本機 e2e 只收斂到 training）。加了它改讀 training 自己產出的 `training_eval_predictions`（見 `pipelines/evaluation/pipeline.py:74` 的三元式）。
- `--model-version <mv>`：不指定會解析成 `data/models/best` symlink，而那個 symlink 要 promote 才有。**promote 是使用者保留的人工步驟，實作者不得自行執行**（CLAUDE.md 不變量）。用上一步 training 印出的 model_version 代入。


---


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
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version <mv>
```
Expected: 成功結束，產出 `data/evaluation/<mv>/<snap>/report.html`，且該檔**不含** quadrant／triage 區塊。

> 這步可能 >2 分鐘，用 `run_in_background` 執行，不要 foreground 阻塞。

- [ ] **Step 3: read-back 確認**

**查原始碼，不要查 report.html**：

```bash
grep -rn "quadrant\|triage\|象限" src/recsys_tfb/evaluation/ src/recsys_tfb/pipelines/evaluation/
```
Expected: 零命中。

> ⚠ **不要用 `grep report.html` 當驗收**。目前 `generate_html_report` 把整份 plotly.js 內嵌進 HTML（`evaluation/report.py:85,118`，約 3.5MB），而 plotly 內部有 `En.prototype.quadrant`（四叉樹實作）。2026-07-19 實測：對 report.html grep `quadrant` 會得到 2 個命中，全部來自 plotly 內部，與本專案無關。**查產生 HTML 的原始碼才是可靠的檢查。**
>
> （Phase 1 把 plotly.js 外置之後這個陷阱會消失，但那時仍建議查原始碼——它更直接。）

- [ ] **Step 4: Commit（若有殘留修正）**

```bash
git add -A && git commit -m "test(eval): Phase 0 清場後 real-run 驗證" || echo "無殘留改動"
```

---

## Phase 0.5：抽樣加權與估計量正確性（地基）

**為什麼這個 Phase 存在。** `draw_diagnosis_sample` 是**分層抽樣**，但沒有把納入機率帶出來：

- **take-all 層**：正例 query 數低於 `min_pos_queries_per_item`（預設 50）的稀有 item，其全部正例 query 整批取用（`src/recsys_tfb/diagnosis/metric/sample.py:88-92`）→ 納入機率 π = 1.0
- **hash-ratio 層**：其餘 query 依 `ratio = min(1.0, budget / n_others)` 抽（`sample.py:115-120`）→ π = `ratio`

`meta` 記了 `ratio` 與 `take_all_items`（`sample.py:148-156`），但 **`sample_pdf` 沒有任何一欄記錄 π，也沒有權重**，下游一律當簡單隨機樣本用。

**後果**：`ratio < 1` 時，take-all 層被系統性高估 `1/ratio` 倍。而 take-all 的 query 帶回該客戶的**全部候選列**——被高估的不只是稀有 item，是所有與稀有 item 共同出現的列。macro mAP、within-item AUC、壓制帳本的缺口分攤、Δ **全部繼承這個偏差**。

**兩個緩和事實（不要誇大問題）**：
- `ratio == 1.0` 時（公司環境的正例 query 總數 ≤ `max_queries`）問題**不存在**，加權退化成全 1。
- 本機合成資料只有 654 個 query，遠低於 `max_queries: 200000`，所以 `ratio` 恆為 1.0——**本機永遠測不到這個問題**，測試必須自己造 `ratio < 1` 的情境。

**這是設計缺口不是 bug**：現有程式碼沒有宣稱自己做了設計無偏的估計。

### Task 0.5.1: 抽樣帶出納入機率權重

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/sample.py`
- Test: `tests/test_diagnosis/test_metric/test_sample.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_diagnosis/test_metric/test_sample.py
def test_inclusion_weight_column_present(spark):
    pdf, meta = draw_diagnosis_sample(_predictions(spark, n_cust=20), _params())
    assert "inclusion_weight" in pdf.columns


def test_all_weights_are_one_when_no_subsampling(spark):
    """ratio == 1.0（樣本小於 max_queries）時加權必須退化成全 1。"""
    pdf, meta = draw_diagnosis_sample(_predictions(spark, n_cust=20), _params())
    assert meta["sample_ratio"] == 1.0
    assert (pdf["inclusion_weight"] == 1.0).all()


def test_take_all_stratum_gets_weight_one_and_others_get_inverse_ratio(spark):
    """ratio < 1 的情境必須自己造——本機資料量永遠觸發不到。"""
    params = _params(max_queries=10, min_pos_queries_per_item=3)
    pdf, meta = draw_diagnosis_sample(_predictions(spark, n_cust=200), params)
    assert 0.0 < meta["sample_ratio"] < 1.0
    take_all = set(meta["take_all_items"])
    assert take_all, "測試情境必須真的產生 take-all 層，否則沒測到分層"
    w = pdf.groupby("stratum")["inclusion_weight"].first().to_dict()
    assert w["take_all"] == pytest.approx(1.0)
    assert w["hash_ratio"] == pytest.approx(1.0 / meta["sample_ratio"])


def test_stratum_column_labels_every_row(spark):
    params = _params(max_queries=10, min_pos_queries_per_item=3)
    pdf, _ = draw_diagnosis_sample(_predictions(spark, n_cust=200), params)
    assert set(pdf["stratum"].unique()) <= {"take_all", "hash_ratio"}
    assert pdf["stratum"].notna().all()


def test_weight_is_constant_within_a_query(spark):
    """權重是 query 級的（抽樣單位是 query），同一 query 的所有列必須同權重。"""
    params = _params(max_queries=10, min_pos_queries_per_item=3)
    pdf, _ = draw_diagnosis_sample(_predictions(spark, n_cust=200), params)
    n = pdf.groupby(["snap_date", "cust_id"])["inclusion_weight"].nunique()
    assert (n == 1).all()
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -k inclusion -v
```
Expected: FAIL — `KeyError: 'inclusion_weight'` 或 `assert 'inclusion_weight' in pdf.columns` 失敗。

- [ ] **Step 3: 實作**

Modify `sample.py`：在 pass 2 選出 `sampled` 之後、`join` 之前，替 query 集合加兩欄：

```python
from pyspark.sql import functions as F

if must is not None:
    must_tagged = must.withColumn("stratum", F.lit("take_all"))
else:
    must_tagged = None
picked_tagged = (picked.withColumn("stratum", F.lit("hash_ratio"))
                 if ratio > 0 else None)
# union 之後：
sampled = sampled.withColumn(
    "inclusion_weight",
    F.when(F.col("stratum") == "take_all", F.lit(1.0))
     .otherwise(F.lit(1.0 / ratio if ratio > 0 else 1.0)),
)
```

`meta` 增補：`"strata": {"take_all": {"n_queries": ..., "weight": 1.0}, "hash_ratio": {"n_queries": ..., "weight": ...}}`。

**邊界情況必須處理**：`ratio == 0.0`（budget ≤ 0，全部 take-all）時 `1/ratio` 會炸——那個情況下沒有 hash_ratio 層，權重全 1.0。上面的 `if ratio > 0 else 1.0` 就是守這件事。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -v
```
Expected: 全部 PASS（含既有的抽樣測試）。

- [ ] **Step 5: mutation check**

把 `inclusion_weight` 的 `.otherwise(...)` 改成 `F.lit(1.0)`（即忽略分層），跑 `test_take_all_stratum_gets_weight_one_and_others_get_inverse_ratio`。
Expected: FAIL。改回後全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): 抽樣帶出 inclusion_weight + stratum（分層設計顯性化）"
```

### Task 0.5.2: 估計量吃權重（向後相容）

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics.py`
- Test: `tests/test_evaluation/test_metrics.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_evaluation/test_metrics.py
import numpy as np
import pytest

from recsys_tfb.evaluation.metrics import compute_macro_per_item_map


def test_weights_none_is_bit_identical_to_before(sample_frame):
    """向後相容契約：不傳 weights 時結果必須與加這個參數之前完全相同。

    compute_macro_per_item_map 也被主指標路徑用，不能因為診斷需求改變它的
    既有行為。
    """
    a = compute_macro_per_item_map(sample_frame, **_metric_kwargs())
    b = compute_macro_per_item_map(sample_frame, weights=None, **_metric_kwargs())
    assert a == b


def test_uniform_weights_equal_unweighted(sample_frame):
    w = np.ones(len(sample_frame))
    a = compute_macro_per_item_map(sample_frame, **_metric_kwargs())
    b = compute_macro_per_item_map(sample_frame, weights=w, **_metric_kwargs())
    assert b == pytest.approx(a, abs=1e-12)


def test_duplicating_a_query_equals_doubling_its_weight(sample_frame):
    """加權的正確性判準：權重 2 等價於把該 query 複製一份。"""
    q = sample_frame.iloc[:1][["snap_date", "cust_id"]].to_dict("records")[0]
    mask = ((sample_frame["snap_date"] == q["snap_date"])
            & (sample_frame["cust_id"] == q["cust_id"]))
    duplicated = pd.concat([sample_frame, sample_frame[mask]], ignore_index=True)
    w = np.where(mask, 2.0, 1.0)
    a = compute_macro_per_item_map(duplicated, **_metric_kwargs())
    b = compute_macro_per_item_map(sample_frame, weights=w, **_metric_kwargs())
    assert b == pytest.approx(a, abs=1e-9)
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py -k weight -v
```
Expected: FAIL — `TypeError: compute_macro_per_item_map() got an unexpected keyword argument 'weights'`

- [ ] **Step 3: 實作**

在 `evaluation/metrics.py` 的 `compute_macro_per_item_map` 加 **optional `weights=None`** 參數：

- `weights is None` → 走原本的程式碼路徑，**一行都不改**（`test_weights_none_is_bit_identical_to_before` 守這件事）。
- 給了 weights → per-item AP 的加總改成加權平均（權重是 query 級的納入權重倒數）。

> **為什麼用 optional 參數而不是另開 `weighted_*` 函式**：`compute_macro_per_item_map` 同時被主指標路徑與診斷路徑使用。另開一套會變成兩份要同步維護的 mAP 實作——而這次重構的主軸之一就是消除重複實作。optional 參數＋位元等價測試同時保住相容與單一實作。

> **更正（2026-07-19 審查修復）**：本節原本還要求對 `positive_row_contributions`
> 與 `macro_from_per_item` 加 `weights`，**兩者都已撤回**，理由不同：
>
> - `positive_row_contributions`：加了 optional `weights` 會讓**回傳 arity 隨參數變動**
>   （2-tuple ↔ 3-tuple）。它有 8＋個既有呼叫點（`diagnosis/metric/uncertainty.py:63`、
>   `scripts/per_item_score_shift_diagnosis.py:397,440`、
>   `scripts/per_item_score_shift_optuna_diagnosis.py:623,666`、
>   `scripts/config_sorting_shift_diagnosis.py:379` …）全部寫 `contrib, row_idx = ...`。
>   改成**固定 2-tuple**，權重的驗證與廣播抽成 `align_positive_row_weights(weights,
>   n_rows, row_idx)`，Task 0.5.3 的 bootstrap 直接呼叫它取那條對齊向量——
>   廣播邏輯仍只有一份，但沒有變動 arity 的 footgun。
> - `macro_from_per_item`：跨 item 的 macro 合併**必須維持等權重**（複製一個 query
>   不會增加 item），加權後的 `n_pos` 已由 `compute_macro_per_item_map` 經既有
>   `n_pos` 參數傳入，第二條權重通道會重複計數。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py tests/test_evaluation/test_statistics.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(metrics): mAP 原語支援 optional weights（None 時位元等價）"
```

### Task 0.5.3: 分層配對 cluster bootstrap

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/uncertainty.py`
- Test: `tests/test_diagnosis/test_metric/test_uncertainty.py`

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_diagnosis/test_metric/test_uncertainty.py
import numpy as np
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta


def test_paired_delta_ci_is_narrower_than_independent_cis():
    """配對的必要性：兩個 mAP 高度相關，分開算 CI 再相減會寬到測不到。"""
    lo, hi = paired_bootstrap_delta(
        _frame(), _metric_kwargs(), shift=_tiny_shift(), n_boot=200, seed=1)
    ind_lo, ind_hi = _independent_delta_ci(_frame(), n_boot=200, seed=1)
    assert (hi - lo) < (ind_hi - ind_lo)


def test_resampling_stays_within_strata():
    """分層設計下，重抽必須在層內進行——跨層重抽會扭曲層的相對比重。"""
    drawn = []
    paired_bootstrap_delta(
        _frame_two_strata(), _metric_kwargs(), shift=_tiny_shift(),
        n_boot=20, seed=1, _record_draws=drawn)
    for replicate in drawn:
        for stratum, n in replicate.items():
            assert n == _stratum_sizes()[stratum], \
                f"層 {stratum} 重抽後大小改變（{n} != {_stratum_sizes()[stratum]}）"


def test_zero_shift_gives_ci_containing_zero():
    lo, hi = paired_bootstrap_delta(
        _frame(), _metric_kwargs(), shift={}, n_boot=200, seed=1)
    assert lo <= 0.0 <= hi


def test_deterministic_given_seed():
    a = paired_bootstrap_delta(_frame(), _metric_kwargs(), shift=_tiny_shift(),
                               n_boot=50, seed=7)
    b = paired_bootstrap_delta(_frame(), _metric_kwargs(), shift=_tiny_shift(),
                               n_boot=50, seed=7)
    assert a == b
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_uncertainty.py -k paired -v
```
Expected: FAIL — `ImportError: cannot import name 'paired_bootstrap_delta'`

- [ ] **Step 3: 實作**

在 `uncertainty.py` 新增 `paired_bootstrap_delta(frame, metric_kwargs, shift, n_boot, seed, strata_col="stratum", weight_col="inclusion_weight")`：

1. 依 `strata_col` 分組，**層內**對 entity（cluster）做有放回重抽，重抽後各層的 cluster 數維持不變。
2. **同一組重抽樣本上同時算 `mAP(F)` 與 `mAP(F − shift)`**，取差。分開算 CI 再相減是錯的。
3. 每列的權重 = `inclusion_weight × 該 cluster 的重抽次數`，傳給 Task 0.5.2 的 `weights` 參數。
4. 回傳 `(lo, hi)` 百分位數。
5. `_record_draws` 是測試用的 hook（預設 `None`），記錄每個 replicate 各層抽到的 cluster 數。

`frame` 沒有 `stratum`／`inclusion_weight` 欄時（例如舊呼叫端）視為單層、全 1 權重——**向後相容，不 raise**。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_uncertainty.py -v
```
Expected: 全綠。

- [ ] **Step 5: mutation check**

把層內重抽改成「忽略 stratum、全體一起重抽」（一行），跑 `test_resampling_stays_within_strata`。
Expected: FAIL。改回後全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): 分層配對 cluster bootstrap（層內重抽＋同組樣本取差）"
```

### Task 0.5.4: 抽樣設計摘要成人看得懂的字串

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/sample.py`
- Test: `tests/test_diagnosis/test_metric/test_sample.py`

> 這一步刻意只做**計算側**。把它顯示到頁面上的部分在 Phase 1（`ScopeNote.sampling` 欄位）與各診斷的 `render()`——`report/` 套件那時才存在。

- [ ] **Step 1: 寫失敗測試**

```python
# 追加到 tests/test_diagnosis/test_metric/test_sample.py
def test_meta_carries_human_readable_sampling_description(spark):
    _, meta = draw_diagnosis_sample(_predictions(spark, n_cust=20), _params())
    assert meta["sampling_description"]


def test_description_says_no_subsampling_when_ratio_is_one(spark):
    """最常見也最令人安心的情況，必須明講而不是沉默。"""
    _, meta = draw_diagnosis_sample(_predictions(spark, n_cust=20), _params())
    assert meta["sample_ratio"] == 1.0
    assert "未抽樣" in meta["sampling_description"]


def test_description_reports_both_strata_when_subsampled(spark):
    params = _params(max_queries=10, min_pos_queries_per_item=3)
    _, meta = draw_diagnosis_sample(_predictions(spark, n_cust=200), params)
    desc = meta["sampling_description"]
    assert "分層" in desc
    assert str(meta["strata"]["take_all"]["n_queries"]) in desc.replace(",", "")
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -k description -v
```
Expected: FAIL — `KeyError: 'sampling_description'`

- [ ] **Step 3: 實作**

在 `sample.py` 的 `meta` 增補 `sampling_description`，由 `ratio` 與 `strata` 動態組出（**不是寫死文案**，`ratio` 每次執行都不同）：

- `ratio == 1.0` → `"未抽樣：全部 12,345 個有正例的 query 都納入。"`
- `ratio < 1.0` → `"分層抽樣：take-all 層 1,200 query（稀有 item，權重 1.0）、hash-ratio 層 18,800 query（權重 1.85）。跨 item 的統計量已依納入機率加權。"`

數字用千分位。這段字串在 Phase 1 之後會被 `ScopeNote.sampling` 帶上每一頁。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -v
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): 抽樣設計摘要字串（ratio 動態組字，未抽樣時明講）"
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

    ``sampling`` 由各診斷的 ``render()`` 從 ``result["sample"]["sampling_
    description"]``（Task 0.5.4 產生）動態帶入，**不是寫死的文案**——抽樣
    比例每次執行都可能不同。
    """
    measures: str
    population: str
    blind_to: tuple[str, ...]
    reference_points: tuple[str, ...] = ()
    sampling: str = ""

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

這是設計約定，不是程式擋得住的事：誰要畫紅綠燈，直接在 figures.py 寫死
色碼就繞過了。曾經想用「測試斷言某些函式名不存在」來守，但那防不了它宣稱
要防的事，卻讓人以為防住了——所以拿掉，改成這段註解＋code review。
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
Expected: PASS（3 passed）

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


---

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境的 evaluation，看三件事：

1. **`sample_ratio` 實際是多少**（在診斷抽樣的 log 與 `metric_ci.json` 的 `sample` 區塊）。**這是本 Plan 最重要的產出**——若 `ratio == 1.0`（正例 query 總數 ≤ `max_queries`），代表公司環境根本沒觸發次抽樣，後面所有數字都不必擔心抽樣偏差；若 `< 1.0`，加權就是必要的，且要記下 take-all 層佔多大。
2. **`report.html` 檔案大小**（plotly.js 外置後應顯著變小）。
3. **既有指標數字沒變**——headline mAP、per-item AP 與 Plan 0 之前跑的結果一致。少的只有 quadrant 與 triage 兩個區塊。

**看完給回饋之後**：若 ① 顯示 `ratio == 1.0`，在 Plan 1 開始前告訴我，我會把後續各 Plan 的抽樣警語調整成「本環境未觸發次抽樣」，減少無謂的告誡文字。
