# Plan 4：score_shift 反事實位移搜尋（診斷重構 5/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作第五項診斷：不重訓模型，只給每個 item 一個固定分數位移，holdout mAP 能不能提升。

**Architecture:** 移植 Optuna 版（非座標下降版）：entity hash 切 tune/holdout 防洩漏，objective 含 L2 正則與 top-k 曝光 guardrail，Δ 附分層配對 bootstrap CI。同時刪掉 `offset_sweep.py` 與 `occupancy_spark.py`。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 3 已完成並 merge。

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

---


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

`contract.py` 的 `DIAGNOSES` 加 `"score_shift"`（第五順位）；`test_contract.py` 的 `EXPECTED_ORDER` 補齊五項。

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


---

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境 evaluation，拷回 `diagnosis/` 目錄，看：

1. **執行時間**——這是五項裡最貴的（約 300 次全量 mAP）。若時間不可接受，調 `n_trials` 或考慮把 `enabled` 預設改成 false、需要時才 `--only-node` 單獨跑。
2. **holdout Δ 與 CI**——區間跨不跨 0？
3. **曝光 guardrail 前後對比**——搜尋有沒有把曝光病態集中到少數 item？
4. **學到的位移分布**——有沒有一堆 item 被推到 `max_abs_shift` 邊界？有的話代表邊界卡住了搜尋，那個數字要調。

**看完給回饋之後**：這一項的結論最容易被過度解讀成「所以該給每個 item 加這個位移」。`SCOPE.blind_to` 的措辭要你特別看過。
