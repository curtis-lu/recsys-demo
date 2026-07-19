# Plan 1：config_shift（樣板診斷）與離線重繪（診斷重構 2/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 立下診斷契約，實作第一項診斷 `config_shift`（抽樣與權重配置引入的排序偏移），並做出離線重繪工具讓後續 review 不必重跑公司環境。

**Architecture:** `diagnosis/metric/config_shift/{compute,render}.py` 子套件 ＋ `contract.py` 契約 ＋ `report_builder` 退化成收集器（不再認識任何單一診斷）＋ `scripts/render_diagnosis.py` 從 JSON 秒級重繪。**本 Plan 立下的形狀，後面三份計畫照抄。**

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 0 已完成並 merge（`recsys_tfb/report/` 存在、抽樣已帶 `inclusion_weight`）。

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
EXPECTED_ORDER = ("config_shift",)


def test_registry_order_is_reading_order():
    assert DIAGNOSES == EXPECTED_ORDER


def test_every_registered_diagnosis_satisfies_contract():
    for name in DIAGNOSES:
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        check_module(mod)  # 缺任何必要符號就 raise


def test_registry_has_no_duplicates():
    assert len(set(DIAGNOSES)) == len(DIAGNOSES)


def test_check_module_rejects_missing_scope():
    class Fake:
        NAME = "fake"
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

from recsys_tfb.report import ScopeNote  # noqa: F401  （契約用到的型別）
from recsys_tfb.report.figures import MAX_FIGURE_POINTS  # noqa: F401

# MAX_FIGURE_POINTS 的唯一定義在 report/figures.py（Task 1.4）——那裡是實際
# 執行檢查的地方。這裡只 re-export 給診斷模組用，**不得另外賦值**，否則兩個
# 常數會各自漂移，而檢查只認 figures.py 那個。

_REQUIRED = ("NAME", "TITLE", "SCOPE", "compute", "render")

# registry：順序即閱讀順序，也決定 HTML 檔名的數字前綴。
# 隨 Phase 逐步補齊（Phase 2 只有第一項）。
DIAGNOSES: tuple[str, ...] = (
    "config_shift",
)


def check_module(mod) -> None:
    """缺任何必要符號就 raise AttributeError（訊息含缺的符號名）。"""
    for sym in _REQUIRED:
        if not hasattr(mod, sym):
            raise AttributeError(
                f"診斷模組 {getattr(mod, 'NAME', mod)!r} 缺少必要符號 {sym}"
            )
```

- [ ] **Step 4: 跑測試確認通過（第 2 條會因 `config_shift` 尚未存在而失敗）**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_contract.py -v`
Expected: `test_registry_order_is_reading_order`、`test_registry_has_no_duplicates`、`test_check_module_rejects_missing_scope` PASS；`test_every_registered_diagnosis_satisfies_contract` FAIL（`ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.config_shift'`）。**這是預期的 RED**，Task 2.2 會補上。若失敗訊息與此不同，停下回報。

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
| `_bootstrap_macro_values`（`:370-409`） | **刪除，改呼叫** Task 0.5.3 建立的 `uncertainty.paired_bootstrap_delta()` | 這是腳本裡重寫的第 1 份 cluster bootstrap。**不要在診斷模組裡再寫一份**——四份同 pattern 的複製正是這次要消除的東西 |
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

__all__ = ["NAME", "TITLE", "SCOPE", "compute", "render"]
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

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.report import Page
    from recsys_tfb.report.pages import write_pages

    pages = []
    for i, name in enumerate(DIAGNOSES, start=1):
        result = results.get(name)
        if result is None:
            continue
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        section = mod.render(result, parameters)
        if section is None:
            continue
        slug = f"{i:02d}-{name.replace('_', '-')}"  # 數字前綴＝閱讀順序
        pages.append(Page(slug=slug, title=mod.TITLE,
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
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version <mv>
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

### Task 2.7: 離線重繪工具（review 迴圈的關鍵）

**Files:**
- Create: `scripts/render_diagnosis.py`
- Test: `tests/scripts/test_render_diagnosis.py`

> **這個 Task 存在的理由**：使用者要進公司環境真跑、看產出物、才能給明確回饋。若每次調欄位順序都要重跑一次公司環境，review 迴圈會死掉。這支工具讓「拷 JSON 回本機 → 秒級重繪」成為可能，第一次真跑之後的所有迭代都不必再進公司環境。
>
> 放 `scripts/` 是 repo 慣例（dev/CLI 工具單檔 ＋ `tests/scripts/` 用 `from scripts.X import`），不另開 `src/` package。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/scripts/test_render_diagnosis.py
import json

import pytest

from scripts.render_diagnosis import main


def _write_result(d, name="config_shift"):
    (d / f"{name}.json").write_text(json.dumps({
        "enabled": True,
        "offset_spread": {"mass": 0.693},
        "offset_matrix": {"mass": {"ccard_ins": 0.693}},
        "baseline_map": 0.42, "corrected_map": 0.42,
        "delta": -0.0008, "delta_ci_low": -0.003, "delta_ci_high": 0.0013,
        "per_item": [{"item": "ccard_ins", "delta_j": 0.04, "n_pos": 10}],
        "per_item_sum_note": "Σ Δ_j ≠ Δ",
        "sample": {"n_queries": 654, "n_items": 8,
                   "sampling_description": "未抽樣：全部 654 個有正例的 query 都納入。"},
    }, ensure_ascii=False), encoding="utf-8")


def test_renders_html_from_json_without_spark(tmp_path, monkeypatch):
    """重繪不得需要 Spark——這是離線迴圈成立的前提。"""
    monkeypatch.setitem(__import__("sys").modules, "pyspark", None)
    src, out = tmp_path / "in", tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_result(src)
    main(["--input-dir", str(src), "--output-dir", str(out)])
    assert (out / "01-config-shift.html").exists()
    assert (out / "index.html").exists()


def test_skips_missing_diagnoses_without_failing(tmp_path):
    src, out = tmp_path / "in", tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_result(src)                       # 只有 5 項中的 1 項
    written = main(["--input-dir", str(src), "--output-dir", str(out)])
    assert any(p.name == "01-config-shift.html" for p in written)


def test_display_config_override_changes_column_order(tmp_path):
    """呈現層可調而不重算——這是中介層的驗收條件。"""
    src, out = tmp_path / "in", tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_result(src)
    cfg = tmp_path / "display.yaml"
    cfg.write_text(
        "evaluation:\n  diagnosis:\n    config_shift:\n      display:\n"
        "        per_item_table:\n          columns: [n_pos, item, delta_j]\n",
        encoding="utf-8")
    main(["--input-dir", str(src), "--output-dir", str(out),
          "--display-config", str(cfg)])
    html = (out / "01-config-shift.html").read_text(encoding="utf-8")
    assert html.index("n_pos") < html.index("delta_j")


def test_fails_loud_on_unknown_display_column(tmp_path):
    src, out = tmp_path / "in", tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_result(src)
    cfg = tmp_path / "display.yaml"
    cfg.write_text(
        "evaluation:\n  diagnosis:\n    config_shift:\n      display:\n"
        "        per_item_table:\n          columns: [does_not_exist]\n",
        encoding="utf-8")
    with pytest.raises(KeyError, match="does_not_exist"):
        main(["--input-dir", str(src), "--output-dir", str(out),
              "--display-config", str(cfg)])
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_render_diagnosis.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.render_diagnosis'`

- [ ] **Step 3: 實作**

`scripts/render_diagnosis.py`：

- CLI：`--input-dir`（必填，放 `<name>.json` 的目錄）、`--output-dir`（必填）、`--display-config`（選填，覆寫 `display` 區塊的 YAML）、`--params`（選填，預設讀 `conf/base/parameters_evaluation.yaml`）。
- `main(argv)` 依 `DIAGNOSES` 順序逐項找 `<input-dir>/<name>.json`，找不到就跳過（不 raise），呼叫該模組的 `render()`，最後 `write_pages()`。
- **不 import pyspark**——測試會 monkeypatch 掉 `pyspark` 來驗這件事。
- `columns` 指到 JSON 沒有的鍵時 `raise KeyError`，訊息**必須列出該 JSON 實際有哪些鍵**（否則使用者會對著空白欄位除錯）。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_render_diagnosis.py -v`
Expected: PASS（5 passed）

- [ ] **Step 5: 用真產物實測**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/render_diagnosis.py \
  --input-dir data/evaluation/*/*/diagnosis --output-dir /tmp/rerender
time PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/render_diagnosis.py \
  --input-dir data/evaluation/*/*/diagnosis --output-dir /tmp/rerender
```
Expected: 六份 HTML 產出，**耗時秒級**（把實測秒數記進 quickstart）。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(scripts): render_diagnosis 離線重繪（拷 JSON 回本機，不需 Spark）"
```


---

### Task 2.8: 樣板檢查點（**停下來給人看**）

- [ ] **Step 1: 產出樣板供審視**

用瀏覽器或 `open data/evaluation/*/*/diagnosis/index.html` 打開，檢查：
- `index.html` 的邏輯架構說明看得懂嗎？
- `01-config-shift.html` 的 ScopeNote 區塊夠明顯嗎？會被略過嗎？
- 圖表的顏色是否只編碼資料、沒有暗示好壞？
- 有沒有任何一句話在替讀者下結論？

- [ ] **Step 2: 用 Task 2.7 的重繪工具確認離線迴圈可用**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/render_diagnosis.py \
  --input-dir data/evaluation/*/*/diagnosis --output-dir /tmp/rerender-check
```
Expected: 秒級完成、產物與 pipeline 產出的頁面內容一致。**這一步先過，使用者的 review 迴圈才便宜。**

- [ ] **Step 3: 交付給使用者確認樣板形狀**

**這是本計畫唯一一個強制的人工檢查點，也是整個重構最重要的一個。** Plan 2–4 的三份計畫會照抄這個樣板，形狀錯了就是錯三次。

交付內容：`data/evaluation/<mv>/<snap>/diagnosis/` 整個目錄（六份檔案）。取得使用者確認後，本 Plan 才算完成，才能開始 Plan 2。

---


## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境的 evaluation，然後**把 `data/evaluation/<mv>/<snap>/diagnosis/` 整個目錄拷回本機**。

看四件事：

1. **`01-config-shift.html` 的形狀**——ScopeNote 區塊夠不夠明顯？會不會被略過？數字的排版讀不讀得下去？
2. **`index.html` 的邏輯架構說明**——只看這一頁，講不講得出五項各回答什麼？
3. **有沒有任何一句話在替你下結論**。有的話指給我，那是設計違規不是風格問題。
4. **離線重繪能不能用**：`python scripts/render_diagnosis.py --input-dir <拷回來的目錄> --output-dir /tmp/x`，應該秒級完成且不需要 Spark。

**看完給回饋之後**：**這是整個重構最重要的檢視點。** 後面三份計畫會照抄這個樣板，形狀錯了就是錯三次。表格欄位順序／欄名／格式這類回饋，改 `display` config 即可、不必重跑公司環境；但版面結構、ScopeNote 寫法、圖表選型的回饋要在這裡給完。
