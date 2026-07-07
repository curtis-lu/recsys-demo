# Phase 2：對帳層（理論偏移 vs 實測校準差距）— 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把「自己的採樣／加權配置理論上會造成的分數偏移」算出來，跟實測 per-item 校準差距對帳——per item 給出 {理論偏移帶、實測 gap、殘差、verdict}，落 `diagnosis/reconciliation.json` 與報表新 section。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 2。

**Architecture:** 新檔 `diagnosis/metric/reconciliation.py` 三個函式——`theoretical_offsets(parameters)`（純 Python 讀 config，零 Spark）、`calibration_gap_by_item(sdf, parameters, score_col)`（Spark groupBy 聚合＋driver 端 logit，無 UDF）、`reconcile(eval_predictions, parameters)`（組裝對帳表＋verdict）。評估 pipeline 加薄節點 `compute_reconciliation`，報表加 `build_reconciliation_section`。config `evaluation.diagnosis.reconciliation`＋consistency **A16**。

**Tech Stack:** PySpark 3.3.2（無 UDF）、純 Python math、pytest、本機 local Spark。

**Scope note:** 本計畫只涵蓋 Phase 2。閘門需要**兩輪完整 dataset→training→evaluation 重跑**（注入＋還原），與 Phase 1 只跑 evaluation 不同。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd <該路徑> && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。
3. **可能超過 2 分鐘的指令（dataset/training/evaluation 真跑）一律背景執行**。evaluation CLI 必帶 `--model-version <mv>`（無 `best` symlink；promote 是使用者保留的人工步驟）。
4. **生產不變量**：no Spark UDF、no new packages；`diagnosis/*` 只 import `core / evaluation(僅 numpy 原語 metrics.py) / io / utils`＋pandas/pyspark/numpy/標準庫。
5. **不動 model_version 語意**：新 config 全在 `evaluation.*`。閘門的注入改 `dataset.sample_ratio_overrides` **會**改 base_dataset_version 與 model_version——這是預期效應（注入本來就要重訓），還原後 config 回原狀。
6. 測試判準＝與 baseline 一致（known-pitfalls §5）。

## 設計定案（所有 task 共用語意，不要各自發明）

- **理論偏移通式（per 完整 group-key cell）**：`offset = ln((r₁·w₁)/(r₀·w₀))`。
  - `r₀`/`r₁`＝該 cell label=0/label=1 的抽樣保留率（`dataset.sample_ratio_overrides` 查表，缺項用 `dataset.sample_ratio` 預設）；`w₀`/`w₁`＝訓練權重（`training.sample_weights` 查表，缺項 1.0）。
  - 退化檢查：只砍負類保留 r（`r₀=r, r₁=1, w=1`）→ `offset = −ln r`（手冊3 Ch10 的 logQ 校正，spec 釘的案例）；正負類同率同權 → 0。
  - **label 不在 `sample_group_keys` → 抽樣對 label 對稱 → 抽樣側貢獻恆 0**；label 不在 `sample_weight_keys` → 權重側貢獻恆 0（現行 config `sample_weight_keys=[prod_name]` 即此情況）。兩者都要有測試。
- **item 層是聚合近似（誠實限制，spec 明定）**：overrides 是多維 key（`cust_segment_typ|prod_name|label`），同一 item 各 segment cell 的 offset 不同。產物按完整 group key **細列**；item 層給 `{min, max, mean, n_cells}` 摘要帶並標 `approx: true`。**verdict 用帶不用單值**：`residual = gap − clip(gap, theory_min − 0, theory_max + 0)`（帶內＝0），`|residual| ≤ explained_threshold` → 可解釋。config 沒列任何 cell 的 item：帶＝[0, 0]。
- **實測 gap**：per item `logit(p̄) − logit(ȳ)`（先平均再取 logit，spec 釘的公式）。`p̄`＝`score_col` 的平均、`ȳ`＝label 平均，Spark `groupBy(item).agg(mean)` 後 driver 端算 logit（22 個 item，collect 極小）。守門：`ȳ` 或 `p̄` ∉ (0,1) → gap＝None、reason 註記，該 item verdict＝`無法評估`（不炸）。
- **score_col 雙欄**：主判 `score_uncalibrated`（校準層本身就在修 level，對帳要看模型原始輸出）；`score`（校準後）同表並列作對照——**已知答案結構**：本 repo `enable_calibration: true` 且校準集無偏抽樣（`calibration_sample_ratio_overrides: {}`），所以注入後 `gap(score_uncalibrated) ≈ 理論值`、`gap(score) ≈ 0`，兩欄對照本身就是校準層有效性的證據。降級：eval_predictions 無 `score_uncalibrated` 欄（monitoring 路徑）→ 自動退回 `score`、輸出 `fallback: true` 並在報表標註，不失敗。
- **執行時修訂（Task 7 Step 2 實證，spec 已同步註記）**：verdict 基準改為 `gap_vs_global = gap − global_reference`（global_reference＝理論帶 [0,0] 的 item 的 gap 中位數；不足 3 個 → 0，退回絕對語意——既有單元測試因此全數不變）。成因＝post-training 母體條件化（只含有正例客戶）造成全 item 一致負移 −0.38～−0.44，絕對式 verdict 全面誤判。絕對 gap 與 global_reference 照列，JSON 增 `global` 區塊。
- **既有 config 的內建已知答案**：conf/base 本來就有 `ccard_ins` 負類 overrides（mass 0.5／affluent 0.9／hnw 0.8）→ 現有模型 6059dcef 的對帳表 `ccard_ins` 理論帶應為 `[ln(10/9), ln 2] ≈ [0.105, 0.693]`、其他 item 帶＝[0,0]。閘門第一步先看這個（不用重訓）。
- **既有測試會被本計畫「合法」改到的只有一處**：`tests/test_pipelines/test_evaluation/test_pipeline.py` 的結構斷言——default/post_training 六個 node → **七個**、compare-source 九個 → **十個**、node 名清單加 `compute_reconciliation`、outputs 加 `evaluation_reconciliation`（Phase 1 同款，**預先授權**）。其他既有測試一行不得改。
- **報表回歸鎖的時效**：閘門 (c) 還原後重訓會覆蓋 6059dcef 的模型檔（重訓位元重現性未鎖），其後 `/tmp/phase1_report_before.html` 的逐字比對**失效**。因此既有 section 的逐字回歸比對必須在 Task 7 **重訓之前**做完。

## 執行模式（controller 注意）

同 Phase 1：機械步驟（baseline、真跑閘門）controller 直跑；Task 2–6 派 sonnet implementer（prompt 附對應 task 全文＋本節＋設計定案）；合併 reviewer 在 Task 6 後一次審 Task 2–6；opus 總審在 Task 7 收尾。

---

### Task 1：pre-flight ＋ baseline

**Files:** 無程式碼變更；產出 `/tmp/phase2_test_baseline.txt`、`/tmp/phase2_report_before.html`。

- [ ] **Step 1: pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd && readlink .venv && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: worktree root、Python 3.10.9、isolation OK、working tree 乾淨。

- [ ] **Step 2: 報表快照（Phase 1 終版產物即改動前基準）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
cp data/evaluation/6059dcef/20260131/report.html /tmp/phase2_report_before.html && ls -la /tmp/phase2_report_before.html
```

- [ ] **Step 3: 相關測試 baseline**（背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_evaluation/test_report_builder.py \
  tests/test_core/test_consistency.py tests/test_evaluation/test_parameters_evaluation_yaml.py \
  -q 2>&1 | tail -8 | tee /tmp/phase2_test_baseline.txt
```
Expected: 全綠（Phase 1 收尾狀態），存檔。

---

### Task 2：`theoretical_offsets`（純 Python，TDD）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/reconciliation.py`
- Test: `tests/test_diagnosis/test_metric/test_reconciliation.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""theoretical_offsets：抽樣×權重的正負類曝險比 → per-cell 理論偏移＋item 摘要帶。"""

import math

import pytest

from recsys_tfb.diagnosis.metric.reconciliation import theoretical_offsets


def _params(overrides=None, sample_ratio=1.0, group_keys=None,
            weights=None, weight_keys=None):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "dataset": {
            "sample_ratio": sample_ratio,
            "sample_group_keys": group_keys
                or ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio_overrides": overrides or {},
        },
        "training": {
            "sample_weight_keys": weight_keys or ["prod_name"],
            "sample_weights": weights or {},
        },
    }


def test_neg_only_retention_gives_minus_log_r():
    # 只砍負類保留 0.5 → offset = −ln 0.5 = +0.693（手冊3 Ch10 logQ）
    out = theoretical_offsets(_params(overrides={"mass|fund_bond|0": 0.5}))
    cell = out["cells"]["mass|fund_bond"]
    assert cell["r_neg"] == 0.5 and cell["r_pos"] == 1.0
    assert cell["offset"] == pytest.approx(math.log(2))
    band = out["by_item"]["fund_bond"]
    assert band["min"] == band["max"] == pytest.approx(math.log(2))
    assert band["approx"] is True and band["n_cells"] == 1


def test_base_config_ccard_ins_band():
    # 現行 conf/base 的實際 overrides → 帶 [ln(10/9), ln 2]
    out = theoretical_offsets(_params(overrides={
        "mass|ccard_ins|0": 0.5,
        "affluent|ccard_ins|0": 0.9,
        "hnw|ccard_ins|0": 0.8,
    }))
    band = out["by_item"]["ccard_ins"]
    assert band["min"] == pytest.approx(math.log(1 / 0.9))
    assert band["max"] == pytest.approx(math.log(2))
    assert band["n_cells"] == 3


def test_symmetric_retention_cancels():
    # 正負類同率 → 0（label 對稱不移動 level）
    out = theoretical_offsets(_params(overrides={
        "mass|fund_bond|0": 0.5, "mass|fund_bond|1": 0.5,
    }))
    assert out["cells"]["mass|fund_bond"]["offset"] == pytest.approx(0.0)


def test_default_sample_ratio_fills_missing_class():
    # 全域 sample_ratio=0.8，只 override 負類 0.4 → offset = ln(0.8/0.4)
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond|0": 0.4}, sample_ratio=0.8,
    ))
    assert out["cells"]["mass|fund_bond"]["offset"] == pytest.approx(
        math.log(0.8 / 0.4)
    )


def test_label_not_in_group_keys_gives_no_sampling_offset():
    # label 不在 sample_group_keys → 抽樣對 label 對稱 → 無 cell
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond": 0.5},
        group_keys=["cust_segment_typ", "prod_name"],
    ))
    assert out["cells"] == {} and out["by_item"] == {}
    assert any("label" in n for n in out["notes"])


def test_label_aware_weights_shift():
    # weight_keys 含 label：正類 boost 2.0 → offset = ln 2；與抽樣疊乘
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond|0": 0.5},
        weights={"fund_bond|1": 2.0},
        weight_keys=["prod_name", "label"],
    ))
    # cell key 是 sample_group_keys 的非 label 維（mass|fund_bond）；
    # 權重 cell key 是 weight_keys 的非 label 維（fund_bond）——兩組維度
    # 不同時各自細列，item 摘要帶取聯集
    band = out["by_item"]["fund_bond"]
    assert band["max"] == pytest.approx(math.log(2) + math.log(2))


def test_label_not_in_weight_keys_gives_no_weight_offset():
    # 現行 config：weight_keys=[prod_name] 無 label → 權重貢獻 0
    out = theoretical_offsets(_params(weights={"fund_bond": 3.0}))
    assert out["cells"] == {} and out["by_item"] == {}
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_reconciliation.py -q 2>&1 | tail -5
```
Expected: ModuleNotFoundError。

- [ ] **Step 3: 實作**（`reconciliation.py` 的第一部分；`calibration_gap_by_item`／`reconcile` 是 Task 3，先不寫）

```python
"""對帳層（spec §3 Phase 2）：理論偏移 vs 實測校準差距。

理論偏移通式（per 完整 group-key cell）：
    offset = ln((r_pos * w_pos) / (r_neg * w_neg))
r＝抽樣保留率（dataset.sample_ratio_overrides，缺項用 dataset.sample_ratio）、
w＝訓練權重（training.sample_weights，缺項 1.0）。退化案例：只砍負類保留 r
→ offset = −ln r（手冊3 Ch10 的 logQ 校正）。label 不在對應 keys 裡 →
該側對 label 對稱、貢獻恆 0。

誠實限制（spec 明定）：overrides 是多維 key（如 cust_segment_typ|prod_name|
label），item 層的單一 offset 是聚合近似——本模組按完整 group key 細列、
item 層只給 {min, max, mean} 摘要帶並標 approx，verdict 用帶不用單值。
"""
from __future__ import annotations

import logging
import math

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def _split_label_dim(keys: list[str], label_col: str):
    """回傳 (label 維 index, 非 label 維名稱 list)；label 不在 keys → (None, keys)。"""
    if label_col not in keys:
        return None, list(keys)
    idx = keys.index(label_col)
    return idx, [k for k in keys if k != label_col]


def _pair_by_cell(table: dict, keys: list[str], label_col: str,
                  default: float) -> dict[str, dict[str, float]]:
    """把 {'a|b|0': v} 形式的 config 表整理成
    {非label鍵: {'pos': v1, 'neg': v0}}；缺項補 default。
    label 不在 keys → 空 dict（該側對 label 對稱）。"""
    idx, _ = _split_label_dim(keys, label_col)
    if idx is None:
        return {}
    cells: dict[str, dict[str, float]] = {}
    for key, val in (table or {}).items():
        parts = key.split("|")
        if len(parts) != len(keys):
            continue  # 段數不符（A9b/A5 是它們的守門，這裡靜默略過）
        label_val = parts[idx]
        rest = "|".join(p for i, p in enumerate(parts) if i != idx)
        slot = "pos" if label_val == "1" else "neg"
        cells.setdefault(rest, {})[slot] = float(val)
    for cell in cells.values():
        cell.setdefault("pos", default)
        cell.setdefault("neg", default)
    return cells


def theoretical_offsets(parameters: dict) -> dict:
    """讀採樣／加權 config，回傳 per-cell 理論偏移＋per-item 摘要帶（JSON-ready）。"""
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {}) or {}
    tr = parameters.get("training", {}) or {}
    sample_ratio = float(ds.get("sample_ratio", 1.0))
    group_keys = list(ds.get("sample_group_keys", []) or [])
    weight_keys = list(tr.get("sample_weight_keys", []) or [])

    notes: list[str] = []
    ratio_cells = _pair_by_cell(
        ds.get("sample_ratio_overrides", {}), group_keys, label_col,
        default=sample_ratio,
    )
    if label_col not in group_keys and (ds.get("sample_ratio_overrides") or {}):
        notes.append(
            f"sample_group_keys 不含 {label_col}——抽樣對 label 對稱，"
            f"理論上不移動 level，抽樣側貢獻為 0。"
        )
    weight_cells = _pair_by_cell(
        tr.get("sample_weights", {}), weight_keys, label_col, default=1.0,
    )
    if label_col not in weight_keys and (tr.get("sample_weights") or {}):
        notes.append(
            f"sample_weight_keys 不含 {label_col}——權重對 label 對稱，"
            f"權重側貢獻為 0。"
        )

    def _item_of(cell_key: str, keys: list[str]) -> str | None:
        _, rest_keys = _split_label_dim(keys, label_col)
        if item_col not in rest_keys:
            return None
        return cell_key.split("|")[rest_keys.index(item_col)]

    cells: dict[str, dict] = {}
    for cell_key, rw in ratio_cells.items():
        offset = math.log(rw["pos"] / rw["neg"])
        cells[cell_key] = {
            "source": "sampling",
            "r_pos": rw["pos"], "r_neg": rw["neg"],
            "w_pos": 1.0, "w_neg": 1.0,
            "offset": offset,
            "item": _item_of(cell_key, group_keys),
        }
    for cell_key, ww in weight_cells.items():
        # 權重 cell 的維度可能與抽樣 cell 不同（weight_keys ≠ group_keys）：
        # 維度相同且同名 cell → 疊乘；否則各自細列
        matched = cells.get(cell_key)
        if matched is not None and _split_label_dim(group_keys, label_col)[1] \
                == _split_label_dim(weight_keys, label_col)[1]:
            matched["w_pos"], matched["w_neg"] = ww["pos"], ww["neg"]
            matched["offset"] += math.log(ww["pos"] / ww["neg"])
            matched["source"] = "sampling+weights"
        else:
            cells[cell_key] = {
                "source": "weights",
                "r_pos": 1.0, "r_neg": 1.0,
                "w_pos": ww["pos"], "w_neg": ww["neg"],
                "offset": math.log(ww["pos"] / ww["neg"]),
                "item": _item_of(cell_key, weight_keys),
            }

    by_item: dict[str, dict] = {}
    for cell in cells.values():
        it = cell["item"]
        if it is None:
            continue
        agg = by_item.setdefault(
            it, {"min": math.inf, "max": -math.inf, "_sum": 0.0, "n_cells": 0}
        )
        agg["min"] = min(agg["min"], cell["offset"])
        agg["max"] = max(agg["max"], cell["offset"])
        agg["_sum"] += cell["offset"]
        agg["n_cells"] += 1
    for it, agg in by_item.items():
        agg["mean"] = agg.pop("_sum") / agg["n_cells"]
        agg["approx"] = True  # item 層是跨 cell 聚合近似（見模組 docstring）

    return {"cells": cells, "by_item": by_item, "notes": notes}
```

注意 `test_label_aware_weights_shift` 的期望：抽樣 cell `mass|fund_bond`（offset ln2）與權重 cell `fund_bond`（offset ln2）維度不同 → 各自細列，item 帶取聯集 → `fund_bond` 的 max 應為…**兩個 cell 分別是 ln2 與 ln2，帶 max＝ln2 不是 2·ln2**。此測試期望「疊乘」語意（同 item 的抽樣＋權重效應相加）——**實作與測試若衝突，以測試的疊乘語意為準修實作**：item 帶的正確聚合是「每個抽樣 cell 的 offset ＋ 該 item 權重側 offset」（權重維度不含 segment 時廣播到該 item 的所有抽樣 cell）。把 `by_item` 聚合改為：先把 weights cell 依 item 廣播疊加到同 item 的 sampling cells（無 sampling cell 的 item 才單獨列 weights cell），再聚合。實作時以通過全部 7 個測試為準。

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_reconciliation.py -q 2>&1 | tail -5
```
Expected: 7 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/metric/reconciliation.py tests/test_diagnosis/test_metric/test_reconciliation.py && \
git commit -m "feat(diagnosis): theoretical_offsets——抽樣×權重曝險比的理論偏移（cell 細列＋item 近似帶）

Claude-Session: https://claude.ai/code/session_01WKyoqUUNoPMYGobdMDjNUd"
```

---

### Task 3：`calibration_gap_by_item` ＋ `reconcile`（Spark，TDD）

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/reconciliation.py`
- Test: `tests/test_diagnosis/test_metric/test_reconciliation.py`

- [ ] **Step 1: 寫失敗測試**（同檔追加；spark fixture 沿用 tests/conftest.py）

```python
def _eval_df(spark, rows):
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score",
                "score_uncalibrated", "label"],
    )


def _full_params(**kw):
    p = _params(**{k: v for k, v in kw.items()
                   if k in ("overrides", "sample_ratio", "group_keys",
                            "weights", "weight_keys")})
    p["evaluation"] = {
        "diagnosis": {
            "reconciliation": {
                "enabled": True,
                "score_col": kw.get("score_col", "score_uncalibrated"),
                "explained_threshold": kw.get("threshold", 0.3),
            },
        },
    }
    return p


def test_calibration_gap_known_value(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import calibration_gap_by_item
    # item A：p̄=0.6、ȳ=0.5 → gap = logit(0.6) − logit(0.5) = ln(1.5)
    rows = [
        ("20240331", "C0", "A", 0.6, 0.6, 1),
        ("20240331", "C1", "A", 0.6, 0.6, 0),
    ]
    out = calibration_gap_by_item(_eval_df(spark, rows), _full_params(), "score")
    assert out["A"]["gap"] == pytest.approx(math.log(1.5))
    assert out["A"]["p_mean"] == pytest.approx(0.6)
    assert out["A"]["y_rate"] == pytest.approx(0.5)
    assert out["A"]["n_rows"] == 2


def test_calibration_gap_degenerate_rate_guarded(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import calibration_gap_by_item
    rows = [("20240331", "C0", "A", 0.6, 0.6, 1)]  # ȳ=1 → logit 未定義
    out = calibration_gap_by_item(_eval_df(spark, rows), _full_params(), "score")
    assert out["A"]["gap"] is None and out["A"]["reason"]


def test_reconcile_verdict_and_dual_columns(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # 理論帶 [ln2, ln2]（注入 0.5）；uncalibrated gap = ln(1.5/0.5... ) 構造：
    # A：score_uncalibrated p̄=2/3、ȳ=1/3 → gap = logit(2/3)−logit(1/3) = 2 ln 2
    #    帶 [ln2,ln2]、residual = 2ln2 − ln2 = ln2 ≈ 0.693 > 0.3 → 不可解釋
    # B：無 override → 帶 [0,0]；gap=0 → 可解釋
    rows = [
        ("20240331", "C0", "A", 0.5, 2 / 3, 1),
        ("20240331", "C1", "A", 0.5, 2 / 3, 0),
        ("20240331", "C2", "A", 0.5, 2 / 3, 0),
        ("20240331", "C0", "B", 0.5, 0.5, 1),
        ("20240331", "C1", "B", 0.5, 0.5, 0),
    ]
    params = _full_params(overrides={"mass|A|0": 0.5})
    out = reconcile(_eval_df(spark, rows), params)
    a = out["by_item"]["A"]
    assert a["theory_min"] == a["theory_max"] == pytest.approx(math.log(2))
    assert a["gap"] == pytest.approx(2 * math.log(2))
    assert a["residual"] == pytest.approx(math.log(2))
    assert a["verdict"] == "不可解釋"
    assert "gap_calibrated" in a  # score 欄對照
    b = out["by_item"]["B"]
    assert b["theory_min"] == b["theory_max"] == 0.0
    assert b["verdict"] == "可解釋"
    assert out["all_explained"] is False
    assert out["score_col_used"] == "score_uncalibrated"
    assert out["fallback"] is False


def test_reconcile_gap_inside_band_is_explained(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # gap = ln2 恰在帶 [ln2, ln2] 內 → residual 0 → 可解釋
    rows = [
        ("20240331", "C0", "A", 0.5, 0.5, 1),
        ("20240331", "C1", "A", 0.5, 0.5, 0),
    ]
    # p̄=0.5、ȳ=0.5 → gap=0；帶 [0,0]（無 override）→ 可解釋
    out = reconcile(_eval_df(spark, rows), _full_params())
    assert out["by_item"]["A"]["verdict"] == "可解釋"
    assert out["all_explained"] is True


def test_reconcile_fallback_when_uncalibrated_missing(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    df = spark.createDataFrame(
        [("20240331", "C0", "A", 0.5, 1), ("20240331", "C1", "A", 0.5, 0)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    out = reconcile(df, _full_params())
    assert out["fallback"] is True and out["score_col_used"] == "score"
```

- [ ] **Step 2: 跑測試確認失敗**（ImportError：`calibration_gap_by_item` 不存在）

- [ ] **Step 3: 實作**（`reconciliation.py` 追加；模組頂部補 `from pyspark.sql import DataFrame as SparkDataFrame` 與 `from pyspark.sql import functions as F`）

```python
def _logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def calibration_gap_by_item(
    sdf: "SparkDataFrame", parameters: dict, score_col: str,
) -> dict[str, dict]:
    """per item 的 logit(p̄) − logit(ȳ)（先平均再 logit，spec 釘的公式）。

    Spark 只做 groupBy 聚合（無 UDF）；logit 在 driver 端對 22 個 item 級的
    小 dict 計算。ȳ 或 p̄ ∉ (0,1) → gap=None＋reason（不炸）。
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]

    rows = (
        sdf.groupBy(item_col)
        .agg(
            F.mean(F.col(score_col).cast("double")).alias("p_mean"),
            F.mean(F.col(label_col).cast("double")).alias("y_rate"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    out: dict[str, dict] = {}
    for r in rows:
        p, y = float(r["p_mean"]), float(r["y_rate"])
        entry: dict = {"p_mean": p, "y_rate": y, "n_rows": int(r["n_rows"])}
        if not (0.0 < y < 1.0):
            entry["gap"] = None
            entry["reason"] = f"y_rate={y} 使 logit 未定義（全正或全負）"
        elif not (0.0 < p < 1.0):
            entry["gap"] = None
            entry["reason"] = f"p_mean={p} 不在 (0,1)——score 欄可能不是機率"
        else:
            entry["gap"] = _logit(p) - _logit(y)
        out[str(r[item_col])] = entry
    return out


def reconcile(eval_predictions: "SparkDataFrame", parameters: dict) -> dict:
    """對帳表：理論帶 × 實測 gap → residual → verdict（JSON-ready）。"""
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("reconciliation", {}) or {})
    score_col = str(cfg.get("score_col", "score_uncalibrated"))
    threshold = float(cfg.get("explained_threshold", 0.3))
    schema = get_schema(parameters)
    base_score_col = schema["score"]

    fallback = False
    if score_col not in eval_predictions.columns:
        logger.warning(
            "reconcile: %s 欄不存在（monitoring 路徑無校準前分數）——"
            "退回 %s，理論對帳將包含校準層效應", score_col, base_score_col,
        )
        score_col, fallback = base_score_col, True

    theory = theoretical_offsets(parameters)
    gaps = calibration_gap_by_item(eval_predictions, parameters, score_col)
    gaps_cal = (
        calibration_gap_by_item(eval_predictions, parameters, base_score_col)
        if score_col != base_score_col else None
    )

    by_item: dict[str, dict] = {}
    all_explained = True
    for item, g in sorted(gaps.items()):
        band = theory["by_item"].get(item)
        t_min = band["min"] if band else 0.0
        t_max = band["max"] if band else 0.0
        entry = {
            "theory_min": t_min, "theory_max": t_max,
            "theory_approx": bool(band and band.get("approx")),
            "gap": g["gap"], "p_mean": g["p_mean"], "y_rate": g["y_rate"],
            "n_rows": g["n_rows"],
        }
        if gaps_cal is not None:
            entry["gap_calibrated"] = gaps_cal.get(item, {}).get("gap")
        if g["gap"] is None:
            entry["residual"] = None
            entry["verdict"] = "無法評估"
            entry["reason"] = g.get("reason")
        else:
            clipped = min(max(g["gap"], t_min), t_max)
            entry["residual"] = g["gap"] - clipped
            entry["verdict"] = (
                "可解釋" if abs(entry["residual"]) <= threshold else "不可解釋"
            )
            if entry["verdict"] != "可解釋":
                all_explained = False
        by_item[item] = entry

    return {
        "enabled": True,
        "score_col_used": score_col,
        "fallback": fallback,
        "explained_threshold": threshold,
        "theory": theory,
        "by_item": by_item,
        "all_explained": all_explained,
    }
```

（注意：`無法評估` 不算破壞 `all_explained`——它是資料退化訊號不是校準訊號；報表會顯示 reason。）

- [ ] **Step 4: 跑測試確認通過**（本檔全部，7＋5＝12 passed）

- [ ] **Step 5: Commit**（訊息：`feat(diagnosis): calibration_gap_by_item＋reconcile——對帳表與帶狀 verdict`，附 Claude-Session 行）

---

### Task 4：config ＋ consistency A16

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`、`src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 失敗測試**——`tests/test_core/test_consistency.py` 追加（照 A15 的 `TestDiagnosisMetricParamsA15` 式樣）：

```python
class TestReconciliationParamsA16:
    def _params(self, recon):
        return {"evaluation": {"diagnosis": {"reconciliation": recon}}}

    def test_absent_and_valid_defaults_clean(self):
        from recsys_tfb.core.consistency import reconciliation_param_errors
        assert reconciliation_param_errors({}) == []
        assert reconciliation_param_errors(self._params(
            {"enabled": True, "score_col": "score_uncalibrated",
             "explained_threshold": 0.3}
        )) == []

    def test_bad_values_report(self):
        from recsys_tfb.core.consistency import reconciliation_param_errors
        errors = reconciliation_param_errors(self._params(
            {"score_col": "rank", "explained_threshold": 0}
        ))
        assert len(errors) == 2
        joined = "\n".join(errors)
        assert "score_col" in joined and "explained_threshold" in joined

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="score_col"):
            validate_config_consistency(self._params({"score_col": "rank"}))
```

`tests/test_evaluation/test_parameters_evaluation_yaml.py` 追加：

```python
def test_reconciliation_block():
    recon = _load()["diagnosis"]["reconciliation"]
    assert recon == {
        "enabled": True,
        "score_col": "score_uncalibrated",
        "explained_threshold": 0.3,
    }
```

- [ ] **Step 2: RED 確認**（ImportError／KeyError）

- [ ] **Step 3: 實作**——yaml 在 `diagnosis:` 區塊的 `ci:` 之後加：

```yaml
    # 對帳層（Phase 2；A16）：理論偏移（抽樣×權重的正負類曝險比，per-cell
    # 細列、item 層近似帶）vs 實測 per-item 校準差距 logit(p̄)−logit(ȳ)。
    # score_col 預設 score_uncalibrated——校準層本身就在修 level，對帳要看
    # 模型原始輸出；eval 資料無此欄（monitoring 路徑）自動退回 score 並標註。
    # explained_threshold 單位是 log-odds：實測 gap 距理論帶超過此值＝不可解釋。
    reconciliation:
      enabled: true
      score_col: score_uncalibrated
      explained_threshold: 0.3
```

`consistency.py`：`diagnosis_metric_param_errors` 之後加 predicate；`validate_config_consistency` 串接；legend 補 A16：

```python
def reconciliation_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.reconciliation parameter domains (A16)."""
    errors: list[str] = []
    recon = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("reconciliation", {}) or {}
    )
    sc = recon.get("score_col", "score_uncalibrated")
    if sc not in ("score", "score_uncalibrated"):
        errors.append(
            f"evaluation.diagnosis.reconciliation.score_col={sc!r} must be "
            f"'score' or 'score_uncalibrated'."
        )
    thr = recon.get("explained_threshold", 0.3)
    if not (_is_number(thr) and float(thr) > 0.0):
        errors.append(
            f"evaluation.diagnosis.reconciliation.explained_threshold={thr!r} "
            f"must be a number > 0 (log-odds units)."
        )
    return errors
```

legend（A15 之後）：

```
* A16 — ``evaluation.diagnosis.reconciliation`` parameter domains:
  ``score_col`` ∈ {score, score_uncalibrated}; ``explained_threshold`` > 0
  (log-odds). Predicate: ``reconciliation_param_errors``.
```

- [ ] **Step 4: 全綠確認＋yaml 煙霧**（`reconciliation_param_errors` 對實際 yaml 印 `[]`）

- [ ] **Step 5: Commit**（`feat(config): evaluation.diagnosis.reconciliation＋consistency A16`）

---

### Task 5：pipeline 節點 ＋ catalog

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`、`src/recsys_tfb/pipelines/evaluation/pipeline.py`、`conf/base/catalog.yaml`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`、`tests/test_pipelines/test_evaluation/test_pipeline.py`

- [ ] **Step 1: 失敗測試**——`test_nodes_spark.py` 追加：

```python
def test_compute_reconciliation_disabled_returns_stub(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_reconciliation
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"reconciliation": {"enabled": False}}},
    }
    assert compute_reconciliation(None, params) == {"enabled": False}


def test_compute_reconciliation_end_to_end_small(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_reconciliation
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.5, 0.5, 1),
            ("20240331", "C1", "A", 0.5, 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score",
                "score_uncalibrated", "label"],
    )
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "dataset": {"sample_ratio": 1.0,
                    "sample_group_keys": ["prod_name", "label"],
                    "sample_ratio_overrides": {}},
        "training": {"sample_weight_keys": ["prod_name"],
                     "sample_weights": {}},
        "evaluation": {"diagnosis": {"reconciliation": {
            "enabled": True, "score_col": "score_uncalibrated",
            "explained_threshold": 0.3}}},
    }
    out = compute_reconciliation(df, params)
    assert out["enabled"] is True
    assert out["by_item"]["A"]["verdict"] == "可解釋"
    assert out["all_explained"] is True
```

`test_pipeline.py`：結構斷言 6→7（default/post_training）、9→10（compare-source）、node 名加 `compute_reconciliation`（`generate_report` 之前）、outputs 加 `evaluation_reconciliation`。**預先授權**（設計定案節）。先 Read 現檔式樣再改。

- [ ] **Step 2: RED 確認**

- [ ] **Step 3: 實作**——`nodes_spark.py` 檔尾（照 `compute_metric_ci` 式樣）：

```python
def compute_reconciliation(
    eval_predictions: Optional[SparkDataFrame],
    parameters: dict,
) -> dict:
    """對帳層薄 node（spec §3 Phase 2）：理論偏移 vs 實測校準差距。

    領域邏輯全在 ``diagnosis.metric.reconciliation``。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("reconciliation", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("reconciliation disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None:
        raise ValueError(
            "compute_reconciliation: eval_predictions is required when "
            "evaluation.diagnosis.reconciliation.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    out = reconcile(eval_predictions, parameters)
    logger.info(
        "reconciliation computed: %d items, all_explained=%s (score_col=%s)",
        len(out["by_item"]), out["all_explained"], out["score_col_used"],
    )
    return out
```

`pipeline.py`：`compute_metric_ci` 節點之後插入：

```python
        Node(
            compute_reconciliation,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_reconciliation",
        ),
```

`generate_report` 的 inputs 追加 `"evaluation_reconciliation"`（見 Task 6 簽名）。

`catalog.yaml`：`evaluation_metric_ci` 之後：

```yaml
evaluation_reconciliation:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/reconciliation.json
```

`generate_report` 簽名加尾參 `reconciliation: Optional[dict] = None,`（本 task 只收不 render）。

- [ ] **Step 4: 全綠確認**（`tests/test_pipelines/test_evaluation/`）

- [ ] **Step 5: Commit**（`feat(evaluation): compute_reconciliation 節點＋reconciliation.json catalog 產物`）

---

### Task 6：報表 reconciliation section

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`、`src/recsys_tfb/pipelines/evaluation/nodes_spark.py`、`conf/base/parameters_evaluation.yaml`
- Test: `tests/test_evaluation/test_report_builder.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 失敗測試**——`test_report_builder.py` 追加：

```python
_RECON_FIXTURE = {
    "enabled": True, "score_col_used": "score_uncalibrated",
    "fallback": False, "explained_threshold": 0.3,
    "theory": {"cells": {}, "by_item": {}, "notes": []},
    "by_item": {
        "A": {"theory_min": 0.693, "theory_max": 0.693, "theory_approx": True,
              "gap": 0.75, "gap_calibrated": 0.02, "residual": 0.057,
              "verdict": "可解釋", "p_mean": 0.4, "y_rate": 0.25, "n_rows": 100},
        "B": {"theory_min": 0.0, "theory_max": 0.0, "theory_approx": False,
              "gap": 0.9, "gap_calibrated": 0.8, "residual": 0.9,
              "verdict": "不可解釋", "p_mean": 0.5, "y_rate": 0.3, "n_rows": 80},
    },
    "all_explained": False,
}


def test_reconciliation_section_renders_table_and_verdict():
    from recsys_tfb.evaluation.report_builder import build_reconciliation_section
    sec = build_reconciliation_section(_RECON_FIXTURE, _params_min())
    tbl = sec.tables[0]
    assert list(tbl.index) == ["A", "B"]
    assert tbl.loc["B", "verdict"] == "不可解釋"
    assert "理論" in sec.description and "近似" in sec.description
    assert "score_uncalibrated" in sec.description


def test_reconciliation_section_none_when_disabled_or_absent():
    from recsys_tfb.evaluation.report_builder import build_reconciliation_section
    assert build_reconciliation_section(None, _params_min()) is None
    assert build_reconciliation_section({"enabled": False}, _params_min()) is None
    params_off = {"evaluation": {"report": {"sections": {"reconciliation": False}}}}
    assert build_reconciliation_section(_RECON_FIXTURE, params_off) is None


def test_reconciliation_fallback_marked():
    from recsys_tfb.evaluation.report_builder import build_reconciliation_section
    fx = dict(_RECON_FIXTURE, fallback=True, score_col_used="score")
    sec = build_reconciliation_section(fx, _params_min())
    assert "退回" in sec.description


def test_assemble_report_renders_reconciliation():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), reconciliation=_RECON_FIXTURE
    )
    assert "對帳" in html
```

`test_parameters_evaluation_yaml.py` 的 `test_reconciliation_block` 已在 Task 4 加；此處追加 sections 斷言（新測試，不改既有）：

```python
def test_report_sections_include_reconciliation():
    assert _load()["report"]["sections"]["reconciliation"] is True
```

- [ ] **Step 2: RED 確認**

- [ ] **Step 3: 實作**：

(a) `parameters_evaluation.yaml` 的 `report.sections` 加 `reconciliation: true`（`diagnostics: true` 之後）。

(b) `report_builder.py` 新 builder（放在 `build_per_item_attr_section` 之後）：

```python
def build_reconciliation_section(
    reconciliation: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "reconciliation"):
        return None
    if not reconciliation or not reconciliation.get("enabled"):
        return None
    by_item = reconciliation.get("by_item", {}) or {}
    cols = ["theory_min", "theory_max", "gap", "gap_calibrated",
            "residual", "verdict", "p_mean", "y_rate", "n_rows"]
    tbl = pd.DataFrame(
        {c: [by_item[it].get(c) for it in by_item] for c in cols},
        index=list(by_item),
    )
    score_col = reconciliation.get("score_col_used")
    desc = (
        "對帳層：理論偏移（由 dataset.sample_ratio_overrides 與 "
        "training.sample_weights 推得的正負類曝險比，單位 log-odds）"
        "vs 實測校準差距 gap = logit(平均預測機率) − logit(實際正類率)，"
        f"主判欄用 {score_col}。theory_min/max 是 item 層的近似帶"
        "（多維 override 跨 segment 聚合，cell 細目見 reconciliation.json）；"
        "residual = gap 距帶的距離，|residual| ≤ "
        f"{reconciliation.get('explained_threshold')} → 可解釋。"
        "gap_calibrated 為校準後分數的同式 gap——校準層有效時應接近 0。"
    )
    if reconciliation.get("fallback"):
        desc += (
            "⚠ 本次執行找不到 score_uncalibrated 欄（monitoring 路徑），"
            "已退回 score——gap 內含校準層效應，判讀時注意。"
        )
    notes = (reconciliation.get("theory", {}) or {}).get("notes") or []
    if notes:
        desc += "／".join(notes)
    return ReportSection(
        title="對帳 Reconciliation（理論偏移 vs 實測校準差距）",
        description=desc,
        tables=[tbl],
        table_titles=["per-item 對帳表"],
    )
```

(c) `assemble_report` 簽名加 `reconciliation: dict | None = None`，candidates 在 `build_per_item_attr_section(...)` 之後插入 `build_reconciliation_section(reconciliation, parameters),`。

(d) `nodes_spark.py::generate_report`：簽名的 `reconciliation` 參數（Task 5 已加）傳給 `assemble_report(..., reconciliation=reconciliation)`。

- [ ] **Step 4: 全綠確認**（`test_report_builder.py`＋`tests/test_pipelines/test_evaluation/`＋yaml 測試）

- [ ] **Step 5: Commit**（`feat(report): reconciliation section——理論帶×實測 gap×verdict 對帳表`）

---

### Task 7：收尾閘門（真跑）＋審查＋使用者閘門

**Files:** 無新程式碼；閘門期間暫改 `conf/base/parameters_dataset.yaml`（結束時還原）。

執行順序嚴格照下（設計定案：報表逐字回歸必須在重訓之前做）：

- [ ] **Step 1: 測試 vs baseline**（背景；Task 1 同組檔案＋新增測試檔）；fail 集合須與 `/tmp/phase2_test_baseline.txt` 一致。

- [ ] **Step 2: 現有模型（6059dcef）evaluation-only 真跑**（背景；不重訓）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version 6059dcef
```

檢視三件事：
1. `data/evaluation/6059dcef/20260131/diagnosis/reconciliation.json` 產出；`ccard_ins` 理論帶 ≈ [0.105, 0.693]、其他 item 帶 [0, 0]。
2. **既有 section 逐字回歸**：對 `/tmp/phase2_report_before.html` 跑 Phase 1 同款 Macro-rows／headline 比對 snippet——必須逐字相同（reconciliation 是新 section、不動既有值）。
3. **現狀判讀（觀察，不硬 assert）**：`ccard_ins` 的 `gap`（uncalibrated）落在帶內或帶附近 → verdict 可解釋；`gap_calibrated` 明顯較小（校準層在修 level 的證據）。若 `ccard_ins` verdict＝不可解釋 → **停下分析原因再繼續**（模型對 base-rate 偏移的吸收程度是統計預期不是定理；此時檢查 explained_threshold 合理性、HPO/特徵是否部分吸收 offset，把發現寫進閘門報告——這正是對帳層存在的目的，不是掩蓋它）。

- [ ] **Step 3: 已知答案注入**——`conf/base/parameters_dataset.yaml` 的 `sample_ratio_overrides` 加三行（**不動既有 ccard_ins 三行**）：

```yaml
    "mass|fund_bond|0": 0.5
    "affluent|fund_bond|0": 0.5
    "hnw|fund_bond|0": 0.5
```

pre-flight grep 確認讀到新值（`grep -n "fund_bond" conf/base/parameters_dataset.yaml`），然後 **dataset→training→evaluation 全鏈重跑**（背景；training 含 HPO 需時較久）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local && \
MV=$(ls -t data/models | grep -v '^_hpo$' | grep -v '^best$' | grep -v '^e2e' | grep -v '^mv' | head -1) && echo "NEW_MV=$MV" && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version $MV
```

（新 model_version 由 training log／`data/models` 最新目錄取得；注入改了 base_dataset_version，MV 必與 6059dcef 不同——若相同＝改錯邊，回頭跑 pre-flight。）

檢視（spec 驗收 2）：新 MV 的 reconciliation.json——`fund_bond` 理論帶 [0.693, 0.693]、`gap`（uncalibrated）同號且 |residual| ≤ 0.3 → verdict 可解釋；`gap_calibrated` ≈ 0；未注入且無 override 的 item 帶 [0,0]、verdict 可解釋。

- [ ] **Step 4: 還原注入、重跑、確認全綠**（spec 驗收 3）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git checkout conf/base/parameters_dataset.yaml && grep -c "fund_bond" conf/base/parameters_dataset.yaml || echo "已還原（零命中）"
```

再跑一輪 dataset→training→evaluation（背景；config 回原狀 → model_version 回 6059dcef，重訓覆蓋其 artifacts——**此後 `/tmp/phase2_report_before.html` 逐字比對失效，屬預期**）。檢視 reconciliation.json：全部 item verdict 可解釋（`all_explained: true`）、`fund_bond` 帶回 [0,0]。

- [ ] **Step 5: graphify 確認最新**（commit hooks 已隨每次 commit rebuild；此步只 `git status --short` 確認乾淨）。

- [ ] **Step 6: 合併 reviewer（sonnet，Task 2–6）＋ opus 總審**（總審給 spec §3 Phase 2＋`git diff <Phase2 起點>..HEAD`＋閘門證據；要求至少 3 個具體問題或列檢查面向；verdict READY-FOR-USER-GATE 判準）。

- [ ] **Step 7: 使用者閘門回報**：附 reconciliation.json（三個狀態：現狀／注入／還原）、report.html 對帳 section、測試 vs baseline、審查結論、「沒做的事」清單。**等使用者檢視通過才進 Phase 3。**

---

## Self-review（計畫作者已核）

- Spec §3 Phase 2 覆蓋：`theoretical_offsets`（Task 2，含多維 key 誠實限制→cell 細列＋item 近似帶）、`calibration_gap_by_item`（Task 3，spec 公式＋score_col 雙欄＋monitoring 降級）、對帳表 verdict（Task 3）、report section＋reconciliation.json（Task 5/6）、config＋A16（Task 4）、驗收 1–3（Task 7 Step 3/4 的注入-還原全鏈重跑，含 spec 特別警告的「全 segment 一起注入」）。
- 超出 spec 字面的設計（記錄）：verdict 用理論「帶」而非單值（多維 key 聚合近似的誠實處理）；權重通式 `ln((r₁w₁)/(r₀w₀))`（spec 只說「同向」，通式退化到 spec 案例）；`無法評估` 第三種 verdict（logit 未定義的資料退化守門）。
- 佔位符檢查：Task 2 Step 3 對 `test_label_aware_weights_shift` 的實作-測試衝突處理已寫明裁決規則（以疊乘語意為準）；Task 7 Step 3 的 MV 擷取指令附防呆。其餘步驟含完整程式碼與預期輸出。
- 識別字一致性：`compute_reconciliation`／`evaluation_reconciliation`／`build_reconciliation_section`／`reconciliation_param_errors`／JSON 鍵（`theory_min/max`、`gap`、`gap_calibrated`、`residual`、`verdict`、`all_explained`、`fallback`、`score_col_used`）在 Task 3/5/6 fixture 間逐字一致。
