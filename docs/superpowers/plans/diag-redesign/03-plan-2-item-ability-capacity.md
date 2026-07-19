# Plan 2：item_ability 與 model_capacity（診斷重構 3/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作第二、三項診斷：模型能不能在 query 內分辨 item（含 raw vs query-centered AUC 對照），以及模型的 gain/split 花在 item 身分還是 context 特徵上。

**Architecture:** 兩項合成一份計畫，因為 `model_capacity` 吃 `item_ability` 的輸出畫 capacity vs ability 散點。`item_ability` 含 sort-once bootstrap 最佳化；`model_capacity` 只讀 `gain_ledger.json`，不碰評測資料。同時刪掉 `discrimination.py`（同一統計量的 Spark 版，用的是校準後分數）。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 1 已完成並 merge（契約與樣板已定案、且你已對樣板形狀給過回饋）。

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

`contract.py` 的 `DIAGNOSES` 加 `"item_ability"`（接在 `"config_shift"` 之後——順序即閱讀順序），`test_contract.py` 的 `EXPECTED_ORDER` 同步加。

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

`contract.py` 的 `DIAGNOSES` 加 `"model_capacity"`（第三順位），`test_contract.py` 的 `EXPECTED_ORDER` 同步加。node input 是 `["gain_ledger", "evaluation_item_ability", "parameters"]`——注意 `gain_ledger` 是跨 pipeline 的 optional 產物。

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


---

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境 evaluation，拷回 `diagnosis/` 目錄，看：

1. **`02-item-ability.html` 的 raw vs centered AUC 散點**——偏離對角線的量是否符合你對「客戶活躍度混入」的預期？
2. **`03-model-capacity.html` 的 gain 三分**——item prior 佔多少？未分配那塊多大？
3. **capacity vs ability 散點**——兩個診斷的數字並排看，有沒有讀出東西？沒有的話這張圖可能該換。
4. **`gain_ledger` 有沒有缺席**（catalog `optional: true`）。缺席時頁面應顯示原因而非空白。

**看完給回饋之後**：這兩項的 `blind_to` 寫得對不對特別重要——within-item AUC 不是指標原生的量，若說明不夠清楚，讀者會拿它當 mAP 的分解來用。
