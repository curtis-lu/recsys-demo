# Plan 3：suppression 壓制帳本與交叉購買（診斷重構 4/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作第四項診斷：哪些 label=0 的 item 常排在 label=1 之前、造成多少 AP 缺口，以及這些組合在資料上本來就多常一起買。

**Architecture:** 移植壓制帳本並把最內層的純 Python 逐 pair 迴圈向量化；併入舊 `cross_purchase.py` 的能力，但改成泡泡格圖（顏色＝lift、大小＝共買客戶數），與壓制矩陣同軸序以便並排對照。同時刪掉 `pair_ledger.py` 與 `cross_purchase.py`。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 2 已完成並 merge。

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

`contract.py` 的 `DIAGNOSES` 加 `"suppression"`（第四順位），`test_contract.py` 的 `EXPECTED_ORDER` 同步加。

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


---

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境 evaluation，拷回 `diagnosis/` 目錄，看：

1. **兩張矩陣並排讀不讀得出東西**——「模型讓 k 壓制 j」與「買 j 的人也常買 k」對照起來，是不是真的能區分「模型排錯」與「商品本來就競爭」？這是本項設計的全部價值所在，讀不出來就要改。
2. **泡泡大小的辨識度**——公司規模下 item 數與共買數的量級，泡泡會不會擠成一團或差距大到看不見小的？
3. **具體案例表**是否足夠具體到能讓你去查那一筆。
4. **執行時間**——這一項在腳本原版是最慢的，向量化後的實際秒數要記下來。

**看完給回饋之後**：若泡泡格圖在公司規模下不好讀，備案是兩張對齊的矩陣（一張 lift、一張共買數）——精確但要來回看。這個取捨要你看過真實資料才決定得了。
