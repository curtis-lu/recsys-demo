# Phase 3：行為層象限（水準 × 條件判別力 ＋ 傷害觀測）— 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 補齊 2×2 象限報表需要的另一軸（within-item ROC-AUC＝條件判別力）與傷害觀測（top-slot 佔據、壓制次數、交叉購買矩陣），與 Phase 2 的 gap_vs_global（水準軸）合成 per-item 象限判定，落 `diagnosis/quadrant_summary.json`、報表新 section（含散布圖）與判讀手冊擴充。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 3（框架診斷項目 3、5、10）。

**Architecture:** 四個新診斷模組——`diagnosis/metric/discrimination.py`（midrank rank-sum AUC，Spark 無 UDF）、`occupancy_spark.py`（top_slot_share＋suppression_counts）、`cross_purchase.py`（label_table 自 join）、`quadrant.py`（組裝＋象限標籤）。評估 pipeline 加薄節點 `compute_quadrant`（吃 eval_predictions＋label_table＋兩個上游診斷 JSON），報表加 `build_quadrant_section`（plotly 散布圖＋兩張表）。config `evaluation.diagnosis.quadrant`＋consistency **A17**。

**Tech Stack:** PySpark 3.3.2（Window 內建函式、無 UDF）、pandas、plotly `go.Figure`（沿 `distributions.py` 既有慣例）、pytest、本機 local Spark。

**Scope note:** 閘門**只跑 evaluation**（同一 model_version 6059dcef，不重訓）；已知答案＝(a) 單元測試的全平手 fixture 與 numpy parity、(b) 方向性：最冷合成 item `fund_mix` 落判別力差半邊、(c) 門檻注入：暫調 `gap_band` 讓 `ccard_ins` 翻成「偏高」（不動 dataset config、零重訓）。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd <該路徑> && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。
3. **可能超過 2 分鐘的指令（evaluation 真跑）一律背景執行**。evaluation CLI 必帶 `--model-version 6059dcef`（無 `best` symlink；promote 是使用者保留的人工步驟）。
4. **生產不變量**：no Spark UDF、no new packages；`diagnosis/*` 只 import `core / evaluation(僅 numpy 原語 metrics.py) / io / utils`＋pandas/pyspark/numpy/plotly 不進 diagnosis（圖在報表側建）／標準庫。
5. **本階段不動 dataset/training config**：閘門注入只動 `evaluation.diagnosis.quadrant` 門檻值（不影響 model_version），結束時還原。
6. 測試判準＝與 baseline 一致（known-pitfalls §5）。
7. 欄名一律經 `get_schema(parameters)` 取（`item`/`label`/`score`/`rank`/`time`/`entity`），勿硬編 `prod_name`/`snap_date`。

## 設計定案（所有 task 共用語意，不要各自發明）

- **兩軸**：縱軸＝水準＝`gap_vs_global`（**直接取 Phase 2 reconciliation 的 by_item 產物，不重算**）；橫軸＝條件判別力＝within-item ROC-AUC。
  - **水準軸用 gap_vs_global、不用 residual**：象限是**行為觀測**（這個 item 實際上有沒有被抬高/壓低、有沒有壓到別人），residual 是**歸因**（抬高可否由配置解釋）。一個理論帶內的 item（如 `ccard_ins`，gap_vs_global ≈ +0.33、residual＝0）行為上仍然偏高、仍會壓別人——歸因歸對帳層管，象限只看行為。
- **within-item AUC（midrank rank-sum，spec 釘死）**：分數升冪 rank，`midrank = F.rank() + (同分列數 − 1)/2`（同分列數＝`F.count(1).over(Window.partitionBy(item, score))`），`AUC = (R⁺ − n⁺(n⁺+1)/2) / (n⁺·n⁻)`，R⁺＝正例 midrank 和。全部 window 內建函式、無 UDF。
  - **全平手（常數分數）item 必須恰得 0.5**——這不是邊角：框架的核心診斷對象正是近常數分數的冷門 item。單元測試必含此 fixture；numpy parity（sklearn-free 手算 midrank）必測。
  - 分數欄用 `schema["score"]`（實際決定上線排序的分數）。AUC 對嚴格單調變換不變，但校準的平坦段會併平手——這正是要量的行為，不是要避開的雜訊。
  - 單類 item（無正例或無負例）→ `auc=None`＋reason（不炸）。
- **傷害觀測**：`top_slot_share`＝per item 佔據 top-k 的 query 比例（identity＝time×entity×item，每 item 每 query 至多一列，故＝rank≤k 列數／總 query 數），並列 `y_rate` 對照；`suppression_counts`＝per item「以**負例**身分排在該 query **首位正例**上方」的次數（`min(when(label=1, rank))` window，spec 釘的實作；無正例 query 條件為 null 自然不貢獻）。`rank` 欄由 `prepare_eval_data` 保證存在（`nodes_spark.py:147-161`）。
- **交叉購買**：`cross_purchase_matrix(label_rows, parameters)`——**label_table 含 label=0 列**（欄位 `snap_date, cust_id, prod_name, label`，合成產生器 `generate_synthetic_data.py:413` 與正式 schema 皆然），**先濾 label=1** 再 (time, entity) 自 join（同 snap_date 內共現、跨 snap_date 加總）。回傳 `(P(買k|買j) 矩陣 pd.DataFrame, n_buyers pd.Series)`——比 spec 簽名多回買家數：P(k|j) 由 10 個買家估與 10000 個估的可信度不同，矩陣必須帶基數才可讀（記錄：spec 只寫回矩陣）。
- **象限標籤（框架手冊 Ch2 的表逐字，不要自創）**：
  | (level, disc) | quadrant |
  |---|---|
  | (正常, 好) | 健康 |
  | (正常, 差) | 冷門受害者（水準對、判別力差） |
  | (偏高, 好) | 加害者（水準偏高、判別力好） |
  | (偏高, 差) | 加害者（常數高分型） |
  | (偏低, 好) | 受害者（水準偏低、判別力好） |
  | (偏低, 差) | 雙重受害 |

  level＝`偏高 if gvg > gap_band / 偏低 if gvg < −gap_band / 正常`；disc＝`好 if auc ≥ auc_threshold / 差`；任一軸缺值 → 該軸「無法評估」→ quadrant「無法評估」。**`is_aggressor = (level == "偏高")`——加害者判準只看水準偏高、與判別力無關**（手冊定案）。
- **best-effort 降級（沿 cases_manifest／triage 慣例）**：上游 `evaluation_reconciliation`／`evaluation_metric_ci` 是停用 stub（`{"enabled": False}`）或 None 時，對應欄位 None、水準軸「無法評估」、`notes` 註記，**不失敗**；AUC／佔據／壓制／交叉購買照算（它們只依賴 eval_predictions 與 label_table）。
- **散布圖走 plotly 不走 matplotlib（spec 已修訂，證據 `distributions.py:9`）**：repo 報表圖的既有慣例是 plotly `go.Figure` 內嵌 report HTML（`report.py:156` `to_html`），**沒有** matplotlib/PNG/base64 路徑。散布圖在**報表側**（`report_builder.py`，它已 `import plotly.graph_objects as go`）從 quadrant JSON 建，樣式鏡射手冊 fig2：橫軸 AUC、縱軸 gap_vs_global、淺藍 hrect＝水準帶、虛線 vline＝auc_threshold、點標 item 名。diagnosis 模組不 import plotly（產物是純 JSON）。
- **回傳型別沿 Phase 2 慣例＝JSON-ready dict**（spec 簽名寫 `-> DataFrame`，Phase 2 的 `calibration_gap_by_item` 已建立 dict 先例；只有 `cross_purchase_matrix` 因矩陣語意回 pandas，由 `quadrant.py` 轉 dict 落 JSON）。
- **既有測試會被本計畫「合法」改到的只有一處**：`tests/test_pipelines/test_evaluation/test_pipeline.py` 結構斷言——default/post_training 七個 node → **八個**、compare-source 十個 → **十一個**、node 名清單加 `compute_quadrant`（`compute_reconciliation` 之後、`generate_report` 之前）、outputs 加 `evaluation_quadrant`（Phase 1/2 同款，**預先授權**）。其他既有測試一行不得改。
- **閘門的已知答案基準數字（2026-07-07 真跑 6059dcef 的 reconciliation.json）**：`ccard_ins` gap_vs_global ≈ **+0.329**；7 個 config 中性 item 的 gap_vs_global 落 **−0.186 ～ +0.071**。故 `gap_band` 注入值選 **0.25**：`ccard_ins` 翻「偏高」、中性 item 全部仍「正常」，一翻一不翻就是門檻管線的端到端證據。
- **文件是一等交付物（spec §3 固定結構）**：Task 10 內建，契約見該 task；寫法鐵則（禁用開發詞彙、真跑示例表印進文件、數感節、讀者 agent 驗洩漏）不可省。

## 執行模式（controller 注意）

同 Phase 1/2：機械步驟（Task 1、Task 9 真跑）controller 直跑；Task 2–8 派 sonnet implementer（prompt 附對應 task 全文＋執行者必讀＋設計定案）；合併 reviewer 在 Task 8 後一次審 Task 2–8；Task 10 文件由 sonnet 依契約起草＋fresh 讀者 agent 通讀＋controller 對鐵則 checklist 終審；opus 總審在 Task 11。

---

### Task 1：pre-flight ＋ baseline

**Files:** 無程式碼變更；產出 `/tmp/phase3_test_baseline.txt`、`/tmp/phase3_report_before.html`。

- [ ] **Step 1: pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd && readlink .venv && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: worktree root、Python 3.10.9、isolation OK、working tree 乾淨。

- [ ] **Step 2: 報表快照（Phase 2 終版產物即改動前基準）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
cp data/evaluation/6059dcef/20260131/report.html /tmp/phase3_report_before.html && ls -la /tmp/phase3_report_before.html
```

- [ ] **Step 3: 相關測試 baseline**（背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_evaluation/test_report_builder.py \
  tests/test_core/test_consistency.py tests/test_evaluation/test_parameters_evaluation_yaml.py \
  -q 2>&1 | tail -8 | tee /tmp/phase3_test_baseline.txt
```
Expected: 全綠（Phase 2 收尾狀態），存檔。

---

### Task 2：`discrimination.py` — within-item AUC（Spark，TDD）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/discrimination.py`
- Test: `tests/test_diagnosis/test_metric/test_discrimination.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""within_item_auc：per-item ROC-AUC（midrank rank-sum，無 UDF）。

關鍵 fixture＝全平手 item 恰得 0.5——框架的核心診斷對象正是近常數分數的
冷門 item；min-rank 直接代入 rank-sum 公式會在這裡系統性偏差。
"""

import numpy as np
import pytest

from recsys_tfb.diagnosis.metric.discrimination import within_item_auc


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
    }


def _df(spark, rows):
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def _numpy_midrank_auc(scores, labels):
    """sklearn-free 手算參考實作（midrank）。"""
    scores = np.asarray(scores, dtype=float)
    labels = np.asarray(labels)
    order = np.argsort(scores, kind="mergesort")
    s_sorted = scores[order]
    ranks = np.empty(len(scores), dtype=float)
    i = 0
    while i < len(s_sorted):
        j = i
        while j + 1 < len(s_sorted) and s_sorted[j + 1] == s_sorted[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    n_pos = int(labels.sum())
    n_neg = len(labels) - n_pos
    r_pos = ranks[labels == 1].sum()
    return (r_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def test_perfect_separation_gives_one(spark):
    rows = [
        ("20240331", "C0", "A", 0.1, 0), ("20240331", "C1", "A", 0.2, 0),
        ("20240331", "C2", "A", 0.8, 1), ("20240331", "C3", "A", 0.9, 1),
    ]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(1.0)
    assert out["A"]["n_pos"] == 2 and out["A"]["n_neg"] == 2


def test_all_ties_constant_score_gives_exactly_half(spark):
    # 常數分數＝條件判別力為零的極端；rank-sum＋midrank 下 AUC 恰為 0.5。
    rows = [("20240331", f"C{i}", "A", 0.7, int(i < 3)) for i in range(10)]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(0.5)


def test_partial_ties_match_midrank_semantics(spark):
    # scores [1,1,2,2]、labels [0,1,0,1]：midrank 1.5,1.5,3.5,3.5
    # R⁺ = 1.5+3.5 = 5 → AUC = (5 − 2·3/2) / (2·2) = 0.5
    rows = [
        ("20240331", "C0", "A", 1.0, 0), ("20240331", "C1", "A", 1.0, 1),
        ("20240331", "C2", "A", 2.0, 0), ("20240331", "C3", "A", 2.0, 1),
    ]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(0.5)


def test_single_class_item_is_none_with_reason(spark):
    rows = [("20240331", "C0", "A", 0.5, 1), ("20240331", "C1", "A", 0.6, 1)]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] is None and out["A"]["reason"]


def test_level_shift_immunity_across_items(spark):
    # 兩個 item 內部排序型態相同、只差整體常數 +5 → AUC 相同。
    # （within-item AUC 從不跨 item 比較，per-item 常數偏移整個被消掉。）
    rows = []
    for i, (s, y) in enumerate([(0.1, 0), (0.4, 1), (0.2, 0), (0.6, 1)]):
        rows.append(("20240331", f"C{i}", "A", s, y))
        rows.append(("20240331", f"C{i}", "B", s + 5.0, y))
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(out["B"]["auc"])


def test_numpy_parity_on_random_data(spark):
    rng = np.random.default_rng(42)
    rows = []
    for item in ("A", "B"):
        n = 60
        scores = np.round(rng.random(n), 1)  # 一位小數 → 大量平手
        labels = (rng.random(n) < 0.3).astype(int)
        if labels.sum() == 0:
            labels[0] = 1
        if labels.sum() == n:
            labels[0] = 0
        for i in range(n):
            rows.append(("20240331", f"C{item}{i}", item,
                         float(scores[i]), int(labels[i])))
    out = within_item_auc(_df(spark, rows), _params())
    for item in ("A", "B"):
        sub = [(r[3], r[4]) for r in rows if r[2] == item]
        expected = _numpy_midrank_auc([s for s, _ in sub], [y for _, y in sub])
        assert out[item]["auc"] == pytest.approx(expected)
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_discrimination.py -q 2>&1 | tail -5
```
Expected: ModuleNotFoundError。

- [ ] **Step 3: 實作**

```python
"""條件判別力（框架診斷項目 3）：per-item within-item ROC-AUC。

只取 item 自己的列（跨所有 query），算「隨機一正例、一負例，正例分數較高」
的機率——從不跨 item 比較，per-item 常數偏移被整個消掉，因此它是條件判別
力軸的專用儀表、對水準軸完全免疫（框架手冊 Ch2/Ch3）。

演算法＝rank-sum（Mann–Whitney U）＋ midrank（平均秩）：
    AUC = (R⁺ − n⁺(n⁺+1)/2) / (n⁺ · n⁻)
R⁺＝正例 midrank 總和（分數升冪）。平手釘死 midrank——rank-sum 公式只在
midrank 下精確，F.rank（min-rank）直接代入會系統性偏差，而大量平手正是
本框架的核心診斷對象（近常數分數的冷門 item，真值應為 0.5）。
midrank = F.rank() + (同分列數 − 1)/2，全部 window 內建函式、無 UDF。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def within_item_auc(sdf: SparkDataFrame, parameters: dict) -> dict[str, dict]:
    """per item 的 within-item ROC-AUC（midrank rank-sum）。

    回傳 {item: {auc, n_pos, n_neg, n_rows}}；單類 item（無正例或無負例）
    → auc=None＋reason（不炸）。分數欄用 schema["score"]（實際決定上線
    排序的分數；AUC 對嚴格單調變換不變，校準平坦段併出的平手是要量的
    行為、不是要避開的雜訊）。
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    w_rank = Window.partitionBy(item_col).orderBy(F.col(score_col).asc())
    w_tie = Window.partitionBy(item_col, score_col)
    midrank = (
        F.rank().over(w_rank)
        + (F.count(F.lit(1)).over(w_tie) - F.lit(1)) / F.lit(2.0)
    )
    lbl = F.col(label_col).cast("double")
    rows = (
        sdf.withColumn("_midrank", midrank)
        .groupBy(item_col)
        .agg(
            F.sum(F.when(lbl == 1.0, F.col("_midrank"))).alias("r_pos_sum"),
            F.sum(lbl).alias("n_pos"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    out: dict[str, dict] = {}
    for r in rows:
        n_pos = int(r["n_pos"] or 0)
        n_rows = int(r["n_rows"])
        n_neg = n_rows - n_pos
        entry: dict = {"n_pos": n_pos, "n_neg": n_neg, "n_rows": n_rows}
        if n_pos == 0 or n_neg == 0:
            entry["auc"] = None
            entry["reason"] = (
                f"單一類別（n_pos={n_pos}, n_neg={n_neg}）——AUC 未定義"
            )
        else:
            r_pos = float(r["r_pos_sum"])
            entry["auc"] = (
                (r_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
            )
        out[str(r[item_col])] = entry
    return out
```

- [ ] **Step 4: 跑測試確認通過**（6 passed）

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/metric/discrimination.py tests/test_diagnosis/test_metric/test_discrimination.py && \
git commit -m "feat(diagnosis): within_item_auc——midrank rank-sum 的 per-item 條件判別力（全平手恰 0.5）

Claude-Session: https://claude.ai/code/session_01WKyoqUUNoPMYGobdMDjNUd"
```

---

### Task 3：`occupancy_spark.py` — top-slot 佔據 ＋ 壓制次數（TDD）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/occupancy_spark.py`
- Test: `tests/test_diagnosis/test_metric/test_occupancy.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""top_slot_share／suppression_counts：水準軸傷害的直接觀測。"""

import pytest

from recsys_tfb.diagnosis.metric.occupancy_spark import (
    suppression_counts,
    top_slot_share,
)


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
    }


def _df(spark, rows):
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def test_top_slot_share_counts_topk_queries(spark):
    # 2 個 query：query1 A 排 1、B 排 2；query2 B 排 1、A 排 2。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1), ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C1", "B", 0.8, 1, 1), ("20240331", "C1", "A", 0.4, 0, 2),
    ]
    out = top_slot_share(_df(spark, rows), _params(), k=1)
    assert out["n_queries"] == 2 and out["k"] == 1
    assert out["by_item"]["A"]["top_share"] == pytest.approx(0.5)
    assert out["by_item"]["A"]["n_top"] == 1
    assert out["by_item"]["A"]["y_rate"] == pytest.approx(0.0)
    assert out["by_item"]["B"]["y_rate"] == pytest.approx(1.0)


def test_top_slot_share_k2_counts_both_slots(spark):
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1), ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C1", "B", 0.8, 1, 1), ("20240331", "C1", "A", 0.4, 0, 2),
    ]
    out = top_slot_share(_df(spark, rows), _params(), k=2)
    assert out["by_item"]["A"]["top_share"] == pytest.approx(1.0)


def test_suppression_counts_negatives_above_first_positive(spark):
    # query1：A(負) 排 1、B(正) 排 2、C(負) 排 3 → 首位正例 rank=2 →
    #   只有 A 壓制（rank 1 < 2）；C 在其下、不算。
    # query2：全負 → min_pos_rank null → 不貢獻。
    rows = [
        ("20240331", "C0", "A", 0.9, 0, 1),
        ("20240331", "C0", "B", 0.5, 1, 2),
        ("20240331", "C0", "C", 0.3, 0, 3),
        ("20240331", "C1", "A", 0.9, 0, 1),
        ("20240331", "C1", "B", 0.5, 0, 2),
    ]
    out = suppression_counts(_df(spark, rows), _params())
    assert out["by_item"]["A"]["suppression_count"] == 1
    assert "B" not in out["by_item"] and "C" not in out["by_item"]
    assert out["n_pos_queries"] == 1


def test_suppression_positive_above_positive_not_counted(spark):
    # 正例壓正例不算（只記「以負例身分」的壓制）。
    rows = [
        ("20240331", "C0", "A", 0.9, 1, 1),
        ("20240331", "C0", "B", 0.5, 1, 2),
    ]
    out = suppression_counts(_df(spark, rows), _params())
    assert out["by_item"] == {}
```

- [ ] **Step 2: 跑測試確認失敗**（ModuleNotFoundError）

- [ ] **Step 3: 實作**

```python
"""名次佔據統計（框架診斷項目 5）：水準軸傷害的直接觀測。

寫法沿 evaluation/diagnostics_spark.py 的聚合家族慣例（Spark 聚合、driver
端只收 item 級小結果），但歸屬診斷域——象限組裝（quadrant.py）在此模組
之上，放 evaluation 會造成跨邊界 import。

rank 欄由 prepare_eval_data 保證存在（缺時已用 rank_within_query 注入）。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def top_slot_share(sdf: SparkDataFrame, parameters: dict, k: int) -> dict:
    """per item 佔據 top-k 的 query 比例，並列該 item 正類率當對照。

    identity＝time×entity×item（每 item 每 query 至多一列），所以
    「佔據 top-k 的 query 數」＝ rank ≤ k 的列數。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    rank_col = schema["rank"]
    query_cols = [schema["time"]] + schema["entity"]

    n_queries = sdf.select(*query_cols).distinct().count()
    rows = (
        sdf.groupBy(item_col)
        .agg(
            F.sum(F.when(F.col(rank_col) <= k, 1).otherwise(0)).alias("n_top"),
            F.mean(F.col(label_col).cast("double")).alias("y_rate"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    by_item = {
        str(r[item_col]): {
            "top_share": (int(r["n_top"]) / n_queries) if n_queries else None,
            "n_top": int(r["n_top"]),
            "y_rate": float(r["y_rate"]),
            "n_rows": int(r["n_rows"]),
        }
        for r in rows
    }
    return {"k": int(k), "n_queries": n_queries, "by_item": by_item}


def suppression_counts(sdf: SparkDataFrame, parameters: dict) -> dict:
    """per item「以負例身分排在該 query 首位正例上方」的次數。

    首位正例 rank 用 min(when(label=1, rank)) window 取得；無正例的 query
    條件為 null、自然不貢獻。零壓制的 item 不出現在 by_item（呼叫端補 0）。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    rank_col = schema["rank"]
    query_cols = [schema["time"]] + schema["entity"]

    w = Window.partitionBy(*query_cols)
    lbl = F.col(label_col).cast("int")
    with_min = sdf.withColumn(
        "_min_pos_rank", F.min(F.when(lbl == 1, F.col(rank_col))).over(w)
    )
    suppressing = with_min.filter(
        (lbl == 0)
        & F.col("_min_pos_rank").isNotNull()
        & (F.col(rank_col) < F.col("_min_pos_rank"))
    )
    rows = suppressing.groupBy(item_col).count().collect()
    n_pos_queries = sdf.filter(lbl == 1).select(*query_cols).distinct().count()
    by_item = {
        str(r[item_col]): {"suppression_count": int(r["count"])} for r in rows
    }
    return {"n_pos_queries": n_pos_queries, "by_item": by_item}
```

- [ ] **Step 4: 跑測試確認通過**（4 passed）

- [ ] **Step 5: Commit**（`feat(diagnosis): top_slot_share＋suppression_counts——名次佔據與壓制次數`，附 Claude-Session 行）

---

### Task 4：`cross_purchase.py` — 交叉購買矩陣（TDD）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/cross_purchase.py`
- Test: `tests/test_diagnosis/test_metric/test_cross_purchase.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""cross_purchase_matrix：P(買 k｜買 j)，label_table 自 join。"""

import pytest

from recsys_tfb.diagnosis.metric.cross_purchase import cross_purchase_matrix


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
    }


def _label_df(spark, rows):
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "label"],
    )


def test_conditional_probabilities_and_diagonal(spark):
    # A 買家 {C1, C2}、B 買家 {C1, C3}；C2 的 B 列 label=0 必須被濾掉。
    rows = [
        ("20240331", "C1", "A", 1), ("20240331", "C1", "B", 1),
        ("20240331", "C2", "A", 1), ("20240331", "C2", "B", 0),
        ("20240331", "C3", "B", 1),
    ]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.loc["A", "A"] == pytest.approx(1.0)
    assert prob.loc["A", "B"] == pytest.approx(0.5)   # A 買家 2 人中 1 人也買 B
    assert prob.loc["B", "A"] == pytest.approx(0.5)
    assert int(n_buyers["A"]) == 2 and int(n_buyers["B"]) == 2


def test_cross_snap_date_not_co_purchase(spark):
    # 同客戶不同 snap_date 的購買不算共現（join 鍵含 time）。
    rows = [
        ("20240331", "C1", "A", 1),
        ("20240630", "C1", "B", 1),
    ]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.loc["A", "B"] == pytest.approx(0.0)
    assert prob.loc["B", "A"] == pytest.approx(0.0)


def test_empty_positive_labels_returns_empty(spark):
    rows = [("20240331", "C1", "A", 0)]
    prob, n_buyers = cross_purchase_matrix(_label_df(spark, rows), _params())
    assert prob.empty and n_buyers.empty
```

- [ ] **Step 2: 跑測試確認失敗**（ModuleNotFoundError）

- [ ] **Step 3: 實作**

```python
"""交叉購買矩陣（框架診斷項目 10）：P(買 k｜買 j)，label_table 自 join。

label_table 含 label=0 列（欄位 snap_date, cust_id, prod_name, label）——
先濾 label=1 再自 join；join 鍵＝(time, entity)：同一 snap_date 內算共現、
跨 snap_date 加總。矩陣連同 per-item 買家數一起回——P(k|j) 由 10 個買家
估與 10000 個估的可信度不同，讀矩陣必須帶基數。
"""
from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def cross_purchase_matrix(
    label_rows: SparkDataFrame, parameters: dict,
) -> tuple[pd.DataFrame, pd.Series]:
    """回傳 (P(買 k｜買 j) 矩陣, per-item 買家數)。

    矩陣 index=j、columns=k、對角線＝1；無正例 → (空 DataFrame, 空 Series)。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    key_cols = [schema["time"]] + schema["entity"]

    pos = (
        label_rows.filter(F.col(label_col).cast("int") == 1)
        .select(*key_cols, item_col)
        .distinct()
    )
    a = pos.select(*key_cols, F.col(item_col).alias("_item_j"))
    b = pos.select(*key_cols, F.col(item_col).alias("_item_k"))
    pairs = a.join(b, on=key_cols).groupBy("_item_j", "_item_k").count().toPandas()
    if pairs.empty:
        return pd.DataFrame(), pd.Series(dtype="int64")

    counts = pairs.pivot_table(
        index="_item_j", columns="_item_k", values="count", fill_value=0
    )
    items = sorted(set(counts.index) | set(counts.columns))
    counts = counts.reindex(index=items, columns=items, fill_value=0)
    n_buyers = pd.Series(
        {j: int(counts.loc[j, j]) for j in items}, name="n_buyers"
    )
    prob = counts.div(n_buyers, axis=0)
    prob.index.name = item_col
    prob.columns.name = item_col
    return prob, n_buyers
```

- [ ] **Step 4: 跑測試確認通過**（3 passed）

- [ ] **Step 5: Commit**（`feat(diagnosis): cross_purchase_matrix——label 正例自 join 的 P(買k|買j)＋買家基數`，附 Claude-Session 行）

---

### Task 5：`quadrant.py` — 象限組裝（TDD）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/quadrant.py`
- Test: `tests/test_diagnosis/test_metric/test_quadrant.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""build_quadrant_summary：兩軸合成象限＋傷害觀測（best-effort 降級）。"""

import pytest

from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary


def _params(auc_threshold=0.6, gap_band=0.35, top_k=1):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "evaluation": {
            "diagnosis": {
                "quadrant": {
                    "enabled": True,
                    "auc_threshold": auc_threshold,
                    "gap_band": gap_band,
                    "top_k_occupancy": top_k,
                },
            },
        },
    }


def _eval_df(spark):
    # 兩個 query。item A：判別力好（正例分高）；item B：常數分數（判別力零）
    # 且永遠佔 rank 1、以負例壓 A 的正例（C1 那個 query）。
    rows = [
        # query C0：A 正例 0.9 排 1、B 負例 0.8 排 2
        ("20240331", "C0", "A", 0.9, 1, 1),
        ("20240331", "C0", "B", 0.8, 0, 2),
        # query C1：B 負例 0.8 排 1、A 正例 0.7 排 2 → B 壓制 +1
        ("20240331", "C1", "B", 0.8, 0, 1),
        ("20240331", "C1", "A", 0.7, 1, 2),
        # query C2：A 負例 0.1 排 2、B 正例 0.8 排 1
        ("20240331", "C2", "B", 0.8, 1, 1),
        ("20240331", "C2", "A", 0.1, 0, 2),
    ]
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def _label_df(spark):
    return spark.createDataFrame(
        [("20240331", "C0", "A", 1), ("20240331", "C1", "A", 1),
         ("20240331", "C2", "B", 1)],
        schema=["snap_date", "cust_id", "prod_name", "label"],
    )


def _recon(by_item):
    return {"enabled": True, "by_item": by_item}


def _ci(per_item):
    return {"enabled": True, "per_item": per_item}


def test_quadrant_labels_and_aggressor(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=_ci({"A": {"ap": 0.8, "ci_low": 0.6, "ci_high": 0.9,
                             "n_pos": 2}}),
        reconciliation=_recon({
            "A": {"gap_vs_global": 0.0},
            "B": {"gap_vs_global": 0.9},
        }),
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    # A：AUC=1.0（正例 0.9/0.7 > 負例 0.1）、gvg=0 → 健康
    assert a["auc"] == pytest.approx(1.0)
    assert a["level_status"] == "正常" and a["disc_status"] == "好"
    assert a["quadrant"] == "健康" and a["is_aggressor"] is False
    assert a["ap_sampled"] == pytest.approx(0.8)
    b = out["by_item"]["B"]
    # B：常數分數 → AUC=0.5（差）；gvg=0.9 > 0.35 → 偏高 → 常數高分型加害者
    assert b["auc"] == pytest.approx(0.5)
    assert b["level_status"] == "偏高" and b["disc_status"] == "差"
    assert b["quadrant"] == "加害者（常數高分型）"
    assert b["is_aggressor"] is True
    assert b["suppression_count"] == 1
    assert b["ap_sampled"] is None  # metric_ci 沒給 B → None 不炸
    assert a["suppression_count"] == 0  # 零壓制補 0
    assert out["thresholds"]["gap_band"] == pytest.approx(0.35)
    assert out["cross_purchase"]["n_buyers"]["A"] == 2


def test_low_level_side_labels(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=None,
        reconciliation=_recon({
            "A": {"gap_vs_global": -0.9},
            "B": {"gap_vs_global": -0.9},
        }),
        parameters=_params(),
    )
    assert out["by_item"]["A"]["quadrant"] == "受害者（水準偏低、判別力好）"
    assert out["by_item"]["B"]["quadrant"] == "雙重受害"


def test_degrades_when_upstreams_are_stubs(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci={"enabled": False},
        reconciliation={"enabled": False},
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    assert a["gap_vs_global"] is None
    assert a["level_status"] == "無法評估" and a["quadrant"] == "無法評估"
    assert a["auc"] == pytest.approx(1.0)  # AUC 軸照算
    assert out["sources"] == {"reconciliation": False, "metric_ci": False}
    assert len(out["notes"]) == 2
```

- [ ] **Step 2: 跑測試確認失敗**（ModuleNotFoundError）

- [ ] **Step 3: 實作**

```python
"""象限組裝（框架 Ch2 的 2×2 象限）：合併兩軸與傷害觀測。

兩軸＝水準（gap_vs_global，取對帳層產物——行為觀測、不含歸因；歸因看
reconciliation 的 residual/verdict）×條件判別力（within-item AUC）。
象限標籤照框架手冊 Ch2 的表；「加害者」判準只看水準偏高、與判別力無關。
上游停用（reconciliation/metric_ci stub 或 None）→ 對應欄位 None、該軸
「無法評估」、notes 註記，不失敗（best-effort，沿 cases_manifest 慣例）。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.diagnosis.metric.cross_purchase import cross_purchase_matrix
from recsys_tfb.diagnosis.metric.discrimination import within_item_auc
from recsys_tfb.diagnosis.metric.occupancy_spark import (
    suppression_counts,
    top_slot_share,
)

logger = logging.getLogger(__name__)

_QUADRANT_LABELS = {
    ("正常", "好"): "健康",
    ("正常", "差"): "冷門受害者（水準對、判別力差）",
    ("偏高", "好"): "加害者（水準偏高、判別力好）",
    ("偏高", "差"): "加害者（常數高分型）",
    ("偏低", "好"): "受害者（水準偏低、判別力好）",
    ("偏低", "差"): "雙重受害",
}


def _level_status(gap_vs_global: float | None, band: float) -> str:
    if gap_vs_global is None:
        return "無法評估"
    if gap_vs_global > band:
        return "偏高"
    if gap_vs_global < -band:
        return "偏低"
    return "正常"


def _disc_status(auc: float | None, threshold: float) -> str:
    if auc is None:
        return "無法評估"
    return "好" if auc >= threshold else "差"


def build_quadrant_summary(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    metric_ci: dict | None,
    reconciliation: dict | None,
    parameters: dict,
) -> dict:
    """兩軸＋傷害觀測 → per-item 象限判定（JSON-ready）。"""
    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("quadrant", {}) or {}
    )
    auc_threshold = float(cfg.get("auc_threshold", 0.6))
    gap_band = float(cfg.get("gap_band", 0.35))
    top_k = int(cfg.get("top_k_occupancy", 1))

    auc = within_item_auc(eval_predictions, parameters)
    occupancy = top_slot_share(eval_predictions, parameters, top_k)
    suppression = suppression_counts(eval_predictions, parameters)
    prob, n_buyers = cross_purchase_matrix(label_table, parameters)

    recon_ok = bool(reconciliation and reconciliation.get("enabled"))
    recon_items = (reconciliation.get("by_item", {}) or {}) if recon_ok else {}
    ci_ok = bool(metric_ci and metric_ci.get("enabled"))
    ci_items = (metric_ci.get("per_item", {}) or {}) if ci_ok else {}

    notes: list[str] = []
    if not recon_ok:
        notes.append("reconciliation 停用或缺席——水準軸無法評估。")
    if not ci_ok:
        notes.append("metric_ci 停用或缺席——AP±CI 欄從缺。")

    by_item: dict[str, dict] = {}
    for item in sorted(auc):
        a = auc[item]
        gvg = (recon_items.get(item) or {}).get("gap_vs_global")
        level = _level_status(gvg, gap_band)
        disc = _disc_status(a.get("auc"), auc_threshold)
        if "無法評估" in (level, disc):
            label = "無法評估"
        else:
            label = _QUADRANT_LABELS[(level, disc)]
        ci = ci_items.get(item) or {}
        occ = occupancy["by_item"].get(item) or {}
        by_item[item] = {
            "auc": a.get("auc"),
            "auc_reason": a.get("reason"),
            "n_pos": a["n_pos"],
            "n_neg": a["n_neg"],
            "n_rows": a["n_rows"],
            "gap_vs_global": gvg,
            "level_status": level,
            "disc_status": disc,
            "quadrant": label,
            "is_aggressor": level == "偏高",
            "ap_sampled": ci.get("ap"),
            "ci_low": ci.get("ci_low"),
            "ci_high": ci.get("ci_high"),
            "top_share": occ.get("top_share"),
            "n_top": occ.get("n_top"),
            "y_rate": occ.get("y_rate"),
            "suppression_count": (
                (suppression["by_item"].get(item) or {})
                .get("suppression_count", 0)
            ),
        }

    return {
        "enabled": True,
        "thresholds": {
            "auc_threshold": auc_threshold,
            "gap_band": gap_band,
            "top_k_occupancy": top_k,
        },
        "n_queries": occupancy["n_queries"],
        "n_pos_queries": suppression["n_pos_queries"],
        "by_item": by_item,
        "cross_purchase": {
            "matrix": (
                {j: {k: float(prob.loc[j, k]) for k in prob.columns}
                 for j in prob.index}
                if not prob.empty else {}
            ),
            "n_buyers": (
                {j: int(n_buyers[j]) for j in n_buyers.index}
                if not n_buyers.empty else {}
            ),
        },
        "sources": {"reconciliation": recon_ok, "metric_ci": ci_ok},
        "notes": notes,
    }
```

- [ ] **Step 4: 跑測試確認通過**（3 passed；順跑同目錄全部確認無互擾）

- [ ] **Step 5: Commit**（`feat(diagnosis): build_quadrant_summary——兩軸象限判定＋傷害觀測（best-effort 降級）`，附 Claude-Session 行）

---

### Task 6：config ＋ consistency A17

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`、`src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 失敗測試**——`tests/test_core/test_consistency.py` 追加（照 A16 的 `TestReconciliationParamsA16` 式樣）：

```python
class TestQuadrantParamsA17:
    def _params(self, quad):
        return {"evaluation": {"diagnosis": {"quadrant": quad}}}

    def test_absent_and_valid_defaults_clean(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        assert quadrant_param_errors({}) == []
        assert quadrant_param_errors(self._params(
            {"enabled": True, "auc_threshold": 0.6, "gap_band": 0.35,
             "top_k_occupancy": 1}
        )) == []

    def test_bad_values_report(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        errors = quadrant_param_errors(self._params(
            {"auc_threshold": 0.4, "gap_band": 0, "top_k_occupancy": 0,
             "enabled": "false"}
        ))
        assert len(errors) == 4
        joined = "\n".join(errors)
        assert "auc_threshold" in joined and "gap_band" in joined
        assert "top_k_occupancy" in joined and "enabled" in joined

    def test_auc_threshold_boundaries(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        assert quadrant_param_errors(self._params({"auc_threshold": 0.5})) == []
        assert quadrant_param_errors(self._params({"auc_threshold": 1.0})) != []

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="auc_threshold"):
            validate_config_consistency(self._params({"auc_threshold": 0.4}))
```

`tests/test_evaluation/test_parameters_evaluation_yaml.py` 追加：

```python
def test_quadrant_block():
    quad = _load()["diagnosis"]["quadrant"]
    assert quad == {
        "enabled": True,
        "auc_threshold": 0.6,
        "gap_band": 0.35,
        "top_k_occupancy": 1,
    }
```

- [ ] **Step 2: RED 確認**（ImportError／KeyError）

- [ ] **Step 3: 實作**——yaml 在 `diagnosis:` 區塊的 `reconciliation:` 之後加：

```yaml
    # 象限層（A17）：縱軸 gap_vs_global（水準，取對帳層產物；行為觀測不含
    # 歸因）× 橫軸 within-item AUC（條件判別力，midrank rank-sum）。
    # gap_band 單位 log-odds：|gap_vs_global| 超出帶＝水準偏（偏高＝加害者，
    # 判準與判別力無關）；auc_threshold 以下＝條件判別力差。
    # top_k_occupancy：top-slot 佔據統計的 k。
    quadrant:
      enabled: true
      auc_threshold: 0.6
      gap_band: 0.35
      top_k_occupancy: 1
```

`consistency.py`：`reconciliation_param_errors` 之後加 predicate；`validate_config_consistency` 在 A16 的 `errors.extend(...)` 之後串接 `errors.extend(quadrant_param_errors(parameters))`；模組 docstring 的 Invariant legend 在 A16 之後補：

```python
def quadrant_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.quadrant parameter domains (A17)."""
    errors: list[str] = []
    quad = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("quadrant", {}) or {}
    )
    thr = quad.get("auc_threshold", 0.6)
    if not (_is_number(thr) and 0.5 <= float(thr) < 1.0):
        errors.append(
            f"evaluation.diagnosis.quadrant.auc_threshold={thr!r} must be a "
            f"number in [0.5, 1)."
        )
    band = quad.get("gap_band", 0.35)
    if not (_is_number(band) and float(band) > 0.0):
        errors.append(
            f"evaluation.diagnosis.quadrant.gap_band={band!r} must be a "
            f"number > 0 (log-odds units)."
        )
    k = quad.get("top_k_occupancy", 1)
    if not (isinstance(k, int) and not isinstance(k, bool) and k >= 1):
        errors.append(
            f"evaluation.diagnosis.quadrant.top_k_occupancy={k!r} must be an "
            f"integer >= 1."
        )
    en = quad.get("enabled", True)
    if not isinstance(en, bool):
        errors.append(
            f"evaluation.diagnosis.quadrant.enabled={en!r} must be a bool "
            f"(true/false without quotes in YAML)."
        )
    return errors
```

legend（A16 之後）：

```
* A17 — ``evaluation.diagnosis.quadrant`` parameter domains:
  ``auc_threshold`` ∈ [0.5, 1); ``gap_band`` > 0 (log-odds);
  ``top_k_occupancy`` integer >= 1. Predicate: ``quadrant_param_errors``.
```

- [ ] **Step 4: 全綠確認＋yaml 煙霧**（`quadrant_param_errors` 對實際 yaml 印 `[]`）

- [ ] **Step 5: Commit**（`feat(config): evaluation.diagnosis.quadrant＋consistency A17`，附 Claude-Session 行）

---

### Task 7：pipeline 節點 ＋ catalog

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`、`src/recsys_tfb/pipelines/evaluation/pipeline.py`、`conf/base/catalog.yaml`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`、`tests/test_pipelines/test_evaluation/test_pipeline.py`

- [ ] **Step 1: 失敗測試**——`test_nodes_spark.py` 追加：

```python
def test_compute_quadrant_disabled_returns_stub(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_quadrant
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"quadrant": {"enabled": False}}},
    }
    assert compute_quadrant(None, None, None, None, params) == {"enabled": False}


def test_compute_quadrant_requires_inputs_when_enabled(spark):
    import pytest as _pytest
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_quadrant
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"quadrant": {"enabled": True}}},
    }
    with _pytest.raises(ValueError, match="compute_quadrant"):
        compute_quadrant(None, None, None, None, params)


def test_compute_quadrant_end_to_end_small(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_quadrant
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1, 1),
            ("20240331", "C0", "B", 0.5, 0, 2),
            ("20240331", "C1", "A", 0.2, 0, 2),
            ("20240331", "C1", "B", 0.6, 1, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )
    labels = spark.createDataFrame(
        [("20240331", "C0", "A", 1), ("20240331", "C1", "B", 1)],
        schema=["snap_date", "cust_id", "prod_name", "label"],
    )
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"quadrant": {
            "enabled": True, "auc_threshold": 0.6, "gap_band": 0.35,
            "top_k_occupancy": 1}}},
    }
    out = compute_quadrant(df, labels, {"enabled": False},
                           {"enabled": False}, params)
    assert out["enabled"] is True
    assert set(out["by_item"]) == {"A", "B"}
    assert out["by_item"]["A"]["quadrant"] == "無法評估"  # 上游 stub → 水準軸缺
```

`test_pipeline.py`：結構斷言 7→8（default/post_training）、10→11（compare-source）、node 名清單加 `compute_quadrant`（`compute_reconciliation` 之後、`generate_report` 之前）、outputs 加 `evaluation_quadrant`（**預先授權**，設計定案節）。先 Read 現檔式樣再改。

- [ ] **Step 2: RED 確認**

- [ ] **Step 3: 實作**——`nodes_spark.py` 檔尾（照 `compute_reconciliation` 式樣）：

```python
def compute_quadrant(
    eval_predictions: Optional[SparkDataFrame],
    label_table: Optional[SparkDataFrame],
    metric_ci: Optional[dict],
    reconciliation: Optional[dict],
    parameters: dict,
) -> dict:
    """象限層薄 node（框架診斷項目 3/5/10）。

    領域邏輯全在 ``diagnosis.metric.quadrant``。停用時寫 stub；上游診斷
    產物（metric_ci/reconciliation）是停用 stub 時 best-effort 降級不失敗。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {}).get("quadrant", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("quadrant disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None or label_table is None:
        raise ValueError(
            "compute_quadrant: eval_predictions and label_table are required "
            "when evaluation.diagnosis.quadrant.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary
    out = build_quadrant_summary(
        eval_predictions, label_table, metric_ci, reconciliation, parameters
    )
    logger.info(
        "quadrant computed: %d items, %d aggressors",
        len(out["by_item"]),
        sum(1 for v in out["by_item"].values() if v["is_aggressor"]),
    )
    return out
```

`pipeline.py`：`compute_reconciliation` 節點之後插入（default 與 compare 兩處建構都要，照該檔既有節點列法）：

```python
        Node(
            compute_quadrant,
            inputs=["eval_predictions", "label_table", "evaluation_metric_ci",
                    "evaluation_reconciliation", "parameters"],
            outputs="evaluation_quadrant",
        ),
```

`generate_report` node 的 inputs 追加 `"evaluation_quadrant"`（尾位）；`generate_report` 函式簽名加尾參 `quadrant: Optional[dict] = None,`（本 task 只收不 render，Task 8 才接）。

`catalog.yaml`：`evaluation_reconciliation` 之後：

```yaml
evaluation_quadrant:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/quadrant_summary.json
```

- [ ] **Step 4: 全綠確認**（`tests/test_pipelines/test_evaluation/`）

- [ ] **Step 5: Commit**（`feat(evaluation): compute_quadrant 節點＋quadrant_summary.json catalog 產物`，附 Claude-Session 行）

---

### Task 8：報表 quadrant section（表＋plotly 散布圖）

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`、`src/recsys_tfb/pipelines/evaluation/nodes_spark.py`、`conf/base/parameters_evaluation.yaml`
- Test: `tests/test_evaluation/test_report_builder.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 失敗測試**——`test_report_builder.py` 追加：

```python
_QUAD_FIXTURE = {
    "enabled": True,
    "thresholds": {"auc_threshold": 0.6, "gap_band": 0.35,
                   "top_k_occupancy": 1},
    "n_queries": 1000, "n_pos_queries": 400,
    "by_item": {
        "A": {"auc": 0.82, "auc_reason": None, "n_pos": 120, "n_neg": 880,
              "n_rows": 1000, "gap_vs_global": 0.05, "level_status": "正常",
              "disc_status": "好", "quadrant": "健康", "is_aggressor": False,
              "ap_sampled": 0.61, "ci_low": 0.55, "ci_high": 0.68,
              "top_share": 0.2, "n_top": 200, "y_rate": 0.12,
              "suppression_count": 30},
        "B": {"auc": 0.51, "auc_reason": None, "n_pos": 20, "n_neg": 980,
              "n_rows": 1000, "gap_vs_global": 0.9, "level_status": "偏高",
              "disc_status": "差", "quadrant": "加害者（常數高分型）",
              "is_aggressor": True, "ap_sampled": 0.7, "ci_low": 0.5,
              "ci_high": 0.85, "top_share": 0.6, "n_top": 600,
              "y_rate": 0.02, "suppression_count": 480},
    },
    "cross_purchase": {
        "matrix": {"A": {"A": 1.0, "B": 0.3}, "B": {"A": 0.5, "B": 1.0}},
        "n_buyers": {"A": 100, "B": 60},
    },
    "sources": {"reconciliation": True, "metric_ci": True},
    "notes": [],
}


def test_quadrant_section_renders_table_figure_and_matrix():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    sec = build_quadrant_section(_QUAD_FIXTURE, _params_min())
    tbl = sec.tables[0]
    assert list(tbl.index) == ["A", "B"]
    assert tbl.loc["B", "quadrant"] == "加害者（常數高分型）"
    assert len(sec.figures) == 1          # 散布圖
    assert len(sec.tables) == 2           # 象限表＋交叉購買矩陣
    assert sec.tables[1].loc["B", "A"] == pytest.approx(0.5)
    assert "判讀" in sec.description
    assert "evaluation-diagnosis" in sec.description


def test_quadrant_section_none_when_disabled_or_absent():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    assert build_quadrant_section(None, _params_min()) is None
    assert build_quadrant_section({"enabled": False}, _params_min()) is None
    params_off = {"evaluation": {"report": {"sections": {"quadrant": False}}}}
    assert build_quadrant_section(_QUAD_FIXTURE, params_off) is None


def test_quadrant_section_notes_and_missing_axis():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    fx = dict(
        _QUAD_FIXTURE,
        by_item={"A": dict(_QUAD_FIXTURE["by_item"]["A"],
                           gap_vs_global=None, level_status="無法評估",
                           quadrant="無法評估")},
        notes=["reconciliation 停用或缺席——水準軸無法評估。"],
    )
    sec = build_quadrant_section(fx, _params_min())
    assert "無法評估" in sec.tables[0]["quadrant"].tolist()
    assert "reconciliation 停用" in sec.description
    # 缺軸的 item 不進散布圖：唯一 item 缺 y → 無圖
    assert sec.figures == []


def test_assemble_report_renders_quadrant():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), quadrant=_QUAD_FIXTURE
    )
    assert "象限" in html
```

`test_parameters_evaluation_yaml.py` 追加：

```python
def test_report_sections_include_quadrant():
    assert _load()["report"]["sections"]["quadrant"] is True
```

- [ ] **Step 2: RED 確認**

- [ ] **Step 3: 實作**：

(a) `parameters_evaluation.yaml` 的 `report.sections` 加 `quadrant: true`（`reconciliation: true` 之後）。

(b) `report_builder.py` 新 builder（放在 `build_reconciliation_section` 之後；模組已 `import plotly.graph_objects as go`）：

```python
def _quadrant_scatter(by_item: dict, thresholds: dict) -> go.Figure | None:
    """象限散布圖：橫軸 AUC（→ 判別力好）、縱軸 gap_vs_global（↑ 水準偏高）。

    樣式鏡射框架手冊 fig2-quadrant-map：淺藍水平帶＝水準大致正確的範圍、
    垂直虛線＝判別力門檻。任一軸缺值的 item 不進圖（表中仍列）。
    """
    pts = {
        it: v for it, v in by_item.items()
        if v.get("auc") is not None and v.get("gap_vs_global") is not None
    }
    if not pts:
        return None
    band = float(thresholds.get("gap_band", 0.35))
    thr = float(thresholds.get("auc_threshold", 0.6))
    fig = go.Figure(
        go.Scatter(
            x=[v["auc"] for v in pts.values()],
            y=[v["gap_vs_global"] for v in pts.values()],
            mode="markers+text",
            text=list(pts),
            textposition="top center",
        )
    )
    fig.add_hrect(y0=-band, y1=band, fillcolor="lightblue", opacity=0.3,
                  line_width=0)
    fig.add_vline(x=thr, line_dash="dash")
    fig.update_layout(
        title="象限地圖：水準（縱）× 條件判別力（橫）",
        xaxis_title="within-item AUC（→ 判別力好）",
        yaxis_title="gap_vs_global（↑ 水準偏高）",
    )
    return fig


def build_quadrant_section(
    quadrant: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "quadrant"):
        return None
    if not quadrant or not quadrant.get("enabled"):
        return None
    by_item = quadrant.get("by_item", {}) or {}
    cols = ["quadrant", "gap_vs_global", "auc", "ap_sampled", "ci_low",
            "ci_high", "top_share", "y_rate", "suppression_count",
            "n_pos", "n_rows"]
    tbl = pd.DataFrame(
        {c: [by_item[it].get(c) for it in by_item] for c in cols},
        index=list(by_item),
    )
    thresholds = quadrant.get("thresholds", {}) or {}
    fig = _quadrant_scatter(by_item, thresholds)
    tables = [tbl]
    table_titles = ["per-item 象限表"]
    cp = (quadrant.get("cross_purchase", {}) or {}).get("matrix", {}) or {}
    if cp:
        cp_tbl = pd.DataFrame.from_dict(cp, orient="index")
        order = sorted(cp_tbl.index)
        cp_tbl = cp_tbl.reindex(index=order, columns=order)
        tables.append(cp_tbl)
        table_titles.append("交叉購買矩陣 P(買 k｜買 j)（列＝j、欄＝k）")
    desc = (
        "行為層象限：縱軸 gap_vs_global（水準）、橫軸 within-item AUC"
        "（條件判別力）。判讀順序：(1) 先看散布圖每個 item 落在哪個象限；"
        f"(2) 水準帶外（|gap_vs_global| > {thresholds.get('gap_band')}）的 "
        "item 回對帳表查可否由配置解釋；(3) AUC 低於 "
        f"{thresholds.get('auc_threshold')} 的 item 看 suppression_count 與 "
        "top_share 評估傷害；(4) 交叉購買矩陣看高共購 item 之間的壓制是否"
        "實質。完整判讀：docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = quadrant.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="象限 Quadrant（水準 × 條件判別力）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=tables,
        table_titles=table_titles,
    )
```

(c) `assemble_report` 簽名加 `quadrant: dict | None = None`，candidates 在 `build_reconciliation_section(...)` 之後插入 `build_quadrant_section(quadrant, parameters),`。

(d) `nodes_spark.py::generate_report`：把 Task 7 加的 `quadrant` 參數傳給 `assemble_report(..., quadrant=quadrant)`。

- [ ] **Step 4: 全綠確認**（`test_report_builder.py`＋`tests/test_pipelines/test_evaluation/`＋yaml 測試）

- [ ] **Step 5: Commit**（`feat(report): quadrant section——象限表＋plotly 散布圖＋交叉購買矩陣`，附 Claude-Session 行）

---

### Task 9：真跑閘門（evaluation-only，含門檻注入）

**Files:** 無新程式碼；閘門期間暫改 `conf/base/parameters_evaluation.yaml` 的 `gap_band`（結束時還原）。

執行順序嚴格照下（既有 section 逐字回歸必須在第一輪重跑後立即比，報表會被後續注入輪覆蓋）：

- [ ] **Step 1: 測試 vs baseline**（背景；Task 1 同組檔案＋新增測試檔）；fail 集合須與 `/tmp/phase3_test_baseline.txt` 一致。

- [ ] **Step 2: 現狀真跑（evaluation-only，不重訓）**（背景）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version 6059dcef
```

檢視四件事：
1. `data/evaluation/6059dcef/20260131/diagnosis/quadrant_summary.json` 產出，8 個 item 齊。
2. **既有 section 逐字回歸**：對 `/tmp/phase3_report_before.html` 跑 Phase 1/2 同款 Macro-rows／headline／reconciliation 表比對——必須逐字相同（quadrant 是新 section、不動既有值）。
3. **方向性已知答案（spec 驗收 3；觀察，不硬 assert）**：最冷合成 item `fund_mix`（正類率 0.02，`generate_synthetic_data.py:43-48` 全 8 item 最低）應落「判別力差」半邊（AUC < 0.6 或至少為全 item 最低一側）。**若不符 → 停下分析原因再繼續**（檢查母體條件化對 AUC 樣本組成的影響、fund_* 特徵訊號強度），把發現寫進閘門報告。
4. **現狀判讀（觀察）**：預設 `gap_band=0.35` 下水準軸應全「正常」（已知 `ccard_ins` gap_vs_global ≈ +0.329 < 0.35、中性 item 皆在 ±0.19 內）；`is_aggressor` 全 False；report.html 的象限散布圖 8 點齊、`ccard_ins` 最接近帶頂。

- [ ] **Step 3: 門檻注入（不動 dataset/training config、零重訓）**——`conf/base/parameters_evaluation.yaml` 的 `quadrant.gap_band` 暫改 `0.25`：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
grep -n "gap_band" conf/base/parameters_evaluation.yaml
```

改後 pre-flight grep 確認讀到 `0.25`，再跑同一條 evaluation 指令（背景）。

檢視（已知答案）：`ccard_ins`（gap_vs_global ≈ +0.329 > 0.25）翻成 `level_status="偏高"`、`is_aggressor=true`、quadrant 為兩種加害者之一；**其餘 7 個中性 item 全部維持「正常」**（它們的 |gap_vs_global| ≤ 0.186 < 0.25）。一翻一不翻＝門檻管線端到端證據。

- [ ] **Step 4: 還原注入、重跑、確認回到全正常**：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git checkout conf/base/parameters_evaluation.yaml && grep -n "gap_band" conf/base/parameters_evaluation.yaml
```

Expected: `gap_band: 0.35`。再跑一輪 evaluation（背景），確認 `quadrant_summary.json` 回到 Step 2 的狀態（水準軸全「正常」、`is_aggressor` 全 False）。**注意 yaml 測試 `test_quadrant_block` 斷言 0.35——還原後重跑該測試檔確認綠**。

- [ ] **Step 5: git 乾淨確認**（`git status --short` 只剩 data/ 產物；config 已還原）。

---

### Task 10：判讀手冊擴充（文件是一等交付物）

**Files:**
- Modify: `docs/pipelines/evaluation-diagnosis.md`
- （報表描述已在 Task 8 內建短判讀順序＋指向手冊，本 task 不再改 code）

**寫法鐵則（Phase 2 四輪返工的教訓，違反任一條＝重做）**：
1. 禁用開發詞彙——交付前 grep 手冊中的「本機／Phase／spec／驗收／真跑／本次／我們的」必須零命中（「示例資料」「示例模型」是合法稱呼）。
2. 貫穿範例契約——**把 Task 9 Step 2 真跑產出的象限表直接印進文件**（從 `quadrant_summary.json` 生成 markdown 表），各小節走讀這張看得見的表；嚴禁敘述讀者看不到的報表。數字出現前必先介紹其來源（沿用文件開頭既有的示例資料宣告）。
3. 對無直覺尺度建「數感」——AUC 需要錨點節（見下）。
4. 交付前派 fresh 讀者 agent 通讀，prompt 必含「列出所有指涉你看不到的東西的詞（內部代號、開發階段、未宣告的數據來源）」＋至少 3 個卡關處或列出檢查面向。

- [ ] **Step 1: 內容契約（新節放在既有注入實驗走讀之後、metric_ci 節之前；編號依現檔順延，先 Read 現檔再定）**

必含內容（順序照計算鏈）：
1. **象限在回答什麼**：分數分解 `s = query 效應 + item 水準 + 條件部分` 的白話版（同 query 內加常數不動名次 → query 效應無關緊要；水準與條件部分全額影響）；兩軸各管一個病灶；為什麼要兩軸一起看（同樣「分不出誰該買」的 item，常數高分＝加害者、常數低分＝受害者——單看任何一軸都會判錯）。
2. **AUC 數感節**（照既有 log-odds 數感節的體例）：定義「隨機抽該 item 的一個正例、一個負例，正例分數較高的機率」；錨點表 0.5＝丟銅板（完全分不出）／0.6＝略有訊號／0.75＝可用／0.9＝很強；常數分數 item 恰為 0.5 的直覺（怎麼抽都平手）；**對水準免疫**的直覺（整個 item 加 +10 分，內部誰高誰低完全不變）——這就是它跟 gap_vs_global 正交、適合當第二軸的原因。
3. **水準軸為什麼用 gap_vs_global 不用 residual**：象限是行為觀測（有沒有被抬高、有沒有壓到人），對帳表的 residual/verdict 是歸因（抬高可否由配置解釋）；用示例資料的具體 item 說明兩者可以不同（理論帶內但行為上偏高的 item 仍會壓別人）。
4. **印進文件的示例象限表**＋逐欄走讀（quadrant／gap_vs_global／auc／ap_sampled±CI（標明來自診斷抽樣）／top_share vs y_rate 對照怎麼讀（佔據率遠高於正類率＝水準訊號）／suppression_count（分母是 n_pos_queries）／n_pos）。
5. **散布圖讀法**：軸向、淺藍帶＝水準帶、虛線＝判別力門檻、四個角落各是什麼、示例資料的點各落哪裡。
6. **象限標籤表**（六格＋無法評估）＋「加害者判準只看水準偏高」的一句話理由。
7. **交叉購買矩陣讀法**：P(買k|買j) 的方向性（列→欄）、對角線＝1、必看 n_buyers 基數、跟壓制的關係（高共購 item 之間的壓制才是實質傷害——買 j 的人本來也會買 k，k 被壓就是真損失）。
8. **名詞速查表**追加：`within_item_auc`／`gap_band`／`auc_threshold`／`top_share`／`suppression_count`／`quadrant`／`is_aggressor`／`cross_purchase`。
9. **計算鏈總覽圖**（文件開頭既有）補上象限層的位置。
10. **限制節**追加：AUC 樣本是條件化後的母體；`ap_sampled` 來自診斷抽樣非全量；門檻（0.6／0.35）是起手值非定論，帶寬語意同對帳層 explained_threshold 的夾擠論證但作用對象不同。

- [ ] **Step 2: 從真跑 JSON 生成示例表**（controller 直跑，產 markdown 表片段給 implementer 或自嵌）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'EOF'
import json
d = json.load(open("data/evaluation/6059dcef/20260131/diagnosis/quadrant_summary.json"))
cols = ["quadrant", "gap_vs_global", "auc", "ap_sampled", "top_share",
        "y_rate", "suppression_count", "n_pos"]
print("| item | " + " | ".join(cols) + " |")
print("|---|" + "---|" * len(cols))
for it, v in sorted(d["by_item"].items()):
    cells = [f"{v[c]:.3f}" if isinstance(v[c], float) else str(v[c]) for c in cols]
    print(f"| {it} | " + " | ".join(cells) + " |")
EOF
```

- [ ] **Step 3: 起草＋讀者 agent 通讀＋修稿**（執行模式節的分工）；禁用詞 grep：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
grep -n "本機\|Phase\|spec\|驗收\|真跑\|本次\|我們的" docs/pipelines/evaluation-diagnosis.md
```
Expected: 零命中（既有內文已通過同標準；新內容不得引入）。

- [ ] **Step 4: Commit**（`docs: evaluation-diagnosis.md 補象限層——AUC 數感＋示例表走讀＋交叉購買讀法`，附 Claude-Session 行）

---

### Task 11：審查 ＋ 使用者閘門

- [ ] **Step 1: 合併 reviewer（sonnet，Task 2–8）**：給 `git diff <Phase3 起點>..HEAD` 範圍＋spec §3 Phase 3 全文＋本計畫設計定案；要求至少 3 個具體問題（附 檔案:行號 與失敗情境）或逐項列出檢查面向。
- [ ] **Step 2: opus 總審**：fresh-context，給 spec §3 Phase 3＋diff＋閘門證據（三個狀態的 quadrant_summary.json、report 象限 section、測試 vs baseline）；不給作者結論；verdict READY-FOR-USER-GATE 判準。
- [ ] **Step 3: 修完發現後使用者閘門回報**：附 quadrant_summary.json（現狀／門檻注入／還原三狀態）、report.html 象限 section 與散布圖、`fund_mix` 方向性檢查結果、手冊新節、測試 vs baseline、審查結論、「沒做的事」清單（至少含：公司規模未驗；`ap_sampled` 依賴診斷抽樣；monitoring 路徑（非 post-training）下 reconciliation 會 fallback、水準軸含校準層效應——照 reconciliation 的既有標註連動）。**等使用者檢視通過才進 Phase 4。**

---

## Self-review（計畫作者已核）

- **Spec §3 Phase 3 覆蓋**：`discrimination.py` within-item AUC＋midrank＋全平手 fixture＋numpy parity（Task 2）；`occupancy_spark.py` top_slot_share＋suppression_counts＋min-pos-rank window（Task 3）；`cross_purchase.py` label_table 自 join＋既有輸入接線（Task 4、7）；`quadrant.py` 合併 {gap（Phase 2）、AUC、AP±CI（Phase 1）、suppression} → 象限＋散布圖＋`quadrant_summary.json`（Task 5、7、8）；report section `quadrant`（Task 8）；config 四鍵（Task 6）；A17（Task 6）；驗收 1–3（Task 9：evaluation-only 重跑、JSON＋散布圖檢視、fund_mix 方向性＋unit parity）。
- **超出/偏離 spec 字面的設計（記錄）**：(1) 散布圖 plotly 非 matplotlib——spec 該行與 repo 實況矛盾，已修訂 spec 並註記證據（`distributions.py:9`）；(2) 回傳型別 dict 非 DataFrame（Phase 2 既有先例）；(3) `cross_purchase_matrix` 多回 n_buyers（可信度基數）；(4) 門檻注入閘門（gap_band 0.25 翻 `ccard_ins`）是 spec 驗收之上的追加已知答案（零重訓成本）；(5) 水準軸用 gap_vs_global 不用 residual 的裁決＋理由寫進設計定案與手冊。
- **佔位符檢查**：所有 code 步驟含完整程式碼；測試期望值全部手算可驗（AUC 0.5/1.0、P(B|A)=0.5、壓制 1 次）；Task 9 的基準數字（+0.329、±0.186、0.25）出處已標。唯二「先 Read 現檔再改」的步驟（pipeline 結構測試、手冊節編號）是既有檔案的既有式樣對齊，非佔位符。
- **識別字一致性**：`compute_quadrant`／`evaluation_quadrant`／`build_quadrant_summary`／`build_quadrant_section`／`quadrant_param_errors`／JSON 鍵（`auc`、`gap_vs_global`、`level_status`、`disc_status`、`quadrant`、`is_aggressor`、`ap_sampled`、`ci_low/high`、`top_share`、`n_top`、`y_rate`、`suppression_count`、`thresholds`、`cross_purchase.matrix/n_buyers`、`sources`、`notes`）在 Task 5/7/8/10 fixture 間逐字一致。
- **邊界**：diagnosis 四模組只 import `core.schema`＋pyspark/pandas/標準庫；plotly 只在 `report_builder.py`（evaluation 側）；`quadrant.py` 消費 reconciliation/metric_ci 的**產物 dict**、不 import evaluation 內部。
